"""
api/routes/dimension.py
--------------------------
Endpoints para migração de dimensões SCD2 (DataStage → Snowflake).

Routes:
  POST /api/dimension/analyze      → recebe XML, retorna DimensionSpec
  POST /api/dimension/generate     → recebe DimensionSpec, retorna 5 SQLs (Oracle-first)
  POST /api/dimension/homologate   → recebe spec + PROD table, retorna MINUS queries
  GET  /api/dimension/jobs         → lista jobs de dimensão registrados
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.core.logging import get_logger
from app.models.logs import DimensionJob
from app.services.dimension import (
    DimensionHomologator,
    DimensionSpec,
    DimensionSpecExtractor,
    DimensionSqlGenerator,
    fetch_dim_columns,
    fetch_dim_constraints,
)

router = APIRouter(prefix="/api/dimension", tags=["dimension"])
logger = get_logger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────────

async def _resolve_xml(file: UploadFile | None, xml_content: str | None) -> str:
    if file and file.filename:
        data = await file.read()
        return data.decode("utf-8", errors="replace")
    if xml_content:
        return xml_content
    raise HTTPException(
        status_code=422,
        detail="Envie um arquivo .dsx/.xml no campo 'file' ou o XML no campo 'xml_content'.",
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/analyze", response_model=dict, status_code=status.HTTP_200_OK)
async def analyze_dimension_xml(
    file: UploadFile | None = File(None, description="Arquivo .dsx ou .xml"),
    xml_content: str | None = Form(None, description="XML como texto (fallback)"),
):
    """
    Analisa um arquivo DSX e extrai o DimensionSpec completo:
    job_name, tabela alvo, fl_mn, SELECT ODS, colunas, lookups, surrogate/business key.
    """
    content = await _resolve_xml(file, xml_content)
    try:
        extractor = DimensionSpecExtractor.from_string(content)
        spec = extractor.extract()
    except Exception as exc:
        logger.error("DimensionSpec extraction failed", error=str(exc))
        raise HTTPException(
            status_code=422, detail=f"Erro ao analisar XML: {exc}"
        ) from exc

    return spec.model_dump()


@router.post("/generate", response_model=dict, status_code=status.HTTP_200_OK)
async def generate_dimension_sql(
    spec_data: dict,
    db: AsyncSession = Depends(get_db_session),
):
    """
    Recebe um DimensionSpec (JSON) e gera os 5 SQLs na ordem correta.

    REGRA ABSOLUTA: busca colunas do Oracle antes de gerar qualquer SQL.
    Se Oracle indisponível, gera com inferência do DataStage (com aviso).
    Persiste o registro no banco para histórico.
    """
    try:
        spec = DimensionSpec.model_validate(spec_data)
    except Exception as exc:
        raise HTTPException(
            status_code=422, detail=f"DimensionSpec inválido: {exc}"
        ) from exc

    # Buscar colunas do Oracle (fonte obrigatória para DDL)
    oracle_cols = []
    oracle_cons = []
    try:
        oracle_cols = await fetch_dim_columns(spec.oracle_schema, spec.target_table)
        oracle_cons = await fetch_dim_constraints(spec.oracle_schema, spec.target_table)
        if not oracle_cols:
            logger.warning(
                "Oracle returned 0 columns — table may not exist in schema",
                schema=spec.oracle_schema,
                table=spec.target_table,
            )
        else:
            logger.info(
                "Oracle columns fetched",
                table=spec.target_table,
                count=len(oracle_cols),
            )
    except Exception as exc:
        logger.warning(
            "Oracle fetch failed — generating with DataStage inference",
            error=str(exc),
            table=spec.target_table,
        )

    spec = spec.model_copy(update={
        "oracle_columns": oracle_cols,
        "oracle_constraints": oracle_cons,
    })

    try:
        generator = DimensionSqlGenerator(spec)
        sqls = generator.generate_all()
    except Exception as exc:
        logger.error("SQL generation failed", job=spec.job_name, error=str(exc))
        raise HTTPException(
            status_code=500, detail=f"Erro ao gerar SQL: {exc}"
        ) from exc

    # Persistir no banco
    try:
        job_record = DimensionJob(
            job_name=spec.job_name,
            target_table=spec.target_table,
            schema=spec.schema,
            fl_mn=spec.fl_mn,
            nom_sis_ori=spec.nom_sis_ori,
            spec_json=spec.model_dump(),
            generated_sqls=sqls,
        )
        db.add(job_record)
        await db.commit()
        await db.refresh(job_record)
        job_id = job_record.id
    except Exception as exc:
        logger.warning("Failed to persist dimension job record", error=str(exc))
        job_id = None

    logger.info("Dimension SQLs generated", job=spec.job_name, table=spec.target_table)
    return {
        "job_name": spec.job_name,
        "target_table": spec.target_table,
        "fl_mn": spec.fl_mn,
        "record_id": job_id,
        "sqls": sqls,
    }


@router.post("/homologate", response_model=dict, status_code=status.HTTP_200_OK)
async def generate_homologation_queries(
    spec_json: str = Form(..., description="DimensionSpec serializado como JSON"),
    data_teste: str = Form(..., description="Data de referência para Time Travel (YYYY-MM-DD)"),
    bsk_id: str = Form("", description="Valor da business key para query 07 (UNION ALL)"),
    offset_hours: int = Form(23, description="Horas de offset sobre data_teste - 1 (default 23)"),
):
    """
    Gera 7 queries de homologação DEV vs PROD:
    snapshot PROD, COUNT totais, COUNT por data, MINUS e UNION ALL por BSK_ID.
    """
    try:
        spec = DimensionSpec.model_validate(json.loads(spec_json))
    except Exception as exc:
        raise HTTPException(
            status_code=422, detail=f"DimensionSpec inválido: {exc}"
        ) from exc

    homologator = DimensionHomologator(
        spec=spec,
        data_teste=data_teste,
        bsk_id=bsk_id,
        offset_hours=offset_hours,
    )
    queries = homologator.generate()

    logger.info(
        "Homologation queries generated",
        job=spec.job_name,
        data_teste=data_teste,
        bsk_id=bsk_id,
    )
    return {
        "dev_table": f"{spec.schema}.{spec.target_table}",
        "prod_table": f"{spec.prod_sf_schema}.{spec.target_table}",
        "data_teste": data_teste,
        "offset_hours": offset_hours,
        "queries": queries,
    }


@router.get("/jobs", response_model=list[dict])
async def list_dimension_jobs(
    table_name: str | None = Query(None, description="Filtrar por tabela alvo"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=5, le=100),
    db: AsyncSession = Depends(get_db_session),
):
    """Lista jobs de dimensão registrados com paginação."""
    query = select(DimensionJob).order_by(DimensionJob.created_at.desc())
    if table_name:
        query = query.where(DimensionJob.target_table == table_name.upper())

    offset = (page - 1) * page_size
    query = query.offset(offset).limit(page_size)
    result = await db.execute(query)
    jobs = result.scalars().all()

    return [
        {
            "id": j.id,
            "job_name": j.job_name,
            "target_table": j.target_table,
            "schema": j.schema,
            "fl_mn": j.fl_mn,
            "nom_sis_ori": j.nom_sis_ori,
            "created_at": j.created_at.isoformat() if j.created_at else None,
        }
        for j in jobs
    ]
