"""
api/routes/datastage.py
------------------------
DataStage documentation endpoints.

Routes:
  POST /api/datastage/analyze          → structured JSON (para o frontend)
  POST /api/datastage/report           → HTML report como download
  GET  /api/datastage/report/{job_id}  → abre relatório no browser (inline)
  GET  /api/datastage/lineage/{job_id} → lineage graph JSON

O relatório HTML é gerado pelo xml_analyzer.py com:
  - SQL syntax highlighting via Prism.js
  - SQL indentado automaticamente
  - Design dark (vars do globals.css)
  - Cards por stage com botão copy SQL
"""

import hashlib
import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse, Response

from app.core.logging import get_logger
from app.models.schemas import LineageGraphResponse
from app.services.datastage.xml_analyzer import DataStagePrecisionMapper

router = APIRouter(prefix="/api/datastage", tags=["datastage"])
logger = get_logger(__name__)

# In-memory cache: sha256(xml_content) → html_str
# Evita re-parsear o mesmo XML em chamadas repetidas
_report_cache: dict[str, str] = {}
_MAX_CACHE = 20


def _cache_key(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def _get_mapper_from_content(xml_content: str) -> DataStagePrecisionMapper:
    """Parse XML string via temp file (o mapper espera um path)."""
    with tempfile.NamedTemporaryFile(
        suffix=".xml", mode="w", encoding="utf-8", delete=False
    ) as f:
        f.write(xml_content)
        tmp_path = Path(f.name)
    try:
        mapper = DataStagePrecisionMapper(tmp_path)
        mapper.run()
        return mapper
    finally:
        tmp_path.unlink(missing_ok=True)


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/analyze", response_model=dict)
async def analyze_xml(file: UploadFile = File(..., description="DataStage .dsx XML file")):
    content_bytes = await file.read()
    xml_content = content_bytes.decode("utf-8", errors="replace")

    """
    Parse DataStage XML and return structured JSON.
    Used by the frontend /docs page to render the job list.
    """
    try:
        mapper = _get_mapper_from_content(xml_content)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"XML parse error: {exc}") from exc

    jobs_out = []
    for job_name, comps in mapper.jobs.items():
        stages = []
        for data in comps.items():
            stages.append({
                "stage_name": data["name"],
                "stage_type": data["type"],
                "table_name": next(iter(data["tables_tgt"]), None) or next(iter(data["tables_src"]), None),
                "is_write": data.get("is_write", False),
                "source_tables": sorted(data["tables_src"]),
                "target_tables": sorted(data["tables_tgt"]),
                "sql_count": len(data["sqls"]),
                "sqls": data["sqls"][:3],
                "logic_count": len(data["logic"]),
                "columns": [
                    {"name": l.split(" ← ")[0], "derivation": l.split(" ← ")[1] if " ← " in l else ""}
                    for l in data["logic"][:20]
                ],
            })

        all_src = set()
        all_tgt = set()
        for d in comps.values():
            all_src.update(d["tables_src"])
            all_tgt.update(d["tables_tgt"])

        jobs_out.append({
            "job_name": job_name,
            "job_type": "Parallel",
            "description": "",
            "stages": stages,
            "source_tables": sorted(all_src),
            "target_tables": sorted(all_tgt),
            "sql_queries": [sql for d in comps.values() for sql in d["sqls"][:2]],
        })

    return {"jobs": jobs_out}


@router.post("/report", response_class=HTMLResponse)
async def generate_report_from_post(
    file: UploadFile = File(..., description="DataStage .dsx XML file"),
    inline: bool = Form(True, description="True = abre no browser; False = força download"),
):
    content_bytes = await file.read()
    xml_content = content_bytes.decode("utf-8", errors="replace")

    """
    Gera o relatório HTML completo e retorna para o browser.
    Com inline=True o browser renderiza direto (sem download).
    """
    key = _cache_key(xml_content)
    if key in _report_cache:
        html_str = _report_cache[key]
    else:
        try:
            mapper = _get_mapper_from_content(xml_content)
            html_str = mapper.generate_html_report()
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"XML parse error: {exc}") from exc

        # Cache
        if len(_report_cache) >= _MAX_CACHE:
            oldest = next(iter(_report_cache))
            del _report_cache[oldest]
        _report_cache[key] = html_str

    disposition = "inline" if inline else 'attachment; filename="datastage_report.html"'
    return Response(
        content=html_str,
        media_type="text/html; charset=utf-8",
        headers={"Content-Disposition": disposition},
    )


@router.post("/report/upload", response_class=HTMLResponse)
async def generate_report_from_upload(
    file: UploadFile = File(..., description="DataStage .dsx or .xml file"),
):
    """
    Upload direto de arquivo .dsx e abre o relatório no browser.
    Permite usar o input type=file do frontend.
    """
    content_bytes = await file.read()
    xml_content = content_bytes.decode("utf-8", errors="replace")
    return await generate_report_from_post(xml_content=xml_content, inline=True)


@router.post("/lineage", response_model=LineageGraphResponse)
async def get_lineage(
    file: UploadFile = File(..., description="DataStage XML file"),
):
    """
    Parse XML e retorna o grafo de linhagem SOURCE → JOB → TARGET.
    """
    content_bytes = await file.read()
    xml_content = content_bytes.decode("utf-8", errors="replace")

    try:
        mapper = _get_mapper_from_content(xml_content)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    metadata = {"file": "uploaded", "jobs": []}
    for job_name, comps in mapper.jobs.items():
        all_src = set()
        all_tgt = set()
        for d in comps.values():
            all_src.update(d["tables_src"])
            all_tgt.update(d["tables_tgt"])
        metadata["jobs"].append({
            "job_name": job_name,
            "job_type": "Parallel",
            "description": "",
            "source_tables": sorted(all_src),
            "target_tables": sorted(all_tgt),
        })

    from app.services.datastage.xml_parser import DataStageXMLParser
    parser = DataStageXMLParser()
    return parser.build_lineage_graph(metadata)
