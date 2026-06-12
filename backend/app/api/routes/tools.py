from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.core.logging import get_logger
from app.services.tools.ddl_service import (
    extract_oracle_metadata,
    build_ddl,
    create_snowflake_tables,
    run_copy_into,
    run_merge,
)

router = APIRouter(prefix="/api/tools", tags=["tools"])
logger = get_logger(__name__)


# ── Schemas ────────────────────────────────────────────────────────────────

class CreateTableRequest(BaseModel):
    table_name: str
    schema_source: str = "DWADM"
    schema_target: str = "DWADM"
    dry_run: bool = False


class CopyIntoRequest(BaseModel):
    table_name: str
    schema: str = "DWADM"
    s3_key: str


class MergeRequest(BaseModel):
    table_name: str
    schema: str = "DWADM"
    partition_column: str = "DT_REFERENCIA"
    partition_date: str | None = None


# ── Routes ─────────────────────────────────────────────────────────────────

@router.get("/metadata")
async def extract_metadata(
    table: str = Query(..., description="Oracle table name"),
    schema: str = Query("DWADM", description="Oracle schema name"),
):
    log = logger.bind(table=table, schema=schema, operation="metadata_extract")
    log.info("Extracting Oracle metadata")
    try:
        meta = await extract_oracle_metadata(table, schema)
    except Exception as exc:
        log.error("Metadata extraction failed", error=str(exc))
        raise HTTPException(status_code=500, detail=f"Oracle metadata error: {exc}") from exc

    if not meta:
        raise HTTPException(
            status_code=404,
            detail=f"Table {schema.upper()}.{table.upper()} not found or has no columns",
        )
    log.info("Metadata extracted", columns=meta["total_columns"])
    return meta


@router.post("/create-table")
async def create_snowflake_table(request: CreateTableRequest):
    log = logger.bind(
        table=request.table_name,
        schema_source=request.schema_source,
        schema_target=request.schema_target,
        operation="table_create",
    )
    log.info("Creating Snowflake table")

    try:
        meta = await extract_oracle_metadata(request.table_name, request.schema_source)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Oracle metadata error: {exc}") from exc

    if not meta:
        raise HTTPException(
            status_code=404,
            detail=f"Table {request.schema_source}.{request.table_name} not found",
        )

    columns = meta["columns"]
    ddl = build_ddl(request.table_name, request.schema_target, columns)

    if request.dry_run:
        return {
            "table": request.table_name,
            "schema": request.schema_target,
            "ddl": ddl,
            "created": False,
            "message": "dry_run — DDL generated, not executed",
        }

    try:
        await create_snowflake_tables(request.table_name, request.schema_target, columns)
    except Exception as exc:
        log.error("Table creation failed", error=str(exc))
        raise HTTPException(status_code=500, detail=f"Snowflake DDL error: {exc}") from exc

    log.info("Table created in Snowflake")
    return {
        "table": request.table_name,
        "schema": request.schema_target,
        "ddl": ddl,
        "created": True,
        "message": f"Created {request.schema_target}.{request.table_name} and {request.table_name}_RAW",
    }


@router.post("/copy-into")
async def copy_into_snowflake(request: CopyIntoRequest):
    log = logger.bind(table=request.table_name, s3_key=request.s3_key, operation="copy_s3")
    log.info("Running COPY INTO")
    try:
        rows = await run_copy_into(
            table=request.table_name,
            schema=request.schema,
            s3_key=request.s3_key,
            columns=[],
        )
    except Exception as exc:
        log.error("COPY INTO failed", error=str(exc))
        raise HTTPException(status_code=500, detail=f"COPY INTO error: {exc}") from exc

    log.info("COPY INTO completed", rows=rows)
    return {
        "table": f"{request.schema}.{request.table_name}_RAW",
        "s3_key": request.s3_key,
        "rows_loaded": rows,
        "status": "done",
    }


@router.post("/merge")
async def merge_into_snowflake(request: MergeRequest):
    log = logger.bind(
        table=request.table_name,
        partition_date=request.partition_date,
        operation="merge_run",
    )
    log.info("Running MERGE")
    try:
        rows = await run_merge(
            table=request.table_name,
            schema=request.schema,
            partition_col=request.partition_column,
            date_from=request.partition_date,
        )
    except Exception as exc:
        log.error("MERGE failed", error=str(exc))
        raise HTTPException(status_code=500, detail=f"MERGE error: {exc}") from exc

    log.info("MERGE completed", rows=rows)
    return {
        "table": f"{request.schema}.{request.table_name}",
        "partition_date": request.partition_date,
        "rows_affected": rows,
        "status": "done",
    }
