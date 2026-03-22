from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.core.logging import get_logger
from app.core.oracle_client import get_oracle_pool
from app.core.snowflake_client import get_snowflake_engine
from app.core.s3_client import get_s3_client
from app.core.config import get_settings

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
    partition_date: str | None = None   

# ── Helpers ────────────────────────────────────────────────────────────────

def _oracle_to_snowflake_type(oracle_type: str, length: int | None, precision: int | None, scale: int | None) -> str:
    """Map Oracle data types to Snowflake equivalents."""
    t = oracle_type.upper()
    if t in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"):
        return f"VARCHAR({length or 255})"
    if t == "NUMBER":
        if scale and scale > 0:
            return f"NUMBER({precision or 38},{scale})"
        if precision:
            return f"NUMBER({precision})"
        return "NUMBER"
    if t in ("DATE", "TIMESTAMP"):
        return "TIMESTAMP_NTZ"
    if t.startswith("TIMESTAMP"):
        return "TIMESTAMP_NTZ"
    if t in ("CLOB", "NCLOB", "LONG"):
        return "TEXT"
    if t in ("BLOB", "RAW", "LONG RAW"):
        return "BINARY"
    if t == "FLOAT":
        return "FLOAT"
    if t in ("INTEGER", "INT", "SMALLINT"):
        return "INTEGER"
    return f"VARCHAR(4000)  -- unmapped Oracle type: {oracle_type}"


def _build_ddl(table: str, schema: str, columns: list[dict]) -> str:
    col_defs = []
    for col in columns:
        sf_type = _oracle_to_snowflake_type(
            col["type"], col.get("length"), col.get("precision"), col.get("scale")
        )
        nullable = "" if col.get("nullable", True) else " NOT NULL"
        comment = f"  -- {col['comment']}" if col.get("comment") else ""
        col_defs.append(f"    {col['name']:40s} {sf_type}{nullable}{comment}")

    cols_str = ",\n".join(col_defs)
    ddl = f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{cols_str}\n);"
    raw_ddl = f"CREATE TABLE IF NOT EXISTS {schema}.{table}_RAW (\n{cols_str}\n);"
    return f"-- Main table\n{ddl}\n\n-- RAW staging table\n{raw_ddl}"


# ── Routes ─────────────────────────────────────────────────────────────────

@router.get("/metadata")
async def extract_metadata(
    table: str = Query(..., description="Oracle table name"),
    schema: str = Query("DWADM", description="Oracle schema name"),
):
    log = logger.bind(table=table, schema=schema, operation="metadata_extract")
    log.info("Extracting Oracle metadata")

    pool = get_oracle_pool()
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT
                        c.COLUMN_NAME,
                        c.DATA_TYPE,
                        c.DATA_LENGTH,
                        c.DATA_PRECISION,
                        c.DATA_SCALE,
                        c.NULLABLE,
                        c.COLUMN_ID
                    FROM ALL_TAB_COLUMNS c
                    WHERE c.OWNER = :schema
                      AND c.TABLE_NAME = :table
                    ORDER BY c.COLUMN_ID
                """, schema=schema.upper(), table=table.upper())
                col_rows = await cur.fetchall()

                if not col_rows:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Table {schema}.{table} not found or no columns returned"
                    )
                
                await cur.execute("""
                    SELECT COLUMN_NAME, COMMENTS
                    FROM ALL_COL_COMMENTS
                    WHERE OWNER = :schema
                      AND TABLE_NAME = :table
                """, schema=schema.upper(), table=table.upper())
                comment_rows = await cur.fetchall()
                comments = {r[0]: r[1] for r in comment_rows if r[1]}

        columns = [
            {
                "name": r[0],
                "type": r[1],
                "length": r[2],
                "precision": r[3],
                "scale": r[4],
                "nullable": r[5] == "Y",
                "comment": comments.get(r[0]),
            }
            for r in col_rows
        ]

        log.info("Metadata extracted", columns=len(columns))
        return {
            "table_name": table.upper(),
            "source_schema": schema.upper(),
            "total_columns": len(columns),
            "columns": columns,
        }

    except HTTPException:
        raise
    except Exception as exc:
        log.error("Metadata extraction failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/create-table")
async def create_snowflake_table(request: CreateTableRequest):
    from sqlalchemy import text
    log = logger.bind(
        table=request.table_name,
        schema_source=request.schema_source,
        schema_target=request.schema_target,
        operation="table_create",
    )
    log.info("Creating Snowflake table")
    meta_response = await extract_metadata(
        table=request.table_name,
        schema=request.schema_source,
    )
    columns = meta_response["columns"]
    ddl = _build_ddl(request.table_name, request.schema_target, columns)

    if request.dry_run:
        return {"table": request.table_name, "schema": request.schema_target, "ddl": ddl, "created": False, "message": "dry_run — DDL generated, not executed"}

    try:
        engine = get_snowflake_engine()
        async with engine.connect() as conn:
            for stmt in ddl.split(";"):
                stmt = stmt.strip()
                if stmt and not stmt.startswith("--"):
                    await conn.execute(text(stmt))
            await conn.commit()

        log.info("Table created in Snowflake", ddl_len=len(ddl))
        return {
            "table": request.table_name,
            "schema": request.schema_target,
            "ddl": ddl,
            "created": True,
            "message": f"Created {request.schema_target}.{request.table_name} and {request.table_name}_RAW",
        }
    except Exception as exc:
        log.error("Table creation failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/copy-into")
async def copy_into_snowflake(request: CopyIntoRequest):
    from sqlalchemy import text
    log = logger.bind(
        table=request.table_name,
        s3_key=request.s3_key,
        operation="copy_s3",
    )
    log.info("Running COPY INTO")

    settings = get_settings()
    s3 = settings.s3
    full_path = f"s3://{s3.bucket}/{s3.prefix.rstrip('/')}/{request.s3_key.lstrip('/')}"

    raw_table = f"{request.schema}.{request.table_name}_RAW"
    sql = f"""
        COPY INTO {raw_table}
        FROM '{full_path}'
        CREDENTIALS = (
            AWS_KEY_ID = '{s3.access_key_id.get_secret_value()}'
            AWS_SECRET_KEY = '{s3.secret_access_key.get_secret_value()}'
        )
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL', 'null')
            EMPTY_FIELD_AS_NULL = TRUE
        )
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
    """

    try:
        engine = get_snowflake_engine()
        async with engine.connect() as conn:
            result = await conn.execute(text(sql))
            await conn.commit()
            rows = result.rowcount or 0

        log.info("COPY INTO completed", rows=rows)
        return {
            "table": raw_table,
            "s3_key": request.s3_key,
            "rows_loaded": rows,
            "status": "done",
        }
    except Exception as exc:
        log.error("COPY INTO failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/merge")
async def run_merge(request: MergeRequest):
    """
    MERGE from {TABLE}_RAW into {TABLE}.
    Optionally filter RAW by a specific partition date.
    """
    from sqlalchemy import text
    log = logger.bind(
        table=request.table_name,
        partition_date=request.partition_date,
        operation="merge_run",
    )
    log.info("Running MERGE")

    raw_table  = f"{request.schema}.{request.table_name}_RAW"
    tgt_table  = f"{request.schema}.{request.table_name}"
    date_filter = ""
    if request.partition_date:
        date_filter = f"WHERE DT_REFERENCIA = TO_DATE('{request.partition_date}', 'YYYY-MM-DD')"

    sql = f"""
        MERGE INTO {tgt_table} AS tgt
        USING (
            SELECT * FROM {raw_table}
            {date_filter}
        ) AS src
        ON tgt.ID = src.ID
        WHEN MATCHED THEN
            UPDATE SET tgt.UPDATED_AT = src.UPDATED_AT
        WHEN NOT MATCHED THEN
            INSERT VALUES (src.*)
    """

    try:
        engine = get_snowflake_engine()
        async with engine.connect() as conn:
            result = await conn.execute(text(sql))
            await conn.commit()
            rows_inserted = getattr(result, 'rowcount', 0) or 0

        log.info("MERGE completed", rows=rows_inserted)
        return {
            "table": tgt_table,
            "partition_date": request.partition_date,
            "rows_inserted": rows_inserted,
            "rows_updated": 0,   
            "status": "done",
        }
    except Exception as exc:
        log.error("MERGE failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))