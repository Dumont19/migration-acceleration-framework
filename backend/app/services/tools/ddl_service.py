"""
services/tools/ddl_service.py
-------------------------------
Lógica de negócio para geração de DDL e operações Snowflake.
Sem imports de FastAPI — puro Python.
"""
from app.core.config import get_settings
from app.core.logging import get_logger
from app.core.oracle_client import get_oracle_pool
from app.core.snowflake_client import get_snowflake_engine

logger = get_logger(__name__)


def oracle_to_snowflake_type(
    oracle_type: str,
    length: int | None,
    precision: int | None,
    scale: int | None,
) -> str:
    """Mapeia tipos Oracle para equivalentes Snowflake."""
    t = oracle_type.upper()
    if t in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"):
        return f"VARCHAR({length or 255})"
    if t == "NUMBER":
        if scale and scale > 0:
            return f"NUMBER({precision or 38},{scale})"
        if precision:
            return f"NUMBER({precision})"
        return "NUMBER"
    if t in ("DATE", "TIMESTAMP") or t.startswith("TIMESTAMP"):
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


def build_ddl(table: str, schema: str, columns: list[dict]) -> str:
    """Gera DDL para tabela principal e tabela _RAW."""
    col_defs = []
    for col in columns:
        sf_type = oracle_to_snowflake_type(
            col["type"], col.get("length"), col.get("precision"), col.get("scale")
        )
        nullable = "" if col.get("nullable", True) else " NOT NULL"
        comment = f"  -- {col['comment']}" if col.get("comment") else ""
        col_defs.append(f"    {col['name']:40s} {sf_type}{nullable}{comment}")

    cols_str = ",\n".join(col_defs)
    ddl = f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{cols_str}\n);"
    raw_ddl = f"CREATE TABLE IF NOT EXISTS {schema}.{table}_RAW (\n{cols_str}\n);"
    return f"-- Main table\n{ddl}\n\n-- RAW staging table\n{raw_ddl}"


def build_copy_into_sql(
    full_table: str,
    s3_path: str,
    columns: list[str],
) -> str:
    """
    Gera SQL COPY INTO para Snowflake via S3.
    Credenciais lidas de settings — nunca passadas como parâmetro.
    """
    settings_s3 = get_settings().s3
    col_list = ", ".join(columns) if columns else "*"
    return f"""
        COPY INTO {full_table} ({col_list})
        FROM '{s3_path}'
        CREDENTIALS = (
            AWS_KEY_ID = '{settings_s3.access_key_id.get_secret_value()}'
            AWS_SECRET_KEY = '{settings_s3.secret_access_key.get_secret_value()}'
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


async def extract_oracle_metadata(table: str, schema: str) -> dict:
    """Extrai colunas e comentários de uma tabela Oracle."""
    pool = get_oracle_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT c.COLUMN_NAME, c.DATA_TYPE, c.DATA_LENGTH,
                       c.DATA_PRECISION, c.DATA_SCALE, c.NULLABLE, c.COLUMN_ID
                FROM ALL_TAB_COLUMNS c
                WHERE c.OWNER = :schema AND c.TABLE_NAME = :table
                ORDER BY c.COLUMN_ID
                """,
                schema=schema.upper(),
                table=table.upper(),
            )
            col_rows = await cur.fetchall()

            if not col_rows:
                return {}

            await cur.execute(
                """
                SELECT COLUMN_NAME, COMMENTS
                FROM ALL_COL_COMMENTS
                WHERE OWNER = :schema AND TABLE_NAME = :table
                """,
                schema=schema.upper(),
                table=table.upper(),
            )
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
    return {
        "table_name": table.upper(),
        "source_schema": schema.upper(),
        "total_columns": len(columns),
        "columns": columns,
    }


async def create_snowflake_tables(table: str, schema: str, columns: list[dict]) -> str:
    """Executa DDL de criação de tabela e _RAW no Snowflake. Retorna o DDL gerado."""
    from sqlalchemy import text

    ddl = build_ddl(table, schema, columns)
    engine = get_snowflake_engine()
    async with engine.connect() as conn:
        for stmt in ddl.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                await conn.execute(text(stmt))
        await conn.commit()
    logger.info("Tables created in Snowflake", table=table, schema=schema)
    return ddl


async def run_copy_into(
    table: str, schema: str, s3_key: str, columns: list[str]
) -> int:
    """Executa COPY INTO e retorna número de linhas carregadas."""
    from sqlalchemy import text

    settings_s3 = get_settings().s3
    full_table = f"{schema}.{table}_RAW"
    s3_path = f"s3://{settings_s3.bucket}/{settings_s3.prefix.rstrip('/')}/{s3_key.lstrip('/')}"
    sql = build_copy_into_sql(full_table, s3_path, columns)

    engine = get_snowflake_engine()
    async with engine.connect() as conn:
        result = await conn.execute(text(sql))
        await conn.commit()
        return result.rowcount or 0


async def run_merge(
    table: str,
    schema: str,
    partition_col: str,
    date_from: str | None = None,
) -> int:
    """Executa MERGE de _RAW para tabela final. Retorna rowcount."""
    from sqlalchemy import text

    raw_table = f"{schema}.{table}_RAW"
    tgt_table = f"{schema}.{table}"
    date_filter = (
        f"WHERE {partition_col} = TO_DATE('{date_from}', 'YYYY-MM-DD')"
        if date_from
        else ""
    )
    sql = f"""
        MERGE INTO {tgt_table} AS tgt
        USING (SELECT * FROM {raw_table} {date_filter}) AS src
        ON tgt.ID = src.ID
        WHEN MATCHED THEN UPDATE SET tgt.UPDATED_AT = src.UPDATED_AT
        WHEN NOT MATCHED THEN INSERT VALUES (src.*)
    """
    engine = get_snowflake_engine()
    async with engine.connect() as conn:
        result = await conn.execute(text(sql))
        await conn.commit()
        return getattr(result, "rowcount", 0) or 0
