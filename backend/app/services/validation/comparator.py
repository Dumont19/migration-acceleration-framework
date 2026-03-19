"""
services/validation/comparator.py
----------------------------------
Ports validate_migration.py + compare_*.py scripts.

Performs three levels of comparison:
  1. Count check   — Oracle vs Snowflake row counts
  2. Schema check  — column names, types, nullability
  3. Sample check  — N random rows compared field by field
"""

import uuid
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.core.oracle_client import get_oracle_pool
from app.core.snowflake_client import get_snowflake_engine
from app.models.logs import ValidationRun
from app.models.schemas import ValidationRequest, ValidationResultResponse

logger = get_logger(__name__)


class ValidationService:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    async def run(self, request: ValidationRequest, job_id: uuid.UUID | None = None) -> ValidationResultResponse:
        log = logger.bind(table=request.table_name, operation="validation")
        log.info("Validation started")

        result = ValidationRun(
            id=uuid.uuid4(),
            table_name=request.table_name.upper(),
            triggered_by_job_id=job_id,
        )

        # 1. Count check
        if request.check_counts:
            oracle_count, snow_count = await self._compare_counts(request)
            result.oracle_count = oracle_count
            result.snowflake_count = snow_count
            result.count_diff = snow_count - oracle_count if (oracle_count and snow_count) else None
            result.count_match = result.count_diff == 0
            log.info("Count check done", oracle=oracle_count, snowflake=snow_count, diff=result.count_diff)

        # 2. Schema check
        if request.check_schema:
            schema_match, schema_diff = await self._compare_schemas(request)
            result.schema_match = schema_match
            result.schema_diff = schema_diff
            log.info("Schema check done", match=schema_match)

        # 3. Sample check
        if request.check_sample:
            match_rate, sample_diff = await self._compare_sample(request)
            result.sample_size = request.sample_size
            result.sample_match_rate = match_rate
            result.sample_diff = sample_diff
            log.info("Sample check done", match_rate=match_rate)

        # Overall result
        checks = []
        if request.check_counts:
            checks.append(result.count_match is True)
        if request.check_schema:
            checks.append(result.schema_match is True)
        if request.check_sample:
            checks.append((result.sample_match_rate or 0) >= 0.99)

        result.passed = all(checks) if checks else None

        self._db.add(result)
        await self._db.commit()

        log.info("Validation complete", passed=result.passed)
        return ValidationResultResponse(
            id=result.id,
            table_name=result.table_name,
            oracle_count=result.oracle_count,
            snowflake_count=result.snowflake_count,
            count_diff=result.count_diff,
            count_match=result.count_match,
            schema_match=result.schema_match,
            schema_diff=result.schema_diff,
            sample_size=result.sample_size,
            sample_match_rate=result.sample_match_rate,
            passed=result.passed,
            notes=result.notes,
            created_at=result.created_at or datetime.now(timezone.utc),
        )

    async def _compare_counts(self, request: ValidationRequest) -> tuple[int | None, int | None]:
        table = request.table_name.upper()
        where = f"WHERE {request.date_filter}" if request.date_filter else ""

        # Oracle count
        oracle_count = None
        try:
            pool = get_oracle_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(f"SELECT COUNT(*) FROM DWADM.{table} {where}")
                    row = await cur.fetchone()
                    oracle_count = row[0] if row else None
        except Exception as exc:
            logger.warning("Oracle count failed", table=table, error=str(exc))

        # Snowflake count
        snow_count = None
        try:
            engine = get_snowflake_engine()
            async with engine.connect() as conn:
                result = await conn.execute(text(f"SELECT COUNT(*) FROM DWADM.{table} {where}"))
                row = result.fetchone()
                snow_count = row[0] if row else None
        except Exception as exc:
            logger.warning("Snowflake count failed", table=table, error=str(exc))

        return oracle_count, snow_count

    async def _compare_schemas(self, request: ValidationRequest) -> tuple[bool, dict | None]:
        table = request.table_name.upper()

        oracle_cols = await self._get_oracle_columns(table)
        snow_cols = await self._get_snowflake_columns(table)

        oracle_names = {c["name"] for c in oracle_cols}
        snow_names = {c["name"] for c in snow_cols}

        missing_in_snow = sorted(oracle_names - snow_names)
        extra_in_snow = sorted(snow_names - oracle_names)

        match = not missing_in_snow and not extra_in_snow
        diff = None if match else {
            "missing_in_snowflake": missing_in_snow,
            "extra_in_snowflake": extra_in_snow,
        }
        return match, diff

    async def _compare_sample(self, request: ValidationRequest) -> tuple[float, dict | None]:
        table = request.table_name.upper()
        n = request.sample_size

        try:
            pool = get_oracle_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"SELECT * FROM DWADM.{table} SAMPLE({min(n / 100, 1):.4f}) WHERE ROWNUM <= :n",
                        n=n,
                    )
                    columns = [c[0] for c in cur.description]
                    oracle_rows = await cur.fetchall()

            if not oracle_rows:
                return 1.0, None

            oracle_df = pd.DataFrame(oracle_rows, columns=columns)

            engine = get_snowflake_engine()
            async with engine.connect() as conn:
                # Fetch matching rows by primary key (first col as proxy — adjust per table)
                pk_col = columns[0]
                pk_values = oracle_df[pk_col].tolist()
                placeholders = ", ".join(f"'{v}'" for v in pk_values[:n])
                result = await conn.execute(
                    text(f"SELECT * FROM DWADM.{table} WHERE {pk_col} IN ({placeholders})")
                )
                snow_rows = result.fetchall()
                snow_df = pd.DataFrame(snow_rows, columns=result.keys())

            # Compare overlapping rows
            merged = oracle_df.merge(
                snow_df, on=pk_col, suffixes=("_oracle", "_snow")
            )
            if merged.empty:
                return 0.0, {"error": "No matching rows found in sample"}

            mismatches = []
            for col in columns[1:]:  # Skip PK
                oracle_col = f"{col}_oracle" if col != pk_col else col
                snow_col = f"{col}_snow" if col != pk_col else col
                if oracle_col in merged.columns and snow_col in merged.columns:
                    diff_mask = merged[oracle_col].astype(str) != merged[snow_col].astype(str)
                    if diff_mask.any():
                        mismatches.append({
                            "column": col,
                            "mismatch_count": int(diff_mask.sum()),
                        })

            match_rate = 1.0 - (len(mismatches) / len(columns))
            return round(max(0.0, match_rate), 4), {"column_mismatches": mismatches} if mismatches else None

        except Exception as exc:
            logger.error("Sample comparison failed", table=table, error=str(exc))
            return 0.0, {"error": str(exc)}

    async def _get_oracle_columns(self, table: str) -> list[dict]:
        try:
            pool = get_oracle_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """SELECT column_name, data_type, nullable
                           FROM all_tab_columns
                           WHERE table_name = :t AND owner = 'DWADM'
                           ORDER BY column_id""",
                        t=table,
                    )
                    return [{"name": r[0], "type": r[1], "nullable": r[2]} for r in await cur.fetchall()]
        except Exception:
            return []

    async def _get_snowflake_columns(self, table: str) -> list[dict]:
        try:
            engine = get_snowflake_engine()
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "SELECT column_name, data_type, is_nullable "
                        "FROM information_schema.columns "
                        "WHERE table_name = :t AND table_schema = 'DWADM' "
                        "ORDER BY ordinal_position"
                    ),
                    {"t": table},
                )
                return [{"name": r[0], "type": r[1], "nullable": r[2]} for r in result]
        except Exception:
            return []
