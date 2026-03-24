import asyncio
import csv
import gzip
import io
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.core.oracle_client import get_oracle_pool
from app.core.s3_client import get_s3_client
from app.core.snowflake_client import get_snowflake_engine
from app.models.logs import JobStatus, OperationType
from app.models.schemas import MigrationRequest
from app.services.migration.state import get_job_state_service

logger = get_logger(__name__)


class PartitionedMigrationService:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db
        self._state = get_job_state_service(db)
        self._progress_broadcaster = None  # Injected by WebSocket handler

    def set_broadcaster(self, broadcaster) -> None:
        self._progress_broadcaster = broadcaster

    async def run(self, request: MigrationRequest, triggered_by: str = "api") -> uuid.UUID:
        job_id = await self._state.create_job(
            table_name=request.table_name,
            operation=OperationType.MIGRATION_PARTITIONED,
            config=request.model_dump(),
            triggered_by=triggered_by,
        )
        await self._db.commit()

        # Fire-and-forget — caller tracks via WebSocket or polling
        asyncio.create_task(
            self._execute(job_id, request),
            name=f"migration_{job_id}",
        )
        return job_id

    async def _execute(self, job_id: uuid.UUID, request: MigrationRequest) -> None:
        log = logger.bind(
            job_id=str(job_id),
            table=request.table_name,
            operation="partitioned_migration",
        )
        try:
            partitions = self._build_partition_list(request)
            await self._state.start_job(job_id, total_partitions=len(partitions))
            await self._db.commit()
            log.info("Migration started", total_partitions=len(partitions))

            await self._state.register_partitions(job_id, [p["key"] for p in partitions])
            await self._db.commit()

            semaphore = asyncio.Semaphore(request.max_workers)
            tasks = [
                asyncio.create_task(
                    self._migrate_partition_safe(job_id, p, request, semaphore, log)
                )
                for p in partitions
            ]
            await asyncio.gather(*tasks)

            total_rows = sum(p.rows_loaded for p in await self._state.get_partitions(job_id))
            await self._state.finish_job(job_id, total_rows=total_rows)
            await self._db.commit()
            log.info("Migration completed", total_rows=total_rows)

        except Exception as exc:
            import traceback
            await self._state.fail_job(
                job_id,
                error=str(exc),
                traceback=traceback.format_exc(),
            )
            await self._db.commit()
            log.error("Migration failed with unhandled exception", exc_info=True)

    async def _migrate_partition_safe(
        self,
        job_id: uuid.UUID,
        partition: dict,
        request: MigrationRequest,
        semaphore: asyncio.Semaphore,
        log,
    ) -> None:
        async with semaphore:
            key = partition["key"]
            part_log = log.bind(partition=key)
            try:
                rows = await self._migrate_partition(partition, request, part_log)
                await self._state.update_partition(job_id, key, JobStatus.DONE, rows_loaded=rows)
                await self._db.commit()
                await self._broadcast_progress(job_id, last_log=f"Partition {key} loaded {rows:,} rows")
            except Exception as exc:
                await self._state.update_partition(
                    job_id, key, JobStatus.ERROR, error=str(exc)
                )
                await self._db.commit()
                part_log.error("Partition failed", error=str(exc), partition=key)

    async def _migrate_partition(
        self, partition: dict, request: MigrationRequest, log
    ) -> int:
        table = request.table_name
        key = partition["key"]
        date_from = partition["date_from"]
        date_to = partition["date_to"]

        # 1. Extract from Oracle
        log.info("Extracting from Oracle", date_from=date_from, date_to=date_to)
        rows_data, columns = await self._extract_from_oracle(
            table=table,
            schema=request.schema_source,
            partition_col=request.partition_column,
            date_from=date_from,
            date_to=date_to,
        )

        if not rows_data:
            log.info("Partition empty — skipping", partition=key)
            return 0

        # 2. Write to CSV.GZ in memory
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
            text_buffer = io.StringIO()
            writer = csv.writer(text_buffer, quoting=csv.QUOTE_ALL)
            writer.writerow(columns)
            writer.writerows(rows_data)
            gz.write(text_buffer.getvalue().encode("utf-8"))
        buffer.seek(0)

        # 3. Upload to S3
        s3_key = f"{table}/{key}/{table}_{key}.csv.gz"
        tmp_path = Path(f"/tmp/{table}_{key}.csv.gz")
        tmp_path.write_bytes(buffer.read())
        s3 = get_s3_client()
        await s3.upload_file(tmp_path, s3_key)
        tmp_path.unlink(missing_ok=True)
        log.info("Uploaded to S3", s3_key=s3_key, rows=len(rows_data))

        # 4. COPY INTO Snowflake RAW
        await self._copy_into_snowflake(
            table=f"{table}_RAW",
            schema=request.schema_target,
            s3_key=s3_key,
            columns=columns,
        )

        # 5. MERGE RAW → Final
        await self._run_merge(
            table=table,
            schema=request.schema_target,
            partition_col=request.partition_column,
            date_from=date_from,
        )

        return len(rows_data)

    async def _extract_from_oracle(
        self,
        table: str,
        schema: str,
        partition_col: str,
        date_from: str,
        date_to: str,
    ) -> tuple[list[tuple], list[str]]:
        pool = get_oracle_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql = f"""
                    SELECT *
                    FROM {schema}.{table}
                    WHERE {partition_col} >= TO_DATE(:date_from, 'YYYY-MM-DD')
                      AND {partition_col} <  TO_DATE(:date_to,   'YYYY-MM-DD')
                """
                await cur.execute(sql, date_from=date_from, date_to=date_to)
                columns = [col[0] for col in cur.description]
                rows = await cur.fetchall()
        return rows, columns

    async def _copy_into_snowflake(
        self, table: str, schema: str, s3_key: str, columns: list[str]
    ) -> None:
        from sqlalchemy import text
        settings_s3 = __import__(
            "app.core.config", fromlist=["get_settings"]
        ).get_settings().s3
        full_s3_path = f"s3://{settings_s3.bucket}/{settings_s3.prefix}{s3_key}"
        col_list = ", ".join(columns)
        sql = f"""
            COPY INTO {schema}.{table} ({col_list})
            FROM '{full_s3_path}'
            CREDENTIALS = (
                AWS_KEY_ID = '{settings_s3.access_key_id.get_secret_value()}'
                AWS_SECRET_KEY = '{settings_s3.secret_access_key.get_secret_value()}'
            )
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = 'CONTINUE'
        """
        engine = get_snowflake_engine()
        async with engine.connect() as conn:
            await conn.execute(text(sql))
            await conn.commit()

    async def _run_merge(
        self, table: str, schema: str, partition_col: str, date_from: str
    ) -> None:
        from sqlalchemy import text
        sql = f"""
            MERGE INTO {schema}.{table} AS tgt
            USING (
                SELECT * FROM {schema}.{table}_RAW
                WHERE {partition_col} = TO_DATE('{date_from}', 'YYYY-MM-DD')
            ) AS src
            ON tgt.ID = src.ID
            WHEN MATCHED THEN UPDATE SET tgt.UPDATED_AT = src.UPDATED_AT
            WHEN NOT MATCHED THEN INSERT VALUES (src.*)
        """
        engine = get_snowflake_engine()
        async with engine.connect() as conn:
            await conn.execute(text(sql))
            await conn.commit()

    def _build_partition_list(self, request: MigrationRequest) -> list[dict]:
        partitions = []
        if not request.date_from or not request.date_to:
            # Single partition — full table
            return [{"key": "full", "date_from": None, "date_to": None}]
        start = date.fromisoformat(request.date_from)
        end = date.fromisoformat(request.date_to)
        current = start
        while current < end:
            next_date = current + timedelta(days=request.batch_size_days)
            if next_date > end:
                next_date = end
            partitions.append({
                "key": current.isoformat(),
                "date_from": current.isoformat(),
                "date_to": next_date.isoformat(),
            })
            current = next_date
        return partitions

    async def _broadcast_progress(self, job_id: uuid.UUID, last_log: str | None = None) -> None:
        if not self._progress_broadcaster:
            return
        progress = await self._state.get_progress(job_id)
        if progress:
            await self._progress_broadcaster(job_id, progress, last_log)
