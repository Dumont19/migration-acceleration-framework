"""
services/migration/state.py
----------------------------
Centralized job state manager.

Replaces the scattered *_fast_migration_state.json files from the original codebase.
All state is stored in PostgreSQL (migration_jobs + job_partitions tables).
In-memory cache provides sub-millisecond reads for WebSocket broadcasts.

Usage:
    state = get_job_state_service()

    job_id = await state.create_job(table="F_CEL_NETWORK_EVENT", operation=..., config={})
    await state.start_job(job_id)
    await state.update_partition(job_id, "2024-03-01", JobStatus.DONE, rows=1_204_881)
    await state.finish_job(job_id, total_rows=50_000_000)
    await state.fail_job(job_id, error="ORA-01555: snapshot too old")
"""

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.core.logging import get_logger
from app.models.logs import JobPartition, JobStatus, MigrationJob, OperationType
from app.models.schemas import JobProgressResponse, PartitionStatus

logger = get_logger(__name__)

# In-memory snapshot for WebSocket broadcasts (avoids DB read on every WS message)
_job_cache: dict[str, dict] = {}


class JobStateService:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    # ── Create / Lifecycle ────────────────────────────────────────────────

    async def create_job(
        self,
        table_name: str,
        operation: OperationType,
        config: dict[str, Any] | None = None,
        triggered_by: str = "api",
    ) -> uuid.UUID:
        job = MigrationJob(
            table_name=table_name.upper(),
            operation=operation,
            status=JobStatus.PENDING,
            config=config or {},
            triggered_by=triggered_by,
        )
        self._db.add(job)
        await self._db.flush()  # Get the generated UUID before commit
        job_id = job.id
        _job_cache[str(job_id)] = {"status": JobStatus.PENDING, "done": 0, "failed": 0}
        logger.info("Job created", job_id=str(job_id), table=table_name, operation=operation)
        return job_id

    async def start_job(self, job_id: uuid.UUID, total_partitions: int | None = None) -> None:
        await self._db.execute(
            update(MigrationJob)
            .where(MigrationJob.id == job_id)
            .values(
                status=JobStatus.RUNNING,
                started_at=datetime.now(timezone.utc),
                total_partitions=total_partitions,
            )
        )
        _job_cache[str(job_id)] = {
            "status": JobStatus.RUNNING,
            "total": total_partitions,
            "done": 0,
            "failed": 0,
        }

    async def finish_job(self, job_id: uuid.UUID, total_rows: int | None = None) -> None:
        now = datetime.now(timezone.utc)
        job = await self._get_job(job_id)
        if not job:
            return
        duration = (now - job.started_at).total_seconds() if job.started_at else None
        await self._db.execute(
            update(MigrationJob)
            .where(MigrationJob.id == job_id)
            .values(
                status=JobStatus.DONE,
                finished_at=now,
                duration_seconds=duration,
                total_rows=total_rows,
                loaded_rows=total_rows or 0,
            )
        )
        _job_cache.pop(str(job_id), None)
        logger.info("Job finished", job_id=str(job_id), duration_seconds=duration, total_rows=total_rows)

    async def fail_job(self, job_id: uuid.UUID, error: str, traceback: str | None = None) -> None:
        now = datetime.now(timezone.utc)
        job = await self._get_job(job_id)
        duration = (now - job.started_at).total_seconds() if job and job.started_at else None
        await self._db.execute(
            update(MigrationJob)
            .where(MigrationJob.id == job_id)
            .values(
                status=JobStatus.ERROR,
                finished_at=now,
                duration_seconds=duration,
                error_message=error[:2000],
                error_traceback=traceback,
            )
        )
        _job_cache.pop(str(job_id), None)
        logger.error("Job failed", job_id=str(job_id), error=error)

    # ── Partition tracking ────────────────────────────────────────────────

    async def register_partitions(self, job_id: uuid.UUID, partition_keys: list[str]) -> None:
        partitions = [
            JobPartition(job_id=job_id, partition_key=k, status=JobStatus.PENDING)
            for k in partition_keys
        ]
        self._db.add_all(partitions)
        await self._db.flush()

    async def update_partition(
        self,
        job_id: uuid.UUID,
        partition_key: str,
        status: JobStatus,
        rows_loaded: int = 0,
        error: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        await self._db.execute(
            update(JobPartition)
            .where(
                JobPartition.job_id == job_id,
                JobPartition.partition_key == partition_key,
            )
            .values(
                status=status,
                rows_loaded=rows_loaded,
                finished_at=now,
                error_message=error,
            )
        )
        # Update in-memory counter
        cache = _job_cache.get(str(job_id), {})
        if status == JobStatus.DONE:
            cache["done"] = cache.get("done", 0) + 1
        elif status == JobStatus.ERROR:
            cache["failed"] = cache.get("failed", 0) + 1

        # Propagate counter update to DB
        await self._db.execute(
            update(MigrationJob)
            .where(MigrationJob.id == job_id)
            .values(
                done_partitions=cache.get("done", 0),
                failed_partitions=cache.get("failed", 0),
                loaded_rows=MigrationJob.loaded_rows + rows_loaded,
            )
        )

    # ── Query ─────────────────────────────────────────────────────────────

    async def get_progress(self, job_id: uuid.UUID) -> JobProgressResponse | None:
        job = await self._get_job(job_id)
        if not job:
            return None
        pct = 0.0
        if job.total_partitions and job.total_partitions > 0:
            pct = round((job.done_partitions / job.total_partitions) * 100, 1)
        return JobProgressResponse(
            job_id=job.id,
            table_name=job.table_name,
            operation=job.operation,
            status=job.status,
            total_partitions=job.total_partitions,
            done_partitions=job.done_partitions,
            failed_partitions=job.failed_partitions,
            percent=pct,
            total_rows=job.total_rows,
            loaded_rows=job.loaded_rows,
            started_at=job.started_at,
            updated_at=job.updated_at,
        )

    async def get_partitions(self, job_id: uuid.UUID) -> list[PartitionStatus]:
        result = await self._db.execute(
            select(JobPartition)
            .where(JobPartition.job_id == job_id)
            .order_by(JobPartition.partition_key)
        )
        return [
            PartitionStatus(
                partition_key=p.partition_key,
                status=p.status,
                rows_loaded=p.rows_loaded,
                attempts=p.attempts,
                error_message=p.error_message,
                started_at=p.started_at,
                finished_at=p.finished_at,
            )
            for p in result.scalars().all()
        ]

    # ── Helpers ───────────────────────────────────────────────────────────

    async def _get_job(self, job_id: uuid.UUID) -> MigrationJob | None:
        result = await self._db.execute(
            select(MigrationJob).where(MigrationJob.id == job_id)
        )
        return result.scalar_one_or_none()


def get_job_state_service(db: AsyncSession) -> JobStateService:
    return JobStateService(db)
