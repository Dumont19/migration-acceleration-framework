"""
api/routes/logs.py
------------------
Read-only endpoints for querying the persistent log database.

Routes:
  GET /api/logs              → paginated log search
  GET /api/logs/{job_id}     → all logs for a specific job
  GET /api/logs/stats        → log counts by level / table / date
"""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.models.logs import JobLog, LogLevel
from app.models.schemas import LogEntryResponse

router = APIRouter(prefix="/api/logs", tags=["logs"])


@router.get("", response_model=dict)
async def query_logs(
    job_id: uuid.UUID | None = Query(None),
    table_name: str | None = Query(None),
    level: LogLevel | None = Query(None),
    from_dt: datetime | None = Query(None),
    to_dt: datetime | None = Query(None),
    search: str | None = Query(None, description="Free-text search in message"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=10, le=500),
    db: AsyncSession = Depends(get_db_session),
):
    """
    Paginated log search with filters.
    Supports: job_id, table_name, level, date range, free-text search.
    """
    query = select(JobLog).order_by(JobLog.created_at.desc())

    if job_id:
        query = query.where(JobLog.job_id == job_id)
    if table_name:
        query = query.where(JobLog.table_name == table_name.upper())
    if level:
        query = query.where(JobLog.level == level)
    if from_dt:
        query = query.where(JobLog.created_at >= from_dt)
    if to_dt:
        query = query.where(JobLog.created_at <= to_dt)
    if search:
        query = query.where(JobLog.message.ilike(f"%{search}%"))

    # Count total (for pagination UI)
    count_q = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_q)
    total = total_result.scalar() or 0

    offset = (page - 1) * page_size
    query = query.offset(offset).limit(page_size)
    result = await db.execute(query)
    logs = result.scalars().all()

    return {
        "items": [
            LogEntryResponse(
                id=log.id,
                job_id=log.job_id,
                table_name=log.table_name,
                operation=log.operation,
                level=log.level,
                message=log.message,
                extra=log.extra,
                created_at=log.created_at,
            ).model_dump(mode="json")
            for log in logs
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size,
    }


@router.get("/job/{job_id}", response_model=list[LogEntryResponse])
async def get_job_logs(
    job_id: uuid.UUID,
    level: LogLevel | None = Query(None),
    limit: int = Query(500, ge=10, le=5000),
    db: AsyncSession = Depends(get_db_session),
):
    """All logs for a specific job, oldest-first (for log stream display)."""
    query = (
        select(JobLog)
        .where(JobLog.job_id == job_id)
        .order_by(JobLog.created_at.asc())
        .limit(limit)
    )
    if level:
        query = query.where(JobLog.level == level)

    result = await db.execute(query)
    logs = result.scalars().all()
    return [
        LogEntryResponse(
            id=log.id,
            job_id=log.job_id,
            table_name=log.table_name,
            operation=log.operation,
            level=log.level,
            message=log.message,
            extra=log.extra,
            created_at=log.created_at,
        )
        for log in logs
    ]


@router.get("/stats", response_model=dict)
async def log_stats(
    from_dt: datetime | None = Query(None),
    to_dt: datetime | None = Query(None),
    db: AsyncSession = Depends(get_db_session),
):
    """Aggregated log statistics — for the dashboard overview."""
    base = select(
        JobLog.level,
        func.count(JobLog.id).label("count"),
    ).group_by(JobLog.level)

    if from_dt:
        base = base.where(JobLog.created_at >= from_dt)
    if to_dt:
        base = base.where(JobLog.created_at <= to_dt)

    result = await db.execute(base)
    by_level = {row.level: row.count for row in result}

    # Top tables by error count
    error_q = (
        select(
            JobLog.table_name,
            func.count(JobLog.id).label("errors"),
        )
        .where(JobLog.level.in_(["ERROR", "CRITICAL"]))
        .where(JobLog.table_name.isnot(None))
        .group_by(JobLog.table_name)
        .order_by(func.count(JobLog.id).desc())
        .limit(10)
    )
    error_result = await db.execute(error_q)
    top_errors = [{"table": r.table_name, "errors": r.errors} for r in error_result]

    return {
        "by_level": by_level,
        "top_error_tables": top_errors,
        "total": sum(by_level.values()),
    }
