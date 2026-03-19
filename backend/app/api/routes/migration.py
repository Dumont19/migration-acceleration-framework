"""
api/routes/migration.py
------------------------
Migration endpoints — thin layer, all logic in services/.

Routes:
  POST   /api/migration/start           → start migration, returns job_id
  GET    /api/migration/jobs            → list all jobs (paginated)
  GET    /api/migration/jobs/{job_id}   → job progress
  GET    /api/migration/jobs/{job_id}/partitions → partition breakdown
  POST   /api/migration/jobs/{job_id}/cancel     → cancel running job
  POST   /api/migration/jobs/{job_id}/retry      → retry failed partitions
  WS     /ws/progress/{job_id}          → real-time progress stream
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.ws.progress import handle_progress_websocket, ws_manager
from app.core.database import get_db_session
from app.core.logging import get_logger
from app.models.logs import MigrationJob, JobStatus
from app.models.schemas import (
    JobListResponse,
    JobProgressResponse,
    JobSummaryResponse,
    MigrationRequest,
    PartitionStatus,
)
from app.services.migration.partitioned import PartitionedMigrationService
from app.services.migration.state import get_job_state_service

router = APIRouter(prefix="/api/migration", tags=["migration"])
logger = get_logger(__name__)


@router.post("/start", response_model=dict, status_code=status.HTTP_202_ACCEPTED)
async def start_migration(
    request: MigrationRequest,
    db: AsyncSession = Depends(get_db_session),
):
    """
    Start a migration job. Returns job_id immediately.
    Track progress via GET /jobs/{job_id} or WS /ws/progress/{job_id}.
    """
    svc = PartitionedMigrationService(db)

    # Inject WebSocket broadcaster so the service can push progress
    async def broadcaster(job_id, progress, log_line=None):
        await ws_manager.send_progress(
            str(job_id),
            progress.model_dump(mode="json"),
            log_line,
        )
    svc.set_broadcaster(broadcaster)

    job_id = await svc.run(request)
    logger.info("Migration job started via API", job_id=str(job_id), table=request.table_name)

    return {
        "job_id": str(job_id),
        "table": request.table_name,
        "status": "accepted",
        "ws_url": f"/api/migration/ws/progress/{job_id}",
        "poll_url": f"/api/migration/jobs/{job_id}",
    }


@router.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=5, le=100),
    table_name: str | None = Query(None),
    job_status: JobStatus | None = Query(None, alias="status"),
    db: AsyncSession = Depends(get_db_session),
):
    """List all migration jobs with optional filters."""
    query = select(MigrationJob).order_by(MigrationJob.created_at.desc())
    if table_name:
        query = query.where(MigrationJob.table_name == table_name.upper())
    if job_status:
        query = query.where(MigrationJob.status == job_status)

    count_query = select(MigrationJob).where(True)
    offset = (page - 1) * page_size
    query = query.offset(offset).limit(page_size)

    result = await db.execute(query)
    jobs = result.scalars().all()
    total = len(jobs) + offset  # Approximate — good enough for UI

    return JobListResponse(
        items=[
            JobSummaryResponse(
                job_id=j.id,
                table_name=j.table_name,
                operation=j.operation,
                status=j.status,
                duration_seconds=j.duration_seconds,
                loaded_rows=j.loaded_rows,
                failed_partitions=j.failed_partitions,
                created_at=j.created_at,
            )
            for j in jobs
        ],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/jobs/{job_id}", response_model=JobProgressResponse)
async def get_job_progress(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db_session),
):
    """Get current progress for a specific job."""
    svc = get_job_state_service(db)
    progress = await svc.get_progress(job_id)
    if not progress:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return progress


@router.get("/jobs/{job_id}/partitions", response_model=list[PartitionStatus])
async def get_job_partitions(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db_session),
):
    """Get partition-level breakdown for a job."""
    svc = get_job_state_service(db)
    return await svc.get_partitions(job_id)


@router.post("/jobs/{job_id}/cancel", status_code=status.HTTP_200_OK)
async def cancel_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db_session),
):
    """Cancel a running job. In-flight partitions complete; pending ones are skipped."""
    svc = get_job_state_service(db)
    progress = await svc.get_progress(job_id)
    if not progress:
        raise HTTPException(status_code=404, detail="Job not found")
    if progress.status not in (JobStatus.PENDING, JobStatus.RUNNING):
        raise HTTPException(status_code=400, detail=f"Cannot cancel job in status {progress.status}")
    await svc.fail_job(job_id, error="Cancelled by user")
    await ws_manager.send_error(str(job_id), "Job cancelled by user")
    return {"job_id": str(job_id), "status": "cancelled"}


# ── WebSocket ─────────────────────────────────────────────────────────────────

@router.websocket("/ws/progress/{job_id}")
async def ws_progress(websocket: WebSocket, job_id: str):
    """
    Real-time progress stream for a migration job.
    Note: WebSocket routes must NOT be under the /api prefix.
    This is registered separately in main.py via app.include_router.
    """
    await handle_progress_websocket(websocket, job_id)
