"""
api/routes/health.py
--------------------
Health check endpoints for all external connections.
Used by the Settings page in the frontend to verify config.

Routes:
  GET /api/health        → all connections
  GET /api/health/oracle
  GET /api/health/snowflake
  GET /api/health/s3
  GET /api/health/database
"""

from fastapi import APIRouter
from sqlalchemy import text

from app.core.config import get_settings
from app.core.database import get_engine
from app.core.logging import get_logger
from app.core.oracle_client import test_oracle_connection
from app.core.s3_client import get_s3_client
from app.core.snowflake_client import test_snowflake_connection
from app.models.schemas import ConnectionHealth, HealthResponse

router = APIRouter(prefix="/api/health", tags=["health"])
logger = get_logger(__name__)


async def _check_database() -> ConnectionHealth:
    import time
    try:
        start = time.monotonic()
        engine = get_engine()
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        elapsed_ms = round((time.monotonic() - start) * 1000, 2)
        return ConnectionHealth(status="ok", latency_ms=elapsed_ms)
    except Exception as exc:
        return ConnectionHealth(status="error", error=str(exc))


@router.get("", response_model=HealthResponse)
async def health_all():
    """Parallel health check for all connections."""
    import asyncio

    oracle, snowflake, s3_raw, db = await asyncio.gather(
        test_oracle_connection(),
        test_snowflake_connection(),
        get_s3_client().test_connection(),
        _check_database(),
        return_exceptions=True,
    )

    def _safe(result) -> ConnectionHealth:
        if isinstance(result, Exception):
            return ConnectionHealth(status="error", error=str(result))
        return ConnectionHealth(**result) if isinstance(result, dict) else result

    settings = get_settings().app
    return HealthResponse(
        oracle=_safe(oracle),
        snowflake=_safe(snowflake),
        s3=_safe(s3_raw),
        database=db if isinstance(db, ConnectionHealth) else _safe(db),
        app_version=settings.app_version,
        environment=settings.environment,
    )


@router.get("/oracle", response_model=ConnectionHealth)
async def health_oracle():
    result = await test_oracle_connection()
    return ConnectionHealth(**result)


@router.get("/snowflake", response_model=ConnectionHealth)
async def health_snowflake():
    result = await test_snowflake_connection()
    return ConnectionHealth(**result)


@router.get("/s3", response_model=ConnectionHealth)
async def health_s3():
    result = await get_s3_client().test_connection()
    return ConnectionHealth(**result)


@router.get("/database", response_model=ConnectionHealth)
async def health_database():
    return await _check_database()
