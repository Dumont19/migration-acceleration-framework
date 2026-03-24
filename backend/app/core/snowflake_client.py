from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_engine: AsyncEngine | None = None

def _build_connection_url() -> str:
    settings = get_settings().snowflake
    base = (
        f"snowflake://{settings.user}"
        f"@{settings.account}"
        f"/{settings.database}/{settings.schema_name}"
        f"?role={settings.role}&warehouse={settings.warehouse}"
    )
    if settings.authenticator and settings.authenticator != "snowflake":
        base += f"&authenticator={settings.authenticator}"
    if settings.password:
        # Password-based auth (not SSO)
        pwd = settings.password.get_secret_value()
        base = base.replace(settings.user, f"{settings.user}:{pwd}", 1)
    return base


def init_snowflake_engine() -> None:
    global _engine
    settings = get_settings().snowflake

    logger.info(
        "Initializing Snowflake engine",
        account=settings.account,
        database=settings.database,
        schema=settings.schema_name,
        warehouse=settings.warehouse,
        authenticator=settings.authenticator,
    )

    _engine = create_async_engine(
        _build_connection_url(),
        pool_size=3,
        max_overflow=5,
        pool_pre_ping=True,
        pool_recycle=1800,  # Snowflake sessions expire after ~4h; recycle proactively
        echo=False,
    )
    logger.info("Snowflake engine initialized")


async def close_snowflake_engine() -> None:
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None
        logger.info("Snowflake engine disposed")


def get_snowflake_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("Snowflake engine not initialized.")
    return _engine


async def test_snowflake_connection() -> dict:
    import time
    try:
        start = time.monotonic()
        async with get_snowflake_engine().connect() as conn:
            result = await conn.execute(
                text("SELECT CURRENT_WAREHOUSE(), CURRENT_ROLE(), CURRENT_VERSION()")
            )
            row = result.fetchone()
        elapsed_ms = round((time.monotonic() - start) * 1000, 2)
        return {
            "status": "ok",
            "latency_ms": elapsed_ms,
            "warehouse": row[0] if row else None,
            "role": row[1] if row else None,
            "version": row[2] if row else None,
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}
