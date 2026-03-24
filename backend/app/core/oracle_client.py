import oracledb
from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_pool: oracledb.AsyncConnectionPool | None = None


async def init_oracle_pool() -> None:
    global _pool
    settings = get_settings().oracle

    logger.info(
        "Initializing Oracle connection pool",
        host=settings.host,
        service=settings.service,
        pool_min=settings.pool_min,
        pool_max=settings.pool_max,
    )

    _pool = oracledb.create_pool_async(
        host=settings.host,
        port=settings.port,
        service_name=settings.service,
        user=settings.user,
        password=settings.password.get_secret_value(),
        min=settings.pool_min,
        max=settings.pool_max,
        increment=settings.pool_increment,
    )
    logger.info("Oracle pool initialized", operation="oracle_pool_init")


async def close_oracle_pool() -> None:
    global _pool
    if _pool:
        await _pool.close(force=False)  # Wait for in-flight queries
        _pool = None
        logger.info("Oracle pool closed")


def get_oracle_pool() -> oracledb.AsyncConnectionPool:
    if _pool is None:
        raise RuntimeError("Oracle pool not initialized. Call init_oracle_pool() first.")
    return _pool


async def test_oracle_connection() -> dict:
    import time
    try:
        start = time.monotonic()
        async with get_oracle_pool().acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
                row = await cur.fetchone()
        elapsed_ms = round((time.monotonic() - start) * 1000, 2)
        return {
            "status": "ok",
            "latency_ms": elapsed_ms,
            "version": row[0] if row else "unknown",
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}
