"""
app/main.py
-----------
FastAPI application factory.

Lifespan:
  startup  → configure logging, init DB engine, Oracle pool, Snowflake engine, S3 client
  shutdown → close all connections gracefully

All routers registered here.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import datastage, health, logs, migration
from app.core.config import get_settings
from app.core.database import close_db_engine, init_db_engine
from app.core.logging import configure_logging, get_logger
from app.core.oracle_client import close_oracle_pool, init_oracle_pool
from app.core.s3_client import init_s3_client
from app.core.snowflake_client import close_snowflake_engine, init_snowflake_engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan — runs once on startup and shutdown.
    Order matters: logging first, then DB, then external services.
    """
    settings = get_settings()

    # 1. Logging (must be first — everything else logs)
    configure_logging(
        log_level=settings.app.log_level,
        environment=settings.app.environment,
    )
    logger = get_logger("lifespan")
    logger.info(
        "Starting MAF API",
        version=settings.app.app_version,
        environment=settings.app.environment,
    )

    # 2. PostgreSQL (log DB)
    init_db_engine()
    logger.info("PostgreSQL engine initialized")

    # 3. Oracle pool
    try:
        await init_oracle_pool()
    except Exception as exc:
        logger.warning("Oracle pool init failed — health check will report error", error=str(exc))

    # 4. Snowflake engine
    try:
        init_snowflake_engine()
    except Exception as exc:
        logger.warning("Snowflake engine init failed", error=str(exc))

    # 5. S3 client
    try:
        init_s3_client()
    except Exception as exc:
        logger.warning("S3 client init failed", error=str(exc))

    logger.info("All services initialized — API ready")
    yield  # ← Application runs here

    # ── Shutdown ──────────────────────────────────────────────────────────
    logger.info("Shutting down MAF API")
    await close_oracle_pool()
    await close_snowflake_engine()
    await close_db_engine()
    logger.info("Shutdown complete")


def create_app() -> FastAPI:
    settings = get_settings().app

    app = FastAPI(
        title="Migration Acceleration Framework",
        description=(
            "REST + WebSocket API for Oracle → Snowflake migration orchestration. "
            "Replaces the Tkinter GUI with a fully async backend."
        ),
        version=settings.app_version,
        lifespan=lifespan,
        # Disable docs in production
        docs_url="/docs" if settings.environment != "production" else None,
        redoc_url="/redoc" if settings.environment != "production" else None,
    )

    # ── Middleware ────────────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # ── Routers ───────────────────────────────────────────────────────────
    app.include_router(health.router)
    app.include_router(migration.router)
    app.include_router(logs.router)
    app.include_router(datastage.router)

    # ── Global exception handler ──────────────────────────────────────────
    @app.exception_handler(Exception)
    async def global_exception_handler(request, exc):
        logger = get_logger("exception_handler")
        logger.error(
            "Unhandled exception",
            path=str(request.url),
            method=request.method,
            error=str(exc),
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error", "type": type(exc).__name__},
        )

    return app


app = create_app()
