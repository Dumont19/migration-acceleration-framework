"""
core/database.py
----------------
Async SQLAlchemy engine and session factory for PostgreSQL.
All ORM models and Alembic migrations target this database.

Used for:
  - Persistent job execution logs
  - Migration run history
  - Audit trail (who ran what, when, result)
  - WebSocket job state persistence (survive browser refresh)
"""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.core.config import get_settings


class Base(DeclarativeBase):
    """Base class for all ORM models."""
    pass


def _create_engine():
    settings = get_settings()
    return create_async_engine(
        settings.db.url,
        pool_size=settings.db.pool_size,
        max_overflow=settings.db.max_overflow,
        echo=settings.db.echo_sql,
        pool_pre_ping=True,          # Drop stale connections automatically
        pool_recycle=3600,           # Recycle connections every hour
    )


# Module-level singletons — created once at startup via lifespan
_engine = None
_session_factory = None


def init_db_engine() -> None:
    """Called once during FastAPI lifespan startup."""
    global _engine, _session_factory
    _engine = _create_engine()
    _session_factory = async_sessionmaker(
        _engine,
        class_=AsyncSession,
        expire_on_commit=False,      # Avoid lazy-load errors after commit
    )


async def close_db_engine() -> None:
    """Called once during FastAPI lifespan shutdown."""
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None


def get_engine():
    if _engine is None:
        raise RuntimeError("DB engine not initialized. Call init_db_engine() first.")
    return _engine


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency for injecting an async DB session.

    Usage:
        @router.get("/jobs")
        async def list_jobs(db: AsyncSession = Depends(get_db_session)):
            ...
    """
    if _session_factory is None:
        raise RuntimeError("DB engine not initialized. Call init_db_engine() first.")
    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
