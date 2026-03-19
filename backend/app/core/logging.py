"""
core/logging.py
---------------
Structured logging setup using structlog + standard logging.

Two outputs:
  1. Console  — human-readable (dev) or JSON (production)
  2. Database — async sink writing to `job_logs` table for persistent audit trail

Usage:
    from app.core.logging import get_logger
    logger = get_logger(__name__)

    # In a service:
    await logger.ainfo("Partition loaded", table="F_CEL_NETWORK_EVENT", rows=120_000)

    # Bind context for a full request:
    log = logger.bind(job_id=job_id, table=table_name)
    await log.ainfo("Migration started")
"""

import logging
import sys
from typing import Any

import structlog
from structlog.types import EventDict, WrappedLogger


# ── Custom processors ────────────────────────────────────────────────────────

def add_app_context(
    logger: WrappedLogger, method_name: str, event_dict: EventDict
) -> EventDict:
    """Inject static app metadata into every log entry."""
    event_dict.setdefault("app", "maf")
    event_dict.setdefault("version", "4.0.0")
    return event_dict


def drop_color_message_key(
    logger: WrappedLogger, method_name: str, event_dict: EventDict
) -> EventDict:
    """Remove uvicorn's color_message key to keep JSON clean."""
    event_dict.pop("color_message", None)
    return event_dict


# ── DB Sink (async) ──────────────────────────────────────────────────────────

class DatabaseLogSink:
    """
    Async structlog processor that persists log entries to PostgreSQL.
    Only active for WARNING+ in production; INFO+ in development.

    Inserted fields: job_id, table_name, operation, level, message,
                     extra (JSONB), created_at (auto).
    """

    def __init__(self, min_level: str = "INFO") -> None:
        self._min_level = getattr(logging, min_level.upper(), logging.INFO)

    async def __call__(
        self,
        logger: WrappedLogger,
        method_name: str,
        event_dict: EventDict,
    ) -> EventDict:
        level_no = getattr(logging, method_name.upper(), logging.INFO)
        if level_no < self._min_level:
            return event_dict

        # Lazy import to avoid circular dependency
        from app.core.database import _session_factory
        from app.models.logs import JobLog

        if _session_factory is None:
            return event_dict  # DB not ready yet (startup phase)

        try:
            async with _session_factory() as session:
                entry = JobLog(
                    job_id=event_dict.get("job_id"),
                    table_name=event_dict.get("table"),
                    operation=event_dict.get("operation"),
                    level=method_name.upper(),
                    message=str(event_dict.get("event", "")),
                    extra={
                        k: v
                        for k, v in event_dict.items()
                        if k not in {"event", "job_id", "table", "operation",
                                     "level", "timestamp", "app", "version"}
                    },
                )
                session.add(entry)
                await session.commit()
        except Exception:
            # Never let logging crash the application
            pass

        return event_dict


# ── Setup ────────────────────────────────────────────────────────────────────

_db_sink = DatabaseLogSink(min_level="INFO")


def configure_logging(log_level: str = "INFO", environment: str = "development") -> None:
    """
    Call once at application startup (inside lifespan).
    """
    is_production = environment == "production"

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        add_app_context,
        drop_color_message_key,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if is_production:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level.upper())

    # Silence noisy third-party loggers
    for noisy in ("oracledb", "snowflake.connector", "boto3", "botocore", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Returns a structlog bound logger.
    Bind job context with .bind(job_id=..., table=..., operation=...)
    """
    return structlog.get_logger(name)
