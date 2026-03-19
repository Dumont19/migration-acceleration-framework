"""
models/logs.py
--------------
SQLAlchemy ORM models for persistent log storage.

Tables:
  - migration_jobs   : one row per migration execution (lifecycle tracking)
  - job_logs         : append-only log entries per job (audit trail)
  - job_partitions   : partition-level granularity for partitioned migrations
  - validation_runs  : Oracle vs Snowflake comparison results
"""

import uuid
from datetime import datetime
from enum import Enum as PyEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


# ── Enums ────────────────────────────────────────────────────────────────────

class JobStatus(str, PyEnum):
    PENDING   = "pending"
    RUNNING   = "running"
    DONE      = "done"
    ERROR     = "error"
    CANCELLED = "cancelled"


class OperationType(str, PyEnum):
    MIGRATION_PARTITIONED = "migration_partitioned"
    MIGRATION_FAST        = "migration_fast"
    MIGRATION_DBLINK      = "migration_dblink"
    MIGRATION_SIMPLE      = "migration_simple"
    VALIDATION            = "validation"
    GAP_ANALYSIS          = "gap_analysis"
    DATASTAGE_DOC         = "datastage_doc"
    LINEAGE_BUILD         = "lineage_build"
    METADATA_EXTRACT      = "metadata_extract"
    TABLE_CREATE          = "table_create"
    MERGE_RUN             = "merge_run"
    COPY_S3               = "copy_s3"


class LogLevel(str, PyEnum):
    DEBUG    = "DEBUG"
    INFO     = "INFO"
    WARNING  = "WARNING"
    ERROR    = "ERROR"
    CRITICAL = "CRITICAL"


# ── Models ───────────────────────────────────────────────────────────────────

class MigrationJob(Base):
    """
    One row per migration run.
    Tracks lifecycle: created → running → done | error.
    """
    __tablename__ = "migration_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    table_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    operation: Mapped[OperationType] = mapped_column(
        Enum(OperationType), nullable=False
    )
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus), nullable=False, default=JobStatus.PENDING, index=True
    )

    # Progress counters
    total_partitions: Mapped[int | None] = mapped_column(Integer)
    done_partitions: Mapped[int] = mapped_column(Integer, default=0)
    failed_partitions: Mapped[int] = mapped_column(Integer, default=0)
    total_rows: Mapped[int | None] = mapped_column(BigInteger)
    loaded_rows: Mapped[int] = mapped_column(BigInteger, default=0)

    # Timing
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[float | None] = mapped_column(Float)

    # Config snapshot (what parameters were used)
    config: Mapped[dict | None] = mapped_column(JSONB)

    # Error info
    error_message: Mapped[str | None] = mapped_column(Text)
    error_traceback: Mapped[str | None] = mapped_column(Text)

    # Metadata
    triggered_by: Mapped[str] = mapped_column(String(64), default="api")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Relationships
    logs: Mapped[list["JobLog"]] = relationship(
        "JobLog", back_populates="job", cascade="all, delete-orphan", lazy="dynamic"
    )
    partitions: Mapped[list["JobPartition"]] = relationship(
        "JobPartition", back_populates="job", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_migration_jobs_table_status", "table_name", "status"),
        Index("ix_migration_jobs_created_at", "created_at"),
    )


class JobLog(Base):
    """
    Append-only log entries for a migration job.
    Written by the DatabaseLogSink in core/logging.py.
    Never update or delete rows — audit trail must be immutable.
    """
    __tablename__ = "job_logs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("migration_jobs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    table_name: Mapped[str | None] = mapped_column(String(128))
    operation: Mapped[str | None] = mapped_column(String(64))
    level: Mapped[LogLevel] = mapped_column(
        Enum(LogLevel), nullable=False, default=LogLevel.INFO, index=True
    )
    message: Mapped[str] = mapped_column(Text, nullable=False)
    extra: Mapped[dict[str, Any] | None] = mapped_column(JSONB)  # Arbitrary structured context
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    # Relationship back to job (optional — log can exist without a job)
    job: Mapped["MigrationJob | None"] = relationship("MigrationJob", back_populates="logs")

    __table_args__ = (
        Index("ix_job_logs_job_level", "job_id", "level"),
        Index("ix_job_logs_created_at", "created_at"),
        # Partial index for errors only — fast error queries
        Index(
            "ix_job_logs_errors",
            "job_id",
            "created_at",
            postgresql_where=text("level IN ('ERROR', 'CRITICAL')"),
        ),
    )


class JobPartition(Base):
    """
    Granular tracking of each partition in a partitioned migration.
    Allows retry of individual failed partitions.
    """
    __tablename__ = "job_partitions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("migration_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    partition_key: Mapped[str] = mapped_column(String(64), nullable=False)  # e.g. "2024-03-01"
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus), nullable=False, default=JobStatus.PENDING
    )
    rows_loaded: Mapped[int] = mapped_column(BigInteger, default=0)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    job: Mapped["MigrationJob"] = relationship("MigrationJob", back_populates="partitions")

    __table_args__ = (
        Index("ix_job_partitions_job_status", "job_id", "status"),
    )


class ValidationRun(Base):
    """
    Results of an Oracle vs Snowflake validation comparison.
    Stored separately from job logs for structured querying.
    """
    __tablename__ = "validation_runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    table_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    triggered_by_job_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))

    # Count comparison
    oracle_count: Mapped[int | None] = mapped_column(BigInteger)
    snowflake_count: Mapped[int | None] = mapped_column(BigInteger)
    count_diff: Mapped[int | None] = mapped_column(BigInteger)
    count_match: Mapped[bool | None] = mapped_column(Boolean)

    # Schema comparison
    schema_match: Mapped[bool | None] = mapped_column(Boolean)
    schema_diff: Mapped[dict | None] = mapped_column(JSONB)

    # Sample comparison (N rows checked)
    sample_size: Mapped[int | None] = mapped_column(Integer)
    sample_match_rate: Mapped[float | None] = mapped_column(Float)  # 0.0 - 1.0
    sample_diff: Mapped[dict | None] = mapped_column(JSONB)

    # Overall result
    passed: Mapped[bool | None] = mapped_column(Boolean)
    notes: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    __table_args__ = (
        Index("ix_validation_runs_table_created", "table_name", "created_at"),
    )
