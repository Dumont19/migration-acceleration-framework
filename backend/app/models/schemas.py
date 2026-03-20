"""
models/schemas.py
-----------------
Pydantic v2 schemas for API request/response bodies.
Separate from ORM models — these are the public API contract.
"""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

from app.models.logs import JobStatus, LogLevel, OperationType


# ── Base ─────────────────────────────────────────────────────────────────────

class TimestampMixin(BaseModel):
    created_at: datetime
    updated_at: datetime


# ── Migration ────────────────────────────────────────────────────────────────

class MigrationRequest(BaseModel):
    """POST /api/migration/start"""
    table_name: str = Field(..., min_length=1, max_length=128, description="Oracle table name")
    operation: OperationType = Field(OperationType.MIGRATION_PARTITIONED)
    date_from: str | None = Field(None, description="Partition start date YYYY-MM-DD")
    date_to: str | None = Field(None, description="Partition end date YYYY-MM-DD")
    partition_column: str | None = Field("DT_REFERENCIA", description="Partition key column")
    batch_size_days: int = Field(1, ge=1, le=365)
    max_workers: int = Field(4, ge=1, le=16)
    use_dblink: bool = Field(False, description="Use DB Link instead of S3 staging")
    schema_source: str = Field("DWADM")
    schema_target: str = Field("DWADM")

    @field_validator("table_name")
    @classmethod
    def uppercase_table(cls, v: str) -> str:
        return v.upper().strip()


class PartitionStatus(BaseModel):
    partition_key: str
    status: JobStatus
    rows_loaded: int
    attempts: int
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


class JobProgressResponse(BaseModel):
    """GET /api/migration/jobs/{job_id}/progress — also sent over WebSocket"""
    job_id: uuid.UUID
    table_name: str
    operation: OperationType
    status: JobStatus
    total_partitions: int | None
    done_partitions: int
    failed_partitions: int
    percent: float = Field(0.0, ge=0.0, le=100.0)
    total_rows: int | None
    loaded_rows: int
    started_at: datetime | None
    estimated_completion: datetime | None = None
    current_partition: str | None = None
    last_log_message: str | None = None
    updated_at: datetime


class JobSummaryResponse(BaseModel):
    job_id: uuid.UUID
    table_name: str
    operation: OperationType
    status: JobStatus
    duration_seconds: float | None
    loaded_rows: int
    failed_partitions: int
    created_at: datetime


class JobListResponse(BaseModel):
    items: list[JobSummaryResponse]
    total: int
    page: int
    page_size: int


# ── WebSocket message ─────────────────────────────────────────────────────────

class WSProgressMessage(BaseModel):
    """Message sent over WebSocket /ws/progress/{job_id}"""
    type: str = "progress"  # progress | log | done | error
    job_id: str
    table_name: str
    operation: str
    status: JobStatus
    progress: dict[str, Any]
    log: str | None = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# ── Validation ───────────────────────────────────────────────────────────────

class ValidationRequest(BaseModel):
    """POST /api/validation/run"""
    table_name: str
    sample_size: int = Field(100, ge=10, le=10_000)
    check_schema: bool = True
    check_counts: bool = True
    check_sample: bool = True
    date_filter: str | None = Field(None, description="Optional WHERE clause filter")


class ValidationResultResponse(BaseModel):
    id: uuid.UUID
    table_name: str
    oracle_count: int | None
    snowflake_count: int | None
    count_diff: int | None
    count_match: bool | None
    schema_match: bool | None
    schema_diff: dict | None
    sample_size: int | None
    sample_match_rate: float | None
    passed: bool | None
    notes: str | None
    created_at: datetime


# ── Logs ─────────────────────────────────────────────────────────────────────

class LogEntryResponse(BaseModel):
    id: int
    job_id: uuid.UUID | None
    table_name: str | None
    operation: str | None
    level: LogLevel
    message: str
    extra: dict | None
    created_at: datetime


class LogQueryParams(BaseModel):
    """Query parameters for GET /api/logs"""
    job_id: uuid.UUID | None = None
    table_name: str | None = None
    level: LogLevel | None = None
    from_dt: datetime | None = None
    to_dt: datetime | None = None
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=10, le=500)


# ── Health ───────────────────────────────────────────────────────────────────

class ConnectionHealth(BaseModel):
    status: str  # ok | error
    latency_ms: float | None = None
    error: str | None = None
    extra: dict | None = None


class HealthResponse(BaseModel):
    oracle: ConnectionHealth
    snowflake: ConnectionHealth
    s3: ConnectionHealth
    database: ConnectionHealth
    app_version: str
    environment: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# ── Analysis ─────────────────────────────────────────────────────────────────

class GapAnalysisRequest(BaseModel):
    table_name: str
    date_from: str
    date_to: str
    date_column: str = "DT_REFERENCIA"
    granularity: str = Field("day", pattern="^(day|week|month)$")


class GapAnalysisResult(BaseModel):
    table_name: str
    date: str
    oracle_count: int
    snowflake_count: int
    diff: int
    diff_pct: float


class LineageNode(BaseModel):
    id: str
    label: str
    type: str  # source | job | target
    db_schema: str = Field(..., alias="schema")
    extra: dict | None = None


class LineageEdge(BaseModel):
    source: str
    target: str
    label: str | None = None


class LineageGraphResponse(BaseModel):
    nodes: list[LineageNode]
    edges: list[LineageEdge]
    job_name: str
    generated_at: datetime = Field(default_factory=datetime.utcnow)
