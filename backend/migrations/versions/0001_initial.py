"""Initial schema — migration_jobs, job_logs, job_partitions, validation_runs

Revision ID: 0001_initial
Revises:
Create Date: 2026-06-12

Cross-database: SQLite (dev) e PostgreSQL (prod).
Usa sa.Uuid, sa.JSON e sa.String para enums — sem tipos específicos de dialeto.
"""
from alembic import op
import sqlalchemy as sa

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None

_NOW = sa.text("(CURRENT_TIMESTAMP)")


def upgrade() -> None:
    # ── migration_jobs ────────────────────────────────────────────────────────
    op.create_table(
        "migration_jobs",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("table_name", sa.String(128), nullable=False),
        sa.Column("operation", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("total_partitions", sa.Integer, nullable=True),
        sa.Column("done_partitions", sa.Integer, nullable=False, server_default="0"),
        sa.Column("failed_partitions", sa.Integer, nullable=False, server_default="0"),
        sa.Column("total_rows", sa.BigInteger, nullable=True),
        sa.Column("loaded_rows", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Float, nullable=True),
        sa.Column("config", sa.JSON, nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("error_traceback", sa.Text, nullable=True),
        sa.Column("triggered_by", sa.String(64), nullable=False, server_default="api"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
    )
    op.create_index("ix_migration_jobs_table_name", "migration_jobs", ["table_name"])
    op.create_index("ix_migration_jobs_status", "migration_jobs", ["status"])
    op.create_index("ix_migration_jobs_table_status", "migration_jobs", ["table_name", "status"])
    op.create_index("ix_migration_jobs_created_at", "migration_jobs", ["created_at"])

    # ── job_logs ──────────────────────────────────────────────────────────────
    op.create_table(
        "job_logs",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "job_id",
            sa.Uuid(as_uuid=True),
            sa.ForeignKey("migration_jobs.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("table_name", sa.String(128), nullable=True),
        sa.Column("operation", sa.String(64), nullable=True),
        sa.Column("level", sa.String(16), nullable=False, server_default="INFO"),
        sa.Column("message", sa.Text, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
    )
    op.create_index("ix_job_logs_job_id", "job_logs", ["job_id"])
    op.create_index("ix_job_logs_level", "job_logs", ["level"])
    op.create_index("ix_job_logs_created_at", "job_logs", ["created_at"])
    op.create_index("ix_job_logs_job_level", "job_logs", ["job_id", "level"])

    # ── job_partitions ────────────────────────────────────────────────────────
    op.create_table(
        "job_partitions",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "job_id",
            sa.Uuid(as_uuid=True),
            sa.ForeignKey("migration_jobs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("partition_key", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("rows_loaded", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_job_partitions_job_id", "job_partitions", ["job_id"])
    op.create_index("ix_job_partitions_job_status", "job_partitions", ["job_id", "status"])

    # ── validation_runs ───────────────────────────────────────────────────────
    op.create_table(
        "validation_runs",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("table_name", sa.String(128), nullable=False),
        sa.Column("triggered_by_job_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("oracle_count", sa.BigInteger, nullable=True),
        sa.Column("snowflake_count", sa.BigInteger, nullable=True),
        sa.Column("count_diff", sa.BigInteger, nullable=True),
        sa.Column("count_match", sa.Boolean, nullable=True),
        sa.Column("schema_match", sa.Boolean, nullable=True),
        sa.Column("schema_diff", sa.JSON, nullable=True),
        sa.Column("sample_size", sa.Integer, nullable=True),
        sa.Column("sample_match_rate", sa.Float, nullable=True),
        sa.Column("sample_diff", sa.JSON, nullable=True),
        sa.Column("passed", sa.Boolean, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
    )
    op.create_index("ix_validation_runs_table_name", "validation_runs", ["table_name"])
    op.create_index("ix_validation_runs_table_created", "validation_runs",
                    ["table_name", "created_at"])


def downgrade() -> None:
    op.drop_table("validation_runs")
    op.drop_table("job_partitions")
    op.drop_table("job_logs")
    op.drop_table("migration_jobs")
