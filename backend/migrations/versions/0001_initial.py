"""
Alembic initial migration — creates all MAF tables.
Restored version with idempotency fixes for Postgres Enums.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Enums (Criação Segura) ─────────────────────────────────────────────
    # Usamos blocos anônimos PL/pgSQL para evitar o erro DuplicateObject
    op.execute("""
        DO $$ 
        BEGIN 
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'jobstatus') THEN 
                CREATE TYPE jobstatus AS ENUM ('pending', 'running', 'done', 'error', 'cancelled');
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'operationtype') THEN 
                CREATE TYPE operationtype AS ENUM (
                    'migration_partitioned', 'migration_fast', 'migration_dblink',
                    'migration_simple', 'validation', 'gap_analysis', 'datastage_doc',
                    'lineage_build', 'metadata_extract', 'table_create', 'merge_run', 'copy_s3'
                );
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'loglevel') THEN 
                CREATE TYPE loglevel AS ENUM ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL');
            END IF;
        END $$;
    """)

    # ── migration_jobs ─────────────────────────────────────────────────
    op.create_table(
        "migration_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("table_name", sa.String(128), nullable=False),
        sa.Column("operation", postgresql.ENUM(name="operationtype", create_type=False), nullable=False),
        sa.Column("status", postgresql.ENUM(name="jobstatus", create_type=False), 
                  nullable=False, server_default="pending"),
        sa.Column("total_partitions", sa.Integer),
        sa.Column("done_partitions", sa.Integer, nullable=False, server_default="0"),
        sa.Column("failed_partitions", sa.Integer, nullable=False, server_default="0"),
        sa.Column("total_rows", sa.BigInteger),
        sa.Column("loaded_rows", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("duration_seconds", sa.Float),
        sa.Column("config", postgresql.JSONB),
        sa.Column("error_message", sa.Text),
        sa.Column("error_traceback", sa.Text),
        sa.Column("triggered_by", sa.String(64), nullable=False, server_default="api"),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("NOW()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("NOW()"), nullable=False),
    )
    op.create_index("ix_migration_jobs_table_status", "migration_jobs", ["table_name", "status"])
    op.create_index("ix_migration_jobs_created_at", "migration_jobs", ["created_at"])

    # ── job_logs ───────────────────────────────────────────────────────
    op.create_table(
        "job_logs",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("migration_jobs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("table_name", sa.String(128)),
        sa.Column("operation", sa.String(64)),
        sa.Column("level", postgresql.ENUM(name="loglevel", create_type=False), 
                  nullable=False, server_default="INFO"),
        sa.Column("message", sa.Text, nullable=False),
        sa.Column("extra", postgresql.JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("NOW()"), nullable=False),
    )
    op.create_index("ix_job_logs_job_id", "job_logs", ["job_id"])
    op.create_index("ix_job_logs_level", "job_logs", ["level"])
    op.create_index("ix_job_logs_created_at", "job_logs", ["created_at"])
    op.create_index("ix_job_logs_job_level", "job_logs", ["job_id", "level"])

    # ── job_partitions ─────────────────────────────────────────────────
    op.create_table(
        "job_partitions",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("migration_jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("partition_key", sa.String(64), nullable=False),
        sa.Column("status", postgresql.ENUM(name="jobstatus", create_type=False), 
                  nullable=False, server_default="pending"),
        sa.Column("rows_loaded", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_job_partitions_job_status", "job_partitions", ["job_id", "status"])

    # ── validation_runs ────────────────────────────────────────────────
    op.create_table(
        "validation_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("table_name", sa.String(128), nullable=False),
        sa.Column("triggered_by_job_id", postgresql.UUID(as_uuid=True)),
        sa.Column("oracle_count", sa.BigInteger),
        sa.Column("snowflake_count", sa.BigInteger),
        sa.Column("count_diff", sa.BigInteger),
        sa.Column("count_match", sa.Boolean),
        sa.Column("schema_match", sa.Boolean),
        sa.Column("schema_diff", postgresql.JSONB),
        sa.Column("sample_size", sa.Integer),
        sa.Column("sample_match_rate", sa.Float),
        sa.Column("sample_diff", postgresql.JSONB),
        sa.Column("passed", sa.Boolean),
        sa.Column("notes", sa.Text),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("NOW()"), nullable=False),
    )
    op.create_index("ix_validation_runs_table_created", "validation_runs",
                    ["table_name", "created_at"])


def downgrade() -> None:
    op.drop_table("validation_runs")
    op.drop_table("job_partitions")
    op.drop_table("job_logs")
    op.drop_table("migration_jobs")
    op.execute("DROP TYPE IF EXISTS jobstatus")
    op.execute("DROP TYPE IF EXISTS operationtype")
    op.execute("DROP TYPE IF EXISTS loglevel")