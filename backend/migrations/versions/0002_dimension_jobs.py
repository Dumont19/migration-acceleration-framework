"""Add dimension_jobs table

Revision ID: 0002
Revises: 0001_initial
Create Date: 2026-06-12
"""
from alembic import op
import sqlalchemy as sa

revision = "0002_dimension_jobs"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dimension_jobs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("job_name", sa.String(256), nullable=False),
        sa.Column("target_table", sa.String(256), nullable=False),
        sa.Column("schema", sa.String(128), nullable=False, server_default="DWDEV.MATHEUSDR"),
        sa.Column("fl_mn", sa.String(1), nullable=False, server_default="1"),
        sa.Column("nom_sis_ori", sa.String(128), nullable=False, server_default="ALGAR SOM"),
        sa.Column("spec_json", sa.JSON(), nullable=True),
        sa.Column("generated_sqls", sa.JSON(), nullable=True),
        sa.Column("generated_by", sa.String(128), nullable=True, server_default="api"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_dimension_jobs_job_name", "dimension_jobs", ["job_name"])
    op.create_index("ix_dimension_jobs_target_table", "dimension_jobs", ["target_table"])
    op.create_index(
        "ix_dimension_jobs_table_created",
        "dimension_jobs",
        ["target_table", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_dimension_jobs_table_created", table_name="dimension_jobs")
    op.drop_index("ix_dimension_jobs_target_table", table_name="dimension_jobs")
    op.drop_index("ix_dimension_jobs_job_name", table_name="dimension_jobs")
    op.drop_table("dimension_jobs")
