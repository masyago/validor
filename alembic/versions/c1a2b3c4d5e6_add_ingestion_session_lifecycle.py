"""add ingestion session lifecycle columns and cascade FKs

Revision ID: c1a2b3c4d5e6
Revises: c1a2b3c4d5e5
Create Date: 2026-07-10 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c1a2b3c4d5e6"
down_revision: Union[str, Sequence[str], None] = "c1a2b3c4d5e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# (constraint_name, table, column, ref_table, ref_column)
CASCADE_FKS = [
    ("raw_data_ingestion_id_fkey", "raw_data", "ingestion_id", "ingestion", "ingestion_id"),
    ("panel_ingestion_id_fkey", "panel", "ingestion_id", "ingestion", "ingestion_id"),
    ("test_panel_id_fkey", "test", "panel_id", "panel", "panel_id"),
    ("diagnostic_report_ingestion_id_fkey", "diagnostic_report", "ingestion_id", "ingestion", "ingestion_id"),
    ("diagnostic_report_panel_id_fkey", "diagnostic_report", "panel_id", "panel", "panel_id"),
    ("observation_ingestion_id_fkey", "observation", "ingestion_id", "ingestion", "ingestion_id"),
    ("observation_diagnostic_report_id_fkey", "observation", "diagnostic_report_id", "diagnostic_report", "diagnostic_report_id"),
    ("observation_test_id_fkey", "observation", "test_id", "test", "test_id"),
    ("processing_event_ingestion_id_fkey", "processing_event", "ingestion_id", "ingestion", "ingestion_id"),
    ("ai_annotation_ingestion_id_fkey", "ai_annotation", "ingestion_id", "ingestion", "ingestion_id"),
    ("ai_generation_job_ingestion_id_fkey", "ai_generation_job", "ingestion_id", "ingestion", "ingestion_id"),
    ("patient_message_ingestion_id_fkey", "patient_message", "ingestion_id", "ingestion", "ingestion_id"),
]


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "ingestion",
        sa.Column("kind", sa.Text(), nullable=False, server_default="SESSION"),
    )
    op.add_column(
        "ingestion",
        sa.Column("session_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "ingestion",
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.alter_column("ingestion", "kind", server_default=None)

    op.create_check_constraint(
        "check_ingestion_kind",
        "ingestion",
        "kind IN ('SEED', 'SESSION')",
    )
    op.create_index(
        "ix_ingestion_session_id", "ingestion", ["session_id"]
    )

    for constraint_name, table, column, ref_table, ref_column in CASCADE_FKS:
        op.drop_constraint(constraint_name, table, type_="foreignkey")
        op.create_foreign_key(
            constraint_name,
            table,
            ref_table,
            [column],
            [ref_column],
            ondelete="CASCADE",
        )


def downgrade() -> None:
    """Downgrade schema."""
    for constraint_name, table, column, ref_table, ref_column in CASCADE_FKS:
        op.drop_constraint(constraint_name, table, type_="foreignkey")
        op.create_foreign_key(
            constraint_name,
            table,
            ref_table,
            [column],
            [ref_column],
        )

    op.drop_index("ix_ingestion_session_id", table_name="ingestion")
    op.drop_constraint("check_ingestion_kind", "ingestion", type_="check")
    op.drop_column("ingestion", "expires_at")
    op.drop_column("ingestion", "session_id")
    op.drop_column("ingestion", "kind")
