"""add ai_generation_job correlation-map table

Revision ID: c1a2b3c4d5e3
Revises: c1a2b3c4d5e2
Create Date: 2026-07-08 09:02:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c1a2b3c4d5e3"
down_revision: Union[str, Sequence[str], None] = "c1a2b3c4d5e2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "ai_generation_job",
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.Column(
            "job_type",
            sa.Enum(
                "ENRICHMENT",
                "PATIENT_MESSAGE",
                name="ai_generation_job_type_enum",
            ),
            nullable=False,
        ),
        sa.Column("patient_id", sa.Text(), nullable=False),
        sa.Column("ingestion_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("consumed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["patient_id"], ["patient.patient_id"]),
        sa.ForeignKeyConstraint(
            ["ingestion_id"], ["ingestion.ingestion_id"]
        ),
        sa.PrimaryKeyConstraint("correlation_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("ai_generation_job")
    op.execute("DROP TYPE IF EXISTS ai_generation_job_type_enum")
