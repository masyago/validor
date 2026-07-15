"""add patient_message table

Revision ID: c1a2b3c4d5e5
Revises: c1a2b3c4d5e4
Create Date: 2026-07-08 09:04:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "c1a2b3c4d5e5"
down_revision: Union[str, Sequence[str], None] = "c1a2b3c4d5e4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "patient_message",
        sa.Column("patient_message_id", sa.Uuid(), nullable=False),
        sa.Column("patient_id", sa.Text(), nullable=False),
        sa.Column("ingestion_id", sa.Uuid(), nullable=False),
        sa.Column(
            "draft_content_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "final_content_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("content_schema_version", sa.Text(), nullable=True),
        # Generation provenance
        sa.Column("correlation_id", sa.Uuid(), nullable=True),
        sa.Column("generation_event_id", sa.Uuid(), nullable=True),
        sa.Column("provider", sa.Text(), nullable=True),
        sa.Column("model_id", sa.Text(), nullable=True),
        sa.Column("prompt_version", sa.Text(), nullable=True),
        sa.Column("temperature", sa.Text(), nullable=True),
        sa.Column("input_hash", sa.Text(), nullable=True),
        sa.Column(
            "retrieved_refs_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        # Machine gate
        sa.Column(
            "validation_status",
            sa.Enum(
                "PENDING",
                "ACCEPTED",
                "REJECTED",
                name="patient_message_validation_status_enum",
            ),
            server_default=sa.text(
                "'PENDING'::patient_message_validation_status_enum"
            ),
            nullable=False,
        ),
        sa.Column("validated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("validation_error", sa.Text(), nullable=True),
        # Human gate
        sa.Column(
            "review_status",
            sa.Enum(
                "DRAFT",
                "PENDING_REVIEW",
                "APPROVED",
                "CHANGES_REQUESTED",
                "REJECTED",
                "SENT",
                "SUPERSEDED",
                name="patient_message_review_status_enum",
            ),
            server_default=sa.text(
                "'DRAFT'::patient_message_review_status_enum"
            ),
            nullable=False,
        ),
        sa.Column("reviewed_by", sa.Text(), nullable=True),
        sa.Column("approved_by", sa.Text(), nullable=True),
        sa.Column("reviewed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("approved_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("sent_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("review_note", sa.Text(), nullable=True),
        sa.Column("superseded_by", sa.Uuid(), nullable=True),
        sa.CheckConstraint(
            "final_content_json IS NULL "
            "OR review_status IN ('APPROVED', 'SENT')",
            name="ck_patient_message_final_requires_approval",
        ),
        sa.CheckConstraint(
            "superseded_by IS NULL OR superseded_by <> patient_message_id",
            name="ck_patient_message_no_self_supersede",
        ),
        sa.ForeignKeyConstraint(["patient_id"], ["patient.patient_id"]),
        sa.ForeignKeyConstraint(
            ["ingestion_id"], ["ingestion.ingestion_id"]
        ),
        sa.ForeignKeyConstraint(
            ["generation_event_id"], ["processing_event.event_id"]
        ),
        sa.ForeignKeyConstraint(
            ["superseded_by"], ["patient_message.patient_message_id"]
        ),
        sa.PrimaryKeyConstraint("patient_message_id"),
    )
    op.create_index(
        "ix_patient_message_ingestion_id",
        "patient_message",
        ["ingestion_id"],
        unique=False,
    )
    op.create_index(
        "ix_patient_message_review_status",
        "patient_message",
        ["review_status"],
        unique=False,
    )
    op.create_index(
        "ux_patient_message_active_ingestion",
        "patient_message",
        ["ingestion_id"],
        unique=True,
        postgresql_where=sa.text(
            "review_status NOT IN ('SUPERSEDED', 'REJECTED')"
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ux_patient_message_active_ingestion", table_name="patient_message"
    )
    op.drop_index(
        "ix_patient_message_review_status", table_name="patient_message"
    )
    op.drop_index(
        "ix_patient_message_ingestion_id", table_name="patient_message"
    )
    op.drop_table("patient_message")
    op.execute("DROP TYPE IF EXISTS patient_message_review_status_enum")
    op.execute("DROP TYPE IF EXISTS patient_message_validation_status_enum")
