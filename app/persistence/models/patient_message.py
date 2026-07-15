from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy.sql.sqltypes import TIMESTAMP
from sqlalchemy.sql.expression import text
from sqlalchemy import (
    CheckConstraint,
    Enum as SqlEnum,
    Text,
    func,
    Uuid,
    ForeignKey,
    Index,
)
import uuid
from typing import Optional
from sqlalchemy.dialects.postgresql import JSONB
import enum
from datetime import datetime
from app.persistence.base import Base


class PatientMessageValidationStatus(enum.Enum):
    """Machine gate: service-layer acceptance of the LLM draft."""

    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


patient_message_validation_status_enum = SqlEnum(
    PatientMessageValidationStatus,
    name="patient_message_validation_status_enum",
    create_type=True,
)


class PatientMessageReviewStatus(enum.Enum):
    """Human gate: clinician clinical sign-off (kept separate from the
    machine gate on purpose)."""

    DRAFT = "DRAFT"
    PENDING_REVIEW = "PENDING_REVIEW"
    APPROVED = "APPROVED"
    CHANGES_REQUESTED = "CHANGES_REQUESTED"
    REJECTED = "REJECTED"
    SENT = "SENT"
    SUPERSEDED = "SUPERSEDED"


patient_message_review_status_enum = SqlEnum(
    PatientMessageReviewStatus,
    name="patient_message_review_status_enum",
    create_type=True,
)


class PatientMessage(Base):
    """
    One plain-language message covering a WHOLE ingestion (a patient's result
    set for that run). References the ingestion, not individual reports.

    Two separate status columns on purpose:
      - `validation_status` is the machine gate (service accepts the schema-valid
        LLM output).
      - `review_status` is the human clinical sign-off.
    A draft cannot leave DRAFT for human review (PENDING_REVIEW) unless
    `validation_status = ACCEPTED` — the machine gate precedes the human gate.
    This state-transition invariant is enforced in the service layer.

    PHI (name/email) is applied only at render/send time; it is never baked into
    `draft_content_json`.
    """

    __tablename__ = "patient_message"
    __table_args__ = (
        # At most one ACTIVE message per ingestion (superseded/rejected excluded).
        Index(
            "ux_patient_message_active_ingestion",
            "ingestion_id",
            unique=True,
            postgresql_where=text(
                "review_status NOT IN ('SUPERSEDED', 'REJECTED')"
            ),
        ),
        Index("ix_patient_message_ingestion_id", "ingestion_id"),
        Index("ix_patient_message_review_status", "review_status"),
        # final_content_json only allowed once APPROVED/SENT.
        CheckConstraint(
            "final_content_json IS NULL "
            "OR review_status IN ('APPROVED', 'SENT')",
            name="ck_patient_message_final_requires_approval",
        ),
        # No self-supersede.
        CheckConstraint(
            "superseded_by IS NULL OR superseded_by <> patient_message_id",
            name="ck_patient_message_no_self_supersede",
        ),
    )

    patient_message_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    patient_id: Mapped[str] = mapped_column(
        Text, ForeignKey("patient.patient_id"), nullable=False
    )
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey("ingestion.ingestion_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Content
    draft_content_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    final_content_json: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True
    )
    content_schema_version: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )

    # Generation provenance
    correlation_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        Uuid, nullable=True
    )
    generation_event_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        Uuid, ForeignKey("processing_event.event_id"), nullable=True
    )
    provider: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    model_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    prompt_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    temperature: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    input_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    retrieved_refs_json: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    # Machine gate
    validation_status: Mapped[PatientMessageValidationStatus] = mapped_column(
        patient_message_validation_status_enum,
        nullable=False,
        server_default=text("'PENDING'::patient_message_validation_status_enum"),
    )
    validated_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    validation_error: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )

    # Human gate
    review_status: Mapped[PatientMessageReviewStatus] = mapped_column(
        patient_message_review_status_enum,
        nullable=False,
        server_default=text("'DRAFT'::patient_message_review_status_enum"),
    )
    # Clinician ids — future FK to a users table.
    reviewed_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    approved_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    approved_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    sent_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    review_note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    superseded_by: Mapped[Optional[uuid.UUID]] = mapped_column(
        Uuid,
        ForeignKey("patient_message.patient_message_id"),
        nullable=True,
    )
