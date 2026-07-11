from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy.sql.sqltypes import TIMESTAMP
from sqlalchemy.sql.expression import text
from sqlalchemy import (
    Enum as SqlEnum,
    Text,
    func,
    Uuid,
    ForeignKey,
    Boolean,
)
import uuid
from typing import Optional
import enum
from datetime import datetime
from app.persistence.base import Base


class Patient(Base):
    """
    Minimal demo patient — the FHIR "subject" anchor for a result set and the
    source of the rendered "To:" line on a patient message.

    Deliberately excludes age/gender/birth_date/address/phone/consent: none are
    needed for the demo, and keeping them out makes it explicit that no real PHI
    is stored (see `is_synthetic`). `patient_id` is the natural key already
    carried on panel/diagnostic_report/observation, which now FK back to here.
    """

    __tablename__ = "patient"

    patient_id: Mapped[str] = mapped_column(Text, primary_key=True)
    given_name: Mapped[str] = mapped_column(Text, nullable=False)
    family_name: Mapped[str] = mapped_column(Text, nullable=False)
    # Synthetic; only used to render the demo message header.
    email: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # Explicit marker that this row holds no real PHI.
    is_synthetic: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class AiGenerationJobType(enum.Enum):
    ENRICHMENT = "ENRICHMENT"
    PATIENT_MESSAGE = "PATIENT_MESSAGE"


ai_generation_job_type_enum = SqlEnum(
    AiGenerationJobType,
    name="ai_generation_job_type_enum",
    create_type=True,
)


class AiGenerationJob(Base):
    """
    Trusted-side correlation map for the de-identification boundary.

    The service layer mints a random, job-scoped `correlation_id` (NOT derived
    from `patient_id`) and stores the token -> patient/ingestion mapping HERE,
    on the trusted side only. The AI layer/external LLM only ever sees the
    `correlation_id` in the request envelope plus a de-identified clinical
    payload. On return the service LOOKS UP the token here (one-time use;
    `consumed_at`) to recover `patient_id` — it never "decodes" the token.

    Prod option: Redis with a TTL instead of this table.
    """

    __tablename__ = "ai_generation_job"

    correlation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    job_type: Mapped[AiGenerationJobType] = mapped_column(
        ai_generation_job_type_enum, nullable=False
    )
    patient_id: Mapped[str] = mapped_column(
        Text, ForeignKey("patient.patient_id"), nullable=False
    )
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey("ingestion.ingestion_id", ondelete="CASCADE"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    consumed_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
