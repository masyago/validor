from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy.sql.sqltypes import TIMESTAMP
from sqlalchemy.sql.expression import text
from sqlalchemy import (
    CheckConstraint,
    Text,
    Uuid,
    ForeignKey,
    Numeric,
    UniqueConstraint,
)
import uuid
from typing import Optional
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from app.persistence.base import Base


class DiagnosticReport(Base):
    __tablename__ = "diagnostic_report"
    __table_args__ = (
        CheckConstraint(
            "status IN ('FINAL')", name="check_diagnostic_report_status"
        ),
    )

    diagnostic_report_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("ingestion.ingestion_id"), nullable=False
    )
    panel_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("panel.panel_id"), nullable=False, unique=True
    )
    patient_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    panel_code: Mapped[str] = mapped_column(Text, nullable=False)

    # Same as `collection_timestamp` from `Panel` model
    effective_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    normalized_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    resource_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    status: Mapped[str] = mapped_column(Text, default="FINAL", nullable=False)


class Observation(Base):
    __tablename__ = "observation"
    __table_args__ = (
        UniqueConstraint(
            "diagnostic_report_id",
            "code",
            name="unique_diagnostic_report_id_code",
        ),
        CheckConstraint(
            "status IN ('FINAL')", name="check_observation_status"
        ),
        CheckConstraint(
            "comparator IS NULL OR comparator IN ('<', '<=', '>', '>=', '=')",
            name="check_observation_comparator",
        ),
    )
    # Identity/provenance
    observation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    test_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("test.test_id"), nullable=False, unique=True
    )
    diagnostic_report_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey("diagnostic_report.diagnostic_report_id"),
        nullable=False,
        index=True,
    )
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("ingestion.ingestion_id"), nullable=False
    )
    patient_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)

    # Clinical content
    code: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    display: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Same as `collection_timestamp` from `Panel` model
    effective_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    normalized_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    value_num: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    value_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    comparator: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    unit: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ref_low_num: Mapped[Optional[float]] = mapped_column(
        Numeric, nullable=True
    )
    ref_high_num: Mapped[Optional[float]] = mapped_column(
        Numeric, nullable=True
    )
    flag_analyzer_interpretation: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
    flag_system_interpretation: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
    discrepancy: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, default="FINAL", nullable=False)
    resource_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
