from sqlalchemy.orm import relationship, DeclarativeBase, mapped_column, Mapped
from sqlalchemy.sql.sqltypes import TIMESTAMP
from sqlalchemy.sql.expression import text
from sqlalchemy import (
    Column,
    Integer,
    String,
    LargeBinary,
    Integer,
    Text,
    BigInteger,
    func,
    Uuid,
    text,
    ForeignKey,
    Enum as SqlEnum,
    Numeric,
    UniqueConstraint,
    Index,
)
import uuid
from typing import Optional
from sqlalchemy.dialects.postgresql import JSONB, ENUM
import enum
from datetime import datetime
from app.persistence.base import Base


# Replace with CheckConstraint in DiagnosticReport
# class DiagnosticReportStatus(enum.Enum):
#     FINAL = "final"


# diagnostic_report_status_enum = SqlEnum(
#     DiagnosticReportStatus,
#     name="diagnostic_report_status_enum",
#     create_type=True,  # Set to False after first migration

# Replace with CheckConstraint in Observation
# class ResultComparator(enum.Enum):
#     LESS = "<"
#     LESS_OR_EQUAL = "<="
#     GREATER = ">"
#     GREATER_OR_EQUAL = ">="
#     EQUAL = "="


# result_comparator_enum = SqlEnum(
#     ResultComparator,
#     name="result_comparator_enum",
#     create_type=True,  # TODO: Set to False after first migration
# )


class DiagnosticReport(Base):
    __tablename__ = "diagnostic_report"

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
    effective_at: Mapped = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    issued_at: Mapped = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    resource_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    # Update with CheckConstraint
    status: Mapped[Optional[DiagnosticReportStatus]] = mapped_column(
        diagnostic_report_status_enum, nullable=True
    )


class Observation(Base):
    __tablename__ = "observation"
    __table_args__ = UniqueConstraint(
        "diagnostic_report_id", "code", name="unique_report_code"
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
    effective_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    value_num: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    value_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Replace with CHECK
    comparator: Mapped[Optional[ResultComparator]] = mapped_column(
        result_comparator_enum, nullable=True
    )
    unit: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ref_low_num: Mapped[Optional[float]] = mapped_column(
        Numeric, nullable=True
    )
    ref_high_num: Mapped[Optional[float]] = mapped_column(
        Numeric, nullable=True
    )
    flag: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    resource_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
