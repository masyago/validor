from flask_sqlalchemy import SQLAlchemy
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

db = SQLAlchemy()


class IngestionIdempotencyDisposition(enum.Enum):
    CREATED = "CREATED"
    DUPLICATE_IDENTICAL = "DUPLICATE_IDENTICAL"
    CONFLICT = "CONFLICT"


ingestion_idempotency_enum = SqlEnum(
    IngestionIdempotencyDisposition,
    name="ingestion_idempotency_disposition_enum",
    create_type=True,
)  # TODO: Set to False after first migration


class ResultComparator(enum.Enum):
    LESS = "<"
    GREATER = ">"
    EQUAL = "="


result_comparator_enum = SqlEnum(
    ResultComparator,
    name="result_comparator_enum",
    create_type=True,  # TODO: Set to False after first migration
)


class DiagnosticReportStatus(enum.Enum):
    FINAL = "final"


diagnostic_report_status_enum = SqlEnum(
    DiagnosticReportStatus,
    name="diagnostic_report_status_enum",
    create_type=True,  # Set to False after first migration
)


# Declarative base class
class Base(DeclarativeBase):
    pass


class RawData(Base):
    __tablename__ = "raw_data"

    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid.uuid4,  # Generate UUID in Python
        foreign_key=ForeignKey("ingestion.ingestion_id"),
    )  # FK to ingestion table
    content_bytes: Mapped[bytes] = mapped_column(
        LargeBinary, nullable=False
    )  # BYTEA
    content_mime: Mapped[str] = mapped_column(
        Text, default="text/csv", nullable=False
    )
    content_size_bytes: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True
    )


class Ingestion(Base):
    __tablename__ = "ingestion"
    __table_args__ = (
        UniqueConstraint(
            "instrument_id", "run_id", name="unique_instrument_run"
        ),
    )

    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    instrument_id: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[str] = mapped_column(Text, nullable=False)
    uploader_id: Mapped[str] = mapped_column(Text, nullable=False)
    spec_version: Mapped[str] = mapped_column(Text, nullable=False)
    uploader_received_at: Mapped = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    api_received_at: Mapped = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    submitted_sha256: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
    server_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    error_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_detail: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    source_filename: Mapped[str] = mapped_column(Text, nullable=False)
    ingestion_idempotency_disposition: Mapped[
        Optional[IngestionIdempotencyDisposition]
    ] = mapped_column(ingestion_idempotency_enum, nullable=True)


class Panel(Base):
    __tablename__ = "panel"
    __table_args__ = (
        UniqueConstraint(
            "ingestion_id",
            "panel_code",
            "sample_id",
            name="unique_ingestion_panel_sample",
        ),
    )

    panel_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("ingestion.ingestion_id"), nullable=False
    )
    patient_id: Mapped[str] = mapped_column(Text, nullable=False)
    panel_code: Mapped[str] = mapped_column(Text, nullable=False)
    sample_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    collection_timestamp: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )


class Test(Base):
    __tablename__ = "test"
    __table_args__ = (
        UniqueConstraint(
            "panel_id",
            "test_code",
            "analyte_type",
            name="unique_panel_test_analyte",
        ),
    )

    test_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    panel_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("panel.panel_id"), nullable=False
    )
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    test_code: Mapped[str] = mapped_column(Text, nullable=False)
    test_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    analyte_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    result_raw: Mapped[str] = mapped_column(Text, nullable=False)
    units_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    result_value_num: Mapped[Optional[float]] = mapped_column(
        Numeric, nullable=True
    )
    result_comparator: Mapped[Optional[ResultComparator]] = mapped_column(
        result_comparator_enum, nullable=True
    )

    ref_low_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ref_high_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    flag: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


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
