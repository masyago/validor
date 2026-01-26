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
from pgvector.sqlalchemy import Vector

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


class VectorSourceType(enum.Enum):
    DOCUMENT = "DOCUMENT"
    OBSERVATION = "OBSERVATION"
    DIAGNOSTIC_REPORT = "DIAGNOSTIC_REPORT"


vector_source_type_enum = SqlEnum(
    VectorSourceType,
    name="vector_source_type_enum",
    create_type=True,  # Set to False after first migration
)


class VectorStore(Base):
    __tablename__ = "vector_store"
    __table_args__ = (
        UniqueConstraint(
            "source_type",
            "source_id",
            "chunk_index",
            "embedding_model",
            "pipeline_version",
            name="unique_vector_source_chunk_model_pipeline",
        ),
    )

    embedding_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    embedding: Mapped[list[float]] = mapped_column(
        Vector(768), nullable=False
    )  # change vector dimension depending on the model
    source_type: Mapped[VectorSourceType] = mapped_column(
        vector_source_type_enum, nullable=False
    )
    source_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    chunk_index: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    chunk_text: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)

    embedding_model: Mapped[str] = mapped_column(Text, nullable=False)
    pipeline_version: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )


class DocumentTargetType(enum.Enum):
    PANEL = "PANEL"
    ANALYTE = "ANALYTE"


document_target_type_enum = SqlEnum(
    DocumentTargetType,
    name="document_target_type_enum",
    create_type=True,  # Set to False after first migration
)


class Document(Base):
    __tablename__ = "document"

    doc_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    target_type: Mapped[DocumentTargetType] = mapped_column(
        document_target_type_enum, nullable=False
    )
    target_code: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_format: Mapped[str] = mapped_column(
        Text, nullable=False, default="text/plain"
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    last_updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)


# AI Annotation model and supporting classes
class AIAnnotationTargetType(enum.Enum):
    DIAGNOSTIC_REPORT = "DIAGNOSTIC_REPORT"
    OBSERVATION = "OBSERVATION"


ai_annotation_target_type_enum = SqlEnum(
    AIAnnotationTargetType,
    name="ai_annotation_target_type_enum",
    create_type=True,
)


class AIAnnotationType(enum.Enum):
    ANOMALY_FLAG = "anomaly_flag"
    POSSIBLE_INTERFERENCE = "possible_interference"
    FOLLOWUP_SUGGESTION = "followup_suggestion"


ai_annotation_type_enum = SqlEnum(
    AIAnnotationType,
    name="ai_annotation_type_enum",
    create_type=True,
)


class AIAnnotationValidationStatus(enum.Enum):
    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


ai_annotation_validation_status_enum = SqlEnum(
    AIAnnotationValidationStatus,
    name="ai_annotation_validation_status_enum",
    create_type=True,
)


class AiAnnotation(Base):
    __tablename__ = "ai_annotation"
    __table_args__ = (
        Index("ix_ai_annotation_target_type_id", "target_type", "target_id"),
        Index("ix_ai_annotation_annotation_type", "annotation_type"),
        Index("ix_ai_annotation_validation_status", "validation_status"),
    )

    # Identifiers
    ai_annotation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("ingestion.ingestion_id"), nullable=False
    )
    target_type: Mapped[Optional[AIAnnotationTargetType]] = mapped_column(
        ai_annotation_target_type_enum, nullable=True
    )
    target_id: Mapped[Optional[uuid.UUID]] = mapped_column(Uuid, nullable=True)

    # Annotation
    annotation_type: Mapped[Optional[AIAnnotationType]] = mapped_column(
        ai_annotation_type_enum, nullable=True
    )
    content_json: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # Traceability
    provider: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    model_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    prompt_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    temperature: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content_schema_version: Mapped[str] = mapped_column(Text, nullable=False)
    input_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    # Status
    validation_status: Mapped[Optional[AIAnnotationValidationStatus]] = (
        mapped_column(ai_annotation_validation_status_enum, nullable=True)
    )
    validated_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    rejection_reason: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )


# Provenance: ProcessingEvent class and supporting classes
class ProcessingEventTargetType(enum.Enum):
    INGESTION = "ingestion"
    PANEL = "panel"
    TEST = "test"
    DIAGNOSTIC_REPORT = "diagnostic_report"
    OBSERVATION = "observation"
    AI_ANNOTATION = "ai_annotation"


processing_event_target_type_enum = SqlEnum(
    ProcessingEventTargetType,
    name="processing_event_target_type_enum",
    create_type=True,
)


class ProcessingEventActor(enum.Enum):
    INGESTION_API = "ingestion-api"
    PARSER = "parser"
    VALIDATOR = "validator"
    NORMALIZER = "normalizer"
    AI_WORKER = "ai-worker"


processing_event_actor_enum = SqlEnum(
    ProcessingEventActor,
    name="processing_event_actor_enum",
    create_type=True,
)


class ProcessingEventSeverity(enum.Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


processing_event_severity_enum = SqlEnum(
    ProcessingEventSeverity,
    name="processing_event_severity_enum",
    create_type=True,
)


class ProcessingEventType(enum.Enum):
    INGESTION_ACCEPTED = "INGESTION_ACCEPTED"
    INGESTION_DEDUPED_IDENTICAL = "INGESTION_DEDUPED_IDENTICAL"
    INGESTION_CONFLICT = "INGESTION_CONFLICT"
    PARSE_STARTED = "PARSE_STARTED"
    PARSE_SUCCEEDED = "PARSE_SUCCEEDED"
    PARSE_FAILED = "PARSE_FAILED"
    VALIDATION_STARTED = "VALIDATION_STARTED"
    VALIDATION_SUCCEEDED = "VALIDATION_SUCCEEDED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    NORMALIZATION_STARTED = "NORMALIZATION_STARTED"
    NORMALIZATION_SUCCEEDED = "NORMALIZATION_SUCCEEDED"
    NORMALIZATION_FAILED = "NORMALIZATION_FAILED"
    AI_ENRICHMENT_STARTED = "AI_ENRICHMENT_STARTED"
    AI_ENRICHMENT_SKIPPED = "AI_ENRICHMENT_SKIPPED"
    AI_ENRICHMENT_SUCCEEDED = "AI_ENRICHMENT_SUCCEEDED"
    AI_ENRICHMENT_FAILED = "AI_ENRICHMENT_FAILED"


processing_event_type_enum = SqlEnum(
    ProcessingEventType,
    name="processing_event_type_enum",
    create_type=True,
)


class ProcessingEvent(Base):
    __tablename__ = "processing_event"
    __table_args__ = (
        UniqueConstraint(
            "ingestion_id",
            "event_type",
            "dedupe_key",
            name="unique_ingestion_event_dedupe",
        ),
        Index(
            "ix_processing_event_ingestion_sequence",
            "ingestion_id",
            "sequence",
        ),
        Index(
            "ix_processing_event_ingestion_event_type",
            "ingestion_id",
            "event_type",
        ),
        Index(
            "ix_processing_event_target_type_id", "target_type", "target_id"
        ),
    )

    # Event identity
    event_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("ingestion.ingestion_id"), nullable=False
    )
    sequence: Mapped[int] = mapped_column(
        BigInteger, nullable=False, unique=True
    )
    dedupe_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # What the event is about
    target_type: Mapped[Optional[ProcessingEventTargetType]] = mapped_column(
        processing_event_target_type_enum, nullable=True
    )
    target_id: Mapped[Optional[uuid.UUID]] = mapped_column(Uuid, nullable=True)

    # When
    event_type: Mapped[ProcessingEventType] = mapped_column(
        processing_event_type_enum, nullable=False
    )
    occurred_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )

    # Who
    actor: Mapped[ProcessingEventActor] = mapped_column(
        processing_event_actor_enum, nullable=False
    )
    actor_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    artifact_versions: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True
    )

    # Explanation
    severity: Mapped[Optional[ProcessingEventSeverity]] = mapped_column(
        processing_event_severity_enum, nullable=True
    )
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    details: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
