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
from app.persistence.base import Base


# Replace with CheckConstraint
# Provenance: ProcessingEvent class and supporting classes
# class ProcessingEventTargetType(enum.Enum):
#     INGESTION = "ingestion"
#     PANEL = "panel"
#     TEST = "test"
#     DIAGNOSTIC_REPORT = "diagnostic_report"
#     OBSERVATION = "observation"
#     AI_ANNOTATION = "ai_annotation"


# processing_event_target_type_enum = SqlEnum(
#     ProcessingEventTargetType,
#     name="processing_event_target_type_enum",
#     create_type=True,
# )


# class ProcessingEventActor(enum.Enum):
#     INGESTION_API = "ingestion-api"
#     PARSER = "parser"
#     VALIDATOR = "validator"
#     NORMALIZER = "normalizer"
#     AI_WORKER = "ai-worker"


# processing_event_actor_enum = SqlEnum(
#     ProcessingEventActor,
#     name="processing_event_actor_enum",
#     create_type=True,
# )


# class ProcessingEventSeverity(enum.Enum):
#     INFO = "INFO"
#     WARN = "WARN"
#     ERROR = "ERROR"


# processing_event_severity_enum = SqlEnum(
#     ProcessingEventSeverity,
#     name="processing_event_severity_enum",
#     create_type=True,
# )


# class ProcessingEventType(enum.Enum):
#     INGESTION_ACCEPTED = "INGESTION_ACCEPTED"
#     INGESTION_DEDUPED_IDENTICAL = "INGESTION_DEDUPED_IDENTICAL"
#     INGESTION_CONFLICT = "INGESTION_CONFLICT"
#     PARSE_STARTED = "PARSE_STARTED"
#     PARSE_SUCCEEDED = "PARSE_SUCCEEDED"
#     PARSE_FAILED = "PARSE_FAILED"
#     VALIDATION_STARTED = "VALIDATION_STARTED"
#     VALIDATION_SUCCEEDED = "VALIDATION_SUCCEEDED"
#     VALIDATION_FAILED = "VALIDATION_FAILED"
#     NORMALIZATION_STARTED = "NORMALIZATION_STARTED"
#     NORMALIZATION_SUCCEEDED = "NORMALIZATION_SUCCEEDED"
#     NORMALIZATION_FAILED = "NORMALIZATION_FAILED"
#     AI_ENRICHMENT_STARTED = "AI_ENRICHMENT_STARTED"
#     AI_ENRICHMENT_SKIPPED = "AI_ENRICHMENT_SKIPPED"
#     AI_ENRICHMENT_SUCCEEDED = "AI_ENRICHMENT_SUCCEEDED"
#     AI_ENRICHMENT_FAILED = "AI_ENRICHMENT_FAILED"


# processing_event_type_enum = SqlEnum(
#     ProcessingEventType,
#     name="processing_event_type_enum",
#     create_type=True,
# )


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
