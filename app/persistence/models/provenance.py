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
    NORMALIZATION_RELATIONAL_SUCCEEDED = "NORMALIZATION_RELATIONAL_SUCCEEDED"
    NORMALIZATION_RELATIONAL_FAILED = "NORMALIZATION_RELATIONAL_FAILED"
    FHIR_JSON_GENERATION_SUCCEEDED = "FHIR_JSON_GENERATION_SUCCEEDED"
    FHIR_JSON_GENERATION_FAILED = "FHIR_JSON_GENERATION_FAILED"
    FHIR_JSON_RESOURCE_FAILED = "FHIR_JSON_RESOURCE_FAILED"

    # Both phases succeeded
    NORMALIZATION_SUCCEEDED = "NORMALIZATION_SUCCEEDED"

    # Only phase 1 successful, phase 2 failed
    NORMALIZATION_SUCCEEDED_WITH_WARNINGS = (
        "NORMALIZATION_SUCCEEDED_WITH_WARNINGS"
    )

    # Phase 1 failed
    NORMALIZATION_FAILED = "NORMALIZATION_FAILED"

    AI_ENRICHMENT_STARTED = "AI_ENRICHMENT_STARTED"
    AI_ENRICHMENT_SKIPPED = "AI_ENRICHMENT_SKIPPED"
    AI_ENRICHMENT_SUCCEEDED = "AI_ENRICHMENT_SUCCEEDED"
    AI_ENRICHMENT_FAILED = "AI_ENRICHMENT_FAILED"


processing_event_type_enum = SqlEnum(
    ProcessingEventType,
    name="processing_event_type_enum",
    native_enum=True,
    create_type=True,
    values_callable=lambda enum_cls: [e.value for e in enum_cls],
)


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
    native_enum=True,
    create_type=True,
    values_callable=lambda enum_cls: [e.value for e in enum_cls],
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
    native_enum=True,
    create_type=True,
    values_callable=lambda enum_cls: [e.value for e in enum_cls],
)


class ProcessingEventSeverity(enum.Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


processing_event_severity_enum = SqlEnum(
    ProcessingEventSeverity,
    name="processing_event_severity_enum",
    native_enum=True,
    create_type=True,
    values_callable=lambda enum_cls: [e.value for e in enum_cls],
)


class ProcessingEvent(Base):
    __tablename__ = "processing_event"
    __table_args__ = (
        Index(
            "ux_processing_event_dedupe",
            "ingestion_id",
            "event_type",
            "dedupe_key",
            unique=True,
            postgresql_where=text("dedupe_key IS NOT NULL"),
        ),
        Index(
            "ix_processing_event_ingestion_event_type",
            "ingestion_id",
            "event_type",
        ),
        Index(
            "ix_processing_event_target_type_id", "target_type", "target_id"
        ),
        Index(
            "ix_processing_event_ingestion_occurred_at",
            "ingestion_id",
            "occurred_at",
        ),
        CheckConstraint(
            """
            (
              (target_type = 'ingestion'::processing_event_target_type_enum AND target_id IS NULL)
              OR
              (target_type <> 'ingestion'::processing_event_target_type_enum AND target_id IS NOT NULL)
            )
            """,
            name="ck_processing_event_target_consistency",
        ),
    )

    # Event identity
    event_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("ingestion.ingestion_id"), nullable=False
    )
    # Generated once at the start of a job invocation
    execution_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)

    """
    dedupe_key is assigned by runner/orchestrator. Example format: 
    "ingestion_id:execution_id:content_sha256".
    for resource-level failure: "serializer_version:target_id:error_code"
    for stage-level: "actor:event_type:execution_id"
    """
    dedupe_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # What entity the event is about
    target_type: Mapped[ProcessingEventTargetType] = mapped_column(
        processing_event_target_type_enum, nullable=False
    )
    target_id: Mapped[Optional[uuid.UUID]] = mapped_column(Uuid, nullable=True)

    # When
    event_type: Mapped[ProcessingEventType] = mapped_column(
        processing_event_type_enum, nullable=False
    )
    occurred_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    # Who
    actor: Mapped[ProcessingEventActor] = mapped_column(
        processing_event_actor_enum, nullable=False
    )
    actor_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Explanation
    severity: Mapped[ProcessingEventSeverity] = mapped_column(
        processing_event_severity_enum,
        nullable=False,
        server_default=text("'INFO'::processing_event_severity_enum"),
    )
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    details: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
