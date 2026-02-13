from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy.sql.sqltypes import TIMESTAMP
from sqlalchemy.sql.expression import text
from sqlalchemy import (
    CheckConstraint,
    Text,
    func,
    Uuid,
    ForeignKey,
    UniqueConstraint,
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
    NORMALIZATION_RELATIONAL_SUCCEEDED = "NORMALIZATION_PHASE1_SUCCEEDED"
    NORMALIZATION_RELATIONAL_FAILED = "NORMALIZATION_PHASE1_FAILED"
    FHIR_JSON_GENERATION_SUCCEEDED = "FHIR_JSON_GENERATION_SUCCEEDED"
    FHIR_JSON_GENERATION_FAILED = "FHIR_JSON_GENERATION_FAILED"
    FHIR_JSON_RESOURCE_FAILED = "FHIR_JSON_RESOURCE_FAILED"
    NORMALIZATION_SUCCEEDED = (
        "NORMALIZATION_SUCCEEDED"  # Both phase 1 and 2 succeeded
    )
    NORMALIZATION_FAILED = "NORMALIZATION_FAILED"

    AI_ENRICHMENT_STARTED = "AI_ENRICHMENT_STARTED"
    AI_ENRICHMENT_SKIPPED = "AI_ENRICHMENT_SKIPPED"
    AI_ENRICHMENT_SUCCEEDED = "AI_ENRICHMENT_SUCCEEDED"
    AI_ENRICHMENT_FAILED = "AI_ENRICHMENT_FAILED"


PROCESSING_EVENT_TARGET_TYPES = (
    "ingestion",
    "panel",
    "test",
    "diagnostic_report",
    "observation",
    "ai_annotation",
)

PROCESSING_EVENT_ACTORS = (
    "ingestion-api",
    "parser",
    "validator",
    "normalizer",
    "ai-worker",
)

PROCESSING_EVENT_SEVERITIES = ("INFO", "WARN", "ERROR")

PROCESSING_EVENT_TYPES = tuple(e.value for e in ProcessingEventType)


def _sql_in_list(values: tuple[str, ...]) -> str:
    """
    Convert to Postgres-accepted literal. Returns ('A', 'B', 'C')
    """
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


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
            f"event_type IN {_sql_in_list(PROCESSING_EVENT_TYPES)}",
            name="check_processing_event_event_type",
        ),
        CheckConstraint(
            f"actor IN {_sql_in_list(PROCESSING_EVENT_ACTORS)}",
            name="check_processing_event_actor",
        ),
        CheckConstraint(
            f"severity IN {_sql_in_list(PROCESSING_EVENT_SEVERITIES)}",
            name="check_processing_event_severity",
        ),
        CheckConstraint(
            f"target_type IN {_sql_in_list(PROCESSING_EVENT_TARGET_TYPES)}",
            name="check_processing_event_target_type",
        ),
        CheckConstraint(
            """
            (
              (target_type = 'ingestion' AND target_id IS NULL)
              OR
              (target_type <> 'ingestion' AND target_id IS NOT NULL)
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
    execution_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)

    """
    dedupe_key is assigned by runner/orchestrator. Format: "ingestion_id:execution_id:content_sha256"
    """
    dedupe_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # What the event is about
    target_type: Mapped[str] = mapped_column(Text, nullable=False)
    target_id: Mapped[Optional[uuid.UUID]] = mapped_column(Uuid, nullable=True)

    # When
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    # Who
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    actor_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Explanation
    severity: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'INFO'")
    )
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    details: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
