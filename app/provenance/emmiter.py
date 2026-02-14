#
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from uuid import UUID
import uuid

from app.persistence.models.provenance import (
    ProcessingEvent,
    ProcessingEventActor,
    ProcessingEventSeverity,
    ProcessingEventTargetType,
    ProcessingEventType,
)
from app.persistence.repositories.processing_event_repo import (
    ProcessingEventRepository,
)


@dataclass(frozen=True)
class EventContext:
    """
    Context created once per stage invocation (parser/validator/normalizer/ai-worker).

    - execution_id: correlation id for this invocation attempt
    - actor: which service/stage emits events
    - actor_version: optional git SHA or build version
    - artifact_versions: optional structured versions (serializer/schema/prompt/model)
      Stored under details["artifact_versions"] by default.
    """

    ingestion_id: UUID
    actor: ProcessingEventActor
    execution_id: UUID = field(default_factory=uuid.uuid4)
    actor_version: Optional[str] = None
    artifact_versions: Optional[Dict[str, Any]] = None

    # Asses if I need this
    def child(
        self,
        *,
        actor: Optional[ProcessingEventActor] = None,
        execution_id: Optional[UUID] = None,
        actor_version: Optional[str] = None,
        artifact_versions: Optional[Dict[str, Any]] = None,
    ) -> "EventContext":
        """
        Create a modified context (useful if one orchestrator calls another stage).
        """
        return EventContext(
            ingestion_id=self.ingestion_id,
            actor=actor or self.actor,
            execution_id=execution_id or self.execution_id,
            actor_version=(
                actor_version
                if actor_version is not None
                else self.actor_version
            ),
            artifact_versions=(
                artifact_versions
                if artifact_versions is not None
                else self.artifact_versions
            ),
        )


def _merge_details(
    ctx: EventContext,
    details: Optional[Dict[str, Any]],
    extra: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    merged: Dict[str, Any] = {}
    if ctx.artifact_versions:
        merged["artifact_versions"] = ctx.artifact_versions
    if details:
        merged.update(details)
    if extra:
        merged.update(extra)
    return merged or None


def emit(
    repo: ProcessingEventRepository,
    ctx: EventContext,
    *,
    event_type: ProcessingEventType,
    severity: ProcessingEventSeverity = ProcessingEventSeverity.INFO,
    message: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    dedupe_key: Optional[str] = None,
    # Targeting (choose ONE approach):
    target_type: Optional[
        ProcessingEventTargetType
    ] = ProcessingEventTargetType.INGESTION,
    target_id: Optional[UUID] = None,
    # Writes
    deduped: bool = False,
) -> bool | ProcessingEvent:
    """
    Emit a ProcessingEvent.

    Targeting:
      - Default is ingestion-scoped: (INGESTION, NULL)
      - For resource-scoped events, pass (target_type, target_id)

    Dedupe strategy:
      - deduped=False: uses ORM insert (repo.create) and returns ProcessingEvent
      - deduped=True: uses repo.create_deduped and returns bool (inserted vs ignored)
      - Note on emitting events for newly created Observation/DiagnosticReport,
        call session.flush() first so IDs exist, then use target_id.
    """

    # Add a small set of predictable machine-readable fields automatically.
    extra = {
        "event_type": event_type.value,
        "actor": ctx.actor.value,
        "execution_id": str(ctx.execution_id),
    }

    merged_details = _merge_details(ctx, details, extra)

    if deduped:
        values = {
            "ingestion_id": ctx.ingestion_id,
            "execution_id": ctx.execution_id,
            "dedupe_key": dedupe_key,
            "target_type": target_type,
            "target_id": target_id,
            "event_type": event_type,
            "actor": ctx.actor,
            "actor_version": ctx.actor_version,
            "severity": severity,
            "message": message,
            "details": merged_details,
        }
        return repo.create_deduped(values)

    pe = ProcessingEvent(
        ingestion_id=ctx.ingestion_id,
        execution_id=ctx.execution_id,
        dedupe_key=dedupe_key,
        target_type=target_type,
        target_id=target_id,
        event_type=event_type,
        actor=ctx.actor,
        actor_version=ctx.actor_version,
        severity=severity,
        message=message,
        details=merged_details,
    )
    return repo.create(pe)


# Convenience wrappers (optional, but they keep call sites clean)
def emit_started(
    repo: ProcessingEventRepository,
    ctx: EventContext,
    *,
    event_type: ProcessingEventType,
    message: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    dedupe_key: Optional[str] = None,
    deduped: bool = True,
) -> bool | ProcessingEvent:
    return emit(
        repo,
        ctx,
        event_type=event_type,
        severity=ProcessingEventSeverity.INFO,
        message=message,
        details=details,
        dedupe_key=dedupe_key,
        target_type=ProcessingEventTargetType.INGESTION,
        target_id=None,
        deduped=deduped,
    )


def emit_failed(
    repo: ProcessingEventRepository,
    ctx: EventContext,
    *,
    event_type: ProcessingEventType,
    error: Exception,
    message: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    target_type: Optional[
        ProcessingEventTargetType
    ] = ProcessingEventTargetType.INGESTION,
    target_id: Optional[UUID] = None,
    dedupe_key: Optional[str] = None,
    deduped: bool = False,
) -> bool | ProcessingEvent:
    err_details = {
        "error_type": type(error).__name__,
        "error_message": str(error),
    }
    merged = dict(details or {})
    merged.update(err_details)

    return emit(
        repo,
        ctx,
        event_type=event_type,
        severity=ProcessingEventSeverity.ERROR,
        message=message or str(error),
        details=merged,
        dedupe_key=dedupe_key,
        target_type=target_type,
        target_id=target_id,
        deduped=deduped,
    )
