# Append only
from sqlalchemy.orm import Session
from sqlalchemy import select, desc, asc
from uuid import UUID
from typing import Optional

from app.persistence.models.provenance import (
    ProcessingEvent,
    ProcessingEventType,
    ProcessingEventTargetType,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import text


class ProcessingEventRepository:
    def __init__(self, session: Session):
        self.session = session

    # Use when retries/duplicates don't happen
    def create(self, processing_event: ProcessingEvent) -> ProcessingEvent:
        self.session.add(processing_event)
        self.session.flush()
        return processing_event

    # Use when retries happen, especially for Normalizer
    _DEDUPE_PREDICATE = text("dedupe_key IS NOT NULL")

    def create_deduped(self, values: dict) -> bool:
        """
        Insert one event; ignore duplicates when dedupe_key is present and conflicts.
        Returns True if inserted, False if ignored.
        """
        stmt = pg_insert(ProcessingEvent).values(**values)

        # Only apply ON CONFLICT when dedupe_key is actually present.
        if values.get("dedupe_key") is not None:
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["ingestion_id", "event_type", "dedupe_key"],
                index_where=ProcessingEventRepository._DEDUPE_PREDICATE,
            )
        stmt = stmt.returning(ProcessingEvent.event_id)
        res = self.session.execute(stmt)
        return res.first() is not None  # inserted if a row returned

    def list_by_ingestion_id(
        self, ingestion_id: UUID
    ) -> list[ProcessingEvent]:
        stmt = (
            select(ProcessingEvent)
            .where(ProcessingEvent.ingestion_id == ingestion_id)
            .order_by(
                asc(ProcessingEvent.occurred_at), asc(ProcessingEvent.event_id)
            )
        )
        return list(self.session.scalars(stmt).all())

    def list_by_ingestion_id_and_event_type(
        self, ingestion_id: UUID, event_type: ProcessingEventType
    ) -> list[ProcessingEvent]:
        stmt = (
            select(ProcessingEvent)
            .where(
                ProcessingEvent.ingestion_id == ingestion_id,
                ProcessingEvent.event_type == event_type,
            )
            .order_by(
                asc(ProcessingEvent.occurred_at), asc(ProcessingEvent.event_id)
            )
        )
        return list(self.session.scalars(stmt).all())

    def list_by_target(
        self, target_type: ProcessingEventTargetType, target_id: UUID | None
    ) -> list[ProcessingEvent]:
        stmt = (
            select(ProcessingEvent)
            .where(
                ProcessingEvent.target_type == target_type,
                ProcessingEvent.target_id == target_id,
            )
            .order_by(
                asc(ProcessingEvent.occurred_at), asc(ProcessingEvent.event_id)
            )
        )
        return list(self.session.scalars(stmt).all())

    # Get latest record for the ingestion and (if provided) for event type
    def get_latest_for_ingestion(
        self,
        ingestion_id: UUID,
        event_type: Optional[ProcessingEventType] = None,
    ) -> ProcessingEvent | None:
        stmt = select(ProcessingEvent).where(
            ProcessingEvent.ingestion_id == ingestion_id
        )
        if event_type is not None:
            stmt = stmt.where(ProcessingEvent.event_type == event_type)

        stmt = stmt.order_by(
            desc(ProcessingEvent.occurred_at),
            desc(ProcessingEvent.event_id),  # tie-breaker
        ).limit(1)
        return self.session.scalars(stmt).one_or_none()
