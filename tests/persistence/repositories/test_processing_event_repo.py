from __future__ import annotations

from datetime import datetime, timezone
import uuid

import pytest
from sqlalchemy import select

from app.persistence.models.core import Ingestion
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


def _make_ingestion(*, ingestion_id: uuid.UUID) -> Ingestion:
    return Ingestion(
        ingestion_id=ingestion_id,
        instrument_id="INST-1",
        run_id=f"RUN-{ingestion_id.hex[:8]}",
        uploader_id="uploader_001",
        spec_version="analyzer_csv_v1",
        uploader_received_at=datetime(2026, 2, 15, 12, 0, tzinfo=timezone.utc),
        api_received_at=datetime(2026, 2, 15, 12, 10, tzinfo=timezone.utc),
        server_sha256="0" * 64,
        status="RECEIVED",
        source_filename="fixture.csv",
    )


@pytest.fixture
def make_event():
    def _make_event(
        *,
        ingestion_id: uuid.UUID,
        event_id: uuid.UUID | None = None,
        event_type: ProcessingEventType = ProcessingEventType.PARSE_STARTED,
        occurred_at: datetime | None = None,
        execution_id: uuid.UUID | None = None,
        dedupe_key: str | None = None,
        target_type: ProcessingEventTargetType = ProcessingEventTargetType.INGESTION,
        target_id: uuid.UUID | None = None,
        actor: ProcessingEventActor = ProcessingEventActor.PARSER,
        actor_version: str | None = None,
        severity: ProcessingEventSeverity = ProcessingEventSeverity.INFO,
        message: str | None = None,
        details: dict | None = None,
    ) -> ProcessingEvent:
        return ProcessingEvent(
            event_id=event_id or uuid.uuid4(),
            ingestion_id=ingestion_id,
            execution_id=execution_id or uuid.uuid4(),
            dedupe_key=dedupe_key,
            target_type=target_type,
            target_id=target_id,
            event_type=event_type,
            occurred_at=occurred_at
            or datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc),
            actor=actor,
            actor_version=actor_version,
            severity=severity,
            message=message,
            details=details,
        )

    return _make_event


def test_create_deduped_inserts_once_then_ignores_duplicate(db_session):
    ingestion_id = uuid.uuid4()
    db_session.add(_make_ingestion(ingestion_id=ingestion_id))
    db_session.flush()

    repo = ProcessingEventRepository(db_session)

    execution_id = uuid.uuid4()
    values = {
        "ingestion_id": ingestion_id,
        "execution_id": execution_id,
        "dedupe_key": "normalizer:NORMALIZATION_STARTED:exec-1",
        "target_type": ProcessingEventTargetType.INGESTION,
        "target_id": None,
        "event_type": ProcessingEventType.NORMALIZATION_STARTED,
        "actor": ProcessingEventActor.NORMALIZER,
        "actor_version": None,
        "severity": ProcessingEventSeverity.INFO,
        "message": "started",
        "details": {"k": "v"},
    }

    inserted_1 = repo.create_deduped(values)
    inserted_2 = repo.create_deduped(values)

    assert inserted_1 is True
    assert inserted_2 is False

    rows = (
        db_session.execute(
            select(ProcessingEvent).where(
                ProcessingEvent.ingestion_id == ingestion_id,
                ProcessingEvent.event_type
                == ProcessingEventType.NORMALIZATION_STARTED,
                ProcessingEvent.dedupe_key == values["dedupe_key"],
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 1


def test_create_deduped_without_dedupe_key_does_not_dedupe(db_session):
    ingestion_id = uuid.uuid4()
    db_session.add(_make_ingestion(ingestion_id=ingestion_id))
    db_session.flush()

    repo = ProcessingEventRepository(db_session)

    values = {
        "ingestion_id": ingestion_id,
        "execution_id": uuid.uuid4(),
        "dedupe_key": None,
        "target_type": ProcessingEventTargetType.INGESTION,
        "target_id": None,
        "event_type": ProcessingEventType.PARSE_STARTED,
        "actor": ProcessingEventActor.PARSER,
        "actor_version": None,
        "severity": ProcessingEventSeverity.INFO,
        "message": "parse started",
        "details": None,
    }

    inserted_1 = repo.create_deduped(values)
    inserted_2 = repo.create_deduped(values)

    assert inserted_1 is True
    assert inserted_2 is True

    rows = (
        db_session.execute(
            select(ProcessingEvent).where(
                ProcessingEvent.ingestion_id == ingestion_id,
                ProcessingEvent.event_type
                == ProcessingEventType.PARSE_STARTED,
                ProcessingEvent.dedupe_key.is_(None),
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 2


def test_list_by_ingestion_id(db_session, make_event):
    ingestion_id1 = uuid.uuid4()
    db_session.add(_make_ingestion(ingestion_id=ingestion_id1))

    ingestion_id2 = uuid.uuid4()
    db_session.add(_make_ingestion(ingestion_id=ingestion_id2))

    db_session.flush()

    repo = ProcessingEventRepository(db_session)

    early = datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc)
    later = datetime(2026, 2, 15, 12, 1, 0, tzinfo=timezone.utc)
    ingestion_id1_low_event_id = uuid.UUID(int=1)
    ingestion_id1_high_event_id = uuid.UUID(int=2)
    ingestion_id1_later_event_id = uuid.UUID(int=0)
    ingestion_id2_event_id = uuid.UUID(int=3)

    e1 = make_event(
        ingestion_id=ingestion_id1,
        event_id=ingestion_id1_low_event_id,
        event_type=ProcessingEventType.PARSE_STARTED,
        occurred_at=early,
        message="e1",
    )
    e2 = make_event(
        ingestion_id=ingestion_id1,
        event_id=ingestion_id1_high_event_id,
        event_type=ProcessingEventType.PARSE_FAILED,
        occurred_at=early,
        message="e2",
    )
    e3 = make_event(
        ingestion_id=ingestion_id1,
        event_id=ingestion_id1_later_event_id,
        event_type=ProcessingEventType.VALIDATION_STARTED,
        occurred_at=later,
        message="e3",
    )
    other_ingestion = make_event(
        ingestion_id=ingestion_id2,
        event_id=ingestion_id2_event_id,
        event_type=ProcessingEventType.PARSE_STARTED,
        occurred_at=early,
        message="other",
    )

    db_session.add_all([e1, e2, e3, other_ingestion])
    db_session.flush()

    events_for_ingestion_id = repo.list_by_ingestion_id(ingestion_id1)
    assert [e.ingestion_id for e in events_for_ingestion_id] == [
        ingestion_id1,
        ingestion_id1,
        ingestion_id1,
    ]
    # Ordering: occurred_at ASC, event_id ASC
    assert [e.event_id for e in events_for_ingestion_id] == [
        ingestion_id1_low_event_id,
        ingestion_id1_high_event_id,
        ingestion_id1_later_event_id,
    ]


def test_list_by_ingestion_id_and_event_type(db_session, make_event):
    ingestion_id = uuid.uuid4()
    db_session.add(_make_ingestion(ingestion_id=ingestion_id))
    db_session.flush()

    repo = ProcessingEventRepository(db_session)

    early = datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc)
    later = datetime(2026, 2, 15, 12, 1, 0, tzinfo=timezone.utc)
    early_event_id = uuid.UUID(int=1)
    later_event_id1 = uuid.UUID(int=2)
    later_event_id2 = uuid.UUID(int=0)

    early_e = make_event(
        ingestion_id=ingestion_id,
        event_id=early_event_id,
        event_type=ProcessingEventType.PARSE_STARTED,
        occurred_at=early,
        message="early_e",
    )

    late_e1 = make_event(
        ingestion_id=ingestion_id,
        event_id=later_event_id1,
        event_type=ProcessingEventType.PARSE_STARTED,
        occurred_at=later,
        message="late_e1",
    )

    late_e2 = make_event(
        ingestion_id=ingestion_id,
        event_id=later_event_id2,
        event_type=ProcessingEventType.PARSE_STARTED,
        occurred_at=later,
        message="late_e2",
    )

    db_session.add_all([early_e, late_e1, late_e2])
    db_session.flush()

    events_for_ingestion_id_and_event_type = (
        repo.list_by_ingestion_id_and_event_type(
            ingestion_id, event_type=ProcessingEventType.PARSE_STARTED
        )
    )
    assert [
        e.ingestion_id for e in events_for_ingestion_id_and_event_type
    ] == [
        ingestion_id,
        ingestion_id,
        ingestion_id,
    ]

    assert [e.event_type for e in events_for_ingestion_id_and_event_type] == [
        ProcessingEventType.PARSE_STARTED,
        ProcessingEventType.PARSE_STARTED,
        ProcessingEventType.PARSE_STARTED,
    ]

    # Ordering: earliest occurred_at first, then ASC event_id in case of tie
    assert [e.event_id for e in events_for_ingestion_id_and_event_type] == [
        early_event_id,
        later_event_id2,
        later_event_id1,
    ]


def test_get_latest_for_ingestion_uses_event_id_tiebreak(
    db_session, make_event
):
    ingestion_id = uuid.uuid4()
    db_session.add(_make_ingestion(ingestion_id=ingestion_id))
    db_session.flush()

    repo = ProcessingEventRepository(db_session)

    occurred_at = datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc)
    low_event_id = uuid.UUID(int=1)
    high_event_id = uuid.UUID(int=2)

    e1 = make_event(
        ingestion_id=ingestion_id,
        event_id=low_event_id,
        event_type=ProcessingEventType.PARSE_STARTED,
        occurred_at=occurred_at,
        message="e1",
    )
    e2 = make_event(
        ingestion_id=ingestion_id,
        event_id=high_event_id,
        event_type=ProcessingEventType.PARSE_STARTED,
        occurred_at=occurred_at,
        message="e2",
    )
    db_session.add_all([e1, e2])
    db_session.flush()

    latest_any = repo.get_latest_for_ingestion(ingestion_id)
    assert latest_any is not None
    assert latest_any.event_id == high_event_id

    latest_filtered = repo.get_latest_for_ingestion(
        ingestion_id, event_type=ProcessingEventType.PARSE_STARTED
    )
    assert latest_filtered is not None
    assert latest_filtered.event_id == high_event_id


def test_get_latest_for_ingestion_returns_none_when_empty(db_session):
    ingestion_id = uuid.uuid4()
    db_session.add(_make_ingestion(ingestion_id=ingestion_id))
    db_session.flush()

    repo = ProcessingEventRepository(db_session)
    assert repo.get_latest_for_ingestion(ingestion_id) is None
