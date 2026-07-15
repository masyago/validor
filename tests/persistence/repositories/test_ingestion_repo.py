from __future__ import annotations

from datetime import datetime, timezone
import uuid

import pytest

from app.core.ingestion_status_enums import IngestionStatus
from app.persistence.models.core import Ingestion
from app.persistence.repositories.ingestion_repo import IngestionRepository


def _make_ingestion(
    *, ingestion_id: uuid.UUID, status: IngestionStatus
) -> Ingestion:
    return Ingestion(
        ingestion_id=ingestion_id,
        instrument_id="INST-1",
        run_id=f"RUN-{ingestion_id.hex[:8]}",
        uploader_id="uploader_001",
        spec_version="analyzer_csv_v1",
        uploader_received_at=datetime(2026, 2, 15, 12, 0, tzinfo=timezone.utc),
        api_received_at=datetime(2026, 2, 15, 12, 10, tzinfo=timezone.utc),
        server_sha256="0" * 64,
        status=status,
        source_filename="fixture.csv",
    )


def test_requeue_processing_moves_processing_to_received_and_clears_errors(
    db_session,
):
    ingestion_id = uuid.uuid4()
    ingestion = _make_ingestion(
        ingestion_id=ingestion_id, status=IngestionStatus.PROCESSING
    )
    ingestion.error_code = "boom"
    ingestion.error_detail = {"k": "v"}
    db_session.add(ingestion)
    db_session.flush()

    repo = IngestionRepository(db_session)

    ok = repo.requeue_processing(ingestion_id)
    assert ok is True

    db_session.expire_all()
    reloaded = repo.get_by_ingestion_id(ingestion_id)
    assert reloaded is not None
    assert reloaded.status == IngestionStatus.RECEIVED
    assert reloaded.error_code is None
    assert reloaded.error_detail is None


@pytest.mark.parametrize(
    "status",
    [
        IngestionStatus.RECEIVED,
        IngestionStatus.COMPLETED,
        IngestionStatus.FAILED,
        IngestionStatus.FAILED_VALIDATION,
    ],
)
def test_requeue_processing_returns_false_when_not_processing(
    db_session, status: IngestionStatus
):
    ingestion_id = uuid.uuid4()
    db_session.add(_make_ingestion(ingestion_id=ingestion_id, status=status))
    db_session.flush()

    repo = IngestionRepository(db_session)

    ok = repo.requeue_processing(ingestion_id)
    assert ok is False

    db_session.expire_all()
    reloaded = repo.get_by_ingestion_id(ingestion_id)
    assert reloaded is not None
    assert reloaded.status == status


def test_requeue_processing_returns_false_when_missing(db_session):
    repo = IngestionRepository(db_session)
    ok = repo.requeue_processing(uuid.uuid4())
    assert ok is False


def _add_session_ingestion(
    db_session, *, kind: str, expires_at, session_id=None
) -> uuid.UUID:
    ingestion_id = uuid.uuid4()
    ingestion = _make_ingestion(
        ingestion_id=ingestion_id, status=IngestionStatus.COMPLETED
    )
    ingestion.kind = kind
    ingestion.session_id = session_id
    ingestion.expires_at = expires_at
    db_session.add(ingestion)
    db_session.flush()
    return ingestion_id


def test_delete_expired_sessions_purges_expired_and_orphaned_but_keeps_seed(
    db_session,
):
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    past = datetime(2026, 3, 1, 11, 0, tzinfo=timezone.utc)
    future = datetime(2026, 3, 1, 13, 0, tzinfo=timezone.utc)

    expired = _add_session_ingestion(
        db_session, kind="SESSION", expires_at=past, session_id=uuid.uuid4()
    )
    # Cookie-less legacy orphan: SESSION with no expiry — must self-heal.
    orphan = _add_session_ingestion(
        db_session, kind="SESSION", expires_at=None
    )
    active = _add_session_ingestion(
        db_session, kind="SESSION", expires_at=future, session_id=uuid.uuid4()
    )
    seed = _add_session_ingestion(db_session, kind="SEED", expires_at=None)

    repo = IngestionRepository(db_session)
    deleted = repo.delete_expired_sessions(now=now)

    assert deleted == 2
    db_session.expire_all()
    assert repo.get_by_ingestion_id(expired) is None
    assert repo.get_by_ingestion_id(orphan) is None
    assert repo.get_by_ingestion_id(active) is not None
    assert repo.get_by_ingestion_id(seed) is not None
