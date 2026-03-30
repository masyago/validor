from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
import uuid

import pytest

from app.persistence.models.core import Ingestion
from app.persistence.models.normalization import DiagnosticReport
from app.persistence.models.parsing import Panel
from app.persistence.repositories.diagnostic_report_repo import (
    DiagnosticReportRepository,
)


@dataclass(frozen=True)
class DiagnosticReportSetup:
    ingestion_id: uuid.UUID
    patient_id: str
    panel_code: str
    panel_id_1: uuid.UUID
    panel_id_2: uuid.UUID
    effective_at_1: datetime
    effective_at_2: datetime
    normalized_at: datetime


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
def setup_two_panels(db_session) -> DiagnosticReportSetup:
    ingestion_id = uuid.uuid4()
    db_session.add(_make_ingestion(ingestion_id=ingestion_id))
    db_session.flush()

    patient_id = "PAT-1"
    panel_code = "BMP"
    effective_at_1 = datetime(2026, 1, 28, 16, 5, 33, tzinfo=timezone.utc)
    effective_at_2 = datetime(2026, 1, 28, 16, 6, 33, tzinfo=timezone.utc)
    normalized_at = datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc)

    panel_id_1 = uuid.uuid4()
    panel_id_2 = uuid.uuid4()

    p1 = Panel(
        panel_id=panel_id_1,
        ingestion_id=ingestion_id,
        sample_id="SAM-1",
        patient_id=patient_id,
        panel_code=panel_code,
        collection_timestamp=effective_at_1,
    )
    p2 = Panel(
        panel_id=panel_id_2,
        ingestion_id=ingestion_id,
        sample_id="SAM-2",
        patient_id=patient_id,
        panel_code=panel_code,
        collection_timestamp=effective_at_2,
    )
    db_session.add_all([p1, p2])
    db_session.flush()

    return DiagnosticReportSetup(
        ingestion_id=ingestion_id,
        patient_id=patient_id,
        panel_code=panel_code,
        panel_id_1=panel_id_1,
        panel_id_2=panel_id_2,
        effective_at_1=effective_at_1,
        effective_at_2=effective_at_2,
        normalized_at=normalized_at,
    )


def _payload_for_panel(
    *,
    setup: DiagnosticReportSetup,
    panel_id: uuid.UUID,
    effective_at: datetime,
    resource_json: dict | None = None,
) -> dict:
    payload: dict = {
        "ingestion_id": setup.ingestion_id,
        "panel_id": panel_id,
        "patient_id": setup.patient_id,
        "panel_code": setup.panel_code,
        "effective_at": effective_at,
        "normalized_at": setup.normalized_at,
        "status": "FINAL",
    }
    if resource_json is not None:
        payload["resource_json"] = resource_json
    return payload


def test_upsert_from_payload_inserts_and_returns_inserted_true(
    db_session, setup_two_panels
):
    repo = DiagnosticReportRepository(db_session)

    payload = _payload_for_panel(
        setup=setup_two_panels,
        panel_id=setup_two_panels.panel_id_1,
        effective_at=setup_two_panels.effective_at_1,
    )
    diagnostic_report_id, inserted = repo.upsert_from_payload(payload)

    assert inserted is True

    dr = repo.get_by_diagnostic_report_id(diagnostic_report_id)
    assert dr is not None
    assert dr.panel_id == setup_two_panels.panel_id_1
    assert dr.resource_json is None

    dr2 = repo.get_by_panel_id(setup_two_panels.panel_id_1)
    assert dr2 is not None
    assert dr2.diagnostic_report_id == diagnostic_report_id


def test_upsert_from_payload_conflict_returns_existing_id_and_does_not_overwrite_resource_json(
    db_session, setup_two_panels
):
    repo = DiagnosticReportRepository(db_session)

    payload = _payload_for_panel(
        setup=setup_two_panels,
        panel_id=setup_two_panels.panel_id_1,
        effective_at=setup_two_panels.effective_at_1,
    )
    diagnostic_report_id_1, inserted_1 = repo.upsert_from_payload(payload)
    assert inserted_1 is True

    repo.update_resource_json(diagnostic_report_id_1, {"keep": True})
    db_session.expire_all()

    payload_attempt_overwrite = _payload_for_panel(
        setup=setup_two_panels,
        panel_id=setup_two_panels.panel_id_1,
        effective_at=setup_two_panels.effective_at_1,
        resource_json={"try": "overwrite"},
    )
    diagnostic_report_id_2, inserted_2 = repo.upsert_from_payload(
        payload_attempt_overwrite
    )

    assert inserted_2 is False
    assert diagnostic_report_id_2 == diagnostic_report_id_1

    db_session.expire_all()
    dr = repo.get_by_diagnostic_report_id(diagnostic_report_id_1)
    assert dr is not None
    assert dr.resource_json == {"keep": True}


def test_upsert_many_from_payloads_raises_runtime_error_if_unresolved(
    db_session, setup_two_panels, monkeypatch
):
    repo = DiagnosticReportRepository(db_session)

    # Create an existing row so one payload hits the conflict path.
    payload_existing = _payload_for_panel(
        setup=setup_two_panels,
        panel_id=setup_two_panels.panel_id_1,
        effective_at=setup_two_panels.effective_at_1,
    )
    existing_id, inserted = repo.upsert_from_payload(payload_existing)
    assert inserted is True
    assert existing_id is not None

    payload_new = _payload_for_panel(
        setup=setup_two_panels,
        panel_id=setup_two_panels.panel_id_2,
        effective_at=setup_two_panels.effective_at_2,
    )

    # Monkeypatch execute so the "fetch ids for conflict rows" query returns no rows,
    # simulating an unexpected race/deletion between the INSERT and SELECT.
    orig_execute = db_session.execute

    def _fake_execute(statement, *args, **kwargs):
        if getattr(statement, "__visit_name__", None) == "select":
            cols = list(getattr(statement, "selected_columns", []))
            if (
                len(cols) == 2
                and getattr(cols[0], "name", None) == "panel_id"
                and getattr(cols[1], "name", None) == "diagnostic_report_id"
            ):
                return SimpleNamespace(all=lambda: [])
        return orig_execute(statement, *args, **kwargs)

    monkeypatch.setattr(db_session, "execute", _fake_execute)

    with pytest.raises(RuntimeError, match="bulk upsert failed to resolve"):
        repo.upsert_many_from_payloads([payload_existing, payload_new])

    db_session.expire_all()
    assert repo.get_by_panel_id(setup_two_panels.panel_id_1) is not None
