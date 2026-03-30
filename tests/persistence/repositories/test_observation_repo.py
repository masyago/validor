from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
import uuid

import pytest

from app.persistence.models.core import Ingestion
from app.persistence.models.normalization import DiagnosticReport, Observation
from app.persistence.models.parsing import Panel, Test
from app.persistence.repositories.observation_repo import ObservationRepository


@dataclass(frozen=True)
class ObservationSetup:
    ingestion_id: uuid.UUID
    panel_id: uuid.UUID
    diagnostic_report_id: uuid.UUID
    patient_id: str
    test_id_1: uuid.UUID
    test_id_2: uuid.UUID
    effective_at: datetime
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
def setup_two_test_rows(db_session) -> ObservationSetup:
    ingestion_id = uuid.uuid4()
    db_session.add(_make_ingestion(ingestion_id=ingestion_id))
    db_session.flush()

    effective_at = datetime(2026, 1, 28, 16, 5, 33, tzinfo=timezone.utc)
    normalized_at = datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc)

    panel_id = uuid.uuid4()
    patient_id = "PAT-1"
    panel = Panel(
        panel_id=panel_id,
        ingestion_id=ingestion_id,
        sample_id="SAM-1",
        patient_id=patient_id,
        panel_code="BMP",
        collection_timestamp=effective_at,
    )
    db_session.add(panel)
    db_session.flush()

    test_id_1 = uuid.uuid4()
    test_id_2 = uuid.uuid4()
    t1 = Test(
        test_id=test_id_1,
        panel_id=panel_id,
        row_number=1,
        test_code="A",
        test_name=None,
        analyte_type=None,
        result_raw="1",
        units_raw=None,
        result_value_num=1.0,
        result_comparator=None,
        ref_low_raw=None,
        ref_high_raw=None,
        flag=None,
    )
    t2 = Test(
        test_id=test_id_2,
        panel_id=panel_id,
        row_number=2,
        test_code="B",
        test_name=None,
        analyte_type=None,
        result_raw="2",
        units_raw=None,
        result_value_num=2.0,
        result_comparator=None,
        ref_low_raw=None,
        ref_high_raw=None,
        flag=None,
    )
    db_session.add_all([t1, t2])
    db_session.flush()

    diagnostic_report_id = uuid.uuid4()
    dr = DiagnosticReport(
        diagnostic_report_id=diagnostic_report_id,
        ingestion_id=ingestion_id,
        panel_id=panel_id,
        patient_id=patient_id,
        panel_code=panel.panel_code,
        effective_at=effective_at,
        normalized_at=normalized_at,
        resource_json=None,
        status="FINAL",
    )
    db_session.add(dr)
    db_session.flush()

    return ObservationSetup(
        ingestion_id=ingestion_id,
        panel_id=panel_id,
        diagnostic_report_id=diagnostic_report_id,
        patient_id=patient_id,
        test_id_1=test_id_1,
        test_id_2=test_id_2,
        effective_at=effective_at,
        normalized_at=normalized_at,
    )


def _payload_for_test(
    *,
    obs_setup: ObservationSetup,
    test_id: uuid.UUID,
    code: str = "GLU",
    resource_json: dict | None = None,
) -> dict:
    payload: dict = {
        "test_id": test_id,
        "diagnostic_report_id": obs_setup.diagnostic_report_id,
        "ingestion_id": obs_setup.ingestion_id,
        "patient_id": obs_setup.patient_id,
        "code": code,
        "display": "Glucose",
        "effective_at": obs_setup.effective_at,
        "normalized_at": obs_setup.normalized_at,
        "status": "FINAL",
        # Keep the rest nullable.
        "value_num": 1.0,
        "value_text": None,
        "comparator": None,
        "unit": "mg/dL",
        "ref_low_num": None,
        "ref_high_num": None,
        "flag_analyzer_interpretation": None,
        "flag_system_interpretation": "UNKNOWN",
        "discrepancy": None,
    }
    if resource_json is not None:
        payload["resource_json"] = resource_json
    return payload


def test_upsert_from_payload_inserts_and_returns_inserted_true(
    db_session, setup_two_test_rows
):
    repo = ObservationRepository(db_session)

    payload = _payload_for_test(
        obs_setup=setup_two_test_rows, test_id=setup_two_test_rows.test_id_1
    )
    observation_id, inserted = repo.upsert_from_payload(payload)

    assert inserted is True
    obs = repo.get_by_observation_id(observation_id)
    assert obs is not None
    assert obs.test_id == setup_two_test_rows.test_id_1
    assert obs.resource_json is None


def test_upsert_from_payload_conflict_returns_existing_id_and_does_not_overwrite_resource_json(
    db_session, setup_two_test_rows
):
    repo = ObservationRepository(db_session)

    payload = _payload_for_test(
        obs_setup=setup_two_test_rows, test_id=setup_two_test_rows.test_id_1
    )
    observation_id_1, inserted_1 = repo.upsert_from_payload(payload)
    assert inserted_1 is True

    repo.update_resource_json(observation_id_1, {"keep": True})
    db_session.expire_all()

    payload_attempt_overwrite = _payload_for_test(
        obs_setup=setup_two_test_rows,
        test_id=setup_two_test_rows.test_id_1,
        resource_json={"try": "overwrite"},
    )
    observation_id_2, inserted_2 = repo.upsert_from_payload(
        payload_attempt_overwrite
    )

    assert inserted_2 is False
    assert observation_id_2 == observation_id_1

    db_session.expire_all()
    obs = repo.get_by_observation_id(observation_id_1)
    assert obs is not None
    assert obs.resource_json == {"keep": True}


def test_update_resource_json_updates_row(db_session, setup_two_test_rows):
    repo = ObservationRepository(db_session)

    payload = _payload_for_test(
        obs_setup=setup_two_test_rows, test_id=setup_two_test_rows.test_id_1
    )
    observation_id, inserted = repo.upsert_from_payload(payload)
    assert inserted is True

    repo.update_resource_json(observation_id, {"resourceType": "Observation"})
    db_session.expire_all()

    obs = repo.get_by_observation_id(observation_id)
    assert obs is not None
    assert obs.resource_json == {"resourceType": "Observation"}

    repo.update_resource_json(observation_id, None)
    db_session.expire_all()

    obs2 = repo.get_by_observation_id(observation_id)
    assert obs2 is not None
    assert obs2.resource_json is None


def test_upsert_many_from_payload_raises_runtime_error_if_unresolved(
    db_session, setup_two_test_rows, monkeypatch
):
    repo = ObservationRepository(db_session)

    # Create an existing row so one payload hits the conflict path.
    payload_existing = _payload_for_test(
        obs_setup=setup_two_test_rows,
        test_id=setup_two_test_rows.test_id_1,
        code="GLU",
    )
    existing_id, inserted = repo.upsert_from_payload(payload_existing)
    assert inserted is True
    assert existing_id is not None

    payload_new = _payload_for_test(
        obs_setup=setup_two_test_rows,
        test_id=setup_two_test_rows.test_id_2,
        code="BUN",
    )

    # Monkeypatch execute so the "fetch ids for conflict rows" query returns no rows,
    # simulating an unexpected race/deletion between the INSERT and SELECT.
    orig_execute = db_session.execute

    def _fake_execute(statement, *args, **kwargs):
        if getattr(statement, "__visit_name__", None) == "select":
            cols = list(getattr(statement, "selected_columns", []))
            if (
                len(cols) == 2
                and getattr(cols[0], "name", None) == "test_id"
                and getattr(cols[1], "name", None) == "observation_id"
            ):
                return SimpleNamespace(all=lambda: [])
        return orig_execute(statement, *args, **kwargs)

    monkeypatch.setattr(db_session, "execute", _fake_execute)

    with pytest.raises(RuntimeError, match="bulk upsert failed to resolve"):
        repo.upsert_many_from_payload([payload_existing, payload_new])

    # Sanity: the existing observation is still present in-session.
    db_session.expire_all()
    assert repo.get_by_test_id(setup_two_test_rows.test_id_1) is not None
