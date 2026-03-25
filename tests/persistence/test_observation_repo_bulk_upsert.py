from __future__ import annotations

from datetime import datetime, timezone
import uuid

from sqlalchemy import select

from app.persistence.models.core import Ingestion
from app.persistence.models.normalization import DiagnosticReport
from app.persistence.models.normalization import Observation
from app.persistence.models.parsing import Panel
from app.persistence.models.parsing import Test
from app.persistence.repositories.observation_repo import ObservationRepository


def _base_payload(
    *,
    test_id: uuid.UUID,
    diagnostic_report_id: uuid.UUID,
    ingestion_id: uuid.UUID,
    patient_id: str,
    code: str,
) -> dict:
    return {
        "test_id": test_id,
        "diagnostic_report_id": diagnostic_report_id,
        "ingestion_id": ingestion_id,
        "patient_id": patient_id,
        "code": code,
        "display": "Glucose",
        "effective_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "normalized_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "value_num": 1.0,
        "value_text": None,
        "comparator": None,
        "unit": "mg/dL",
        "ref_low_num": 0.0,
        "ref_high_num": 2.0,
        "flag_analyzer_interpretation": None,
        "flag_system_interpretation": "UNKNOWN",
        "discrepancy": None,
        "resource_json": None,
        "status": "FINAL",
    }


def test_upsert_many_from_payload_inserts_and_is_idempotent(db_session):
    ingestion = Ingestion(
        ingestion_id=uuid.uuid4(),
        instrument_id="INST-1",
        run_id="RUN-1",
        uploader_id="uploader_001",
        spec_version="analyzer_csv_v1",
        uploader_received_at=datetime(
            2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc
        ),
        api_received_at=datetime(2026, 2, 15, 12, 10, 0, tzinfo=timezone.utc),
        server_sha256="0" * 64,
        status="RECEIVED",
        source_filename="fixture.csv",
    )
    db_session.add(ingestion)
    db_session.flush()

    panel = Panel(
        panel_id=uuid.uuid4(),
        ingestion_id=ingestion.ingestion_id,
        sample_id="SAM-1",
        patient_id="PAT-1",
        panel_code="BMP",
        collection_timestamp=datetime(
            2026, 1, 28, 16, 5, 33, tzinfo=timezone.utc
        ),
    )
    db_session.add(panel)
    db_session.flush()

    # Normalizer uses ParsingTest rows and diagnostic_report_id from normalized table;
    # for this repository test, we only care that the FK values are syntactically UUIDs.
    # The DB schema requires a real diagnostic_report row, so reuse the normalizer path
    # setup by creating observations via the repository after Phase 1 creates DR.
    #
    # Instead, we’ll insert minimal Observation rows through the repo and assert behavior.

    t1 = Test(
        test_id=uuid.uuid4(),
        panel_id=panel.panel_id,
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
        test_id=uuid.uuid4(),
        panel_id=panel.panel_id,
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

    dr = DiagnosticReport(
        diagnostic_report_id=uuid.uuid4(),
        ingestion_id=ingestion.ingestion_id,
        panel_id=panel.panel_id,
        patient_id=panel.patient_id,
        panel_code=panel.panel_code,
        effective_at=panel.collection_timestamp,
        normalized_at=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        resource_json=None,
        status="FINAL",
    )
    db_session.add(dr)
    db_session.flush()

    patient_id = panel.patient_id

    payloads = [
        _base_payload(
            test_id=t1.test_id,
            diagnostic_report_id=dr.diagnostic_report_id,
            ingestion_id=ingestion.ingestion_id,
            patient_id=patient_id,
            code="GLU",
        ),
        _base_payload(
            test_id=t2.test_id,
            diagnostic_report_id=dr.diagnostic_report_id,
            ingestion_id=ingestion.ingestion_id,
            patient_id=patient_id,
            code="BUN",
        ),
    ]

    repo = ObservationRepository(db_session)

    by_test_id_1, inserted_count_1 = repo.upsert_many_from_payload(payloads)
    assert inserted_count_1 == 2
    assert set(by_test_id_1.keys()) == {t1.test_id, t2.test_id}

    # Idempotent rerun should insert 0 but still resolve ids.
    by_test_id_2, inserted_count_2 = repo.upsert_many_from_payload(payloads)
    assert inserted_count_2 == 0
    assert by_test_id_2 == by_test_id_1

    obs = (
        db_session.execute(
            select(Observation).where(
                Observation.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    assert len(obs) == 2
