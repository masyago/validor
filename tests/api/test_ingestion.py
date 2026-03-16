from fastapi.testclient import TestClient
import pytest
from unittest.mock import patch

from app.api.routers.ingestion import router

# from datetime import datetime
import io
import uuid
import hashlib
from datetime import datetime, timezone, timedelta


from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.api.routers.dependencies import get_session

# Create a client that includes the router
app = FastAPI()
app.include_router(router)


"""
POST "/ingestions" testing:

- Test the success path (202): Simulate a new, unique file upload (with and without content hash)
- Test the hash mismatch (400): Send a file where the client-provided hash doesn't match the server-calculated one.
- Test the duplicate with same content (200): Mock the database call to return an existing record with a matching hash.
- Test the duplicate with different content (409): Mock the database call to return an existing record with a different hash.
- Test the payload too large (413): Simulate a request with a Content-Length header that is too big.
"""


# Override the database dependency to use test database
@pytest.fixture
def client(db_session):
    """Create a test client with overridden database dependency."""

    def override_get_session():
        yield db_session

    app.dependency_overrides[get_session] = override_get_session
    test_client = TestClient(app)
    yield test_client
    app.dependency_overrides.clear()


# Test 202_ACCEPTED response. HTTP request variants: content hash provided and
# not provided.


# @pytest.mark.skip(reason="Works ok. Isolating other tests")
@pytest.mark.parametrize("include_content_hash", [True, False])
def test_202_success(
    client,
    valid_form_data,
    valid_csv_file,
    content_sha256,
    include_content_hash,
    db_session,
):
    # Add optional hash to the form data if this test case requires it
    if include_content_hash:
        valid_form_data["content_sha256"] = content_sha256

    response = client.post(
        "/ingestions",
        data=valid_form_data,
        files=valid_csv_file,
    )

    response_data = response.json()

    assert response.status_code == 202
    assert "ingestion_id" in response_data
    assert response_data["status"] == "RECEIVED"
    assert "Ingestion request received" in response_data["message"]

    # Checks that the ingestion_id in response is valid uuid
    try:
        uuid.UUID(response_data["ingestion_id"])
    except ValueError:
        assert False, "ingestion_id is not a valid UUID"

    # Verify database record was created
    from app.persistence.models.core import Ingestion, RawData
    from sqlalchemy import select

    ingestion_record = db_session.scalars(
        select(Ingestion).where(
            Ingestion.instrument_id == valid_form_data["instrument_id"],
            Ingestion.run_id == valid_form_data["run_id"],
        )
    ).first()

    assert ingestion_record is not None
    assert ingestion_record.instrument_id == valid_form_data["instrument_id"]
    assert ingestion_record.run_id == valid_form_data["run_id"]
    assert ingestion_record.server_sha256 == content_sha256

    # Verify raw data was stored
    raw_data_record = db_session.scalars(
        select(RawData).where(
            RawData.ingestion_id == ingestion_record.ingestion_id
        )
    ).first()

    assert raw_data_record is not None
    assert raw_data_record.content_size_bytes > 0


# Test 400 Hash Mismatch. Content hash provided by client doesn't match server
# generated hash
def test_400_hash_mismatch(client, valid_form_data, valid_csv_file):
    mismatched_hash = "incorrect_content_hash"

    valid_form_data["content_sha256"] = mismatched_hash

    response = client.post(
        "/ingestions",
        data=valid_form_data,
        files=valid_csv_file,
    )

    response_data = response.json()

    assert response.status_code == 400
    assert "CONTENT_HASH_MISMATCH" in response_data["detail"]["code"]
    assert "integrity check failed" in response_data["detail"]["message"]


# Test 200 Duplicate - OK when submit a duplicate with matching content
def test_200_duplicate_ok(
    client, valid_form_data, valid_csv_file, content_sha256, db_session
):
    # existing_ingestion_id = str(uuid.uuid4())

    # Mock the database lookup to return existing record with matching hash
    # with patch("routers.ingestion.get_existing_ingestion") as mock_db:
    #     mock_db.return_value = (existing_ingestion_id, content_sha256)

    from app.persistence.models.core import Ingestion, RawData
    from datetime import datetime
    from app.core.ingestion_status_enums import IngestionStatus

    # Create an existing ingestion and add it to the database
    existing_ingestion_id = uuid.uuid4()
    existing_record = Ingestion(
        ingestion_id=existing_ingestion_id,
        instrument_id=valid_form_data["instrument_id"],
        run_id=valid_form_data["run_id"],
        uploader_id=valid_form_data["uploader_id"],
        spec_version=valid_form_data["spec_version"],
        uploader_received_at=datetime.now(),
        api_received_at=datetime.now(),
        submitted_sha256=None,
        server_sha256=content_sha256,  # Same hash as new submission
        status=IngestionStatus.PROCESSING,
        source_filename="existing_file.csv",
    )

    db_session.add(existing_record)
    db_session.commit()
    db_session.flush()  # To make ingestion_id available for RawData

    raw_data_record = RawData(
        ingestion_id=existing_ingestion_id,
        content_bytes=b"test csv content",  # Raw file bytes
        content_size_bytes=len(b"test csv content"),
    )

    db_session.add(raw_data_record)
    db_session.commit()

    response = client.post(
        "/ingestions",
        data=valid_form_data,
        files=valid_csv_file,
    )

    response_data = response.json()

    assert response.status_code == 200
    assert response_data["existing_ingestion_id"] == str(
        existing_record.ingestion_id
    )
    assert str(existing_record.ingestion_id) in response.headers["Location"]
    assert "already submitted" in response_data["message"]


# Test 409 duplicate. Duplicate error when content of the duplicate doesn't
# match existing record
# @pytest.mark.skip(reason="No database integration yet.")
def test_409_duplicate_error(
    client, valid_form_data, valid_csv_file, content_sha256, db_session
):
    # existing_ingestion_id = str(uuid.uuid4())
    # existing_content_sha256 = "very_different_hash"

    # with patch("routers.ingestion.get_existing_ingestion") as mock_db:
    #     mock_db.return_value = (existing_ingestion_id, existing_content_sha256)
    from app.persistence.models.core import Ingestion, RawData
    from datetime import datetime
    from app.core.ingestion_status_enums import IngestionStatus

    # Create an existing ingestion and add it to the database. Use intentionally incorrect server_sha256
    existing_ingestion_id = uuid.uuid4()
    existing_record = Ingestion(
        ingestion_id=existing_ingestion_id,
        instrument_id=valid_form_data["instrument_id"],
        run_id=valid_form_data["run_id"],
        uploader_id=valid_form_data["uploader_id"],
        spec_version=valid_form_data["spec_version"],
        uploader_received_at=datetime.now(),
        api_received_at=datetime.now(),
        submitted_sha256=None,
        server_sha256="very_wrong_hash",  # Hash differs from the new submission
        status=IngestionStatus.PROCESSING,
        source_filename="existing_file.csv",
    )

    db_session.add(existing_record)
    db_session.commit()
    db_session.flush()  # To make ingestion_id available for RawData

    raw_data_record = RawData(
        ingestion_id=existing_ingestion_id,
        content_bytes=b"test csv content",  # Raw file bytes
        content_size_bytes=len(b"test csv content"),
    )

    db_session.add(raw_data_record)
    db_session.commit()

    response = client.post(
        "/ingestions",
        data=valid_form_data,
        files=valid_csv_file,
    )

    response_data = response.json()

    assert response.status_code == 409
    assert response_data["detail"]["code"] == "RUN_ID_CONTENT_MISMATCH"
    assert response_data["detail"]["existing_ingestion_id"] == str(
        existing_ingestion_id
    )
    assert (
        response_data["detail"]["conflict_key"]["instrument_id"]
        == valid_form_data["instrument_id"]
    )
    assert (
        response_data["detail"]["conflict_key"]["run_id"]
        == valid_form_data["run_id"]
    )
    assert (
        response_data["detail"]["hashes"]["existing"]
        == existing_record.server_sha256
    )
    assert response_data["detail"]["hashes"]["submitted"] == content_sha256
    assert (
        "ingestion already exists"
        in response_data["detail"]["message"].lower()
    )


def test_413_content_too_large(client, valid_form_data, valid_csv_file):

    response = client.post(
        "/ingestions",
        data=valid_form_data,
        files=valid_csv_file,
        headers={"Content-Length": "3000000"},  # 3 MB > 1 MB limit
    )

    response_data = response.json()

    assert response.status_code == 413
    assert response_data["detail"]["code"] == "PAYLOAD_TOO_LARGE"
    assert "exceeds" in response_data["detail"]["message"].lower()
    assert response_data["detail"]["max_bytes"] == 1000000


"""
GET endpoints testing
"""


def _make_patient_id() -> str:
    return f"PAT-{uuid.uuid4()}"


def _seed_ingestion(
    db_session,
    *,
    ingestion_id: uuid.UUID | None = None,
    instrument_id: str | None = None,
    run_id: str | None = None,
    uploader_id: str = "test-uploader",
    spec_version: str = "analyzer_csv_v1",
):
    from app.persistence.models.core import Ingestion
    from app.core.ingestion_status_enums import IngestionStatus

    ingestion_id = ingestion_id or uuid.uuid4()
    now = datetime.now(timezone.utc)

    instrument_id = instrument_id or f"TEST-INSTR-{uuid.uuid4()}"
    run_id = run_id or f"TEST-RUN-{uuid.uuid4()}"

    ingestion = Ingestion(
        ingestion_id=ingestion_id,
        instrument_id=instrument_id,
        run_id=run_id,
        uploader_id=uploader_id,
        spec_version=spec_version,
        uploader_received_at=now,
        api_received_at=now,
        submitted_sha256=None,
        server_sha256="0" * 64,
        status=IngestionStatus.RECEIVED.value,
        error_code=None,
        error_detail=None,
        source_filename="seed.csv",
    )

    db_session.add(ingestion)
    db_session.commit()
    return ingestion


def test_post_process_ingestion_202_queues_task_and_returns_location(
    client, db_session
):
    ingestion = _seed_ingestion(db_session)

    with patch(
        "app.api.routers.ingestion.process_ingestion_task"
    ) as mock_task:
        response = client.post(f"/ingestions/{ingestion.ingestion_id}/process")

    assert response.status_code == 202
    assert (
        response.headers["Location"]
        == f"/v1/ingestions/{ingestion.ingestion_id}"
    )
    mock_task.assert_called_once_with(ingestion.ingestion_id)


def test_post_process_ingestion_404_when_ingestion_missing(client):
    missing_id = uuid.uuid4()

    with patch(
        "app.api.routers.ingestion.process_ingestion_task"
    ) as mock_task:
        response = client.post(f"/ingestions/{missing_id}/process")

    assert response.status_code == 404
    data = response.json()
    assert data["detail"]["code"] == "INGESTION_NOT_FOUND"
    mock_task.assert_not_called()


def test_post_process_ingestion_422_invalid_uuid(client):
    response = client.post("/ingestions/not-a-uuid/process")
    assert response.status_code == 422


def _seed_panel(
    db_session, *, ingestion_id: uuid.UUID, patient_id: str, panel_code: str
):
    from app.persistence.models.parsing import Panel

    panel = Panel(
        panel_id=uuid.uuid4(),
        ingestion_id=ingestion_id,
        patient_id=patient_id,
        panel_code=panel_code,
        sample_id=None,
        collection_timestamp=datetime.now(timezone.utc),
    )
    db_session.add(panel)
    db_session.commit()
    return panel


def _seed_test(
    db_session, *, panel_id: uuid.UUID, row_number: int, test_code: str
):
    from app.persistence.models.parsing import Test

    test = Test(
        test_id=uuid.uuid4(),
        panel_id=panel_id,
        row_number=row_number,
        test_code=test_code,
        test_name=None,
        analyte_type="NUM",
        result_raw="1",
        units_raw="mg/dL",
        result_value_num=1.0,
        result_comparator=None,
        ref_low_raw=None,
        ref_high_raw=None,
        flag=None,
    )
    db_session.add(test)
    db_session.commit()
    return test


def _seed_diagnostic_report(
    db_session,
    *,
    diagnostic_report_id: uuid.UUID,
    ingestion_id: uuid.UUID,
    panel_id: uuid.UUID,
    patient_id: str,
    panel_code: str,
    effective_at: datetime,
    resource_json: dict | None,
):
    from app.persistence.models.normalization import DiagnosticReport

    dr = DiagnosticReport(
        diagnostic_report_id=diagnostic_report_id,
        ingestion_id=ingestion_id,
        panel_id=panel_id,
        patient_id=patient_id,
        panel_code=panel_code,
        effective_at=effective_at,
        normalized_at=effective_at + timedelta(seconds=1),
        resource_json=resource_json,
        status="FINAL",
    )
    db_session.add(dr)
    db_session.commit()
    return dr


def _seed_observation(
    db_session,
    *,
    observation_id: uuid.UUID,
    test_id: uuid.UUID,
    diagnostic_report_id: uuid.UUID,
    ingestion_id: uuid.UUID,
    patient_id: str,
    code: str,
    effective_at: datetime,
    resource_json: dict | None,
):
    from app.persistence.models.normalization import Observation

    obs = Observation(
        observation_id=observation_id,
        test_id=test_id,
        diagnostic_report_id=diagnostic_report_id,
        ingestion_id=ingestion_id,
        patient_id=patient_id,
        code=code,
        display=None,
        effective_at=effective_at,
        normalized_at=effective_at + timedelta(seconds=1),
        value_num=1.0,
        value_text=None,
        comparator=None,
        unit="mg/dL",
        ref_low_num=None,
        ref_high_num=None,
        flag_analyzer_interpretation=None,
        flag_system_interpretation=None,
        discrepancy=None,
        status="FINAL",
        resource_json=resource_json,
    )
    db_session.add(obs)
    db_session.commit()
    return obs


def _seed_patient_resources(
    db_session,
    *,
    patient_id: str,
    ingestion_id: uuid.UUID | None = None,
    instrument_id: str | None = None,
    run_id: str | None = None,
    uploader_id: str = "test-uploader",
    spec_version: str = "analyzer_csv_v1",
):
    """Seeds 4 DR + 4 Obs for a patient (and the required Panel/Test rows).

    Returns a dict with keys:
    ingestion_id, panels, diagnostic_reports, observations
    """

    ingestion = _seed_ingestion(
        db_session,
        ingestion_id=ingestion_id,
        instrument_id=instrument_id,
        run_id=run_id,
        uploader_id=uploader_id,
        spec_version=spec_version,
    )
    base_time = datetime.now(timezone.utc)

    panels = []
    diagnostic_reports = []
    observations = []
    for i in range(4):
        panel_code = f"PANEL-{i}"
        panel = _seed_panel(
            db_session,
            ingestion_id=ingestion.ingestion_id,
            patient_id=patient_id,
            panel_code=panel_code,
        )
        panels.append(panel)

        # Newer effective_at first (patient endpoints sort DESC).
        effective_at = base_time - timedelta(minutes=i)

        dr_id = uuid.UUID(int=i + 1)
        dr_resource = {"resourceType": "DiagnosticReport", "id": str(dr_id)}
        dr = _seed_diagnostic_report(
            db_session,
            diagnostic_report_id=dr_id,
            ingestion_id=ingestion.ingestion_id,
            panel_id=panel.panel_id,
            patient_id=patient_id,
            panel_code=panel_code,
            effective_at=effective_at,
            resource_json=dr_resource,
        )
        diagnostic_reports.append(dr)

        test = _seed_test(
            db_session,
            panel_id=panel.panel_id,
            row_number=i + 1,
            test_code=f"TEST-{i}",
        )

        obs_id = uuid.UUID(int=100 + i)
        obs_resource = {"resourceType": "Observation", "id": str(obs_id)}
        obs = _seed_observation(
            db_session,
            observation_id=obs_id,
            test_id=test.test_id,
            diagnostic_report_id=dr.diagnostic_report_id,
            ingestion_id=ingestion.ingestion_id,
            patient_id=patient_id,
            code=f"CODE-{i}",
            effective_at=effective_at,
            resource_json=obs_resource,
        )
        observations.append(obs)

    return {
        "ingestion_id": ingestion.ingestion_id,
        "panels": panels,
        "diagnostic_reports": diagnostic_reports,
        "observations": observations,
    }


def test_get_ingestion_200(client, db_session):
    ingestion = _seed_ingestion(db_session)

    response = client.get(f"/ingestions/{ingestion.ingestion_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["ingestion_id"] == str(ingestion.ingestion_id)
    assert data["status"] == "RECEIVED"
    assert "api_received_at" in data


def test_get_ingestion_404(client):
    missing_id = uuid.uuid4()
    response = client.get(f"/ingestions/{missing_id}")
    assert response.status_code == 404
    data = response.json()
    assert data["detail"]["ingestion_id"] == str(missing_id)


def test_get_ingestion_422_invalid_uuid(client):
    response = client.get("/ingestions/not-a-uuid")
    assert response.status_code == 422


def test_get_diagnostic_reports_by_ingestion_empty_list_when_ingestion_exists(
    client, db_session
):
    ingestion = _seed_ingestion(db_session)
    response = client.get(
        f"/ingestions/{ingestion.ingestion_id}/diagnostic-reports"
    )
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.parametrize("include_json", [0, 1])
def test_get_diagnostic_reports_by_ingestion_include_json(
    client, db_session, include_json
):
    patient_id = _make_patient_id()
    seeded = _seed_patient_resources(db_session, patient_id=patient_id)
    ingestion_id = seeded["ingestion_id"]

    response = client.get(
        f"/ingestions/{ingestion_id}/diagnostic-reports?include_json={include_json}"
    )
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 4
    # Ingestion endpoint orders by diagnostic_report_id asc.
    assert rows[0]["diagnostic_report_id"] == str(uuid.UUID(int=1))
    if include_json == 1:
        assert rows[0]["resource_json"]["resourceType"] == "DiagnosticReport"
    else:
        assert rows[0]["resource_json"] is None


def test_get_diagnostic_reports_by_ingestion_include_json_invalid_422(
    client, db_session
):
    ingestion = _seed_ingestion(db_session)
    response = client.get(
        f"/ingestions/{ingestion.ingestion_id}/diagnostic-reports?include_json=2"
    )
    assert response.status_code == 422


def test_get_diagnostic_reports_by_ingestion_404_when_ingestion_missing(
    client,
):
    missing_id = uuid.uuid4()
    response = client.get(f"/ingestions/{missing_id}/diagnostic-reports")
    assert response.status_code == 404
    data = response.json()
    assert data["detail"]["ingestion_id"] == str(missing_id)


def test_get_observations_by_ingestion_empty_list_when_ingestion_exists(
    client, db_session
):
    ingestion = _seed_ingestion(db_session)
    response = client.get(f"/ingestions/{ingestion.ingestion_id}/observations")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.parametrize("include_json", [0, 1])
def test_get_observations_by_ingestion_pagination_and_include_json(
    client, db_session, include_json
):
    patient_id = _make_patient_id()
    seeded = _seed_patient_resources(db_session, patient_id=patient_id)
    ingestion_id = seeded["ingestion_id"]

    # Default pagination returns all 4.
    response = client.get(
        f"/ingestions/{ingestion_id}/observations?include_json={include_json}"
    )
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 4
    # Ingestion endpoint orders by observation_id asc.
    assert rows[0]["observation_id"] == str(uuid.UUID(int=100))
    if include_json == 1:
        assert rows[0]["resource_json"]["resourceType"] == "Observation"
    else:
        assert rows[0]["resource_json"] is None

    # limit and offset query parameters provided
    response2 = client.get(
        f"/ingestions/{ingestion_id}/observations?include_json={include_json}&limit=1&offset=1"
    )
    assert response2.status_code == 200
    rows2 = response2.json()
    assert len(rows2) == 1
    assert rows2[0]["observation_id"] == str(uuid.UUID(int=101))

    # Offset beyond total returns empty list (still code 200)
    response3 = client.get(
        f"/ingestions/{ingestion_id}/observations?include_json={include_json}&limit=10&offset=999"
    )
    assert response3.status_code == 200
    assert response3.json() == []


@pytest.mark.parametrize(
    "qs",
    [
        "include_json=2",
        "limit=0",
        "offset=-1",
        "include_json=1&limit=0",
    ],
)
def test_get_observations_by_ingestion_query_validation_422(
    client, db_session, qs
):
    ingestion = _seed_ingestion(db_session)
    response = client.get(
        f"/ingestions/{ingestion.ingestion_id}/observations?{qs}"
    )
    assert response.status_code == 422


def test_get_observations_by_ingestion_404_when_ingestion_missing(client):
    missing_id = uuid.uuid4()
    response = client.get(f"/ingestions/{missing_id}/observations")
    assert response.status_code == 404
    data = response.json()
    assert data["detail"]["ingestion_id"] == str(missing_id)


def test_get_diagnostic_reports_by_patient_empty_list_when_patient_exists(
    client, db_session
):
    patient_id = _make_patient_id()
    ingestion = _seed_ingestion(db_session)
    _seed_panel(
        db_session,
        ingestion_id=ingestion.ingestion_id,
        patient_id=patient_id,
        panel_code="PANEL-EXISTS-NO-DR",
    )

    response = client.get(f"/patients/{patient_id}/diagnostic-reports")
    assert response.status_code == 200
    assert response.json() == []


def test_get_diagnostic_reports_by_patient_404_when_patient_missing(client):
    missing_patient_id = _make_patient_id()
    response = client.get(f"/patients/{missing_patient_id}/diagnostic-reports")
    assert response.status_code == 404
    data = response.json()
    assert data["detail"]["patient_id"] == missing_patient_id


def test_get_diagnostic_reports_by_patient_422_invalid_patient_id(client):
    response = client.get("/patients/not-a-patient-id/diagnostic-reports")
    assert response.status_code == 422


@pytest.mark.parametrize("include_json", [0, 1])
def test_get_diagnostic_reports_by_patient_pagination_and_include_json(
    client, db_session, include_json
):
    patient_id = _make_patient_id()
    seeded = _seed_patient_resources(db_session, patient_id=patient_id)

    response = client.get(
        f"/patients/{patient_id}/diagnostic-reports?include_json={include_json}"
    )
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 4

    # Patient endpoint orders by effective_at desc (newest first).
    # Our seeder makes i=0 newest.
    assert rows[0]["panel_code"] == "PANEL-0"
    if include_json == 1:
        assert rows[0]["resource_json"]["resourceType"] == "DiagnosticReport"
    else:
        assert rows[0]["resource_json"] is None

    # Limit and offset query parameters provided
    response2 = client.get(
        f"/patients/{patient_id}/diagnostic-reports?include_json={include_json}&limit=1&offset=1"
    )
    assert response2.status_code == 200
    rows2 = response2.json()
    assert len(rows2) == 1
    assert rows2[0]["panel_code"] == "PANEL-1"


@pytest.mark.parametrize(
    "qs",
    [
        "include_json=2",
        "limit=0",
        "offset=-1",
    ],
)
def test_get_diagnostic_reports_by_patient_query_validation_422(
    client, db_session, qs
):
    patient_id = _make_patient_id()
    response = client.get(f"/patients/{patient_id}/diagnostic-reports?{qs}")
    assert response.status_code == 422


def test_get_observations_by_patient_empty_list_when_patient_exists(
    client, db_session
):
    patient_id = _make_patient_id()
    ingestion = _seed_ingestion(db_session)
    _seed_panel(
        db_session,
        ingestion_id=ingestion.ingestion_id,
        patient_id=patient_id,
        panel_code="PANEL-EXISTS-NO-OBS",
    )

    response = client.get(f"/patients/{patient_id}/observations")
    assert response.status_code == 200
    assert response.json() == []


def test_get_observations_by_patient_404_when_patient_missing(client):
    missing_patient_id = _make_patient_id()
    response = client.get(f"/patients/{missing_patient_id}/observations")
    assert response.status_code == 404
    data = response.json()
    assert data["detail"]["patient_id"] == missing_patient_id


def test_get_observations_by_patient_422_invalid_patient_id(client):
    response = client.get("/patients/not-a-patient-id/observations")
    assert response.status_code == 422


@pytest.mark.parametrize("include_json", [0, 1])
def test_get_observations_by_patient_pagination_and_include_json(
    client, db_session, include_json
):
    patient_id = _make_patient_id()
    _seed_patient_resources(db_session, patient_id=patient_id)

    response = client.get(
        f"/patients/{patient_id}/observations?include_json={include_json}"
    )
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 4

    # Patient endpoint orders by effective_at desc (newest first).
    assert rows[0]["code"] == "CODE-0"
    if include_json == 1:
        assert rows[0]["resource_json"]["resourceType"] == "Observation"
    else:
        assert rows[0]["resource_json"] is None

    response2 = client.get(
        f"/patients/{patient_id}/observations?include_json={include_json}&limit=1&offset=1"
    )
    assert response2.status_code == 200
    rows2 = response2.json()
    assert len(rows2) == 1
    assert rows2[0]["code"] == "CODE-1"

    # Offset beyond total returns empty list (still 200)
    response3 = client.get(
        f"/patients/{patient_id}/observations?include_json={include_json}&limit=10&offset=999"
    )
    assert response3.status_code == 200
    assert response3.json() == []


@pytest.mark.parametrize(
    "qs",
    [
        "include_json=2",
        "limit=0",
        "offset=-1",
    ],
)
def test_get_observations_by_patient_query_validation_422(
    client, db_session, qs
):
    patient_id = _make_patient_id()
    response = client.get(f"/patients/{patient_id}/observations?{qs}")
    assert response.status_code == 422
