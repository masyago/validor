import csv
import io
import time
from collections.abc import Callable
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import requests

from tests.e2e.conftest import E2EConfig


pytestmark = pytest.mark.e2e


def _make_unique_run_id(*, prefix: str = "e2e") -> str:
    # Timestamp keeps it readable; UUID suffix avoids same-second collisions.
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:8]}"


def _load_and_prepare_csv_bytes(
    *,
    fixture_path: str,
    run_id: str,
    mutate_csv_text: Callable[[str], str] | None = None,
) -> bytes:
    with open(fixture_path, "rb") as f:
        fixture_bytes = f.read()

    fixture_text = fixture_bytes.decode("utf-8")
    csv_text = _replace_csv_column(fixture_text, "run_id", run_id)
    if mutate_csv_text is not None:
        csv_text = mutate_csv_text(csv_text)
    return csv_text.encode("utf-8")


def _mutate_first_row_column(
    csv_text: str, *, column_name: str, new_value: str
) -> str:
    src = io.StringIO(csv_text)
    reader = csv.DictReader(src)

    out = io.StringIO()
    writer: csv.DictWriter | None = None

    for row_index, row in enumerate(reader):
        if row_index == 0:
            row[column_name] = new_value

        if writer is None:
            writer = csv.DictWriter(
                out, fieldnames=list(row.keys()), lineterminator="\n"
            )
            writer.writeheader()
        writer.writerow(row)

    return out.getvalue()


def _post_ingestion(
    *,
    e2e_config: E2EConfig,
    run_id: str,
    csv_bytes: bytes,
    content_sha256: str,
) -> requests.Response:
    instrument_id = "CANONICAL_CHEM_ANALYZER_V1"

    form = {
        "uploader_id": "e2e_uploader",
        "spec_version": "analyzer_csv_v1",
        "instrument_id": instrument_id,
        "run_id": run_id,
        "uploader_received_at": datetime.now(timezone.utc).isoformat(),
        "content_sha256": content_sha256,
    }

    files = {
        "file": (f"{run_id}.csv", io.BytesIO(csv_bytes), "text/csv"),
    }

    return requests.post(
        f"{e2e_config.api_v1_url}/ingestions",
        data=form,
        files=files,
        timeout=30,
    )


def _extract_ingestion_id_from_post(post_response: requests.Response) -> str:
    post_json = post_response.json()
    if post_response.status_code == 202:
        return post_json["ingestion_id"]
    return post_json["existing_ingestion_id"]


def _replace_csv_column(
    csv_text: str, column_name: str, new_value: str
) -> str:
    src = io.StringIO(csv_text)
    reader = csv.DictReader(src)

    out = io.StringIO()
    writer = None

    for row in reader:
        row[column_name] = new_value
        if writer is None:
            writer = csv.DictWriter(
                out, fieldnames=list(row.keys()), lineterminator="\n"
            )
            writer.writeheader()
        writer.writerow(row)

    return out.getvalue()


def _sha256_hex(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def _poll_ingestion_status(
    *,
    e2e_config: E2EConfig,
    ingestion_id: str,
    timeout_s: float,
) -> dict:
    deadline = time.monotonic() + timeout_s
    last_json: dict | None = None

    while time.monotonic() < deadline:
        resp = requests.get(
            f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}", timeout=5
        )
        resp.raise_for_status()
        last_json = resp.json()

        status = last_json.get("status")
        if status in {"COMPLETED", "FAILED VALIDATION", "FAILED"}:
            return last_json

        time.sleep(0.5)

    raise AssertionError(
        f"Timed out waiting for ingestion {ingestion_id} to finish. Last response: {last_json}"
    )


@pytest.fixture(scope="module")
def completed_ingestion(e2e_config: E2EConfig) -> dict[str, str]:
    """Creates one completed ingestion for read-endpoint e2e checks."""
    run_id = _make_unique_run_id(prefix="e2e_completed")
    csv_bytes = _load_and_prepare_csv_bytes(
        fixture_path="tests/fixtures/csv/valid_csv_20260128_004.csv",
        run_id=run_id,
    )

    post_response = _post_ingestion(
        e2e_config=e2e_config,
        run_id=run_id,
        csv_bytes=csv_bytes,
        content_sha256=_sha256_hex(csv_bytes),
    )
    assert post_response.status_code in {200, 202}, post_response.text
    ingestion_id = _extract_ingestion_id_from_post(post_response)

    process_response = requests.post(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/process",
        timeout=10,
    )
    assert process_response.status_code == 202, process_response.text

    ingestion_status = _poll_ingestion_status(
        e2e_config=e2e_config,
        ingestion_id=ingestion_id,
        timeout_s=60,
    )
    assert ingestion_status["status"] == "COMPLETED", ingestion_status

    dr_response = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/diagnostic-reports",
        timeout=10,
    )
    assert dr_response.status_code == 200, dr_response.text
    diagnostic_reports = dr_response.json()
    assert isinstance(diagnostic_reports, list)
    assert len(diagnostic_reports) >= 1

    patient_id = diagnostic_reports[0]["patient_id"]
    return {"ingestion_id": ingestion_id, "patient_id": patient_id}


def test_e2e_happy_path_ingest_process_and_read_resources(
    e2e_config: E2EConfig,
) -> None:
    """
    Black-box happy path: ingest CSV -> process -> read FHIR resources.
    """

    run_id = _make_unique_run_id(prefix="e2e_happy")
    csv_bytes = _load_and_prepare_csv_bytes(
        fixture_path="tests/fixtures/csv/valid_csv_20260128_004.csv",
        run_id=run_id,
    )

    post_response = _post_ingestion(
        e2e_config=e2e_config,
        run_id=run_id,
        csv_bytes=csv_bytes,
        content_sha256=_sha256_hex(csv_bytes),
    )

    # Accept both "new" (202) and "duplicate" (200) so the test stays stable
    # in case of the same run_id. If the assertion fails, displays response
    # body to provide details.
    assert post_response.status_code in {200, 202}, post_response.text
    ingestion_id = _extract_ingestion_id_from_post(post_response)

    process_response = requests.post(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/process",
        timeout=10,
    )
    # If the assertion fails, displays response body to provide details.
    assert process_response.status_code == 202, process_response.text

    ingestion_status = _poll_ingestion_status(
        e2e_config=e2e_config,
        ingestion_id=ingestion_id,
        timeout_s=float(
            # Parsing/normalization can take a while on cold starts.
            60
        ),
    )

    assert (
        ingestion_status["status"] == "COMPLETED"
    ), f"Ingestion did not complete: {ingestion_status}"

    dr_response = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/diagnostic-reports",
        timeout=10,
    )
    dr_response.raise_for_status()
    diagnostic_reports = dr_response.json()
    assert dr_response.status_code == 200, dr_response.text
    assert isinstance(diagnostic_reports, list)
    assert len(diagnostic_reports) >= 1

    patient_id = diagnostic_reports[0]["patient_id"]

    obs_response = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/observations?limit=5&offset=0",
        timeout=10,
    )
    obs_response.raise_for_status()
    observations = obs_response.json()
    assert obs_response.status_code == 200, obs_response.text
    assert isinstance(observations, list)
    assert len(observations) >= 1

    # Patient-related read endpoints (include_json=1 exercises conditional resource JSON)
    patient_dr_response = requests.get(
        f"{e2e_config.api_v1_url}/patients/{patient_id}/diagnostic-reports?include_json=1&limit=5&offset=0",
        timeout=10,
    )
    patient_dr_response.raise_for_status()
    patient_diagnostic_reports = patient_dr_response.json()
    assert patient_dr_response.status_code == 200, patient_dr_response.text
    assert isinstance(patient_diagnostic_reports, list)
    assert len(patient_diagnostic_reports) >= 1
    assert "resource_json" in patient_diagnostic_reports[0]

    patient_obs_response = requests.get(
        f"{e2e_config.api_v1_url}/patients/{patient_id}/observations?limit=5&offset=0",
        timeout=10,
    )
    patient_obs_response.raise_for_status()
    patient_observations = patient_obs_response.json()
    assert patient_obs_response.status_code == 200, patient_obs_response.text
    assert isinstance(patient_observations, list)
    assert len(patient_observations) >= 1


def test_e2e_validation_invalid_csv_results_in_failed_validation_and_no_resources(
    e2e_config: E2EConfig,
) -> None:
    """
    Uploader sends invalid CSV. Uploader gets 202 or 200 response code and assigned
    `ingestion_id`. When client requests the ingestion status, gets "FAILED
    VALIDATION". DiagnosticReports and Observations don't exist for the
    `ingestion_id`.
    """
    run_id = _make_unique_run_id(prefix="e2e_invalid")
    csv_bytes = _load_and_prepare_csv_bytes(
        fixture_path="tests/fixtures/csv/invalid_csv_missing_fields_20260128_003.csv",
        run_id=run_id,
    )

    post_response = _post_ingestion(
        e2e_config=e2e_config,
        run_id=run_id,
        csv_bytes=csv_bytes,
        content_sha256=_sha256_hex(csv_bytes),
    )

    assert post_response.status_code in {200, 202}, post_response.text
    ingestion_id = _extract_ingestion_id_from_post(post_response)

    process_response = requests.post(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/process",
        timeout=10,
    )
    # If the assertion fails, displays response body to provide details.
    assert process_response.status_code == 202, process_response.text

    ingestion_status = _poll_ingestion_status(
        e2e_config=e2e_config,
        ingestion_id=ingestion_id,
        timeout_s=float(
            # Parsing/normalization can take a while on cold starts.
            60
        ),
    )

    assert ingestion_status["status"] == "FAILED VALIDATION"

    dr_response = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/diagnostic-reports",
        timeout=10,
    )
    dr_response.raise_for_status()
    diagnostic_reports = dr_response.json()
    assert dr_response.status_code == 200, dr_response.text
    assert isinstance(diagnostic_reports, list)
    assert len(diagnostic_reports) == 0

    obs_response = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/observations?limit=5&offset=0",
        timeout=10,
    )
    obs_response.raise_for_status()
    observations = obs_response.json()
    assert obs_response.status_code == 200, obs_response.text
    assert isinstance(observations, list)
    assert len(observations) == 0


def test_e2e_ingestions_post_duplicate_same_content_returns_200(
    e2e_config: E2EConfig,
) -> None:
    run_id = _make_unique_run_id(prefix="e2e_dup_ok")
    csv_bytes = _load_and_prepare_csv_bytes(
        fixture_path="tests/fixtures/csv/valid_csv_20260128_004.csv",
        run_id=run_id,
    )
    sha = _sha256_hex(csv_bytes)

    first = _post_ingestion(
        e2e_config=e2e_config,
        run_id=run_id,
        csv_bytes=csv_bytes,
        content_sha256=sha,
    )
    assert first.status_code in {200, 202}, first.text

    second = _post_ingestion(
        e2e_config=e2e_config,
        run_id=run_id,
        csv_bytes=csv_bytes,
        content_sha256=sha,
    )
    assert second.status_code == 200, second.text
    second_json = second.json()
    assert "existing_ingestion_id" in second_json


def test_e2e_ingestions_post_same_run_id_different_content_returns_409(
    e2e_config: E2EConfig,
) -> None:
    run_id = _make_unique_run_id(prefix="e2e_dup_conflict")
    base_bytes = _load_and_prepare_csv_bytes(
        fixture_path="tests/fixtures/csv/valid_csv_20260128_004.csv",
        run_id=run_id,
    )

    first = _post_ingestion(
        e2e_config=e2e_config,
        run_id=run_id,
        csv_bytes=base_bytes,
        content_sha256=_sha256_hex(base_bytes),
    )
    assert first.status_code in {200, 202}, first.text

    # Mutate content while keeping run_id identical to trigger conflict.
    mutated_bytes = _load_and_prepare_csv_bytes(
        fixture_path="tests/fixtures/csv/valid_csv_20260128_004.csv",
        run_id=run_id,
        mutate_csv_text=lambda t: _mutate_first_row_column(
            t, column_name="result", new_value="98.0"
        ),
    )

    second = _post_ingestion(
        e2e_config=e2e_config,
        run_id=run_id,
        csv_bytes=mutated_bytes,
        content_sha256=_sha256_hex(mutated_bytes),
    )
    assert second.status_code == 409, second.text
    detail = second.json().get("detail", {})
    assert detail.get("code") == "RUN_ID_CONTENT_MISMATCH"


def test_e2e_ingestions_content_hash_mismatch_returns_400(
    e2e_config: E2EConfig,
) -> None:
    run_id = _make_unique_run_id(prefix="e2e_hash_mismatch")
    csv_bytes = _load_and_prepare_csv_bytes(
        fixture_path="tests/fixtures/csv/valid_csv_20260128_004.csv",
        run_id=run_id,
    )

    response = _post_ingestion(
        e2e_config=e2e_config,
        run_id=run_id,
        csv_bytes=csv_bytes,
        content_sha256="0" * 64,
    )
    assert response.status_code == 400, response.text
    detail = response.json().get("detail", {})
    assert detail.get("code") == "CONTENT_HASH_MISMATCH"


def test_e2e_read_endpoints_include_json_toggle_controls_resource_json_key(
    e2e_config: E2EConfig,
    completed_ingestion: dict[str, str],
) -> None:
    ingestion_id = completed_ingestion["ingestion_id"]
    patient_id = completed_ingestion["patient_id"]

    dr_no_json = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/diagnostic-reports",
        timeout=10,
    )
    assert dr_no_json.status_code == 200, dr_no_json.text
    dr_no_json_rows = dr_no_json.json()
    assert isinstance(dr_no_json_rows, list)
    assert len(dr_no_json_rows) >= 1
    assert "resource_json" not in dr_no_json_rows[0]

    dr_with_json = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/diagnostic-reports?include_json=1",
        timeout=10,
    )
    assert dr_with_json.status_code == 200, dr_with_json.text
    dr_with_json_rows = dr_with_json.json()
    assert isinstance(dr_with_json_rows, list)
    assert len(dr_with_json_rows) >= 1
    assert "resource_json" in dr_with_json_rows[0]

    patient_dr_with_json = requests.get(
        f"{e2e_config.api_v1_url}/patients/{patient_id}/diagnostic-reports?include_json=1&limit=1&offset=0",
        timeout=10,
    )
    assert patient_dr_with_json.status_code == 200, patient_dr_with_json.text
    patient_rows = patient_dr_with_json.json()
    assert isinstance(patient_rows, list)
    assert len(patient_rows) >= 1
    assert "resource_json" in patient_rows[0]


def test_e2e_read_endpoints_observations_pagination_limit_offset(
    e2e_config: E2EConfig,
    completed_ingestion: dict[str, str],
) -> None:
    ingestion_id = completed_ingestion["ingestion_id"]

    page0 = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/observations?limit=1&offset=0",
        timeout=10,
    )
    assert page0.status_code == 200, page0.text
    rows0 = page0.json()
    assert isinstance(rows0, list)
    assert len(rows0) == 1

    page1 = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/observations?limit=1&offset=1",
        timeout=10,
    )
    assert page1.status_code == 200, page1.text
    rows1 = page1.json()
    assert isinstance(rows1, list)
    assert len(rows1) == 1
    assert rows0[0]["observation_id"] != rows1[0]["observation_id"]


def test_e2e_contract_unknown_ingestion_id_returns_404(
    e2e_config: E2EConfig,
) -> None:
    unknown_id = str(uuid4())

    response = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{unknown_id}",
        timeout=10,
    )
    assert response.status_code == 404, response.text
    detail = response.json().get("detail", {})
    assert detail.get("ingestion_id") == unknown_id

    resp_obs = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{unknown_id}/observations",
        timeout=10,
    )
    assert resp_obs.status_code == 404, resp_obs.text
    detail_obs = resp_obs.json().get("detail", {})
    assert detail_obs.get("ingestion_id") == unknown_id


def test_e2e_process_endpoint_unknown_ingestion_id_returns_404(
    e2e_config: E2EConfig,
) -> None:
    unknown_id = str(uuid4())
    response = requests.post(
        f"{e2e_config.api_v1_url}/ingestions/{unknown_id}/process",
        timeout=10,
    )
    assert response.status_code == 404, response.text
    detail = response.json().get("detail", {})
    assert detail.get("code") == "INGESTION_NOT_FOUND"
