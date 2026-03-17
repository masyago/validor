import csv
import io
import time
from datetime import datetime, timezone

import pytest
import requests

from tests.e2e.conftest import E2EConfig


pytestmark = pytest.mark.e2e


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


def test_full_ingestion_flow_e2e(e2e_config: E2EConfig) -> None:
    """Black-box happy path: ingest CSV -> process -> read FHIR resources."""

    # Use a unique run_id each time so the test is re-runnable against a persistent DB.
    run_id = f"e2e_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{int(time.time())}"

    fixture_path = "tests/fixtures/csv/valid_csv_20260128_004.csv"
    with open(fixture_path, "rb") as f:
        fixture_bytes = f.read()

    fixture_text = fixture_bytes.decode("utf-8")
    csv_text = _replace_csv_column(fixture_text, "run_id", run_id)
    csv_bytes = csv_text.encode("utf-8")

    # This matches the canonical CSV fixture's instrument_id column.
    instrument_id = "CANONICAL_CHEM_ANALYZER_V1"

    form = {
        "uploader_id": "e2e_uploader",
        "spec_version": "analyzer_csv_v1",
        "instrument_id": instrument_id,
        "run_id": run_id,
        "uploader_received_at": datetime.now(timezone.utc).isoformat(),
        "content_sha256": _sha256_hex(csv_bytes),
    }

    files = {
        "file": (f"{run_id}.csv", io.BytesIO(csv_bytes), "text/csv"),
    }

    post_resp = requests.post(
        f"{e2e_config.api_v1_url}/ingestions",
        data=form,
        files=files,
        timeout=30,
    )

    # Accept both "new" (202) and "duplicate" (200) so the test stays stable
    # even if something external replays the same run_id.
    assert post_resp.status_code in {200, 202}, post_resp.text
    post_json = post_resp.json()

    if post_resp.status_code == 202:
        ingestion_id = post_json["ingestion_id"]
    else:
        ingestion_id = post_json["existing_ingestion_id"]

    process_resp = requests.post(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/process",
        timeout=10,
    )
    assert process_resp.status_code == 202, process_resp.text

    final_ingestion = _poll_ingestion_status(
        e2e_config=e2e_config,
        ingestion_id=ingestion_id,
        timeout_s=float(
            # Parsing/normalization can take a bit on cold starts.
            60
        ),
    )

    assert (
        final_ingestion["status"] == "COMPLETED"
    ), f"Ingestion did not complete: {final_ingestion}"

    dr_resp = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/diagnostic-reports",
        timeout=10,
    )
    dr_resp.raise_for_status()
    diagnostic_reports = dr_resp.json()
    assert isinstance(diagnostic_reports, list)
    assert len(diagnostic_reports) >= 1

    obs_resp = requests.get(
        f"{e2e_config.api_v1_url}/ingestions/{ingestion_id}/observations?limit=5&offset=0",
        timeout=10,
    )
    obs_resp.raise_for_status()
    observations = obs_resp.json()
    assert isinstance(observations, list)
    assert len(observations) >= 1
