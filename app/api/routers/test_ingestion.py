from fastapi.testclient import TestClient
import pytest
from unittest.mock import patch

from .ingestion import router

# from datetime import datetime
import io
import uuid
import hashlib


from fastapi import FastAPI
from fastapi.testclient import TestClient

# Create a client that includes the router
app = FastAPI()
app.include_router(router)
client = TestClient(app)


"""
Plan:
Write Unit Tests for ingestion.py:

- [DONE] Test the success path (202): Simulate a new, unique file upload (with and without content hash)
- [DONE] Test the hash mismatch (422): Send a file where the client-provided hash doesn't match the server-calculated one.
- Test the duplicate with same content (200): Mock the database call to return an existing record with a matching hash.
- Test the duplicate with different content (409): Mock the database call to return an existing record with a different hash.
- Test the payload too large (413): Simulate a request with a Content-Length header that is too big.
"""


# Test 202_ACCEPTED response. HTTP request variant: no content hash provided


@pytest.mark.parametrize("include_content_hash", [True, False])
def test_202_success(
    valid_form_data, valid_csv_file, content_sha256, include_content_hash
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
    assert response_data["status"] == "PROCESSING"
    assert "Ingestion request received" in response_data["message"]

    # Checks that the ingestion_id in response is valid uuid
    try:
        uuid.UUID(response_data["ingestion_id"])
    except ValueError:
        assert False, "ingestion_id is not a valid UUID"


# Test 422 Hash Mismatch. Content hahs provided by client doesn't match server
# generated hash
def test_422_hash_mismatch(valid_form_data, valid_csv_file):
    mismatched_hash = "incorrect_content_hash"

    valid_form_data["content_sha256"] = mismatched_hash

    response = client.post(
        "/ingestions",
        data=valid_form_data,
        files=valid_csv_file,
    )

    response_data = response.json()

    assert response.status_code == 422
    assert "CONTENT_HASH_MISMATCH" in response_data["detail"]["code"]
    assert "integrity check failed" in response_data["detail"]["message"]


# Test 200 Duplicate - OK when submit a duplicate with matching content
def test_200_duplicate_ok(valid_form_data, valid_csv_file, content_sha256):
    existing_ingestion_id = str(uuid.uuid4())

    # Mock the database lookup to return existing record with matching hash
    with patch("routers.ingestion.get_existing_ingestion") as mock_db:
        mock_db.return_value = (existing_ingestion_id, content_sha256)

        response = client.post(
            "/ingestions",
            data=valid_form_data,
            files=valid_csv_file,
        )

    response_data = response.json()

    assert response.status_code == 200
    assert response_data["existing_ingestion_id"] == existing_ingestion_id
    assert existing_ingestion_id in response.headers["Location"]
    assert "already submitted" in response_data["message"]


# Test 409 duplicate. Duplicate error when content of the duplicate doesn't
# match existing record
def test_409_duplicate_error(valid_form_data, valid_csv_file, content_sha256):
    existing_ingestion_id = str(uuid.uuid4())
    existing_content_sha256 = "very_different_hash"

    with patch("routers.ingestion.get_existing_ingestion") as mock_db:
        mock_db.return_value = (existing_ingestion_id, existing_content_sha256)

        response = client.post(
            "/ingestions",
            data=valid_form_data,
            files=valid_csv_file,
        )

    response_data = response.json()

    assert response.status_code == 409
    assert response_data["detail"]["code"] == "RUN_ID_CONTENT_MISMATCH"
    assert (
        response_data["detail"]["existing_ingestion_id"]
        == existing_ingestion_id
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
        == existing_content_sha256
    )
    assert response_data["detail"]["hashes"]["submitted"] == content_sha256
    assert (
        "ingestion already exists"
        in response_data["detail"]["message"].lower()
    )


def test_413_content_too_large(valid_form_data, valid_csv_file):
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
