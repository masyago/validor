from fastapi.testclient import TestClient
import pytest
from unittest.mock import patch

from app.api.routers.ingestion import router

# from datetime import datetime
import io
import uuid
import hashlib


from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.api.routers.dependencies import get_session

# Create a client that includes the router
app = FastAPI()
app.include_router(router)


"""
Plan:
Write Unit Tests for ingestion.py:

- [DONE] Test the success path (202): Simulate a new, unique file upload (with and without content hash)
- [DONE] Test the hash mismatch (422): Send a file where the client-provided hash doesn't match the server-calculated one.
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


# @pytest.mark.skip(reason="No database integration yet.")
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
