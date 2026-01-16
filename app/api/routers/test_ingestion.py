from fastapi.testclient import TestClient
import pytest

from .ingestion import router

# from datetime import datetime
import io
import uuid


from fastapi import FastAPI
from fastapi.testclient import TestClient

# Create a client that includes the router
app = FastAPI()
app.include_router(router)
client = TestClient(app)


"""
Plan:
Write Unit Tests for ingestion.py:

- Test the success path (202): Simulate a new, unique file upload.
- Test the hash mismatch (422): Send a file where the client-provided hash doesn't match the server-calculated one.
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


def test_422_hash_mismatch():
    pass


# def test_200_duplicate_ok():
#     response = client.get("/")
#     assert response.status_code == 200
#     assert response.json() == {"msg": "Hello World"}


def test_409_duplicate_error():
    pass


def test_415_content_too_large():
    pass
