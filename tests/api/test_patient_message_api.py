from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routers.ingestion import router
from app.api.routers.dependencies import get_session
from app.core.ingestion_status_enums import IngestionStatus
from app.persistence.models.core import Ingestion
from app.persistence.models.patient import Patient
from app.persistence.models.patient_message import (
    PatientMessage,
    PatientMessageReviewStatus,
    PatientMessageValidationStatus,
)

app = FastAPI()
app.include_router(router)


@pytest.fixture
def client(db_session):
    def override_get_session():
        yield db_session

    app.dependency_overrides[get_session] = override_get_session
    test_client = TestClient(app)
    yield test_client
    app.dependency_overrides.clear()


def _seed_message(db_session) -> PatientMessage:
    ingestion_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    db_session.add(
        Ingestion(
            ingestion_id=ingestion_id,
            instrument_id="INST",
            run_id=f"RUN-{uuid.uuid4()}",
            uploader_id="uploader_001",
            spec_version="analyzer_csv_v1",
            uploader_received_at=now,
            api_received_at=now,
            submitted_sha256=None,
            server_sha256=hashlib.sha256(b"x").hexdigest(),
            status=IngestionStatus.COMPLETED,
            error_code=None,
            error_detail=None,
            source_filename="fixture.csv",
            ingestion_idempotency_disposition=None,
        )
    )
    patient_id = f"PAT-{uuid.uuid4()}"
    db_session.add(
        Patient(
            patient_id=patient_id,
            given_name="Sam",
            family_name="Rivera",
            email="sam.rivera@demo.invalid",
        )
    )
    message = PatientMessage(
        patient_id=patient_id,
        ingestion_id=ingestion_id,
        draft_content_json={
            "subject": "Blood test results",
            "opening": "Your results are available.",
            "normal_summary": "All results were within normal ranges.",
            "abnormal_findings": [],
            "recommendation": "Your care team will follow up if needed.",
        },
        content_schema_version="v1.1.0",
        validation_status=PatientMessageValidationStatus.ACCEPTED,
        review_status=PatientMessageReviewStatus.PENDING_REVIEW,
    )
    db_session.add(message)
    db_session.flush()
    return message


def test_get_patient_message_renders_to_line(client, db_session):
    message = _seed_message(db_session)
    res = client.get(f"/ingestions/{message.ingestion_id}/patient_message")
    assert res.status_code == 200
    body = res.json()
    assert body["patient_message_id"] == str(message.patient_message_id)
    assert body["review_status"] == "PENDING_REVIEW"
    # Synthetic PHI applied at render time.
    assert body["patient_given_name"] == "Sam"
    assert body["patient_email"] == "sam.rivera@demo.invalid"


def test_get_patient_message_404(client):
    res = client.get(f"/ingestions/{uuid.uuid4()}/patient_message")
    assert res.status_code == 404


def test_approve_then_send(client, db_session):
    message = _seed_message(db_session)
    mid = message.patient_message_id

    approve = client.post(
        f"/patient_messages/{mid}/approve",
        json={"approved_by": "dr-demo"},
    )
    assert approve.status_code == 200
    assert approve.json()["review_status"] == "APPROVED"

    send = client.post(f"/patient_messages/{mid}/send")
    assert send.status_code == 200
    assert send.json()["review_status"] == "SENT"


def test_send_before_approve_conflicts(client, db_session):
    message = _seed_message(db_session)
    res = client.post(f"/patient_messages/{message.patient_message_id}/send")
    assert res.status_code == 409


def test_approve_unknown_id_404(client):
    res = client.post(
        f"/patient_messages/{uuid.uuid4()}/approve",
        json={"approved_by": "dr-demo"},
    )
    assert res.status_code == 404
