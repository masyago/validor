from __future__ import annotations

import dataclasses
import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from langchain_core.documents import Document
from sqlalchemy.exc import IntegrityError

from app.core.ingestion_status_enums import IngestionStatus
from app.persistence.models.core import Ingestion, RawData
from app.persistence.models.patient import Patient
from app.persistence.models.patient_message import (
    PatientMessage,
    PatientMessageReviewStatus,
    PatientMessageValidationStatus,
)
from app.persistence.models.provenance import ProcessingEventType
from app.persistence.repositories.processing_event_repo import (
    ProcessingEventRepository,
)
from app.provenance.emitter import EventContext, emit
from app.persistence.models.provenance import ProcessingEventActor

from app.ai.ai_orchestration import AIEnrichmentResult
from app.ai.content_versions.ai_annotation_content_v1_0_0 import (
    AIAnnotationContent,
    AnalyteFinding,
)
from app.ai.content_versions.patient_message_content_v1_1_0 import (
    AbnormalFinding,
    PatientMessageContent,
)
from app.ai.patient_message_orchestration import PatientMessageDraftResult
from app.services import ingestion_service as ingestion_service_mod
from app.services.ingestion_service import IngestionService
from app.services.patient_message_service import (
    InvalidTransitionError,
    PatientMessageService,
)

CSV_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "csv"


# ---------------------------------------------------------------------------
# Stub AI results
# ---------------------------------------------------------------------------


def _accepted_enrichment_result() -> AIEnrichmentResult:
    content = AIAnnotationContent(
        annotation_type="anomaly_flag",
        secondary_types=[],
        summary="Reviewed.",
        analyte_findings=[
            AnalyteFinding(
                analyte_code="GLU",
                description="Elevated glucose.",
                trend_direction="increasing",
                confidence=0.9,
            )
        ],
        requires_review=True,
        review_priority="urgent",
    )
    return AIEnrichmentResult(
        guideline_context=[],
        prompt_messages=[],
        llm_response_text="{}",
        llm_response_content=content,
        provider="amazon_bedrock",
        model_id="stub-model",
        prompt_version="v1.0.0",
        temperature="0.0",
        content_schema_version="v1.0.0",
        input_hash="hash-enrich",
        created_at=datetime.now(timezone.utc),
        rejection_reason=None,
        failure_reason=None,
    )


def _valid_message_content() -> PatientMessageContent:
    return PatientMessageContent(
        subject="Blood test results",
        opening="Your recent blood test results are now available.",
        normal_summary="Most results were within normal ranges.",
        abnormal_findings=[
            AbnormalFinding(
                title="Blood sugar (glucose)",
                analyte_codes=["GLU"],
                explanation="Blood sugar slightly high and will be monitored.",
            )
        ],
        recommendation="Your care team will follow up if needed.",
    )


def _accepted_message_result(correlation_id: uuid.UUID) -> PatientMessageDraftResult:
    return PatientMessageDraftResult(
        correlation_id=correlation_id,
        guideline_context=[],
        prompt_messages=[],
        llm_response_text="{}",
        llm_response_content=_valid_message_content(),
        retrieved_refs=[{"embedding_id": "e1", "source_id": "d1", "chunk_index": 0}],
        provider="amazon_bedrock",
        model_id="stub-model",
        prompt_version="v1.0.0",
        temperature="0.0",
        content_schema_version="v1.0.0",
        input_hash="hash-msg",
        created_at=datetime.now(timezone.utc),
        rejection_reason=None,
        failure_reason=None,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _seed_ingestion(db_session, ingestion_id, *, run_id, csv_bytes) -> None:
    now = datetime.now(timezone.utc)
    db_session.add(
        Ingestion(
            ingestion_id=ingestion_id,
            instrument_id="INST",
            run_id=run_id,
            uploader_id="uploader_001",
            spec_version="analyzer_csv_v1",
            uploader_received_at=now,
            api_received_at=now,
            submitted_sha256=None,
            server_sha256=hashlib.sha256(csv_bytes).hexdigest(),
            status=IngestionStatus.RECEIVED,
            error_code=None,
            error_detail=None,
            source_filename="fixture.csv",
            ingestion_idempotency_disposition=None,
        )
    )
    db_session.add(
        RawData(
            ingestion_id=ingestion_id,
            content_bytes=csv_bytes,
            content_mime="text/csv",
            content_size_bytes=len(csv_bytes),
        )
    )
    db_session.flush()


@pytest.fixture
def drafted(db_session, monkeypatch):
    """Run the full pipeline with stubbed AI calls so a patient message is
    drafted, and return (service, ingestion_id, message)."""
    csv_bytes = (CSV_DIR / "valid_csv_20260128_004.csv").read_bytes()
    ingestion_id = uuid.uuid4()
    _seed_ingestion(
        db_session, ingestion_id, run_id=f"RUN-{uuid.uuid4()}", csv_bytes=csv_bytes
    )

    monkeypatch.setattr(
        ingestion_service_mod,
        "orchestrate_ai_enrichment",
        lambda request: _accepted_enrichment_result(),
    )
    monkeypatch.setattr(
        ingestion_service_mod,
        "orchestrate_patient_message_draft",
        lambda request, **kwargs: _accepted_message_result(request.correlation_id),
    )

    service = IngestionService(db_session)
    service.process_ingestion(ingestion_id)

    message = service.patient_message_repo.get_active_by_ingestion_id(
        ingestion_id
    )
    return service, ingestion_id, message


# ---------------------------------------------------------------------------
# Gate
# ---------------------------------------------------------------------------


def _emit(pe_repo, ingestion_id, event_type, actor=ProcessingEventActor.NORMALIZER):
    emit(
        pe_repo,
        EventContext(ingestion_id=ingestion_id, actor=actor),
        event_type=event_type,
        deduped=False,
    )


@pytest.mark.parametrize(
    "events,expected",
    [
        ([ProcessingEventType.NORMALIZATION_SUCCEEDED], False),
        ([ProcessingEventType.AI_ENRICHMENT_SUCCEEDED], False),
        (
            [
                ProcessingEventType.NORMALIZATION_SUCCEEDED,
                ProcessingEventType.AI_ENRICHMENT_SUCCEEDED,
            ],
            True,
        ),
        (
            [
                ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS,
                ProcessingEventType.AI_ENRICHMENT_SUCCEEDED,
            ],
            True,
        ),
    ],
)
def test_gate_truth_table(db_session, events, expected):
    ingestion_id = uuid.uuid4()
    csv_bytes = b"x"
    _seed_ingestion(
        db_session, ingestion_id, run_id=f"RUN-{uuid.uuid4()}", csv_bytes=csv_bytes
    )
    service = IngestionService(db_session)
    for event_type in events:
        actor = (
            ProcessingEventActor.AI_WORKER
            if "AI_ENRICHMENT" in event_type.value
            else ProcessingEventActor.NORMALIZER
        )
        _emit(service.pe_repo, ingestion_id, event_type, actor)
    db_session.flush()
    assert service.should_draft_patient_message(ingestion_id) is expected


# ---------------------------------------------------------------------------
# Draft stage
# ---------------------------------------------------------------------------


def test_draft_creates_pending_review_message(drafted):
    _service, _ingestion_id, message = drafted
    assert message is not None
    assert message.validation_status == PatientMessageValidationStatus.ACCEPTED
    assert message.review_status == PatientMessageReviewStatus.PENDING_REVIEW
    # generation_event_id points at the MESSAGE_DRAFT_SUCCEEDED row.
    assert message.generation_event_id is not None
    assert message.correlation_id is not None
    assert message.retrieved_refs_json
    # PHI is not baked into the draft content.
    assert "patient_id" not in message.draft_content_json


def test_draft_consumes_correlation_token(drafted):
    service, _ingestion_id, message = drafted
    job = service.ai_generation_job_repo.get(message.correlation_id)
    assert job is not None
    assert job.consumed_at is not None
    # One-time use: a second consume returns None.
    assert service.ai_generation_job_repo.consume(message.correlation_id) is None


def test_draft_is_idempotent(drafted, db_session):
    service, ingestion_id, message = drafted
    service.draft_patient_message(ingestion_id)
    messages = service.patient_message_repo.list_by_ingestion_id(ingestion_id)
    assert len(messages) == 1


def test_draft_reuses_enrichment_stage_guideline_context(db_session, monkeypatch):
    # The annotation stage's guideline_context (from its own RAG retrieval)
    # must be threaded through to the message-draft stage rather than being
    # re-retrieved — same current_observations means the same query, so a
    # second retrieval would be pure wasted latency.
    shared_context = [
        Document(
            page_content="shared guideline chunk",
            metadata={"embedding_id": "shared-1", "source_id": "doc-shared", "chunk_index": 0},
        )
    ]
    enrichment_result = dataclasses.replace(
        _accepted_enrichment_result(), guideline_context=shared_context
    )

    captured: dict = {}

    def _stub_patient_message_draft(request, **kwargs):
        captured["guideline_context"] = kwargs.get("guideline_context")
        return _accepted_message_result(request.correlation_id)

    monkeypatch.setattr(
        ingestion_service_mod, "orchestrate_ai_enrichment", lambda request: enrichment_result
    )
    monkeypatch.setattr(
        ingestion_service_mod,
        "orchestrate_patient_message_draft",
        _stub_patient_message_draft,
    )

    csv_bytes = (CSV_DIR / "valid_csv_20260128_004.csv").read_bytes()
    ingestion_id = uuid.uuid4()
    _seed_ingestion(
        db_session, ingestion_id, run_id=f"RUN-{uuid.uuid4()}", csv_bytes=csv_bytes
    )
    service = IngestionService(db_session)
    service.process_ingestion(ingestion_id)

    assert captured["guideline_context"] is shared_context


# ---------------------------------------------------------------------------
# Human gate transitions
# ---------------------------------------------------------------------------


def test_approve_then_send(drafted, db_session):
    _service, ingestion_id, message = drafted
    pm_service = PatientMessageService(db_session)

    approved = pm_service.approve(
        message.patient_message_id, approved_by="dr-demo"
    )
    assert approved.review_status == PatientMessageReviewStatus.APPROVED
    assert approved.final_content_json is not None
    assert approved.approved_by == "dr-demo"

    sent = pm_service.send(message.patient_message_id)
    assert sent.review_status == PatientMessageReviewStatus.SENT
    assert sent.sent_at is not None

    events = ProcessingEventRepository(
        db_session
    ).list_by_ingestion_id(ingestion_id)
    assert any(
        e.event_type == ProcessingEventType.MESSAGE_SENT for e in events
    )


def test_send_requires_approval(drafted, db_session):
    _service, _ingestion_id, message = drafted
    pm_service = PatientMessageService(db_session)
    with pytest.raises(InvalidTransitionError):
        pm_service.send(message.patient_message_id)


def test_request_changes_and_reject(drafted, db_session):
    _service, _ingestion_id, message = drafted
    pm_service = PatientMessageService(db_session)

    changed = pm_service.request_changes(
        message.patient_message_id, reviewed_by="dr-demo", note="soften tone"
    )
    assert changed.review_status == PatientMessageReviewStatus.CHANGES_REQUESTED
    assert changed.review_note == "soften tone"

    rejected = pm_service.reject(
        message.patient_message_id, reviewed_by="dr-demo", note="no"
    )
    assert rejected.review_status == PatientMessageReviewStatus.REJECTED

    events = ProcessingEventRepository(
        db_session
    ).list_by_ingestion_id(_ingestion_id)
    assert any(
        e.event_type == ProcessingEventType.MESSAGE_REJECTED for e in events
    )


# ---------------------------------------------------------------------------
# DB constraints
# ---------------------------------------------------------------------------


def _new_patient(db_session, patient_id="PAT-CONSTRAINT"):
    db_session.add(
        Patient(
            patient_id=patient_id,
            given_name="A",
            family_name="B",
            email=None,
        )
    )
    db_session.flush()
    return patient_id


def _bare_message(ingestion_id, patient_id, **overrides) -> PatientMessage:
    defaults = dict(
        patient_id=patient_id,
        ingestion_id=ingestion_id,
        draft_content_json={"greeting": "hi"},
        content_schema_version="v1.0.0",
        validation_status=PatientMessageValidationStatus.ACCEPTED,
        review_status=PatientMessageReviewStatus.PENDING_REVIEW,
    )
    defaults.update(overrides)
    return PatientMessage(**defaults)


def test_final_content_only_when_approved(db_session):
    ingestion_id = uuid.uuid4()
    _seed_ingestion(
        db_session, ingestion_id, run_id=f"RUN-{uuid.uuid4()}", csv_bytes=b"x"
    )
    patient_id = _new_patient(db_session)
    db_session.add(
        _bare_message(
            ingestion_id,
            patient_id,
            final_content_json={"greeting": "hi"},  # not APPROVED/SENT
        )
    )
    with pytest.raises(IntegrityError):
        db_session.flush()


def test_one_active_message_per_ingestion(db_session):
    ingestion_id = uuid.uuid4()
    _seed_ingestion(
        db_session, ingestion_id, run_id=f"RUN-{uuid.uuid4()}", csv_bytes=b"x"
    )
    patient_id = _new_patient(db_session)
    db_session.add(_bare_message(ingestion_id, patient_id))
    db_session.flush()
    db_session.add(_bare_message(ingestion_id, patient_id))
    with pytest.raises(IntegrityError):
        db_session.flush()


def test_no_self_supersede(db_session):
    ingestion_id = uuid.uuid4()
    _seed_ingestion(
        db_session, ingestion_id, run_id=f"RUN-{uuid.uuid4()}", csv_bytes=b"x"
    )
    patient_id = _new_patient(db_session)
    message = _bare_message(ingestion_id, patient_id)
    db_session.add(message)
    db_session.flush()
    message.superseded_by = message.patient_message_id
    with pytest.raises(IntegrityError):
        db_session.flush()
