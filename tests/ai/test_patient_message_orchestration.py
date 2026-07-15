from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from langchain_core.documents import Document
from langchain_core.messages import AIMessage
from pydantic import ValidationError

from app.ai.ai_orchestration import ObservationContext
from app.ai.content_versions.patient_message_content_v1_2_0 import (
    parse_patient_message_content,
)
from app.ai.patient_message_orchestration import (
    PatientMessageDraftRequest,
    orchestrate_patient_message_draft,
)


_VALID_CONTENT = {
    "subject": "Blood test results",
    "opening": "Your recent blood test results are now available. Here is a summary of the key findings.",
    "normal_summary": "Most results were within normal ranges.",
    "abnormal_findings": [
        {
            "title": "Blood sugar (glucose)",
            "analyte_codes": ["GLU"],
            "explanation": "Your blood sugar was a little above the usual range and will be monitored.",
        }
    ],
    "recommendation": "We recommend scheduling a follow-up appointment to discuss these findings.",
}


class StubRetriever:
    def __init__(self) -> None:
        self.invoke_count = 0

    def invoke(self, query: str) -> list[Document]:
        self.invoke_count += 1
        return [
            Document(
                page_content="guideline chunk",
                metadata={
                    "embedding_id": "emb-1",
                    "source_id": "doc-1",
                    "chunk_index": 0,
                    "source_type": "DOCUMENT",
                    "similarity_score": 0.9,
                },
            )
        ]


class StubLLM:
    def __init__(self, content: str) -> None:
        self._content = content
        self.messages: list = []

    def invoke(self, messages):
        self.messages.append(messages)
        return AIMessage(content=self._content)


def _request() -> PatientMessageDraftRequest:
    obs = ObservationContext(
        code="GLU",
        display="Glucose",
        value_num=145,
        value_text=None,
        unit="mg/dL",
        ref_low_num=70,
        ref_high_num=99,
        interpretation="HIGH",
        effective_at=datetime.now(timezone.utc),
    )
    return PatientMessageDraftRequest(
        ingestion_id=uuid4(),
        correlation_id=uuid4(),
        panel_codes=["BMP"],
        collected_at=datetime.now(timezone.utc),
        current_observations=[obs],
        historical_observations=[],
    )


def test_request_carries_no_patient_identifiers() -> None:
    # The de-identification boundary: patient_id/name/email are not fields.
    field_names = {f.name for f in dataclasses.fields(PatientMessageDraftRequest)}
    assert "patient_id" not in field_names
    assert "correlation_id" in field_names
    assert not (field_names & {"name", "email", "given_name", "family_name"})


def test_orchestrate_returns_failure_reason_without_llm(monkeypatch) -> None:
    monkeypatch.delenv("BEDROCK_MODEL_ID", raising=False)
    result = orchestrate_patient_message_draft(_request(), llm=None)
    assert result.failure_reason is not None
    assert result.llm_response_content is None
    assert result.retrieved_refs == []


def test_orchestrate_accepts_valid_content() -> None:
    request = _request()
    llm = StubLLM(json.dumps(_VALID_CONTENT))
    result = orchestrate_patient_message_draft(
        request, retriever=StubRetriever(), llm=llm
    )
    assert result.failure_reason is None
    assert result.rejection_reason is None
    assert result.llm_response_content is not None
    assert result.llm_response_content.opening
    assert result.llm_response_content.abnormal_findings
    assert result.prompt_version == "v1.3.0"
    assert result.content_schema_version == "v1.2.0"
    # The analyte's human-readable display name reaches the prompt so the model
    # can write friendly finding titles (de-identified clinical data, not PHI).
    prompt_text = "\n".join(str(m.content) for m in result.prompt_messages)
    assert "Glucose" in prompt_text
    # RAG citations recorded.
    assert result.retrieved_refs == [
        {"embedding_id": "emb-1", "source_id": "doc-1", "chunk_index": 0}
    ]
    # correlation_id echoed back in the envelope.
    assert result.correlation_id == request.correlation_id


def test_orchestrate_reuses_shared_guideline_context() -> None:
    # When the annotation stage's guideline_context is passed through, the
    # retriever must not be invoked again — that would be a redundant
    # embedding call + vector search for a query that's already deterministic
    # from the same current_observations.
    request = _request()
    llm = StubLLM(json.dumps(_VALID_CONTENT))
    retriever = StubRetriever()
    shared_context = [
        Document(
            page_content="shared chunk",
            metadata={
                "embedding_id": "emb-shared",
                "source_id": "doc-shared",
                "chunk_index": 0,
            },
        )
    ]
    result = orchestrate_patient_message_draft(
        request,
        retriever=retriever,
        llm=llm,
        guideline_context=shared_context,
    )
    assert retriever.invoke_count == 0
    assert result.guideline_context == shared_context
    assert result.retrieved_refs == [
        {"embedding_id": "emb-shared", "source_id": "doc-shared", "chunk_index": 0}
    ]


def test_orchestrate_rejects_invalid_content() -> None:
    llm = StubLLM('{"greeting":"Hi"}')  # missing required fields
    result = orchestrate_patient_message_draft(
        _request(), retriever=StubRetriever(), llm=llm
    )
    assert result.llm_response_content is None
    assert result.rejection_reason is not None
    # Raw text preserved for audit.
    assert result.llm_response_text == '{"greeting":"Hi"}'


def test_content_schema_rejects_empty_required_field() -> None:
    payload = dict(_VALID_CONTENT)
    payload["opening"] = "   "
    with pytest.raises(ValidationError):
        parse_patient_message_content(json.dumps(payload))
