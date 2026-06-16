from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from langchain_core.documents import Document
from langchain_core.messages import AIMessage
from pydantic import ValidationError

from app.ai.ai_orchestration import (
    AIEnrichmentRequest,
    ObservationContext,
    build_default_llm,
    build_semantic_search_query,
    orchestrate_ai_enrichment,
    parse_annotation_response,
    retrieve_guideline_context,
)


class StubRetriever:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def invoke(self, query: str) -> list[Document]:
        self.queries.append(query)
        return [
            Document(
                page_content="guideline chunk",
                metadata={
                    "source_id": "doc-1",
                    "chunk_index": 0,
                    "source_type": "DOCUMENT",
                    "similarity_score": 0.91,
                },
            )
        ]


class StubLLM:
    def __init__(self) -> None:
        self.messages: list = []

    def invoke(self, messages):
        self.messages.append(messages)
        return AIMessage(
            content='{"annotation_type":"anomaly_flag","secondary_types":[],"summary":"Glucose is elevated and historical data was reviewed.","analyte_findings":[{"analyte_code":"GLU","description":"Glucose is 145 mg/dL, above the reference range and higher than the prior result.","trend_direction":"increasing","confidence":0.88}],"requires_review":true,"review_priority":"urgent"}'
        )


class InvalidStubLLM:
    def invoke(self, messages):
        return AIMessage(content='{"summary":"invalid"}')


class StubBedrockLLM:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs


def test_build_semantic_search_query_only_includes_abnormal_observations() -> (
    None
):
    observations = [
        ObservationContext(
            code="GLU",
            display="Glucose",
            value_num=145,
            value_text=None,
            unit="mg/dL",
            ref_low_num=None,
            ref_high_num=None,
            interpretation="HIGH",
            effective_at=datetime.now(timezone.utc),
        ),
        ObservationContext(
            code="CREAT",
            display="Creatinine",
            value_num=1.8,
            value_text=None,
            unit="mg/dL",
            ref_low_num=None,
            ref_high_num=None,
            interpretation="LOW",
            effective_at=datetime.now(timezone.utc),
        ),
        ObservationContext(
            code="NA",
            display="Sodium",
            value_num=140,
            value_text=None,
            unit="mmol/L",
            ref_low_num=None,
            ref_high_num=None,
            interpretation="NORMAL",
            effective_at=datetime.now(timezone.utc),
        ),
    ]

    query = build_semantic_search_query(observations)

    assert query == (
        "Patient has abnormal results: Glucose 145 mg/dL (High), "
        "Creatinine 1.8 mg/dL (Low)"
    )


def test_retrieve_guideline_context_invokes_retriever_with_built_query() -> (
    None
):
    request = AIEnrichmentRequest(
        ingestion_id=uuid4(),
        patient_id="PAT-1",
        panel_codes=["BMP"],
        collected_at=datetime.now(timezone.utc),
        current_observations=[
            ObservationContext(
                code="GLU",
                display="Glucose",
                value_num=145,
                value_text=None,
                unit="mg/dL",
                ref_low_num=None,
                ref_high_num=None,
                interpretation="HIGH",
                effective_at=datetime.now(timezone.utc),
            )
        ],
        historical_observations=[],
    )
    retriever = StubRetriever()

    documents = retrieve_guideline_context(request, retriever=retriever)

    assert [doc.page_content for doc in documents] == ["guideline chunk"]
    assert retriever.queries == [
        "Patient has abnormal results: Glucose 145 mg/dL (High)"
    ]


def test_retrieve_guideline_context_skips_retriever_when_no_abnormal_results() -> (
    None
):
    request = AIEnrichmentRequest(
        ingestion_id=uuid4(),
        patient_id="PAT-1",
        panel_codes=["BMP"],
        collected_at=datetime.now(timezone.utc),
        current_observations=[
            ObservationContext(
                code="NA",
                display="Sodium",
                value_num=140,
                value_text=None,
                unit="mmol/L",
                ref_low_num=None,
                ref_high_num=None,
                interpretation="NORMAL",
                effective_at=datetime.now(timezone.utc),
            )
        ],
        historical_observations=[],
    )
    retriever = StubRetriever()

    documents = retrieve_guideline_context(request, retriever=retriever)

    assert documents == []
    assert retriever.queries == []


def test_orchestrate_ai_enrichment_builds_prompt_and_invokes_llm() -> None:
    request = AIEnrichmentRequest(
        ingestion_id=uuid4(),
        patient_id="PAT-1",
        panel_codes=["BMP", "LIPID"],
        collected_at=datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc),
        current_observations=[
            ObservationContext(
                code="GLU",
                display="Glucose",
                value_num=145,
                value_text=None,
                unit="mg/dL",
                ref_low_num=70,
                ref_high_num=99,
                interpretation="HIGH",
                effective_at=datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc),
            )
        ],
        historical_observations=[
            ObservationContext(
                code="GLU",
                display="Glucose",
                value_num=100,
                value_text=None,
                unit="mg/dL",
                ref_low_num=70,
                ref_high_num=99,
                interpretation="HIGH",
                effective_at=datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc),
            )
        ],
    )
    retriever = StubRetriever()
    llm = StubLLM()

    result = orchestrate_ai_enrichment(
        request,
        retriever=retriever,
        llm=llm,
    )

    assert result.llm_response_content is not None
    assert result.llm_response_content.annotation_type == "anomaly_flag"
    assert result.llm_response_text is not None
    assert len(result.guideline_context) == 1
    assert len(llm.messages) == 1
    human_message = llm.messages[0][1]
    assert "Panels: BMP, LIPID" in str(human_message.content)
    assert "guideline chunk" in str(human_message.content)


def test_orchestrate_ai_enrichment_returns_rejected_result_for_invalid_llm_output() -> (
    None
):
    request = AIEnrichmentRequest(
        ingestion_id=uuid4(),
        patient_id="PAT-1",
        panel_codes=["LIPID"],
        collected_at=datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc),
        current_observations=[
            ObservationContext(
                code="TC",
                display="Total Cholesterol",
                value_num=245,
                value_text=None,
                unit="mg/dL",
                ref_low_num=0,
                ref_high_num=200,
                interpretation="HIGH",
                effective_at=datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc),
            )
        ],
        historical_observations=[],
    )

    result = orchestrate_ai_enrichment(
        request,
        retriever=StubRetriever(),
        llm=InvalidStubLLM(),
    )

    assert result.llm_response_text == '{"summary":"invalid"}'
    assert result.llm_response_content is None
    assert result.rejection_reason is not None


def test_build_default_llm_uses_bedrock_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BEDROCK_MODEL_ID", "anthropic.test-model-v1:0")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setattr("app.ai.ai_orchestration.ChatBedrock", StubBedrockLLM)

    llm = build_default_llm()

    assert isinstance(llm, StubBedrockLLM)
    assert llm.kwargs == {
        "model_id": "anthropic.test-model-v1:0",
        "model_kwargs": {"temperature": 0.0},
        "region_name": "us-east-1",
    }


def test_orchestrate_ai_enrichment_returns_failure_reason_when_bedrock_config_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BEDROCK_MODEL_ID", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)

    request = AIEnrichmentRequest(
        ingestion_id=uuid4(),
        patient_id="PAT-1",
        panel_codes=["BMP"],
        collected_at=datetime.now(timezone.utc),
        current_observations=[],
        historical_observations=[],
    )

    result = orchestrate_ai_enrichment(request)

    assert result.failure_reason is not None
    assert "BEDROCK_MODEL_ID" in result.failure_reason
    assert result.guideline_context == []
    assert result.prompt_messages == []
    assert result.llm_response_text is None


def test_parse_annotation_response_validates_json_schema() -> None:
    content = parse_annotation_response(
        '{"annotation_type":"anomaly_flag","secondary_types":["followup_suggestion"],"summary":"Abnormal glucose result reviewed.","analyte_findings":[{"analyte_code":"GLU","description":"Glucose is elevated above the reference range.","trend_direction":"increasing","confidence":0.75}],"requires_review":true,"review_priority":"urgent"}'
    )

    assert content is not None
    assert content.analyte_findings[0].analyte_code == "GLU"


def test_parse_annotation_response_rejects_invalid_json() -> None:
    with pytest.raises(ValidationError):
        parse_annotation_response(
            '{"annotation_type":"anomaly_flag","secondary_types":[],"summary":"Missing analyte findings.","analyte_findings":[],"requires_review":false,"review_priority":"urgent"}'
        )
