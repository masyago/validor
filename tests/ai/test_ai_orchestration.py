from __future__ import annotations

from uuid import uuid4

from langchain_core.documents import Document

from app.ai.ai_orchestration import (
    AIEnrichmentRequest,
    build_semantic_search_query,
    retrieve_guideline_context,
)


class StubRetriever:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def invoke(self, query: str) -> list[Document]:
        self.queries.append(query)
        return [Document(page_content="guideline chunk")]


def test_build_semantic_search_query_only_includes_abnormal_observations() -> (
    None
):
    observations = [
        {
            "resourceType": "Observation",
            "code": {
                "coding": [{"code": "GLU", "display": "Glucose"}],
            },
            "valueQuantity": {"value": 145, "unit": "mg/dL"},
            "interpretation": [
                {"coding": [{"code": "HIGH", "display": "High"}]}
            ],
        },
        {
            "resourceType": "Observation",
            "code": {
                "coding": [{"code": "CREAT", "display": "Creatinine"}],
            },
            "valueQuantity": {"value": 1.8, "unit": "mg/dL"},
            "interpretation": [
                {"coding": [{"code": "LOW", "display": "Low"}]}
            ],
        },
        {
            "resourceType": "Observation",
            "code": {
                "coding": [{"code": "NA", "display": "Sodium"}],
            },
            "valueQuantity": {"value": 140, "unit": "mmol/L"},
            "interpretation": [
                {"coding": [{"code": "NORMAL", "display": "Normal"}]}
            ],
        },
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
        current_observations=[
            {
                "resourceType": "Observation",
                "code": {
                    "coding": [{"code": "GLU", "display": "Glucose"}],
                },
                "valueQuantity": {"value": 145, "unit": "mg/dL"},
                "interpretation": [
                    {"coding": [{"code": "HIGH", "display": "High"}]}
                ],
            }
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
        current_observations=[
            {
                "resourceType": "Observation",
                "code": {
                    "coding": [{"code": "NA", "display": "Sodium"}],
                },
                "valueQuantity": {"value": 140, "unit": "mmol/L"},
                "interpretation": [
                    {"coding": [{"code": "NORMAL", "display": "Normal"}]}
                ],
            }
        ],
        historical_observations=[],
    )
    retriever = StubRetriever()

    documents = retrieve_guideline_context(request, retriever=retriever)

    assert documents == []
    assert retriever.queries == []
