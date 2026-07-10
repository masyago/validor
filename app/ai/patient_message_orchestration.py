from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from langchain_core.documents import Document
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langchain_core.retrievers import BaseRetriever
from pydantic import ValidationError

# Reuse the shared AI plumbing so both LLM calls behave identically at the
# boundary (provider inference, hashing, fence-stripping, RAG retrieval).
from app.ai.ai_orchestration import (
    ObservationContext,
    build_default_llm,
    build_default_retriever,
    build_semantic_search_query,
    _build_input_hash,
    _infer_model_id,
    _infer_provider,
    _infer_temperature,
    _llm_response_text,
    _observation_prompt_flag,
    _observation_prompt_value,
    _strip_json_code_fences,
    _to_historical_observation,
    _to_rag_chunk,
)
from app.ai.content_versions.patient_message_content_v1_2_0 import (
    CONTENT_SCHEMA_VERSION,
    PatientMessageContent,
    parse_patient_message_content,
)
from app.ai.prompt_versions.patient_message_prompt_v1_2_0 import (
    NamedObservationRow,
    PATIENT_MESSAGE_PROMPT,
    PROMPT_VERSION,
    build_patient_message_prompt_inputs,
)


@dataclass(frozen=True)
class PatientMessageDraftRequest:
    """
    De-identified request for a patient-message draft.

    Carries a job-scoped `correlation_id` (the identifier de-id) plus a
    structured, allowlisted clinical payload. It deliberately holds NO
    patient_id, name, or email — nothing PHI crosses into the AI layer.
    """

    ingestion_id: UUID
    correlation_id: UUID
    panel_codes: list[str]
    collected_at: datetime
    current_observations: list[ObservationContext]
    historical_observations: list[ObservationContext]


@dataclass(frozen=True)
class PatientMessageDraftResult:
    correlation_id: UUID
    guideline_context: list[Document]
    prompt_messages: list[BaseMessage]
    llm_response_text: str | None
    llm_response_content: PatientMessageContent | None
    retrieved_refs: list[dict[str, Any]]
    provider: str | None
    model_id: str | None
    prompt_version: str
    temperature: str | None
    content_schema_version: str
    input_hash: str
    created_at: datetime
    rejection_reason: str | None
    failure_reason: str | None = None


def _missing_ai_config_reason() -> str:
    return (
        "BEDROCK_MODEL_ID is not set; patient message drafting cannot invoke "
        "a Bedrock model."
    )


def retrieve_guideline_context(
    request: PatientMessageDraftRequest,
    *,
    retriever: BaseRetriever | Any | None = None,
) -> list[Document]:
    query = build_semantic_search_query(request.current_observations)
    if query is None:
        return []
    active_retriever = retriever or build_default_retriever()
    return active_retriever.invoke(query)


def _to_named_observation_row(
    observation: ObservationContext,
) -> NamedObservationRow:
    """Like _to_observation_row, but carries the analyte's display name so the
    message model can write friendly finding titles. `display` is de-identified
    clinical data (already on the allowlist) — no PHI is added."""
    return NamedObservationRow(
        analyte_code=observation.code,
        display=observation.display,
        value=_observation_prompt_value(observation),
        unit=observation.unit,
        reference_low=(
            float(observation.ref_low_num)
            if observation.ref_low_num is not None
            else None
        ),
        reference_high=(
            float(observation.ref_high_num)
            if observation.ref_high_num is not None
            else None
        ),
        flag=_observation_prompt_flag(observation),
        date=observation.effective_at.isoformat(),
    )


def build_patient_message_prompt_messages(
    request: PatientMessageDraftRequest,
    *,
    guideline_context: list[Document],
) -> list[BaseMessage]:
    prompt_inputs = build_patient_message_prompt_inputs(
        ingestion_id=str(request.ingestion_id),
        panel_codes=request.panel_codes,
        collected_at=request.collected_at.isoformat(),
        observations=[
            _to_named_observation_row(observation)
            for observation in request.current_observations
        ],
        historical_observations=[
            _to_historical_observation(observation)
            for observation in request.historical_observations
        ],
        rag_chunks=[_to_rag_chunk(document) for document in guideline_context],
    )
    return PATIENT_MESSAGE_PROMPT.format_messages(**prompt_inputs)


def _retrieved_refs(guideline_context: list[Document]) -> list[dict[str, Any]]:
    """RAG citations: the vector_store/document ids used to ground the draft."""
    refs: list[dict[str, Any]] = []
    for document in guideline_context:
        metadata = document.metadata or {}
        refs.append(
            {
                "embedding_id": metadata.get("embedding_id"),
                "source_id": metadata.get("source_id"),
                "chunk_index": metadata.get("chunk_index"),
            }
        )
    return refs


def parse_patient_message_response(
    llm_response_text: str | None,
) -> PatientMessageContent | None:
    if llm_response_text is None:
        return None
    return parse_patient_message_content(
        _strip_json_code_fences(llm_response_text)
    )


def orchestrate_patient_message_draft(
    request: PatientMessageDraftRequest,
    *,
    retriever: BaseRetriever | Any | None = None,
    llm: BaseChatModel | Any | None = None,
    guideline_context: list[Document] | None = None,
) -> PatientMessageDraftResult:
    active_llm = llm or build_default_llm()
    if active_llm is None:
        prompt_messages: list[BaseMessage] = []
        return PatientMessageDraftResult(
            correlation_id=request.correlation_id,
            guideline_context=[],
            prompt_messages=prompt_messages,
            llm_response_text=None,
            llm_response_content=None,
            retrieved_refs=[],
            provider=None,
            model_id=None,
            prompt_version=PROMPT_VERSION,
            temperature=None,
            content_schema_version=CONTENT_SCHEMA_VERSION,
            input_hash=_build_input_hash(prompt_messages),
            created_at=datetime.now(timezone.utc),
            rejection_reason=None,
            failure_reason=_missing_ai_config_reason(),
        )

    # The annotation stage already retrieved guideline context from the same
    # current_observations (build_semantic_search_query depends only on
    # current_observations, which both stages fetch identically) — reuse it
    # instead of paying for a second embedding call + vector search.
    if guideline_context is None:
        guideline_context = retrieve_guideline_context(
            request, retriever=retriever
        )
    prompt_messages = build_patient_message_prompt_messages(
        request,
        guideline_context=guideline_context,
    )
    response = active_llm.invoke(prompt_messages)
    llm_response_text = _llm_response_text(response)

    llm_response_content: PatientMessageContent | None = None
    rejection_reason: str | None = None
    if llm_response_text is not None:
        try:
            llm_response_content = parse_patient_message_response(
                llm_response_text
            )
        except ValidationError as exc:
            rejection_reason = str(exc)

    return PatientMessageDraftResult(
        correlation_id=request.correlation_id,
        guideline_context=guideline_context,
        prompt_messages=prompt_messages,
        llm_response_text=llm_response_text,
        llm_response_content=llm_response_content,
        retrieved_refs=_retrieved_refs(guideline_context),
        provider=_infer_provider(active_llm),
        model_id=_infer_model_id(active_llm),
        prompt_version=PROMPT_VERSION,
        temperature=_infer_temperature(active_llm),
        content_schema_version=CONTENT_SCHEMA_VERSION,
        input_hash=_build_input_hash(prompt_messages),
        created_at=datetime.now(timezone.utc),
        rejection_reason=rejection_reason,
        failure_reason=None,
    )
