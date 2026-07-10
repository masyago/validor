from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import os
from typing import Any
from uuid import UUID

import requests
from langchain_aws import ChatBedrock
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langchain_core.retrievers import BaseRetriever
from pydantic import ConfigDict, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.ai.content_versions.ai_annotation_content_v1_0_0 import (
    AIAnnotationContent,
    CONTENT_SCHEMA_VERSION,
    parse_ai_annotation_content,
)
from app.ai.prompt_versions.ai_annotation_prompt_v1_1_0 import (
    ANNOTATION_PROMPT,
    HistoricalObservation,
    ObservationRow,
    RagChunk,
    build_annotation_prompt_inputs,
    PROMPT_VERSION,
)
from app.persistence.db import engine
from app.persistence.models.ai import VectorStore

# Keep embedding config aligned with scripts/embed_seed_documents.py and the
# vector_store schema (Vector(1536)).
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_RETRIEVER_TOP_K = 5
DEFAULT_BEDROCK_TEMPERATURE = 0.0
DEFAULT_BEDROCK_PROVIDER = "anthropic"
DEFAULT_BEDROCK_MAX_TOKENS = 6000

_ABNORMAL_INTERPRETATIONS = {"HIGH", "LOW", "ABNORMAL", "CRITICAL"}


@dataclass(frozen=True)
class ObservationContext:
    code: str
    display: str | None
    value_num: Decimal | float | None
    value_text: str | None
    unit: str | None
    ref_low_num: Decimal | float | None
    ref_high_num: Decimal | float | None
    interpretation: str | None
    effective_at: datetime


@dataclass(frozen=True)
class AIEnrichmentRequest:
    ingestion_id: UUID
    # Job-scoped token minted on the trusted side. patient_id/PHI is resolved
    # server-side (for the historical fetch) BEFORE the request is built and is
    # deliberately NOT carried here — nothing PHI crosses into the AI layer.
    correlation_id: UUID
    panel_codes: list[str]
    collected_at: datetime
    current_observations: list[ObservationContext]
    historical_observations: list[ObservationContext]


@dataclass(frozen=True)
class AIEnrichmentResult:
    guideline_context: list[Document]
    prompt_messages: list[BaseMessage]
    llm_response_text: str | None
    llm_response_content: AIAnnotationContent | None
    provider: str | None
    model_id: str | None
    prompt_version: str
    temperature: str | None
    content_schema_version: str
    input_hash: str
    created_at: datetime
    rejection_reason: str | None
    failure_reason: str | None = None


class OpenAICompatibleEmbeddings(Embeddings):
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = DEFAULT_OPENAI_BASE_URL,
        model: str = DEFAULT_EMBEDDING_MODEL,
        timeout: int = 60,
    ) -> None:
        self._model = model
        self._timeout = timeout
        self._endpoint = base_url.rstrip("/") + "/embeddings"
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
        )

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        response = self._session.post(
            self._endpoint,
            json={"model": self._model, "input": texts},
            timeout=self._timeout,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data")
        if not isinstance(data, list) or len(data) != len(texts):
            raise ValueError(
                "Embeddings API returned an unexpected payload shape."
            )

        return [item["embedding"] for item in data]

    def embed_query(self, text: str) -> list[float]:
        embeddings = self.embed_documents([text])
        if not embeddings:
            raise ValueError("Expected one embedding for the query text.")
        return embeddings[0]


class VectorStoreRetriever(BaseRetriever):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    embeddings: Embeddings
    top_k: int = DEFAULT_RETRIEVER_TOP_K
    embedding_model: str = DEFAULT_EMBEDDING_MODEL

    def _get_relevant_documents(self, query: str) -> list[Document]:
        query_embedding = self.embeddings.embed_query(query)

        with Session(engine) as session:
            results = list(
                session.execute(
                    select(VectorStore)
                    .where(VectorStore.is_current.is_(True))
                    .where(VectorStore.embedding_model == self.embedding_model)
                    .order_by(
                        VectorStore.embedding.cosine_distance(query_embedding)
                    )
                    .limit(self.top_k)
                )
                .scalars()
                .all()
            )

        return [
            Document(
                page_content=row.chunk_text,
                metadata={
                    "embedding_id": str(row.embedding_id),
                    "source_id": str(row.source_id),
                    "chunk_index": row.chunk_index,
                    "chunk_type": row.chunk_type.value,
                    "embedding_model": row.embedding_model,
                    "pipeline_version": row.pipeline_version,
                },
            )
            for row in results
        ]


def _observation_code_text(observation: ObservationContext) -> str:
    if observation.display:
        return observation.display
    return observation.code


def _observation_value_text(observation: ObservationContext) -> str | None:
    if observation.value_num is not None and observation.unit:
        return f"{observation.value_num} {observation.unit}"
    if observation.value_num is not None:
        return str(observation.value_num)
    if observation.value_text:
        return observation.value_text
    return None


def _observation_interpretation_text(
    observation: ObservationContext,
) -> str | None:
    return observation.interpretation


def _is_abnormal_observation(observation: ObservationContext) -> bool:
    interpretation = _observation_interpretation_text(observation)
    if interpretation is None:
        return False
    return interpretation.upper() in _ABNORMAL_INTERPRETATIONS


def build_semantic_search_query(
    current_observations: list[ObservationContext],
) -> str | None:
    abnormal_fragments: list[str] = []

    for observation in current_observations:
        if not _is_abnormal_observation(observation):
            continue

        analyte = _observation_code_text(observation)
        value_text = _observation_value_text(observation)
        interpretation = _observation_interpretation_text(observation)

        fragment = analyte
        if value_text is not None:
            fragment += f" {value_text}"
        if interpretation is not None:
            fragment += f" ({interpretation.title()})"

        abnormal_fragments.append(fragment)

    if not abnormal_fragments:
        return None

    return "Patient has abnormal results: " + ", ".join(abnormal_fragments)


def build_default_retriever(
    *, top_k: int = DEFAULT_RETRIEVER_TOP_K
) -> BaseRetriever:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "OPENAI_API_KEY must be set to query semantic guideline embeddings."
        )

    embeddings = OpenAICompatibleEmbeddings(
        api_key=api_key,
        base_url=os.getenv("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL),
        model=DEFAULT_EMBEDDING_MODEL,
    )
    return VectorStoreRetriever(
        embeddings=embeddings,
        top_k=top_k,
        embedding_model=DEFAULT_EMBEDDING_MODEL,
    )


def retrieve_guideline_context(
    request: AIEnrichmentRequest,
    *,
    retriever: BaseRetriever | Any | None = None,
) -> list[Document]:
    query = build_semantic_search_query(request.current_observations)
    if query is None:
        return []

    active_retriever = retriever or build_default_retriever()
    return active_retriever.invoke(query)


def _observation_prompt_value(
    observation: ObservationContext,
) -> float | str:
    if observation.value_num is not None:
        return float(observation.value_num)
    if observation.value_text is not None:
        return observation.value_text
    return "N/A"


def _observation_prompt_flag(observation: ObservationContext) -> str:
    if observation.interpretation:
        return observation.interpretation.upper()
    return "NORMAL"


def _to_observation_row(observation: ObservationContext) -> ObservationRow:
    return ObservationRow(
        analyte_code=observation.code,
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


def _to_historical_observation(
    observation: ObservationContext,
) -> HistoricalObservation:
    return HistoricalObservation(
        analyte_code=observation.code,
        value=_observation_prompt_value(observation),
        unit=observation.unit,
        collected_at=observation.effective_at.isoformat(),
        flag=_observation_prompt_flag(observation),
        date=observation.effective_at.isoformat(),
    )


def _to_rag_chunk(document: Document) -> RagChunk:
    metadata = document.metadata or {}
    source_id = metadata.get("source_id", "")
    chunk_index = metadata.get("chunk_index", 0)
    similarity_score = metadata.get("similarity_score", 0.0)

    return RagChunk(
        source_type=str(metadata.get("source_type", "DOCUMENT")),
        source_id=str(source_id),
        chunk_index=int(chunk_index),
        chunk_text=document.page_content,
        similarity_score=float(similarity_score),
    )


def build_annotation_prompt_messages(
    request: AIEnrichmentRequest,
    *,
    guideline_context: list[Document],
) -> list[BaseMessage]:
    prompt_inputs = build_annotation_prompt_inputs(
        ingestion_id=str(request.ingestion_id),
        panel_codes=request.panel_codes,
        collected_at=request.collected_at.isoformat(),
        observations=[
            _to_observation_row(observation)
            for observation in request.current_observations
        ],
        historical_observations=[
            _to_historical_observation(observation)
            for observation in request.historical_observations
        ],
        rag_chunks=[_to_rag_chunk(document) for document in guideline_context],
    )
    return ANNOTATION_PROMPT.format_messages(**prompt_inputs)


def build_default_llm() -> BaseChatModel | None:
    model_id = os.getenv("BEDROCK_MODEL_ID")
    if not model_id:
        return None

    region_name = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    llm_kwargs: dict[str, Any] = {
        "model_id": model_id,
        "provider": DEFAULT_BEDROCK_PROVIDER,
        "model_kwargs": {
            "temperature": DEFAULT_BEDROCK_TEMPERATURE,
            "max_tokens": DEFAULT_BEDROCK_MAX_TOKENS,
        },
    }
    if region_name:
        llm_kwargs["region_name"] = region_name

    return ChatBedrock(**llm_kwargs)


def _missing_ai_config_reason() -> str:
    return (
        "BEDROCK_MODEL_ID is not set; AI enrichment cannot invoke a "
        "Bedrock model."
    )


def _llm_response_text(response: Any) -> str:
    content = getattr(response, "content", response)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                text_parts.append(item)
                continue
            if isinstance(item, dict) and item.get("type") == "text":
                text = item.get("text")
                if isinstance(text, str):
                    text_parts.append(text)
        return "\n".join(part for part in text_parts if part)
    return str(content)


def invoke_annotation_llm(
    prompt_messages: list[BaseMessage],
    *,
    llm: BaseChatModel | Any | None = None,
) -> str | None:
    active_llm = llm or build_default_llm()
    if active_llm is None:
        return None

    response = active_llm.invoke(prompt_messages)
    return _llm_response_text(response)


def _serialize_prompt_messages(prompt_messages: list[BaseMessage]) -> str:
    parts: list[str] = []
    for message in prompt_messages:
        parts.append(f"{message.type}:{message.content}")
    return "\n\n".join(parts)


def _build_input_hash(prompt_messages: list[BaseMessage]) -> str:
    prompt_text = _serialize_prompt_messages(prompt_messages)
    return hashlib.sha256(prompt_text.encode("utf-8")).hexdigest()


def _infer_provider(active_llm: BaseChatModel | Any | None) -> str | None:
    if active_llm is None:
        return None
    if isinstance(active_llm, ChatBedrock):
        return "amazon_bedrock"
    return getattr(active_llm, "provider", active_llm.__class__.__name__)


def _infer_model_id(active_llm: BaseChatModel | Any | None) -> str | None:
    if active_llm is None:
        return None
    model_id = getattr(active_llm, "model_id", None)
    if model_id is not None:
        return str(model_id)
    model_name = getattr(active_llm, "_model", None)
    if model_name is not None:
        return str(model_name)
    return None


def _infer_temperature(active_llm: BaseChatModel | Any | None) -> str | None:
    if active_llm is None:
        return None
    temperature = getattr(active_llm, "temperature", None)
    if temperature is not None:
        return str(temperature)

    model_kwargs = getattr(active_llm, "model_kwargs", None)
    if (
        isinstance(model_kwargs, dict)
        and model_kwargs.get("temperature") is not None
    ):
        return str(model_kwargs["temperature"])
    return None


def _strip_json_code_fences(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped

    lines = stripped.splitlines()
    if not lines:
        return stripped

    first_line = lines[0].strip().lower()
    if first_line not in {"```", "```json"}:
        return stripped

    if len(lines) >= 2 and lines[-1].strip() == "```":
        return "\n".join(lines[1:-1]).strip()

    # Closing fence absent (truncated response) — strip the opening line anyway.
    if len(lines) >= 2:
        return "\n".join(lines[1:]).strip()

    return stripped


def parse_annotation_response(
    llm_response_text: str | None,
) -> AIAnnotationContent | None:
    if llm_response_text is None:
        return None
    return parse_ai_annotation_content(
        _strip_json_code_fences(llm_response_text)
    )


def orchestrate_ai_enrichment(
    request: AIEnrichmentRequest,
    *,
    retriever: BaseRetriever | Any | None = None,
    llm: BaseChatModel | Any | None = None,
) -> AIEnrichmentResult:
    active_llm = llm or build_default_llm()
    if active_llm is None:
        prompt_messages: list[BaseMessage] = []
        return AIEnrichmentResult(
            guideline_context=[],
            prompt_messages=prompt_messages,
            llm_response_text=None,
            llm_response_content=None,
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

    guideline_context = retrieve_guideline_context(
        request, retriever=retriever
    )
    prompt_messages = build_annotation_prompt_messages(
        request,
        guideline_context=guideline_context,
    )
    llm_response_text = invoke_annotation_llm(
        prompt_messages,
        llm=active_llm,
    )
    llm_response_content: AIAnnotationContent | None = None
    rejection_reason: str | None = None
    if llm_response_text is not None:
        try:
            llm_response_content = parse_annotation_response(llm_response_text)
        except ValidationError as exc:
            rejection_reason = str(exc)
    return AIEnrichmentResult(
        guideline_context=guideline_context,
        prompt_messages=prompt_messages,
        llm_response_text=llm_response_text,
        llm_response_content=llm_response_content,
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
