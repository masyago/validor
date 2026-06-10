from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any
from uuid import UUID

import requests
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.retrievers import BaseRetriever
from pydantic import ConfigDict
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.persistence.db import engine
from app.persistence.models.ai import VectorStore

# Keep embedding config aligned with scripts/embed_seed_documents.py and the
# vector_store schema (Vector(1536)).
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_RETRIEVER_TOP_K = 5

_ABNORMAL_INTERPRETATIONS = {"HIGH", "LOW", "ABNORMAL", "CRITICAL"}


@dataclass(frozen=True)
class AIEnrichmentRequest:
    ingestion_id: UUID
    patient_id: str
    current_observations: list[dict[str, Any]]
    historical_observations: list[dict[str, Any]]


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


def _observation_code_text(observation: dict[str, Any]) -> str:
    code = observation.get("code")
    if isinstance(code, dict):
        codings = code.get("coding")
        if isinstance(codings, list):
            for coding in codings:
                if not isinstance(coding, dict):
                    continue
                display = coding.get("display")
                if isinstance(display, str) and display.strip():
                    return display.strip()
                code_value = coding.get("code")
                if isinstance(code_value, str) and code_value.strip():
                    return code_value.strip()

        text = code.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip()

    return "Unknown analyte"


def _observation_value_text(observation: dict[str, Any]) -> str | None:
    value_quantity = observation.get("valueQuantity")
    if isinstance(value_quantity, dict):
        value = value_quantity.get("value")
        unit = value_quantity.get("unit")
        if value is not None and unit:
            return f"{value} {unit}"
        if value is not None:
            return str(value)

    value_string = observation.get("valueString")
    if isinstance(value_string, str) and value_string.strip():
        return value_string.strip()

    return None


def _observation_interpretation_text(
    observation: dict[str, Any],
) -> str | None:
    interpretation = observation.get("interpretation")
    if not isinstance(interpretation, list):
        return None

    for item in interpretation:
        if not isinstance(item, dict):
            continue
        codings = item.get("coding")
        if isinstance(codings, list):
            for coding in codings:
                if not isinstance(coding, dict):
                    continue
                display = coding.get("display")
                if isinstance(display, str) and display.strip():
                    return display.strip()
                code = coding.get("code")
                if isinstance(code, str) and code.strip():
                    return code.strip().upper()
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip()

    return None


def _is_abnormal_observation(observation: dict[str, Any]) -> bool:
    interpretation = _observation_interpretation_text(observation)
    if interpretation is None:
        return False
    return interpretation.upper() in _ABNORMAL_INTERPRETATIONS


def build_semantic_search_query(
    current_observations: list[dict[str, Any]],
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


def orchestrate_ai_enrichment(
    request: AIEnrichmentRequest,
) -> list[Document]:
    return retrieve_guideline_context(request)
