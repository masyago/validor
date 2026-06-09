from __future__ import annotations

import argparse
import hashlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session
import os
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.persistence.db import engine
from app.persistence.models.ai import ChunkType, Document, VectorStore

DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_PIPELINE_VERSION = "seed-doc-chunks-v1"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"


@dataclass(frozen=True)
class ChunkDraft:
    chunk_index: int
    chunk_type: ChunkType
    chunk_text: str


@dataclass(frozen=True)
class PlannedEmbedding:
    source_id: Any
    chunk_index: int
    chunk_type: ChunkType
    chunk_text: str
    content_hash: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Chunk persisted clinical guideline documents, generate embeddings, "
            "and upsert them into PostgreSQL vector_store."
        )
    )
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_EMBEDDING_MODEL,
        help=f"Embedding model name. Defaults to {DEFAULT_EMBEDDING_MODEL}",
    )
    parser.add_argument(
        "--pipeline-version",
        default=DEFAULT_PIPELINE_VERSION,
        help=f"Pipeline version used for idempotent upserts. Defaults to {DEFAULT_PIPELINE_VERSION}",
    )
    parser.add_argument(
        "--openai-base-url",
        default=os.getenv("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL),
        help="Base URL for the embeddings API. Defaults to OPENAI_BASE_URL or https://api.openai.com/v1",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Number of chunk texts to send per embeddings request. Defaults to 32",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse docs and build chunk plans without calling the embeddings API or writing vector rows.",
    )
    return parser.parse_args()


def load_seed_documents() -> list[Document]:
    with Session(engine) as session:
        documents = list(
            session.execute(
                select(Document).where(
                    Document.content_format == "application/yaml"
                )
            )
            .scalars()
            .all()
        )

    if not documents:
        raise ValueError(
            "No YAML documents found in the document table. Load seed documents first."
        )

    return documents


def _as_clean_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _list_repr(values: list[str]) -> str:
    return f"[{', '.join(values)}]" if values else "[]"


def _one_line(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return " ".join(text.split())


def _format_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, str):
        return _one_line(value) or ""
    if isinstance(value, list):
        return _list_repr(_as_clean_list(value))
    return str(value)


def _flatten_mapping(prefix: str, value: Any) -> list[str]:
    if isinstance(value, dict):
        parts: list[str] = []
        for key, nested_value in value.items():
            nested_prefix = f"{prefix}.{key}" if prefix else str(key)
            parts.extend(_flatten_mapping(nested_prefix, nested_value))
        return parts
    return [f"{prefix}: {_format_scalar(value)}"]


def _build_identity_chunk(payload: dict[str, Any], stem: str) -> str:
    if stem.startswith("analyte_"):
        return (
            f"test_name: {_format_scalar(payload.get('test_name'))} "
            f"· test_code: {_format_scalar(payload.get('test_code'))} "
            f"· panel_code: {_format_scalar(payload.get('panel_code'))}\n\n"
            f"synonyms: {_list_repr(_as_clean_list(payload.get('synonyms')))}\n\n"
            f"aliases: {_list_repr(_as_clean_list(payload.get('aliases')))} "
            f"· analyte_type: {_format_scalar(payload.get('analyte_type'))}"
        )

    return (
        f"panel_name: {_format_scalar(payload.get('panel_name'))} "
        f"· panel_code: {_format_scalar(payload.get('panel_code'))} "
        f"· panel_type: {_format_scalar(payload.get('panel_type'))}\n\n"
        f"includes_analytes: {_list_repr(_as_clean_list(payload.get('includes_analytes')))}\n\n"
        f"synonyms: {_list_repr(_as_clean_list(payload.get('synonyms')))}\n\n"
        f"aliases: {_list_repr(_as_clean_list(payload.get('aliases')))}"
    )


def _build_clinical_context_chunk(payload: dict[str, Any]) -> str:
    clinical_use = _one_line(payload.get("clinical_use"))
    if clinical_use is None:
        clinical_use = "No clinical use text documented."
    return f'clinical_use: "{clinical_use}"'


def _build_reference_ranges_chunk(payload: dict[str, Any]) -> str:
    reference_ranges = payload.get("reference_ranges")
    interpretation = payload.get("interpretation_summary")

    parts: list[str] = []
    if isinstance(reference_ranges, dict) and reference_ranges:
        for key, value in reference_ranges.items():
            if isinstance(value, dict):
                scalar_parts = [
                    f"{nested_key} {_format_scalar(nested_value)}"
                    for nested_key, nested_value in value.items()
                ]
                parts.append(f"{key}: {' · '.join(scalar_parts)}")
            else:
                parts.append(f"{key}: {_format_scalar(value)}")
    else:
        parts.append("reference_ranges: none documented")

    critical_high = None
    critical_low = None
    if isinstance(interpretation, dict):
        critical_high = interpretation.get("critical_high")
        critical_low = interpretation.get("critical_low")

    parts.append(f"critical_high: {_format_scalar(critical_high)}")
    parts.append(f"critical_low: {_format_scalar(critical_low)}")
    return " · ".join(parts)


def _build_interpretation_chunk(payload: dict[str, Any]) -> str:
    interpretation = payload.get("interpretation_summary")
    if not isinstance(interpretation, dict) or not interpretation:
        return "interpretation: none documented"

    parts = [
        part
        for key, value in interpretation.items()
        if key not in {"critical_high", "critical_low"}
        for part in _flatten_mapping(str(key), value)
    ]

    if not parts:
        return "interpretation: none documented"

    return " · ".join(parts)


def _build_confounders_chunk(payload: dict[str, Any]) -> str:
    confounders = _as_clean_list(payload.get("confounders"))
    if not confounders:
        return "confounders: none documented"
    return " · ".join(confounders)


def build_chunks(
    document: Document, payload: dict[str, Any]
) -> list[ChunkDraft]:
    return [
        ChunkDraft(
            chunk_index=0,
            chunk_type=ChunkType.IDENTITY,
            chunk_text=_build_identity_chunk(
                payload,
                (
                    "analyte"
                    if document.target_type.value == "ANALYTE"
                    else "panel"
                ),
            ),
        ),
        ChunkDraft(
            chunk_index=1,
            chunk_type=ChunkType.CLINICAL_CONTEXT,
            chunk_text=_build_clinical_context_chunk(payload),
        ),
        ChunkDraft(
            chunk_index=2,
            chunk_type=ChunkType.REF_RANGES,
            chunk_text=_build_reference_ranges_chunk(payload),
        ),
        ChunkDraft(
            chunk_index=3,
            chunk_type=ChunkType.INTERPRETATION,
            chunk_text=_build_interpretation_chunk(payload),
        ),
        ChunkDraft(
            chunk_index=4,
            chunk_type=ChunkType.CONFOUNDERS,
            chunk_text=_build_confounders_chunk(payload),
        ),
    ]


def preview_chunk_plan() -> tuple[int, int]:
    documents = load_seed_documents()
    chunk_count = 0

    for document in documents:
        payload = yaml.safe_load(document.content) or {}
        if not isinstance(payload, dict):
            raise ValueError(
                f"Document {document.doc_id} must contain a YAML mapping."
            )
        chunk_count += len(build_chunks(document, payload))

    return len(documents), chunk_count


def plan_embeddings() -> list[PlannedEmbedding]:
    documents = load_seed_documents()
    planned: list[PlannedEmbedding] = []
    for document in documents:
        payload = yaml.safe_load(document.content) or {}
        if not isinstance(payload, dict):
            raise ValueError(
                f"Document {document.doc_id} must contain a YAML mapping."
            )

        for chunk in build_chunks(document, payload):
            content_hash = hashlib.sha256(
                f"{document.doc_id}|{chunk.chunk_index}|{chunk.chunk_type.value}|{chunk.chunk_text}".encode(
                    "utf-8"
                )
            ).hexdigest()
            planned.append(
                PlannedEmbedding(
                    source_id=document.doc_id,
                    chunk_index=chunk.chunk_index,
                    chunk_type=chunk.chunk_type,
                    chunk_text=chunk.chunk_text,
                    content_hash=content_hash,
                )
            )

    return planned


def main() -> int:
    args = parse_args()

    if args.dry_run:
        document_count, chunk_count = preview_chunk_plan()
        print(
            "Dry run complete: "
            f"documents={document_count} chunks={chunk_count}"
        )
        return 0

    planned_embeddings = plan_embeddings()

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "OPENAI_API_KEY must be set to generate embeddings."
        )

    client = EmbeddingClient(
        api_key=api_key,
        base_url=args.openai_base_url,
        model=args.embedding_model,
    )
    vectors = batched_embeddings(
        client,
        [item.chunk_text for item in planned_embeddings],
        batch_size=args.batch_size,
    )
    inserted, updated, skipped = upsert_embeddings(
        planned_embeddings,
        vectors,
        embedding_model=args.embedding_model,
        pipeline_version=args.pipeline_version,
    )
    print(
        "Embedding sync complete: "
        f"documents={len(planned_embeddings) // 5} "
        f"chunks={len(planned_embeddings)} inserted={inserted} updated={updated} skipped={skipped}"
    )
    return 0


class EmbeddingClient:
    def __init__(
        self, api_key: str, base_url: str, model: str, timeout: int = 60
    ):
        self._model = model
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
        )
        self._endpoint = base_url.rstrip("/") + "/embeddings"

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
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


def batched_embeddings(
    client: EmbeddingClient, texts: list[str], batch_size: int
) -> list[list[float]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero.")

    embeddings: list[list[float]] = []
    for start in range(0, len(texts), batch_size):
        embeddings.extend(
            client.embed_texts(texts[start : start + batch_size])
        )
    return embeddings


def upsert_embeddings(
    planned_embeddings: list[PlannedEmbedding],
    vectors: list[list[float]],
    embedding_model: str,
    pipeline_version: str,
) -> tuple[int, int, int]:
    if len(planned_embeddings) != len(vectors):
        raise ValueError(
            "Chunk plan and embedding vector counts do not match."
        )

    inserted = 0
    updated = 0
    skipped = 0
    expected_keys = {
        (item.source_id, item.chunk_index) for item in planned_embeddings
    }
    source_ids = sorted(
        {item.source_id for item in planned_embeddings}, key=str
    )

    with Session(engine) as session:
        existing_rows = (
            session.execute(
                select(VectorStore).where(
                    VectorStore.source_id.in_(source_ids),
                    VectorStore.embedding_model == embedding_model,
                    VectorStore.pipeline_version == pipeline_version,
                )
            )
            .scalars()
            .all()
        )
        existing_by_key = {
            (row.source_id, row.chunk_index): row for row in existing_rows
        }

        for plan, vector in zip(planned_embeddings, vectors, strict=True):
            key = (plan.source_id, plan.chunk_index)
            existing = existing_by_key.get(key)

            if existing is None:
                session.add(
                    VectorStore(
                        source_id=plan.source_id,
                        chunk_index=plan.chunk_index,
                        chunk_type=plan.chunk_type,
                        chunk_text=plan.chunk_text,
                        content_hash=plan.content_hash,
                        embedding_model=embedding_model,
                        pipeline_version=pipeline_version,
                        is_current=True,
                        embedding=vector,
                    )
                )
                inserted += 1
                continue

            if (
                existing.content_hash == plan.content_hash
                and existing.chunk_text == plan.chunk_text
                and existing.chunk_type == plan.chunk_type
                and existing.is_current
            ):
                skipped += 1
                continue

            existing.chunk_type = plan.chunk_type
            existing.chunk_text = plan.chunk_text
            existing.content_hash = plan.content_hash
            existing.embedding = vector
            existing.is_current = True
            updated += 1

        for row in existing_rows:
            if (
                row.source_id,
                row.chunk_index,
            ) not in expected_keys and row.is_current:
                row.is_current = False
                updated += 1

        session.commit()

    return inserted, updated, skipped


if __name__ == "__main__":
    raise SystemExit(main())
