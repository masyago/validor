from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.embed_seed_documents import (
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_OPENAI_BASE_URL,
    DEFAULT_PIPELINE_VERSION,
    EmbeddingClient,
    batched_embeddings,
    plan_embeddings,
    preview_chunk_plan,
)

DEFAULT_OUTPUT_PATH = ROOT_DIR / "fixtures" / "guideline_chunks.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Chunk persisted clinical guideline documents, generate "
            "embeddings once, and freeze the result into a fixture file. "
            "Manual/offline step only — never run this on container "
            "startup. Rerun whenever the guideline docs change."
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
        help=(
            "Pipeline version recorded on each fixture row. "
            f"Defaults to {DEFAULT_PIPELINE_VERSION}"
        ),
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
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help=f"Fixture file to write. Defaults to {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse docs and build chunk plans without calling the embeddings API or writing a fixture file.",
    )
    return parser.parse_args()


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

    fixture_rows = [
        {
            "chunk_text": plan.chunk_text,
            "chunk_index": plan.chunk_index,
            "chunk_type": plan.chunk_type.value,
            "source_id": str(plan.source_id),
            "content_hash": plan.content_hash,
            "embedding": vector,
            "embedding_model": args.embedding_model,
            "pipeline_version": args.pipeline_version,
        }
        for plan, vector in zip(planned_embeddings, vectors, strict=True)
    ]

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(fixture_rows, indent=2))

    print(
        "Fixture build complete: "
        f"documents={len(planned_embeddings) // 5} "
        f"chunks={len(fixture_rows)} output={output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
