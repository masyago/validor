from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.persistence.db import engine
from app.persistence.models.ai import Document, DocumentTargetType

DEFAULT_DOCS_DIR = ROOT_DIR / "clinical_guidelines" / "docs_seed"


@dataclass(frozen=True)
class DocumentSeed:
    title: str
    target_type: DocumentTargetType
    target_code: str
    content: str
    content_format: str
    content_hash: str
    path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload seeded clinical guideline documents into PostgreSQL."
    )
    parser.add_argument(
        "--docs-dir",
        type=Path,
        default=DEFAULT_DOCS_DIR,
        help=f"Directory containing YAML guideline documents. Defaults to {DEFAULT_DOCS_DIR}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate the seed files without writing to the database.",
    )
    return parser.parse_args()


def infer_target(file_path: Path) -> tuple[DocumentTargetType, str]:
    stem = file_path.stem

    if stem.startswith("panel_"):
        return DocumentTargetType.PANEL, stem.removeprefix("panel_")

    if stem.startswith("analyte_"):
        return DocumentTargetType.ANALYTE, stem.removeprefix("analyte_")

    raise ValueError(
        f"Unsupported seed filename '{file_path.name}'. Expected panel_*.yaml or analyte_*.yaml."
    )


def load_seed(file_path: Path) -> DocumentSeed:
    raw_content = file_path.read_text(encoding="utf-8")
    payload = yaml.safe_load(raw_content) or {}

    if not isinstance(payload, dict):
        raise ValueError(
            f"Seed file '{file_path.name}' must contain a YAML mapping."
        )

    target_type, target_code = infer_target(file_path)
    title_key = (
        "panel_name"
        if target_type is DocumentTargetType.PANEL
        else "test_name"
    )
    title = (
        payload.get(title_key)
        or payload.get("panel_code")
        or payload.get("test_code")
    )

    if not isinstance(title, str) or not title.strip():
        raise ValueError(
            f"Seed file '{file_path.name}' is missing a non-empty '{title_key}' field."
        )

    return DocumentSeed(
        title=title.strip(),
        target_type=target_type,
        target_code=target_code,
        content=raw_content,
        content_format="application/yaml",
        content_hash=hashlib.sha256(raw_content.encode("utf-8")).hexdigest(),
        path=file_path,
    )


def load_seeds(docs_dir: Path) -> list[DocumentSeed]:
    if not docs_dir.exists():
        raise FileNotFoundError(f"Docs directory does not exist: {docs_dir}")

    seeds = [load_seed(path) for path in sorted(docs_dir.glob("*.yaml"))]

    if not seeds:
        raise ValueError(f"No YAML files found in {docs_dir}")

    seen_targets: set[tuple[DocumentTargetType, str]] = set()
    seen_hashes: set[str] = set()
    for seed in seeds:
        target_key = (seed.target_type, seed.target_code)
        if target_key in seen_targets:
            raise ValueError(
                f"Duplicate target {seed.target_type.value}:{seed.target_code} in seed files."
            )
        if seed.content_hash in seen_hashes:
            raise ValueError(
                f"Duplicate content detected in seed file '{seed.path.name}'."
            )
        seen_targets.add(target_key)
        seen_hashes.add(seed.content_hash)

    return seeds


def sync_documents(
    seeds: list[DocumentSeed], dry_run: bool
) -> tuple[int, int]:
    inserted = 0
    skipped = 0
    now = datetime.now(timezone.utc)

    with Session(engine) as session:
        existing_documents = session.execute(select(Document)).scalars().all()
        existing_by_target = {
            (document.target_type, document.target_code): document
            for document in existing_documents
        }
        existing_by_hash = {
            document.content_hash: document for document in existing_documents
        }

        for seed in seeds:
            target_key = (seed.target_type, seed.target_code)
            existing_target = existing_by_target.get(target_key)
            existing_hash = existing_by_hash.get(seed.content_hash)

            if existing_target:
                if existing_target.content_hash == seed.content_hash:
                    skipped += 1
                    continue
                raise ValueError(
                    "Conflicting document already exists for "
                    f"{seed.target_type.value}:{seed.target_code}. "
                    "The seed content differs from the stored row."
                )

            if existing_hash:
                raise ValueError(
                    "A document with identical content already exists for "
                    f"{existing_hash.target_type.value}:{existing_hash.target_code}, "
                    f"so '{seed.path.name}' would collide on content_hash."
                )

            if dry_run:
                inserted += 1
                continue

            document = Document(
                title=seed.title,
                target_type=seed.target_type,
                target_code=seed.target_code,
                content=seed.content,
                content_format=seed.content_format,
                created_at=now,
                last_updated_at=now,
                content_hash=seed.content_hash,
            )
            session.add(document)
            inserted += 1

        if not dry_run:
            session.commit()

    return inserted, skipped


def main() -> int:
    args = parse_args()
    seeds = load_seeds(args.docs_dir.resolve())
    inserted, skipped = sync_documents(seeds, dry_run=args.dry_run)

    mode = "Dry run" if args.dry_run else "Import"
    print(
        f"{mode} complete: parsed={len(seeds)} inserted={inserted} skipped={skipped}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
