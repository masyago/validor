"""Record validation-accuracy outcomes by querying the `ingestion` table.

This script is intended to be run *after* uploading a folder of CSV fixtures
(e.g., valid + invalid inputs) via `csv_uploader/csv_uploader.py`.

It produces a compact CSV suitable for measuring:
- which files were accepted/processed
- which failed validation and why

Output columns:
- file_name
- ingestion_id
- status
- error_code
- error_detail
- api_received_at

Usage:
  uv run python -m metrics.record_validation_accuracy_results \
    --dir metrics/validation_accuracy/fixed_csv_v1 \
    --out metrics/validation_accuracy/validation_results.csv

Optional:
  --since 2026-03-27T00:00:00+00:00
  --database-url postgresql+psycopg://...

Notes:
- Matches DB rows using `ingestion.source_filename` (uploader sends basename).
- If multiple ingestions exist for the same file name, selects the most recent
  by `api_received_at` (optionally constrained by --since).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.persistence.models.core import Ingestion


DEFAULT_FIXTURE_DIR = Path("metrics/validation_accuracy/fixed_csv_v1")
DEFAULT_OUT_CSV = Path("metrics/validation_accuracy/validation_results.csv")


def _default_database_url() -> str:
    return os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5432/cla",
    )


def _parse_dt(raw: str) -> datetime | None:
    raw = (raw or "").strip()
    if not raw:
        return None

    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _sanitize_cell(value: Any) -> str:
    if value is None:
        return ""
    s = str(value)
    return s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")


def _json_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return _sanitize_cell(value)
    try:
        return _sanitize_cell(json.dumps(value, sort_keys=True))
    except TypeError:
        # Fallback for non-JSON-serializable values.
        return _sanitize_cell(str(value))


@dataclass(frozen=True)
class ValidationResultRow:
    file_name: str
    ingestion_id: str
    status: str
    error_code: str
    error_detail: str
    api_received_at: str


_FIELDNAMES = [
    "file_name",
    "ingestion_id",
    "status",
    "error_code",
    "error_detail",
    "api_received_at",
]


def collect_validation_accuracy_results(
    *,
    session: Session,
    file_names: list[str],
    since: datetime | None = None,
) -> list[ValidationResultRow]:
    results: list[ValidationResultRow] = []

    for file_name in sorted(set(file_names)):
        stmt = select(Ingestion).where(Ingestion.source_filename == file_name)
        if since is not None:
            stmt = stmt.where(Ingestion.api_received_at >= since)
        stmt = stmt.order_by(Ingestion.api_received_at.desc())

        ingestion = session.scalars(stmt).first()
        if ingestion is None:
            results.append(
                ValidationResultRow(
                    file_name=file_name,
                    ingestion_id="",
                    status="",
                    error_code="",
                    error_detail="",
                    api_received_at="",
                )
            )
            continue

        results.append(
            ValidationResultRow(
                file_name=file_name,
                ingestion_id=str(ingestion.ingestion_id),
                status=_sanitize_cell(ingestion.status),
                error_code=_sanitize_cell(ingestion.error_code),
                error_detail=_json_cell(ingestion.error_detail),
                api_received_at=(
                    ""
                    if ingestion.api_received_at is None
                    else ingestion.api_received_at.astimezone(
                        timezone.utc
                    ).isoformat()
                ),
            )
        )

    return results


def write_results_csv(
    *, out_csv: Path, rows: list[ValidationResultRow]
) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDNAMES)
        writer.writeheader()
        for r in rows:
            writer.writerow(
                {
                    "file_name": r.file_name,
                    "ingestion_id": r.ingestion_id,
                    "status": r.status,
                    "error_code": r.error_code,
                    "error_detail": r.error_detail,
                    "api_received_at": r.api_received_at,
                }
            )


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Record validation outcomes by querying the ingestion table for a folder of uploaded CSVs."
        )
    )
    ap.add_argument(
        "--dir",
        default=str(DEFAULT_FIXTURE_DIR),
        help="Directory containing CSV fixtures (default: metrics/validation_accuracy/fixed_csv_v1)",
    )
    ap.add_argument(
        "--out",
        default=str(DEFAULT_OUT_CSV),
        help="Output CSV path (default: metrics/validation_accuracy/validation_results.csv)",
    )
    ap.add_argument(
        "--database-url",
        default=None,
        help="Database URL (default: DATABASE_URL env var or local docker DB)",
    )
    ap.add_argument(
        "--since",
        default=None,
        help=(
            "Optional ISO timestamp; if provided, only considers ingestions with api_received_at >= since. "
            "Useful to avoid mixing historical runs."
        ),
    )
    return ap.parse_args()


def main() -> int:
    args = _parse_args()

    fixture_dir = Path(args.dir)
    if not fixture_dir.exists():
        raise FileNotFoundError(str(fixture_dir))

    file_names = [p.name for p in fixture_dir.glob("*.csv")]
    if not file_names:
        print(f"No CSV files found in {fixture_dir}")
        return 2

    since = _parse_dt(args.since) if args.since else None
    db_url = args.database_url or _default_database_url()

    engine = create_engine(db_url)
    try:
        with Session(engine) as session:
            rows = collect_validation_accuracy_results(
                session=session,
                file_names=file_names,
                since=since,
            )
    finally:
        engine.dispose()

    out_csv = Path(args.out)
    write_results_csv(out_csv=out_csv, rows=rows)

    found = sum(1 for r in rows if r.ingestion_id)
    missing = len(rows) - found
    print(
        f"Wrote {len(rows)} rows to {out_csv} (found={found} missing={missing})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
