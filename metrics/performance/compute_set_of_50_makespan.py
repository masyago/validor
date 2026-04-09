"""Compute end-to-end makespan for a set_of_50 run from benchmark_results.csv.

Definition (with existing recorded timestamps):
- Batch start = min(api_received_at_utc) across ingestion rows
- Batch end   = max(measured_at_utc) across ingestion rows
- Makespan    = batch_end - batch_start

This measures "first accepted upload" -> "last completion" and therefore includes
any gaps in uploading (intentional throttling, pauses, etc.).

Usage:
  uv run python -m metrics.compute_set_of_50_makespan \
    --csv metrics/benchmark_results.csv \
    --dataset set_of_50

Optional filters:
  --batch-id <id>        Filter to a single uploader batch_id if present.
  --since <iso>          Only consider rows with api_received_at_utc >= since.
  --until <iso>          Only consider rows with api_received_at_utc <= until.

The script prints a one-line summary suitable for pasting into notes.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


def _parse_dt(raw: str) -> datetime | None:
    raw = (raw or "").strip()
    if not raw:
        return None

    # Accept both Z and offset ISO-8601.
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None

    # Normalize naive timestamps as UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class Row:
    dataset: str
    measured_at_utc: datetime
    api_received_at_utc: datetime
    status: str
    ingestion_id: str
    source_filename: str
    batch_id: str


def _iter_rows(path: Path):
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            yield r


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Compute set_of_50 end-to-end makespan from benchmark_results.csv"
    )
    ap.add_argument(
        "--csv", required=True, help="Path to benchmark_results.csv"
    )
    ap.add_argument(
        "--dataset",
        default="set_of_50",
        help="Dataset name to filter (default: set_of_50)",
    )
    ap.add_argument(
        "--batch-id",
        default=None,
        help="Optional: restrict to rows with this batch_id (if present)",
    )
    ap.add_argument(
        "--since",
        default=None,
        help="Optional ISO timestamp; filter api_received_at_utc >= since",
    )
    ap.add_argument(
        "--until",
        default=None,
        help="Optional ISO timestamp; filter api_received_at_utc <= until",
    )
    ap.add_argument(
        "--latest-run",
        action="store_true",
        help=(
            "If set, auto-select the most recent contiguous run based on api_received_at_utc gaps. "
            "Useful when benchmark_results.csv contains multiple historical runs."
        ),
    )
    ap.add_argument(
        "--run-gap-seconds",
        type=float,
        default=120.0,
        help=(
            "Gap threshold (seconds) used with --latest-run to split runs. "
            "Default 120s. If your uploader is slower between files, increase this."
        ),
    )

    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(str(csv_path))

    since = _parse_dt(args.since) if args.since else None
    until = _parse_dt(args.until) if args.until else None

    rows: list[Row] = []

    for r in _iter_rows(csv_path):
        dataset = (r.get("dataset") or "").strip()
        if dataset != args.dataset:
            continue

        # Ignore the aggregate batch rows if they exist.
        ingestion_id = (r.get("ingestion_id") or "").strip()
        if not ingestion_id:
            continue

        measured_at = _parse_dt(r.get("measured_at_utc") or "")
        api_received_at = _parse_dt(r.get("api_received_at_utc") or "")
        if measured_at is None or api_received_at is None:
            continue

        if since is not None and api_received_at < since:
            continue
        if until is not None and api_received_at > until:
            continue

        batch_id = (r.get("batch_id") or "").strip()
        if args.batch_id is not None and batch_id != args.batch_id:
            continue

        rows.append(
            Row(
                dataset=dataset,
                measured_at_utc=measured_at,
                api_received_at_utc=api_received_at,
                status=(r.get("status") or "").strip(),
                ingestion_id=ingestion_id,
                source_filename=(r.get("source_filename") or "").strip(),
                batch_id=batch_id,
            )
        )

    if not rows:
        print("No matching ingestion rows found.")
        return 2

    if args.latest_run:
        rows_sorted = sorted(rows, key=lambda r: r.api_received_at_utc)
        selected: list[Row] = []
        last_ts: datetime | None = None
        gap_s = float(args.run_gap_seconds)

        # Walk backwards to pick the last contiguous cluster.
        for r in reversed(rows_sorted):
            if last_ts is None:
                selected.append(r)
                last_ts = r.api_received_at_utc
                continue
            if (last_ts - r.api_received_at_utc).total_seconds() <= gap_s:
                selected.append(r)
                last_ts = r.api_received_at_utc
            else:
                break

        rows = list(reversed(selected))

    start = min(r.api_received_at_utc for r in rows)
    end = max(r.measured_at_utc for r in rows)
    makespan_s = (end - start).total_seconds()

    completed = sum(1 for r in rows if r.status.upper() == "COMPLETED")
    failed = len(rows) - completed

    files_per_min = None
    if makespan_s > 0:
        files_per_min = (len(rows) / makespan_s) * 60.0

    batch_id = args.batch_id or (rows[0].batch_id if rows[0].batch_id else "")

    print(
        " ".join(
            [
                f"dataset={args.dataset}",
                f"batch_id={batch_id or '-'}",
                f"file_count={len(rows)}",
                f"completed={completed}",
                f"failed={failed}",
                f"batch_start_utc={start.isoformat()}",
                f"batch_end_utc={end.isoformat()}",
                f"makespan_s={makespan_s:.3f}",
                (
                    f"files_per_min={(files_per_min if files_per_min is not None else float('nan')):.3f}"
                    if files_per_min is not None
                    else "files_per_min="
                ),
                (
                    f"latest_run_gap_s={float(args.run_gap_seconds):.3f}"
                    if args.latest_run
                    else ""
                ),
            ]
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
