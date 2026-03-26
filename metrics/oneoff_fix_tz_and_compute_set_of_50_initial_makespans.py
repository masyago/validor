"""ONE-OFF: fix measured_at_utc timezone offset (-7h) and compute makespans.

Context
- Early benchmark runs had `measured_at_utc` recorded ~7 hours off.
- We want to correct ONLY `measured_at_utc` by subtracting 7 hours.
- Then compute per-replicate makespan for initial set_of_50 runs using fixed CSV row ranges.

Assumptions (intentionally rigid; one-time script)
- Input CSV: metrics/benchmark_results.csv
- Only rows 21..270 (inclusive) are used.
- Replicate row ranges (inclusive, 1-based row index in the CSV *data*, not counting header):
    replicate 1:  21..70
    replicate 2:  71..120
    replicate 3: 121..170
    replicate 4: 171..220
    replicate 5: 221..270
- Makespan definition for each replicate (matches manual extraction):
    corrected(measured_at_utc at END row) - api_received_at_utc at START row

Usage
  uv run python -m metrics.oneoff_fix_tz_and_compute_set_of_50_initial_makespans

Output
- One line per replicate: start/end/makespan/files_per_min.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


CSV_PATH = Path("metrics/benchmark_results.csv")

# 7-hour correction to measured_at_utc.
# The historical issue was: the value stored in `measured_at_utc` was actually
# local time (UTC-7) but labeled as UTC. To get real UTC, subtract 7 hours.
MEASURED_AT_OFFSET = timedelta(hours=-7)

# Fixed replicate ranges: (replicate_number, start_row, end_row) inclusive.
# IMPORTANT: These are CSV line numbers INCLUDING the header row.
# (So line 1 is the header; line 2 is the first data row.)
REPLICATES = [
    (1, 21, 70),
    (2, 71, 120),
    (3, 121, 170),
    (4, 171, 220),
    (5, 221, 270),
]


def _parse_dt(raw: str) -> datetime:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("missing timestamp")

    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class Times:
    api_received_at_utc: datetime
    measured_at_utc_corrected: datetime


def _load_rows() -> list[dict[str, str]]:
    if not CSV_PATH.exists():
        raise FileNotFoundError(str(CSV_PATH))

    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _extract_times(row: dict[str, str]) -> Times:
    api_received = _parse_dt(row["api_received_at_utc"])
    measured_at = _parse_dt(row["measured_at_utc"]) + MEASURED_AT_OFFSET
    return Times(
        api_received_at_utc=api_received,
        measured_at_utc_corrected=measured_at,
    )


def main() -> int:
    rows = _load_rows()

    for rep, start, end in REPLICATES:
        # Convert CSV line numbers (including header) -> 1-based data row indices.
        # Example: line 2 -> data row 1.
        start_data = start - 1
        end_data = end - 1

        if (
            start_data < 1
            or end_data < 1
            or start_data > len(rows)
            or end_data > len(rows)
        ):
            print(f"replicate={rep} rows={start}-{end} out_of_range")
            continue

        start_row = rows[start_data - 1]
        end_row = rows[end_data - 1]

        start_ingestion_id = (start_row.get("ingestion_id") or "").strip()
        end_ingestion_id = (end_row.get("ingestion_id") or "").strip()
        if not start_ingestion_id or not end_ingestion_id:
            print(f"replicate={rep} rows={start}-{end} missing_ingestion_id")
            continue

        batch_start = _parse_dt(start_row["api_received_at_utc"])
        measured_end_raw = _parse_dt(end_row["measured_at_utc"])
        batch_end = measured_end_raw + MEASURED_AT_OFFSET

        makespan_s = (batch_end - batch_start).total_seconds()

        file_count = end_data - start_data + 1
        files_per_min = (
            (file_count / makespan_s) * 60.0 if makespan_s > 0 else 0.0
        )

        print(
            " ".join(
                [
                    f"replicate={rep}",
                    f"rows={start}-{end}",
                    f"file_count={file_count}",
                    f"batch_start_utc={batch_start.isoformat()}",
                    f"batch_end_utc={batch_end.isoformat()}",
                    f"makespan_s={makespan_s:.3f}",
                    f"files_per_min={files_per_min:.3f}",
                    f"start_ingestion_id={start_ingestion_id}",
                    f"end_ingestion_id={end_ingestion_id}",
                ]
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
