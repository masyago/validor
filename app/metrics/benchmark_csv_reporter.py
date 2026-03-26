from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


try:
    import fcntl  # type: ignore
except Exception:  # pragma: no cover
    fcntl = None  # type: ignore


_DEFAULT_TOP_N = 5
_DEFAULT_FP_MAX_CHARS = 800


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _sanitize_cell(value: Any, *, max_chars: int) -> str:
    if value is None:
        return ""
    s = str(value)
    s = s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    if len(s) > max_chars:
        return s[: max_chars - 3] + "..."
    return s


@dataclass(frozen=True)
class BenchmarkTopItem:
    fingerprint: str
    total_time_s: float
    count: int


def _coerce_top_items(raw_items: Any) -> list[BenchmarkTopItem]:
    if not raw_items:
        return []
    items: list[BenchmarkTopItem] = []
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        fp = it.get("fingerprint")
        if fp is None:
            continue
        items.append(
            BenchmarkTopItem(
                fingerprint=str(fp),
                total_time_s=float(it.get("total_time_s") or 0.0),
                count=int(it.get("count") or 0),
            )
        )
    return items


def benchmark_results_csv_path() -> str | None:
    """Where to append benchmark rows.

    When set, each processed ingestion appends a single CSV row suitable for
    import into Google Sheets.

    Enable via `CLA_BENCHMARK_RESULTS_CSV=path/to/results.csv`.
    """

    p = os.getenv("CLA_BENCHMARK_RESULTS_CSV")
    if not p:
        return None
    return p


def benchmark_fieldnames(*, top_n: int) -> list[str]:
    base = [
        "measured_at_utc",
        "git_sha",
        "api_base_url",
        "dataset",
        "source_filename",
        "ingestion_id",
        "instrument_id",
        "run_id",
        "uploader_id",
        "spec_version",
        "status",
        "idempotency_disposition",
        "error_code",
        "content_size_bytes",
        "server_sha256",
        "submitted_sha256",
        "uploader_received_at_utc",
        "api_received_at_utc",
        "end_to_end_s",
        "wall_time_s",
        "sql_query_count",
        "sql_total_db_time_s",
    ]

    cols: list[str] = []
    for i in range(1, top_n + 1):
        cols.extend(
            [
                f"sql_top_time_{i}_fingerprint",
                f"sql_top_time_{i}_total_time_s",
                f"sql_top_time_{i}_count",
            ]
        )
    for i in range(1, top_n + 1):
        cols.extend(
            [
                f"sql_top_count_{i}_fingerprint",
                f"sql_top_count_{i}_total_time_s",
                f"sql_top_count_{i}_count",
            ]
        )

    # NOTE: Any newly added columns must go at the END to preserve stable
    # positions for existing per-ingestion columns in downstream tooling.
    batch_cols = [
        "result_kind",
        "batch_id",
        "batch_file_count",
        "batch_completed_count",
        "batch_failed_count",
        "batch_total_wall_time_s",
        "batch_files_per_min",
    ]

    return base + cols + batch_cols


def append_benchmark_row(
    *,
    csv_path: str,
    measured_at: datetime,
    git_sha: str | None,
    api_base_url: str | None,
    dataset: str | None,
    source_filename: str | None,
    ingestion_id: str,
    instrument_id: str | None,
    run_id: str | None,
    uploader_id: str | None,
    spec_version: str | None,
    status: str | None,
    idempotency_disposition: str | None,
    error_code: str | None,
    content_size_bytes: int | None,
    server_sha256: str | None,
    submitted_sha256: str | None,
    uploader_received_at: datetime | None,
    api_received_at: datetime | None,
    end_to_end_s: float | None,
    wall_time_s: float | None,
    sql_query_count: int | None,
    sql_total_db_time_s: float | None,
    sql_top_by_total_time: Any = None,
    sql_top_by_count: Any = None,
) -> None:
    """Append a single benchmark row to a results CSV.

    Designed to be:
    - stable column order (good for Sheets import)
    - safe for concurrent append within one host (uses file lock when available)
    """

    top_n = _int_env("CLA_BENCHMARK_TOP_N", _DEFAULT_TOP_N)
    fp_max = _int_env("CLA_BENCHMARK_FP_MAX_CHARS", _DEFAULT_FP_MAX_CHARS)

    top_time = _coerce_top_items(sql_top_by_total_time)
    top_count = _coerce_top_items(sql_top_by_count)

    row: dict[str, Any] = {
        "measured_at_utc": measured_at.isoformat(),
        "git_sha": git_sha or "",
        "api_base_url": api_base_url or "",
        "dataset": dataset or "",
        "result_kind": "",
        "batch_id": "",
        "batch_file_count": "",
        "batch_completed_count": "",
        "batch_failed_count": "",
        "batch_total_wall_time_s": "",
        "batch_files_per_min": "",
        "source_filename": source_filename or "",
        "ingestion_id": ingestion_id,
        "instrument_id": instrument_id or "",
        "run_id": run_id or "",
        "uploader_id": uploader_id or "",
        "spec_version": spec_version or "",
        "status": status or "",
        "idempotency_disposition": idempotency_disposition or "",
        "error_code": error_code or "",
        "content_size_bytes": (
            "" if content_size_bytes is None else int(content_size_bytes)
        ),
        "server_sha256": server_sha256 or "",
        "submitted_sha256": submitted_sha256 or "",
        "uploader_received_at_utc": (
            ""
            if uploader_received_at is None
            else uploader_received_at.isoformat()
        ),
        "api_received_at_utc": (
            "" if api_received_at is None else api_received_at.isoformat()
        ),
        "end_to_end_s": "" if end_to_end_s is None else float(end_to_end_s),
        "wall_time_s": "" if wall_time_s is None else float(wall_time_s),
        "sql_query_count": (
            "" if sql_query_count is None else int(sql_query_count)
        ),
        "sql_total_db_time_s": (
            "" if sql_total_db_time_s is None else float(sql_total_db_time_s)
        ),
    }

    for i in range(1, top_n + 1):
        if i <= len(top_time):
            it = top_time[i - 1]
            row[f"sql_top_time_{i}_fingerprint"] = _sanitize_cell(
                it.fingerprint, max_chars=fp_max
            )
            row[f"sql_top_time_{i}_total_time_s"] = float(it.total_time_s)
            row[f"sql_top_time_{i}_count"] = int(it.count)
        else:
            row[f"sql_top_time_{i}_fingerprint"] = ""
            row[f"sql_top_time_{i}_total_time_s"] = ""
            row[f"sql_top_time_{i}_count"] = ""

    for i in range(1, top_n + 1):
        if i <= len(top_count):
            it = top_count[i - 1]
            row[f"sql_top_count_{i}_fingerprint"] = _sanitize_cell(
                it.fingerprint, max_chars=fp_max
            )
            row[f"sql_top_count_{i}_total_time_s"] = float(it.total_time_s)
            row[f"sql_top_count_{i}_count"] = int(it.count)
        else:
            row[f"sql_top_count_{i}_fingerprint"] = ""
            row[f"sql_top_count_{i}_total_time_s"] = ""
            row[f"sql_top_count_{i}_count"] = ""

    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = benchmark_fieldnames(top_n=top_n)

    # Use a lock for safe concurrent append on Unix.
    # On platforms without fcntl (or if it fails), we still attempt a best-effort append.
    with path.open("a+", newline="", encoding="utf-8") as f:
        if fcntl is not None:  # pragma: no cover (covered on Unix)
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            except Exception:
                pass

        f.seek(0, os.SEEK_END)
        need_header = f.tell() == 0

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        if need_header:
            writer.writeheader()
        writer.writerow(row)
        f.flush()

        if fcntl is not None:  # pragma: no cover
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass


def append_benchmark_batch_row(
    *,
    csv_path: str,
    measured_at: datetime,
    git_sha: str | None,
    api_base_url: str | None,
    dataset: str | None,
    batch_id: str,
    batch_file_count: int,
    batch_completed_count: int,
    batch_failed_count: int,
    batch_total_wall_time_s: float,
    batch_files_per_min: float | None,
) -> None:
    """Append an aggregate row representing a batch run (e.g., 50-file run).

    Uses the same CSV file and fieldnames as per-ingestion rows.
    """

    top_n = _int_env("CLA_BENCHMARK_TOP_N", _DEFAULT_TOP_N)

    row: dict[str, Any] = {
        "measured_at_utc": measured_at.isoformat(),
        "git_sha": git_sha or "",
        "api_base_url": api_base_url or "",
        "dataset": dataset or "",
        "result_kind": "batch",
        "batch_id": batch_id,
        "batch_file_count": int(batch_file_count),
        "batch_completed_count": int(batch_completed_count),
        "batch_failed_count": int(batch_failed_count),
        "batch_total_wall_time_s": float(batch_total_wall_time_s),
        "batch_files_per_min": (
            "" if batch_files_per_min is None else float(batch_files_per_min)
        ),
        # Ingestion-specific fields intentionally blank.
        "source_filename": "",
        "ingestion_id": "",
        "instrument_id": "",
        "run_id": "",
        "uploader_id": "",
        "spec_version": "",
        "status": "",
        "idempotency_disposition": "",
        "error_code": "",
        "content_size_bytes": "",
        "server_sha256": "",
        "submitted_sha256": "",
        "uploader_received_at_utc": "",
        "api_received_at_utc": "",
        "end_to_end_s": "",
        "wall_time_s": "",
        "sql_query_count": "",
        "sql_total_db_time_s": "",
    }

    # Top query columns intentionally blank for batch rows.
    fp_max = _int_env("CLA_BENCHMARK_FP_MAX_CHARS", _DEFAULT_FP_MAX_CHARS)
    for i in range(1, top_n + 1):
        row[f"sql_top_time_{i}_fingerprint"] = _sanitize_cell(
            "", max_chars=fp_max
        )
        row[f"sql_top_time_{i}_total_time_s"] = ""
        row[f"sql_top_time_{i}_count"] = ""

    for i in range(1, top_n + 1):
        row[f"sql_top_count_{i}_fingerprint"] = _sanitize_cell(
            "", max_chars=fp_max
        )
        row[f"sql_top_count_{i}_total_time_s"] = ""
        row[f"sql_top_count_{i}_count"] = ""

    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = benchmark_fieldnames(top_n=top_n)

    with path.open("a+", newline="", encoding="utf-8") as f:
        if fcntl is not None:  # pragma: no cover (covered on Unix)
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            except Exception:
                pass

        f.seek(0, os.SEEK_END)
        need_header = f.tell() == 0

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        if need_header:
            writer.writeheader()
        writer.writerow(row)
        f.flush()

        if fcntl is not None:  # pragma: no cover
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
