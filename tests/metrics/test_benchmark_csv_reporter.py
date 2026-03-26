from __future__ import annotations

from datetime import datetime, timezone

from app.metrics.benchmark_csv_reporter import (
    append_benchmark_batch_row,
    append_benchmark_row,
)


def test_append_benchmark_row_writes_header_once(tmp_path):
    out = tmp_path / "results.csv"

    t = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

    append_benchmark_row(
        csv_path=str(out),
        measured_at=t,
        git_sha="deadbeef",
        api_base_url="http://localhost:8000",
        dataset="small",
        source_filename="small.csv",
        ingestion_id="11111111-1111-1111-1111-111111111111",
        instrument_id="CANONICAL_CHEM_ANALYZER_V1",
        run_id="20260320_foo",
        uploader_id="uploader-1",
        spec_version="analyzer_csv_v1",
        status="COMPLETED",
        idempotency_disposition="CREATED",
        error_code=None,
        content_size_bytes=1234,
        server_sha256="abc",
        submitted_sha256=None,
        uploader_received_at=t,
        api_received_at=t,
        end_to_end_s=1.23,
        wall_time_s=0.98,
        sql_query_count=10,
        sql_total_db_time_s=0.5,
        sql_top_by_total_time=[
            {"fingerprint": "select 1", "total_time_s": 0.4, "count": 2}
        ],
        sql_top_by_count=[
            {"fingerprint": "select 2", "total_time_s": 0.1, "count": 5}
        ],
    )

    append_benchmark_row(
        csv_path=str(out),
        measured_at=t,
        git_sha="deadbeef",
        api_base_url="http://localhost:8000",
        dataset="small",
        source_filename="small.csv",
        ingestion_id="22222222-2222-2222-2222-222222222222",
        instrument_id="CANONICAL_CHEM_ANALYZER_V1",
        run_id="20260320_bar",
        uploader_id="uploader-1",
        spec_version="analyzer_csv_v1",
        status="COMPLETED",
        idempotency_disposition="CREATED",
        error_code=None,
        content_size_bytes=1234,
        server_sha256="abc",
        submitted_sha256=None,
        uploader_received_at=t,
        api_received_at=t,
        end_to_end_s=1.23,
        wall_time_s=0.98,
        sql_query_count=10,
        sql_total_db_time_s=0.5,
        sql_top_by_total_time=[],
        sql_top_by_count=[],
    )

    lines = out.read_text(encoding="utf-8").splitlines()

    # header + 2 rows
    assert len(lines) == 3
    assert lines[0].startswith("measured_at_utc,")
    assert "11111111-1111-1111-1111-111111111111" in lines[1]
    assert "22222222-2222-2222-2222-222222222222" in lines[2]


def test_append_benchmark_batch_row_appends_and_uses_same_header(tmp_path):
    out = tmp_path / "results.csv"

    t = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

    append_benchmark_row(
        csv_path=str(out),
        measured_at=t,
        git_sha="deadbeef",
        api_base_url="http://localhost:8000",
        dataset="set_of_50",
        source_filename="small_001.csv",
        ingestion_id="11111111-1111-1111-1111-111111111111",
        instrument_id="CANONICAL_CHEM_ANALYZER_V1",
        run_id="20260320_foo",
        uploader_id="uploader-1",
        spec_version="analyzer_csv_v1",
        status="COMPLETED",
        idempotency_disposition="CREATED",
        error_code=None,
        content_size_bytes=1234,
        server_sha256="abc",
        submitted_sha256=None,
        uploader_received_at=t,
        api_received_at=t,
        end_to_end_s=1.23,
        wall_time_s=0.98,
        sql_query_count=10,
        sql_total_db_time_s=0.5,
        sql_top_by_total_time=[],
        sql_top_by_count=[],
    )

    append_benchmark_batch_row(
        csv_path=str(out),
        measured_at=t,
        git_sha="deadbeef",
        api_base_url="http://localhost:8000",
        dataset="set_of_50",
        batch_id="set_of_50_run_01",
        batch_file_count=50,
        batch_completed_count=50,
        batch_failed_count=0,
        batch_total_wall_time_s=12.34,
        batch_files_per_min=243.0,
    )

    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3

    header = lines[0].split(",")
    assert "result_kind" in header
    assert "batch_total_wall_time_s" in header
    assert "batch_files_per_min" in header

    # Batch row should have result_kind=batch.
    assert ",batch," in ("," + lines[2] + ",")
