from __future__ import annotations

from datetime import datetime, timezone

from app.metrics.benchmark_csv_reporter import append_benchmark_row


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
