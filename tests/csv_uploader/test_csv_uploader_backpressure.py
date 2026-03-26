import time
from pathlib import Path

import requests

from csv_uploader.csv_uploader import upload_file_and_get_ingestion_id


class _FakeResponse:
    def __init__(self, status_code: int, headers: dict[str, str] | None = None, payload=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload
        self.ok = self.status_code < 400
        self.text = "{}"

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]):
        self._responses = list(responses)
        self.calls = 0

    def post(self, *args, **kwargs):
        self.calls += 1
        if not self._responses:
            raise AssertionError("No more fake responses")
        return self._responses.pop(0)


def test_uploader_retries_429_quickly(monkeypatch, tmp_path: Path):
    # Avoid real sleeping in tests; record sleep calls.
    sleeps: list[float] = []

    def _fake_sleep(s: float) -> None:
        sleeps.append(s)

    monkeypatch.setattr(time, "sleep", _fake_sleep)

    # Clamp sleep so the retry loop is fast.
    monkeypatch.setenv("CSV_UPLOADER_MAX_429_RETRIES", "10")
    monkeypatch.setenv("CSV_UPLOADER_MAX_429_SLEEP_SECONDS", "0.01")

    csv_path = tmp_path / "run_001.csv"
    csv_path.write_text("instrument_id,run_id\nINST,run_001\n")

    config = {
        "api_base_url": "http://example",
        "spec_version": "analyzer_csv_v1",
        "instrument_id": "INST",
    }

    session = _FakeSession(
        [
            _FakeResponse(429, headers={"Retry-After": "1"}),
            _FakeResponse(202, payload={"ingestion_id": "abc123"}),
        ]
    )

    ingestion_id = upload_file_and_get_ingestion_id(
        csv_path=csv_path,
        config=config,
        session=session,  # type: ignore[arg-type]
        processed_dir=tmp_path / "processed",
        failed_dir=tmp_path / "failed",
        stability_delay_seconds=0,
        request_timeout_seconds=1,
        max_upload_retries=1,
        retry_backoff_seconds=1,
        debug_request=False,
        keep_files=True,
    )

    assert ingestion_id == "abc123"
    assert session.calls == 2
    assert len(sleeps) == 1
    # Even though server said Retry-After=1, we clamp for fast batch benchmarks.
    assert 0.0 <= sleeps[0] <= 0.011
