import os
import time
from dataclasses import dataclass

import pytest
import requests


@dataclass(frozen=True)
class E2EConfig:
    base_url: str

    @property
    def api_v1_url(self) -> str:
        return f"{self.base_url}/v1"


def _strip_trailing_slash(url: str) -> str:
    return url[:-1] if url.endswith("/") else url


@pytest.fixture(scope="session")
def e2e_config() -> E2EConfig:
    base_url = _strip_trailing_slash(
        os.environ.get("E2E_BASE_URL", "http://localhost:8000")
    )
    return E2EConfig(base_url=base_url)


def _wait_for_http_200(url: str, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    last_exc: Exception | None = None

    while time.monotonic() < deadline:
        try:
            resp = requests.get(url, timeout=2)
            if resp.status_code == 200:
                return
        except Exception as exc:  # pragma: no cover
            last_exc = exc

        time.sleep(0.25)

    hint = (
        f"Timed out waiting for 200 from {url}. "
        "Is the API running (e.g. `docker compose up` or `uv run fastapi dev app/main.py --port 8000`)?"
    )
    if last_exc:
        raise RuntimeError(hint) from last_exc
    raise RuntimeError(hint)


@pytest.fixture(scope="session", autouse=True)
def wait_for_api(e2e_config: E2EConfig) -> None:
    """Ensures a running API exists before any e2e tests execute."""
    timeout_s = float(os.environ.get("E2E_STARTUP_TIMEOUT_S", "30"))
    _wait_for_http_200(f"{e2e_config.base_url}/", timeout_s=timeout_s)
