*The document describes how to run end-to-end tests.*

E2E tests hit the API over HTTP as a black box (typically the Docker Compose stack), rather than using FastAPI's in-process `TestClient`.

### Run

1) Start the stack with the demo upload rate limiter disabled:
```sh
CLA_DEMO_MAX_UPLOADS_PER_WINDOW=0 docker compose up --build
```

`compose.yaml` defaults the per-IP demo cost guard on (1 upload / 5s). The e2e
tests POST several uploads back-to-back and would otherwise get `429
DEMO_RATE_LIMITED`. Setting the max to `0` disables the guard (see
`_enforce_upload_rate_limit_or_429` in `app/api/routers/ingestion.py`).

2) In another terminal, run e2e tests:
```sh
E2E_BASE_URL=http://localhost:5001 uv run pytest -m e2e -q
```

Notes:
- E2E tests are excluded from the default `pytest` run (they can be slower and require a running API).