*The document describes how to run end-to-end tests.*

E2E tests hit the API over HTTP as a black box (typically the Docker Compose stack), rather than using FastAPI's in-process `TestClient`.

### Run

1) Start the stack:
```sh
docker compose up --build
```

2) In another terminal, run e2e tests:
```sh
E2E_BASE_URL=http://localhost:8000 uv run pytest -m e2e -q
```

Notes:
- E2E tests are excluded from the default `pytest` run (they can be slower and require a running API).