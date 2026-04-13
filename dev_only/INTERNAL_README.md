
### Deploy to Render (API + Web Demo)

Render does not run `docker compose` for a single web service. Instead you deploy:

1) **A Postgres database** (Render managed Postgres)
2) **An API web service** (FastAPI)
3) **A Web Demo web service** (Streamlit)

#### 1) Create Postgres

Create a Render Postgres instance. You will use its connection string as `DATABASE_URL`.

#### 2) Create the API web service

- **Environment:** Docker
- **Root directory:** repo root
- **Start command / Docker command:**
  - `sh scripts/render_start_api.sh`
- **Environment variables:**
  - `DATABASE_URL` = your Render Postgres connection string
  - `ENV` = `production` (optional)

This start script runs Alembic migrations on deploy and then starts Uvicorn.

#### 3) Create the Streamlit web demo service

- **Environment:** Docker
- **Root directory:** repo root
- **Start command / Docker command:**
  - `sh scripts/render_start_web_demo.sh`
- **Environment variables:**
  - `CLA_API_BASE_URL` = your deployed API base URL (e.g. `https://<api-service>.onrender.com`)

The Streamlit app uses `CLA_API_BASE_URL` to call the API (so you don't need to edit `csv_uploader/config.yaml`).


## Local Demo Workflow (Simulator + Uploader)

The analyzer CSV generator and CSV uploader are intentionally kept **outside**
the ingestion service. They simulate external systems in the pipeline:

- `csv_uploader/csv_generator.py`: a canonical analyzer producing CSV exports
- `csv_uploader/csv_uploader.py`: a middleware uploader POSTing exports to the API

### Run in three terminals

1) Start the API:
```sh
uv run fastapi dev app/main.py --port 8000
```

2) Start the uploader watcher:
```sh
uv run python -m csv_uploader.csv_uploader
```

3) Generate a CSV export (one file):
```sh
uv run python -m csv_uploader.csv_generator
```


