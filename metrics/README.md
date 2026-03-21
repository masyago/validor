This document outlines metrics and methodology for benchmarking. 


## KPIs

* End-to-end latency vs CSV row number. End-to-end means ingestion accepted to 
  processing completed.
* Total SQL queries per ingestion.
* Latency vs row count (scaling curve)
* Total DB time, median query time, p95 query time, top query fingerprints.

## Success Criteria

* Reduce query count by at least X percent on large file.
* Reduce end-to-end median latency by at least Y percent.
* Preserve correctness checks at 100 percent pass rate.

## Methodology

* 3 valid standardized CSV files used for the testing. The same 3 fixed files
  are used for before and after measurements. The files contain all possible 
  analytes in the same order (called a batch). Each file contains multiple batches.
   To prevent violation of uniqueness rules, each batch has its own, unique, 
   `sample_id`. Per project CSV rules, values that are constant throughout one 
   CSV file are `run_id`, `patient_id` and `collection_time`. 
* CSV files sizes:
    - small: 108 rows (6 batches of 18 analytes)
    - medium: 1080 rows (60 batches of 18 analytes)
    - large: 10800 rows (600 batches of 18 analytes)
    - set of 50 small CSVs: 50x108 rows. Note: 49 files have exactly 108 rows,
      1 file has only 36 rows. Keeping the set stable for "before" and "after"
      measurements.
* Data provenance for the benchmark CSVs: - maybe set the files to read-only mode
    * file name
    * number of rows and batches
    * checksum (hash)
* Experimental controls:
    * The same fixed set of files used for the measurements.
    * Consistency of system: fixed Python env (deps), warm up before measurements, database
      reset between measurements.
    * Correctness checks
* Run protocol:
  * Benchmark runtime mode: run the API locally, run Postgres in Docker.
    * Why: avoids containerizing the API just for benchmarking, and ensures the results CSV is written to  host filesystem.
    * Consistency: use a fixed git SHA + fixed dependency lockfile (`uv.lock`) for the full “before” phase, then repeat on a different SHA for “after”.
      * At the start of each phase, run `uv sync --frozen` once and do not change dependencies during measurements.
  * Start services (local API + docker DB):
    * Start Postgres:
      * `docker compose up -d db`
    * Run migrations (locally, against the docker DB):
      * `DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/cla uv run alembic upgrade head`
    * Make sure tables were migrated: `docker compose exec -T db psql -U postgres -d cla -c "\\dt"`
    * Start API (locally) in a consistent mode:
      * `DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/cla CLA_QUERY_METRICS=1 CLA_BENCHMARK_RESULTS_CSV=metrics/benchmark_results.csv uv run uvicorn app.main:app --host 0.0.0.0 --port 8000`
    * Ensure no other load generators are running.
    * Record run metadata: git commit SHA, timestamp, dataset name (small/medium/large), and API base URL.
  * Definition: what counts as “one measured run”
    * For small/medium/large: one ingestion of that dataset into a fresh DB, measured end-to-end (upload accepted → processing terminal status).
    * For “50 small CSVs”: one run = ingest 50 distinct small CSVs into a fresh DB, then wait until all 50 reach terminal status.
  * Reset DB to a known baseline before each measured run (recommended for clean comparisons).
    * Why: DB size/state affects performance; also, re-uploading the exact same `(instrument_id, run_id)` is idempotent and will return 200 duplicate (no re-processing), so you need fresh DB.
    * Reset commands (volume + migrations):
      * Stop the local API (Ctrl+C) to avoid pooled-connection weirdness during DB recreation.
      * `docker compose down -v` (deletes the `db-data` volume; all Postgres data is wiped)
      * `docker compose up -d db`
      * `DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/cla uv run alembic upgrade head`
      * Make sure tables were migrated: `docker compose exec -T db psql -U postgres -d cla -c "\\dt"`
      * Restart the local API:
        * `DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/cla CLA_QUERY_METRICS=1 CLA_BENCHMARK_RESULTS_CSV=metrics/benchmark_results.csv uv run uvicorn app.main:app --host 0.0.0.0 --port 8000`
    * If you still get “already submitted” after a reset, you are almost certainly not talking to the DB you think you are.
      * Confirm you ran `docker compose down -v` from the repo root (same Compose project).
      * Confirm no other Postgres is listening on `localhost:5432` (e.g., a local/Homebrew Postgres). If one is, either stop it for the benchmark window or change Docker’s published port (e.g., `5433:5432`) and use that in `DATABASE_URL`.
      * Confirm the uploader is pointing at the API you just restarted (base URL/port).
  * Warmup (cache/JIT priming):
    * Use a dedicated warmup CSV
    * To keep measured runs on a clean DB, do warmup immediately after bringing the stack up, then reset the DB once before starting measured runs.
  * Measuring runs:
    * Repeat each dataset 5 times.
    * Order of operations for each repetition: 
        * reset DB
        * run CSV file once
        * record metrics
  * Between measured runs, do not change code, dependency versions (`uv.lock`), DB container image version, or host conditions.
    * Between dataset sizes (small, medium, large, 50x small), make sure to reset DB
    * Capture outputs for every run: wall-clock end-to-end latency, total SQL query count, total DB time, and query fingerprints (top statements by total time/count).
        * Need to be specific how it's implemented
    * Run correctness checks for every measured run; if any fail, mark the run invalid and stop the set.
        * Manual checks. Add results to a google sheet.
    * Repeat for medium and large datasets.
    * Commit changes when "before" measurements complete.
    * After the full “before” phase is complete:
      * If you want to stop the stack and free disk / guarantee no state carries over: run `docker compose down -v`.
        * This deletes the Postgres volume (all DB data).
      * If you want to stop the stack but keep the DB volume around temporarily: run `docker compose down` (without `-v`).
      * Then switch to the “after” git SHA, run `uv sync --frozen` once, and repeat the same protocol.
    * After completing “before” measurements, apply the optimization(s) and rerun the exact same protocol for “after” measurements.
* Correctness checks for every measured run:
        * ingestion status is "COMPLETED"
        * diagnostic_report and observation counts match expected counts
        * no unexpected validation failures
* Data recorded:
    * number of executed queries
    * query execution time
    * query id data:
        * statement fingerprint (normalized SQL).
        * call-site tag or logical phase tag (parse, validation, phase1 normalize, phase2 JSON, provenance writes, read endpoints).
* Categories of queries:
    * Core domain queries. Queries executed from API receiving CSV file to
      processing completion
    * Provenance/auditability queries
    * Providing data to client queries
* Reporting
    * For each file size:
        * median, p95, min, max for latency
        * median and p95 for query count
        * percent change before vs after


## Tools

SQLAlchemy event listeners `before_cursor_execute` and `after_cursor_execute`
events are used to track queries. 

### Capturing Results for Google Sheets

For each processed ingestion, you can optionally append a single row to a CSV
file (stable columns; includes query totals + top fingerprints).

Set these environment variables when running the API:

* `DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/cla` — required when running the API locally against the dockerized DB.
* `CLA_QUERY_METRICS=1` — enables query totals + fingerprint aggregation.
* `CLA_BENCHMARK_RESULTS_CSV=metrics/benchmark_results.csv` — appends one CSV row per processed ingestion.
* Optional: `CLA_GIT_SHA=$(git rev-parse HEAD)` — recorded into the results CSV.
* Optional: `CLA_BENCHMARK_DATASET=small|medium|large|set_of_50` — recorded into the results CSV.
* Optional: `CLA_API_BASE_URL=http://localhost:8000` — recorded into the results CSV.

Notes:
* `CLA_BENCHMARK_TOP_N` controls how many “top offenders” are flattened into columns (default 5).
* `CLA_BENCHMARK_FP_MAX_CHARS` truncates long SQL fingerprints in CSV cells (default 800).
