# Selecting best number of ingestion API processes at once

## Overview

* Number of ingestions in flight tested `CLA_MAX_INFLIGHT_INGESTIONS`: 8, 12, and 16
* For each `CLA_MAX_INFLIGHT_INGESTIONS` value set_of_50 was run in triplicates
* SELECTED MAX INGESTIONS OF 12 (see Results below)

## Protocol

* Commit changes, lock dependencies
* Reset DB and API per metrics/README.md 
* Run one warmup.csv file through the service
    `uv run python -m csv_uploader.csv_uploader --file /Users/martian_elk/Projects/clinical_lab_analyzer/metrics/data_raw/fixed_csv_v1/small.csv --keep-files --stability-delay-seconds 0 `
* Reset DB and API again.
* Start API with appropriate command:
    * for 8 max ingestions: 
        DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/cla CLA_QUERY_METRICS=1 CLA_BENCHMARK_RESULTS_CSV=metrics/benchmark_results.csv CLA_BENCHMARK_DATASET=set_of_50 CLA_MAX_INFLIGHT_INGESTIONS=8 CLA_RETRY_AFTER_SECONDS=1 uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
    * for 12 max ingestions:
        DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/cla CLA_QUERY_METRICS=1 CLA_BENCHMARK_RESULTS_CSV=metrics/benchmark_results.csv CLA_BENCHMARK_DATASET=set_of_50 CLA_MAX_INFLIGHT_INGESTIONS=12 CLA_RETRY_AFTER_SECONDS=1 uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
    * for 16 max ingestions:
        DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/cla CLA_QUERY_METRICS=1 CLA_BENCHMARK_RESULTS_CSV=metrics/benchmark_results.csv CLA_BENCHMARK_DATASET=set_of_50 CLA_MAX_INFLIGHT_INGESTIONS=16 CLA_RETRY_AFTER_SECONDS=1 uv run uvicorn app.main:app --host 0.0.0.0 --port 8000

* Uploader command is constant across all runs (ok to change run id at the end of the command):
    CSV_UPLOADER_MAX_429_SLEEP_SECONDS=0.1 CSV_UPLOADER_MAX_429_RETRIES=2000 uv run python -m csv_uploader.csv_uploader --watch-dir metrics/data_raw/fixed_csv_v1/set_of_50 --once --keep-files --stability-delay-seconds 0 --wait-for-terminal --batch-results-csv benchmark_results.csv --batch-id set_of_50_max_8_01

* Quick correctness check. Expected value is 5328:
    docker compose exec -T db psql -U postgres -d cla -c "select count(*) as observation_count from observation where resource_json is not null;"

* Compute makespan for the set_of_50:
    uv run python -m metrics.compute_set_of_50_makespan --csv metrics/benchmark_results.csv --dataset set_of_50 --latest-run

* Reset DB and API after each run

* Run each variant 3 times total


* Compare the results and select best option (least makespan)

### RESULTS ###

`CLA_MAX_INFLIGHT_INGESTIONS=8` - 3 replicates
makespan_s=8.266 files_per_min=362.914
makespan_s=8.339 files_per_min=359.735
makespan_s=8.522 files_per_min=352.046


`CLA_MAX_INFLIGHT_INGESTIONS=12` - 3 replicates
makespan_s=8.081 files_per_min=371.253 
makespan_s=8.369 files_per_min=358.445
makespan_s=8.199 files_per_min=365.886

`CLA_MAX_INFLIGHT_INGESTIONS=16` - 3 replicates
makespan_s=8.064 files_per_min=372.010
makespan_s=8.637 files_per_min=347.337
makespan_s=8.504 files_per_min=352.758