Query monitoring is performed to track query performance and identify issues,
such as N+1 problem. 

## KPIs

* End-to-end latency: ingestion accepted to processing completed.
* Total SQL queries per ingestion.
* Total DB time, median query time, p95 query time, top query fingerprints.

## Success Criteria

* Reduce query count by at least X percent on large file.
* Reduce end-to-end median latency by at least Y percent.
* Preserve correctness checks at 100 percent pass rate.

## Methodology

* 3 valid standardized CSV files used for the testing. The same 3 fixed files are used for before and after measurements. The files contain all 
   possible analytes in the same order (aka a batch). Each file contains multiple batches.
   To prevent violation of uniqueness rules, each batch has its own, unique, 
   `sample_id`. Per project CSV rules, values that are constant throughout one CSV file are `run_id`, 
   `patient_id` and `collection_time`. 
* CSV files sizes:
    - small: 108 rows (6 batches of 18 analytes)
    - medium: 1080 rows (60 batches of 18 analytes)
    - large: 10800 rows (600 batches of 18 analytes)
* Experimental controls:
    * For each file measurements performed 5 times before and 5 times after optimizations.
    * 
* Run protocol:
    * Build images once and keep them fixed for the full benchmark (before and after).
    * Start the stack in a consistent mode (no auto-reload/dev tooling) and ensure no other load generators are running.
    * Confirm migrations have completed and the API is ready to accept requests.
    * Record run metadata: git commit SHA, timestamp, dataset name (small/medium/large), and API base URL.
    * Reset DB to a known baseline before each dataset run set (choose one reset approach and keep it the same before/after):
        * Strict reset (most reproducible): recreate the DB volume and rerun migrations.
        * Light reset (faster): truncate all tables on the ingestion path (ingestion/raw_data/panel/test/diagnostic_report/observation/processing_event, etc.).
    * Warmup (not measured): run one full end-to-end ingestion using the small dataset (ingest + process + poll until terminal status). Discard these metrics.
    * Measured runs: for the current dataset, run N times (e.g., N=5) using the same fixed CSV file.
    * Between measured runs, do not change code, images, Compose settings, or host conditions.
    * Capture outputs for every run: wall-clock end-to-end latency, total SQL query count, total DB time, and query fingerprints (top statements by total time/count).
    * Run correctness checks for every measured run; if any fail, mark the run invalid and stop the set (don’t silently exclude).
    * Repeat for medium and large datasets.
    * After completing “before” measurements, apply the optimization(s) and rerun the exact same protocol for “after” measurements.
* Correctness checks for every measured run:
        * ingestion final status is "COMPLETED"
        * diagnostic_report and observation counts match expected counts
        * no unexpected validation failures
* Data recorded:
    * number of executed queries
    * query execution time
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
