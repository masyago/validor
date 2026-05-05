# Validor (Clinical Lab Analyzer)

Validor is a backend service that ingests lab analyzer data, validates and 
normalizes results, and persists FHIR-compliant resources in PostgreSQL with 
full auditability.
Validor makes lab data processing reliable and traceable through deterministic 
validation, standardized normalization, and explicit provenance tracking.

Future iterations will add AI-assisted enrichment via controlled, 
non-authoritative LLM workflows.

## Demo

### Web Demo: [link](https://validor-demo-streamlit.onrender.com/) 
* Select a file from the dropdown menu and click `Upload`.
* The uploader output and ingestion status are displayed.
* If the ingestion completes successfully, use the `DiagnosticReports Data` and `Observations Data` buttons to show/hide persisted results.

<img src="demo/gifs/live_demo_valid1.gif" width="500">

* [Invalid file demo](#web-demo-invalid-file)

### Local Demo: Docker & CLI
See [Installation & Setup](#installation--setup) for the quickest local run.



## Tech Stack

* **Backend :** Python, FastAPI, Pydantic
* **Database:** PostgreSQL, SQLAlchemy (ORM)
* **DevOps:** Docker, CI Testing (GitHub Actions)
* **Healthcare Compliance:** FHIR (Observation and DiagnosticReport resources)
* **Testing:** Pytest
* **Environment & Dependency Management:** uv

## Scope
### In Scope

* Ingestion of canonical analyzer output
    * The system assumes a canonical analyzer output schema in CSV format.
      Instrument-specific formats would be handled via adapter layers in 
      production
    * The service models a subset of chemistry analyzer outputs
* Two FHIR resources:
    * Observation (individual analytes)
    * DiagnosticReport (panel-level grouping)


### Out of Scope

* Processing output from a vendor-specific analyzer
* Frontend dashboards
* Real clinical workflows
* Authentication
* Real device integrations
* PHI

## Service Architecture

### Overview

<img src="supporting_docs/diagrams/service_diagrams/validor_architecture.jpg" width="500">

Validor has a layered architecture to isolate concerns, enforce strict data
boundaries, and ensure auditability across the ingestion pipeline.

---

### External Source: Lab Analyzer Simulator and Data Uploader
* Intentionally external to model real-world system boundaries
* Simulates a canonical lab analyzer output via controlled CSV generation
* Sends data only through the API (no direct database or service access)
      

### API Layer: FastAPI
* Single entry point with strict boundary enforcement
* Orchestrates the ingestion lifecycle and status tracking
* Ensures atomicity
  * Request level: malformed POST requests or failed pre-ingestion checks (e.g.,
  hash mismatch) are rejected, and data is prevented from reaching 
  validated/normalized layers
  * Pipeline level: invalid data is rejected before reaching downstream tables.
  No partial writes to validated and normalized data tables
  * Raw data, metadata, and processing events are always persisted for
  auditability

* **API contracts**
  * [POST data to API](api_contracts/raw_csv_api_contract.md)
  * [Read (GET) data from API](api_contracts/read_api_contract.md)


### Service Layer: Domain and Business Logic
* Implements validation, normalization, and transformation workflows
* Data pipeline: raw ingest -> parsed relations -> validated and normalized 
FHIR artifacts


### Persistence Layer: PostgreSQL
<img src="supporting_docs/diagrams/database/data_pipeline.png" width="500">

* Stores:
  * Raw data and ingestion metadata
  * Validated and normalized data
  * FHIR resource projections (JSONB)
  * Processing events (provenance log)
* Ensures full auditability via append-only processing events at each stage
(for example, VALIDATION_STARTED, VALIDATION_SUCCEEDED, VALIDATION_FAILED)
<img src="supporting_docs/diagrams/database/core_data_provenance.png" width="500">


### Trade-offs

#### Trust Boundary
* Authentication is omitted (assumed trusted internal network)
* Production design would be enforce API keys, mTLS, or service identity

#### FHIR Modeling
* Uses versioned, minimal R4-compliant projections via Pydantic


## Metrics
**Validation accuracy**
* File-level: 30/30 files correctly classified (24 invalid, 6 valid)
* Row-level: precision 100.0%, recall 99.5% across 49,896 rows



**Performance optimization**
* Query efficiency: query count per row reduced by 92% median to median 0.69 
queries per row (N+1 eliminated, batching applied)
* Database time: median database time per ingestion reduced by 80% 
* Throughput: 3.8-fold increase (from 88.6 files/min to 333.8 files/min)



**Test coverage**
* 94% average, 95% median (business logic and repository layers, focus on 
idempotent persistence paths)
* Excludes end-to-end testing


## Data Integrity & Idempotency

* Idempotent ingestion enforced via `(instrument_id, run_id)` uniqueness
* Content-based deduplication using sha-256 (submitted vs. server-computed)  
* Conflict detection: mismatched hashes for the same ingestion key are 
rejected
* Deterministic outcomes: deduplicate identical, conflict, or new ingestion
* No silent overwrites or partial normalization writes


## Installation & Setup

### Prerequisites

* **python**: version >=3.13 
* **Docker Desktop**: latest
* **uv**: package manager, latest

### Quick Start

1. **Clone the repository**
    ```sh
    git clone https://github.com/masyago/validor
    cd validor
    ```

2. **Create and activate virtual environment**
* On Windows:
  ```sh
  python -m venv .venv
  .venv\Scripts\activate
  ```
* On macOS/Linus:

  ```sh
  python3 -m venv .venv
  source .venv/bin/activate
  ```

3. **Build docker images and start containers**
This starts the API and Postgres and runs migrations. It can take a few seconds.

    ```sh
    docker compose up --build
    ```

    #### Troubleshooting: "port is already allocated"

    If Docker fails with something like `Bind for 0.0.0.0:8000 failed: port is already allocated`, another process is already listening on that port. Likely, a previously started Validor instance, another FastAPI app, or a local Postgres.

    * **Quick fix:** stop the old process (or run `docker compose down` in the other window if you started containers there).


4. **Run the CLI demo**

    In a **different** terminal:
    ```sh
    cd validor
    ```
    Then create and activate virtual environment.

  * To generate a CSV and upload it in one command:
    ```sh
    uv run python demo/cli_demo.py --once
    ```
    Terminal output provides details on generated CSV file, its upload and
    processing status, and links to data.

  * Alternatively, you can run the CSV generator and uploader separately:
    
    * Run CSV generator. By default it saves the CSV in `csv_uploader/simulated_exports/pending` directory:
      ```sh
      uv run python csv_uploader/csv_generator.py
      ```

    * Then run CSV uploader:

      ```sh
      uv run python csv_uploader/csv_uploader.py
      ```
    * Links with the data:

      Copy `ingestion_id` from API response.

      - Status: http://localhost:8000/v1/ingestions/<ingestion_id>

      View data in web browser. Note that the data persisted only for valid
      ingestions.
      - DiagnosticReports: http://localhost:8000/v1/ingestions/<ingestion_id>/diagnostic-reports
      - Observations: http://localhost:8000/v1/ingestions/<ingestion_id>/observations
      - FHIR JSON: add `?include_json=1` to DiagnosticReports/Observations.


5. **Stop the application and reset the database**

  Stop the processes first: 
  * On a Mac, `Command+C`
  * On Windows, `Ctrl+C`

  Then, in the first terminal:
  ```sh
  docker compose down -v
  ```
## Screenshots 


*A valid CSV file is generated and uploaded*

  <img src="supporting_docs/screenshots/cli_valid_file_generated_uploaded.png" width="500">

---

*Data is successfully validated and normalized*

  <img src="supporting_docs/screenshots/cli_ingestion_status_complete.png" width="500">

---

*Failed validation. Error details are included for each data row to ensure
traceability*

  <img src="supporting_docs/screenshots/cli_ingestion_status_errors.png" width="500">

---

#### Web Demo: Invalid file
<img src="demo/gifs/live_demo_invalid1.gif" width="500">


## Development Roadmap

* Add AI enrichment of findings, such as reference ranges, historical context,
  and clinical guidelines, used as a controlled augmentation layer (RAG, schema 
  verification, acceptance process, provenance)
* Replace in-process FastAPI background tasks with more durable workers for 
  enhanced reliability and further throughput increase 

## License
MIT


## Version History

### 1.0.1 (2026-04-13)
* Demo deployment on Render
* CI testing on pull requests

### 1.0.0 (2026-04-10)
* Initial stable release

### **0.0.1** Pre-release

**Last Updated:** April 2026



