# Validor (Clinical Lab Analyzer)

Validor is a backend service that ingests lab analyzer data, validates and 
normalizes results, and persists FHIR-complaint resources in PostgreSQL with 
full auditability.

Validor makes lab data processing reliable and traceable through deterministic 
validation, standardized normalization, and explicit provenance tracking.

Future iterations will add AI-assisted enrichment via controlled, 
non-authoritative LLM workflows.

## Demo

### Web demo: <URL>
  * Select a file from the dropdown menu. Click `Upload`
  * Ingestion status and uploading metadata will be displayed. 
  * If the data was validated and normalized without errors, use 
  `DiagnosticReports Data` and `Observation Data` buttons to show and hide 
   the data.

#### Valid file
<img src="demo/gifs/live_demo_valid1.gif" width="500">



#### Invalid file
<img src="demo/gifs/live_demo_invalid1.gif" width="500">


### CLI demo:
* Install package
* In terminal A, initialize docker containers. It will start API and database
containers and migrate schemas. It can take a few seconds.
```sh
docker compose up --build
```

* In terminal B, run demo file. You will see summary of generated CSV file, 
the upload to API, API response, and status for each stage of the data pipeline, 
along with the final status for the ingestion.
```sh
uv run python demo/cli_demo.py --once
```


## Tech Stack

* **Backend :** Python, FastAPI, Pydantic
* **Database:** PostgreSQL, SQLAlchemy (ORM)
* **DevOps:** Docker
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
boundaries, and ensure and ensure auditability across the ingestion pipeline.

---

### External Source: Lab Analyzer Simulator and Data Uploader
* Simulates a canonical lab analyzer output via controlled CSV generation
* Intentionally external to model real-world system boundaries
* Sends data only through the API (no direct database and service access)
      

### API Layer: FastAPI
* Single entry point with strict boundary enforcement
* Orchestrates ingestion lifecycle and status tracking
* Any validation error persists nothing in tables containing results. Raw 
data, metadata and processing events are persisted regardless of validation
status.
* Ensures atomicity
  * Request level: malformed POST-requests or failed pre-ingestion checks (e.g.
  hash mismatch) are rejected and data is prevented from reaching 
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
  * processing events (provenance log)
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
* Query efficiency: query count per row reduced by 92% median (N+1 eliminated, 
batching applied)
* Database time: reduced by 80% median database time per ingestion
* Throughput: 3.8 fold increase (from 88.6 files/min to 333.8 files/min)


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


### Quick Start

1. **Clone the repository**
```sh
git clone <INSERT URL>
cd path/to/folder
```

2. **Create environment files**
? nothing here?

4. **Build docker images and start containers**
   Starting the containers and database migration can take a few seconds.

```sh
docker compose up --build
```

5. **Run the application**
   
   In a different terminal,

  * To generate a CSV and upload it in one command:
      ```sh
      uv run python run demo/cli_demo.py --once
      ```
  * To run the CSV generator and uploader separately:
    
    * Run CSV generator. By default it saves the CSV in a folder 
      `csv_uploader/simulated_exports/pending`:
      ```sh
      uv run python run csv_uploader/csv_generator.py
      ```

    * Run CSV uploader. By default it processes all CSV files from 
      `csv_uploader/simulated_exports/pending` directory and moves them to 
      `csv_uploader/simulated_exports/uploaded` in case of successful API 
      request (code 200 or 202) or to `csv_uploader/simulated_exports/failed`
      if API response indicated error (e.g. 409).

      ```sh
      uv run python run csv_uploader/csv_uploader.py
      ```
6. Stopping the Application
```sh
docker compose down
```
## Screenshots 


1. *A valid CSV file is generated and uploaded*

  <img src="supporting_docs/screenshots/cli_valid_file_generated_uploaded.png" width="500">



2. *Data is successfully validated and normalized*

  <img src="supporting_docs/screenshots/cli_ingestion_status_complete.png" width="500">


3. *Failed validation. Error details are included for each data row to ensure
traceability.*

  <img src="supporting_docs/screenshots/cli_ingestion_status_errors.png" width="500">
 


## Development Roadmap

* Add an AI enrichment of findings, such as reference ranges, historical context,
  and clinical guidelines used as a controlled augmentation layer (RAG, schema 
  verification, acceptance process, provenance)
* Replace in-process FastAPI background tasks with more durable workers for 
  enhanced reliability and further throughput increase 

## License
MIT


## Version History

* **0.0.1** Pre-release

**Last Updated:** April 2026



