# Validor (Clinical Lab Analyzer)

Validor is a backend service that ingests lab analyzer data, validates and 
normalizes results, and persists FHIR-shaped data in PostgreSQL database with 
full auditability.

I worked with a lot analytical lab results and know how challenging it is to keep
the data process it, keep complaint, and preserve auditability. My goal is
to build a service that does it all for you without adding extra complexity.

In the next iterations, I'm planning to add AI enrichment of the findings by 
implementing controlled, non-authoritative LLM workflows.

## Demo

### Web demo: <URL>
  * Select a file from the dropdown menu. Click `Upload`
  * Ingestion status and uploading metadata will be displayed. 
  * If the data was validated and normalized without errors, use 
  `DiagnosticReports Data` and `Observation Data` buttons to show and hide 
   the data.

### CLI demo:
  * Install package
  * Start docker containers
  * Run command:
    `uv run python run demo/cli_demo.py --once`
  * Process and terminal output:
    * CSV generator creates a file with randomly selected file profile (valid 
      or invalid).
    * Uploader sends an API request and receives the response.
    * The service validates and normalizes and persists the data. Polling status
      is shown for each stage. In case of failed validation, error details are
      displayed.


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
    * The system assumes a canonical analyzer output schema. 
      Instrument-specific formats would be handled via adapter layers in 
      production
    * The service models a subset of chemistry analyzer outputs
* Data ingestion format: CSV
* Two FHIR resources:
    * Observation (individual analytes)
    * DiagnosticReport (panel-level grouping)


### Out of Scope

* Processing output from a vendor-specific analyzer
* Frontend dashboards
* Real clinical workflows
* Authentication
* Multi-tenant billing
* Real device integrations
* PHI

## Service Architecture

### High-level overview

TODO: include architecture diagram

The service has layered architecture to isolate concerns and ensure that
each layer has access only to the data appropriate to its responsibility. 

1. External data source: Lab Analyzer Simulator and uploader (middleware)
    * Data flows into the system through a controlled API boundary. No direct
     access to database and service-layer is allowed
    * The system assumes a canonical analyzer output schema. 
    * An uploader/middleware forms a request to API and sends data to the API 
      layer
    * Authentication between uploader and API is intentionally omitted in this
     project; in production this boundary would be secured via 
     service-to-service authentication (e.g., mTLS or signed tokens) and 
     network isolation.

2. API Layer: FastAPI
   * Acts a single entry point
   * Responsible for request orchestration and boundary enforcement
   * API Layer keeps track of each ingestion status:
      - `RECEIVED`
      - `PROCESSING`
      - `COMPLETED`
      - `FAILED VALIDATION` - terminal. Invalid input/schema. Any validation 
        error persists nothing in tables containing results. Uploaded file, 
        it's metadata and processing events are persisted regardless of validation
        status.
      - `FAILED` - terminal non-validation errors

    * TODO: add API contracts links

3. Service Layer: Domain and Business Logic
   * Responsible for data validation, normalization, and conversion into domain
     models
   * FHIR Serializer versions are append-only. No silent edits to existing 
     versions allowed.


4. Persistence Layer: Database
   * Stores raw and normalized data, generated FHIR resources,
     metadata, and and processing events.
    * TODO: add DB diagram (short version)

5. TODO: Think where to mention provenance 

### Trade-offs

#### Authentication and Trust Model
For simplicity, the CSV uploader and ingestion API are assumed to operate 
within a trusted internal network. Authentication is intentionally omitted. 
In a production setting, this boundary would be enforced via API keys, mTLS, or service identity.

#### FHIR Resources
We deliberately don’t use a full FHIR object library. Instead, we emit a 
strictly versioned, minimal R4-compliant projection using Pydantic so the JSON 
exactly reflects our domain semantics and remains reproducible across pipeline 
versions.


## Metrics
* Ingestion validation accuracy
* Performance optimization: Throughput increase
    * Process measured between INGESTION_ACCEPTED and (NORMALIZATION_SUCCEEDED
     or NORMALIZATION_SUCCEEDED_WITH_WARNINGS or NORMALIZATION_FAILED = "NORMALIZATION_FAILED")
    * ingestions per minute
    * number of queries per data row
* Test coverage

## Database
Data pipeline: raw ingest - parsed relations - validated and normalized FHIR artifacts.
At each stage services emit processing event records for ensure auditability.


## Features

### FHIR Resources

The service works with 2 resources: DiagnosticReport and Observation.
DiagnosticReport resource groups Observation resources and provides clinical 
context. Observation resource contains individual test result.


## Installation & Setup

### Prerequisites


### Quick Start

1. **Clone the repository**
```sh
git clone <INSERT URL>
cd path/to/folder
```

2. **Create environment files**

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

## Stopping the Application
```sh
docker compose down
```
## Application Screenshots 

![Data file is generated and uploaded](supporting_docs/screenshots/cli_valid_file_generated_uploaded.png)

![Ingestion status displayed](supporting_docs/screenshots/cli_ingestion_status_complete.png)

In case of errors, error details are included for each data row to ensure
traceability.

![Failed validation: errors included](supporting_docs/screenshots/cli_ingestion_status_errors.png)



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

## End-to-End (E2E) Tests

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

## Development Roadmap

* Add an AI enrichment of findings, such as reference ranges, historical context,
  and clinical guidelines used as a controlled augmentation layer (RAG, schema 
  verification, acceptance process)
* Replace in-process FastAPI background tasks with more durable workers for 
  enhanced reliability and further throughput increase 

## License
MIT


## Version History

* **0.0.1** Pre-release

**Last Updated:** April 2026



