# Clinical Lab Analyzer
Clinical Lab Analyzer is a backend service that ingests laboratory analyzer 
outputs as CSV, validates and normalizes results, enriches findings with schema-validated, non-authoritative annotations using controlled LLM workflows, and persist data in a FHIR-compliant PostgreSQL
database with full auditability.

## Scope
### In Scope

* Ingestion of canonical analyzer output
    * The system assumes a canonical analyzer output schema. 
      Instrument-specific formats would be handled via adapter layers in 
      production
    * The project models a subset of chemistry analyzer outputs. Hematology, 
      coagulation, and hormone testing are out of scope.
* Data ingestion format: CSV
* Two FHIR resources:
    * Observation (individual analytes)
    * DiagnosticReport (panel-level grouping)
* Use of AI for flagging anomalies.  


### Out of Scope

* Processing output from a vendor-specific analyzer
* Frontend dashboards
* Real clinical workflows
* Authentication beyond basic API keys
* Multi-tenant billing
* Real device integrations
* Real PHI

## Tech Stack

* **Backend :** Python, FastAPI, Pydantic
* **Database:** PostgreSQL, SQLAlchemy (ORM), pgvector
* **AI Orchestration:** LangChain or LlamaIndex
* **DevOps:** Docker, AWS Bedrock
* **Healthcare Compliance:** FHIR (Observation and DiagnosticReport resources)
* **Testing:** Pytest
* **Environment & Dependency Management:** uv


## Service Architecture

### High-level overview

The service has layered architecture to isolate concerns and ensure that
each layer has access only to the data appropriate to its responsibility. 
AI is treated as a controlled, non-authoritative augmentation layer

1. External data source: Lab Analyzer Simulator
    * Data flows into the system through a controlled API boundary. No direct
     access to database and service-layer is allowed
    * The system assumes a canonical analyzer output schema. 
      Instrument-specific formats would be handled via adapter layers in 
      production
    * API layer create an export request record
    * A simulated middleware fulfills it and then sends data to the API layer
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
      - `FAILED VALIDATION` - terminal. Invalid input/schema. Any validation error persist nothing for `Panel` and `Test`. 
      - `COMPLETED`
      - `FAILED` - terminal non-validation errors

3. Service Layer: Domain and Business Logic
   * Responsible for data validation, normalization, and conversion into domain
     models
   * Coordinates with AI enrichment workflows
   * FHIR Serializer versions are append-only. No silent edits to existing versions allowed.


4. Persistence Layer: Database
   * Stores raw and normalized data, generated FHIR resources and 
     processing metadata

5. AI Orchestration Layer: LLM and RAG
   * Receives validated and normalized domain models with minimized or
     non-identifying clinical context
   * Retrieves semantic search results from vector store and invokes external 
     LLM
   * Passes only schema-enforced AI outputs to the service layer

6. Vector Store
   * Stores reference ranges, clinical interpretation guidelines, and
     normalized or synthetic historical lab results summaries
   * Provides semantic search results to the AI Orchestration layer

7. External LLM API
   * Treated as an untrusted external dependency
   * All outputs are validated against pre-defined schemas within the AI 
     Orchestration layer before further use

### Trade-offs
Authentication and Trust Model
For simplicity, the CSV uploader and ingestion API are assumed to operate within a trusted internal network. Authentication is intentionally omitted. In a production setting, this boundary would be enforced via API keys, mTLS, or service identity.

FHIR Resources
We deliberately don’t use a full FHIR object library. Instead, we emit a strictly versioned, minimal R4-compliant projection using Pydantic so the JSON exactly reflects our domain semantics and remains reproducible across pipeline versions.

## Metrics
* Ingestion validation accuracy
* LLM output reliability
* Query performance 
* FHIR standardization coverage
* Performance optimization: Throughput increase
* Test coverage

## Database
Data pipeline: raw ingest - parsed relations - validated and normalized FHIR artifacts


## Vector Store Content Disclaimer
The vector store contains high-level, educational summaries derived from 
publicly available sources. Content is paraphrased, non-exhaustive, and used 
solely to ground AI-generated explanations. It does not provide diagnostic or
 treatment guidance.

## Features

### FHIR Resources

The service works with 2 resources: DiagnosticReport and Observation.
DiagnosticReport resource groups Observation resources and provides clinical 
context. Observation resource contains individual test result.

### Note on AI Use

* AI never modifies core clinical data
* AI produces schema-validated annotations and explanations

## Installation & Setup

### Prerequisites


### Quick Start

1. **Clone the repository**
```sh
git clone <INSERT URL>
cd path/to/folder
```

2. **Create environment files**

    
3. **Update secrets and API keys**

4. **Build and run the application**

5. **Access the application**


## Stopping the Application
```sh
docker compose down
```
## Application Screenshots 



## Development Roadmap
- Replace in-process  FAST API background tasks to more durable workers:
 Celery and Redis
- 

### Tech Enhancements


## License
MIT


## Version History

* **0.0.1** Pre-release

**Last Updated:** January 2025



