# Clinical Lab Analyzer
Clinical Lab Analyzer is a backend service that ingests laboratory analyzer 
outputs as CVS, validates and normalizes results, enriches findings using 
controlled LLM workflows, and persist data in a FHIR-compliant PostgreSQL
database with full auditability.



## Tech Stack

* **Backend :** Python, FastAPI, Pydantic
* **Database:** PostgreSQL, SQLAlchemy (ORM), pgvector
* **AI Orchestration:** LangChain or LlamaIndex
* **DevOps:** Docker, AWS Bedrock
* **Healthcare Compliance:** FHIR (Observation and DiagnosticReport resources)
* **Testing:** Pytest
* **Environment & Dependency Management:** uv

## Service Architecture

The service has layered architecture to isolate concerns and ensure that
each layer have access only to the data appropriate to its responsibility. 
AI is treated as a controlled, non-authoritative augmentation layer

1. External data source: Lab Analyzer Simulator
    * Data flows into the system through a controlled API boundary. No direct
     access to database and service-layer is allowed

2. API Layer: FastAPI
   * Acts a single entry point
   * Responsible for request orchestration and boundary enforcement

3. Service Layer: Domain and Business Logic
   * Responsible for data validation, normalization, and conversion into domain
     models
   * Coordinates with AI enrichment workflows

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

## Metrics
* Ingestion validation accuracy
* LLM output reliability
* Query performance 
* FHIR standardization coverage
* Test coverage


## Features


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

### Tech Enhancements


## License
MIT


## Version History

* **0.0.1** Pre-release

**Last Updated:** January 2025



