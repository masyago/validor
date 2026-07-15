# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Validor** is a clinical lab analyzer backend service that ingests raw lab data (CSV), validates and normalizes it to FHIR-compliant resources, and persists results in PostgreSQL with full auditability via append-only processing events.

**Tech Stack:**
- Python 3.13+, FastAPI, Pydantic, SQLAlchemy ORM
- PostgreSQL, Alembic migrations
- Docker/Docker Compose, pytest, `uv` package manager
- Streamlit for web demo

## Quick Start

### Prerequisites
- Python 3.13+
- Docker Desktop (latest)
- `uv` package manager

### Start Local Environment
```sh
# Start API and PostgreSQL (applies migrations automatically)
docker compose up --build

# In another terminal, run the full demo (generate CSV + upload)
uv run python demo/cli_demo.py --once

# Or run CSV generator and uploader separately
uv run python csv_uploader/csv_generator.py
uv run python csv_uploader/csv_uploader.py

# Stop and reset database
docker compose down -v
```

### Testing
```sh
# Run unit tests (excludes e2e by default)
uv run pytest

# Run a single test file
uv run pytest tests/services/test_validator.py

# Run a single test
uv run pytest tests/services/test_validator.py::test_name

# Run e2e tests (requires docker compose running)
uv run pytest -m e2e

# Run with coverage
uv run pytest --cov=app
```

### Linting & Type Checking
```sh
# Use `ruff check` for linting (installed via pyproject.toml dependencies)
ruff check app tests
```

## Codebase Architecture

### Layered Architecture

The application follows strict layering to isolate concerns and enforce data boundaries:

1. **API Layer** (`/app/api`) — FastAPI entry point
   - Single ingestion endpoint at `POST /v1/ingestion`
   - Request validation and orchestration
   - Atomicity: malformed requests rejected before reaching downstream layers
   - Full error tracking with detailed row-level diagnostics

2. **Service Layer** (`/app/services`) — Domain logic
   - `ingestion_service.py` — orchestrates the full pipeline
   - `validator.py` — validates parsed data against business rules
   - `normalizer.py` — transforms validated data into FHIR Observation and DiagnosticReport resources
   - `parser.py` — parses raw CSV into domain relations (Panel, Test)
   - `patient_message_service.py` — human-gate state machine for patient messages (approve/request_changes/reject/send)
   - `synthetic_patient.py` — deterministic synthetic demographics upserted from `patient_id`

3. **Persistence Layer** (`/app/persistence`) — Data access
   - SQLAlchemy models in `/models` (Raw, Panel, Test, DiagnosticReport, Observation, ProcessingEvent)
   - Repository pattern in `/repositories` — encapsulates all database queries
   - Single `db.py` session factory

4. **Domain Layer** (`/app/domain`) — Business entities
   - FHIR R4 Pydantic models in `/domain/fhir/r4`
   - Validated domain objects (e.g., Patient, Specimen, Encounter)
   - Schemas for API requests/responses in `/schemas`

5. **Supporting Layers**
   - `/provenance` — processing event recording (audit trail)
   - `/core` — enums (e.g., ingestion status)
   - `/metrics` — benchmarking utilities
   - `/ai` — controlled LLM workflows: `ai_orchestration.py` (annotation enrichment)
     and `patient_message_orchestration.py` (patient-message drafting), each with
     versioned prompt (`prompt_versions/`) and output schema (`content_versions/`)
     modules. Both flows are PHI-free at the boundary: a job-scoped `correlation_id`
     (minted trusted-side, mapped in `ai_generation_job`) replaces `patient_id`
     before anything reaches the AI layer.

### Data Pipeline

**Raw Data Flow:**
```
CSV → Parser → Parsed Relations (Panel, Test)
            ↓
       Validator (business rules)
            ↓
       Normalizer (FHIR transformation)
            ↓
    Persistence (DiagnosticReport, Observation)
            ↓
 Processing Events (provenance log)
```

**AI stages (gated, PHI-free at the boundary):**
```
Normalization succeeded
        ↓
 AI enrichment (ai_annotation)
        ↓
 [gate] normalization succeeded AND AI enrichment succeeded
        ↓
 Patient-message draft (patient_message)  → machine gate (validation_status)
        ↓
 Clinician review (review_status)  → demo-send (SENT, no external delivery)
```

### Database Design

**Immutable (source of truth):**
- `ingestion` — one upload event
- `raw_data` — uploaded CSV bytes + metadata

**Staged Data (parsed, not normalized):**
- `panel` — parsed analyte group
- `test` — individual analyte

**Normalized Data (FHIR):**
- `diagnostic_report` — panel-level grouping (FHIR DiagnosticReport)
- `observation` — individual result (FHIR Observation)

**Operational:**
- `processing_event` — append-only audit trail (validation started/succeeded/failed, etc.)

**AI Augmentation:**
- `ai_annotation` (enrichment output; carries `correlation_id`), `vector_store`, `document`
- `ai_generation_job` — trusted-side correlation map (`correlation_id` → patient/ingestion),
  shared by both AI flows, consume-on-resolve

**Patient Messaging (clinician-gated):**
- `patient` — minimal synthetic demographics; natural key `patient_id`, back-FK'd
  from panel/diagnostic_report/observation
- `patient_message` — one clinician-gated draft per ingestion; machine gate
  (`validation_status`) + human gate (`review_status`)

See `supporting_docs/database_design.md` for ERDs and detailed schema.

### Key Invariants

1. **Idempotency:** Ingestion uniqueness enforced by `(instrument_id, run_id)` constraint
2. **Deduplication:** Content hash (SHA-256) detects exact duplicates; mismatched hashes for same key are rejected
3. **Atomicity:**
   - Request level: malformed POSTs rejected before any processing
   - Pipeline level: validation/normalization failures don't create partial writes; raw data always persisted
4. **Determinism:** Same input always produces same output; all transformations are reproducible
5. **Auditability:** Every processing step recorded as an event with status and timestamp

## Key Files & Patterns

### API Routing
- `/app/api/routers/ingestion.py` — POST endpoint, status polling, GET operations
- Dependencies injected via `/app/api/dependencies.py`

### Service Orchestration
- `IngestionService.ingest()` — main entry point; coordinates parsing → validation → normalization → persistence
- Wraps all operations in try-catch to record processing events at each stage

### Validation Rules
- `Validator.validate()` — checks parsed data against domain rules
- Collects all errors per row (not fail-fast) for detailed diagnostics
- See `metrics/validation_accuracy/validation_rules.md` for detailed rules

### Normalization
- `Normalizer.normalize()` — transforms validated data into FHIR-compliant artifacts
- Produces Observation (individual result) and DiagnosticReport (panel grouping)
- Adds transform metadata (created, modified timestamps, version)
- Validates produced FHIR JSON to ensure representation-level consistency

### Database Migrations
- Alembic versioning in `alembic/versions/`
- Applied automatically on `docker compose up` via the `migrate` service
- To create a new migration: `alembic revision --autogenerate -m "description"`

### Testing Patterns
- **Unit tests:** Mock repositories, focus on business logic
- **Integration tests:** Use real database (per-test isolation via transactions)
- **E2E tests:** Hit running API via Docker Compose; marked with `@pytest.mark.e2e`
- **Fixtures:** Defined in `conftest.py` (db_session, sample_csv_bytes, ingestion_factory)
- Test database auto-created via `conftest.py` helper functions

## Configuration

### Environment Variables
- `DATABASE_URL` — PostgreSQL connection string (set in `compose.yaml`)
- `TEST_DATABASE_URL` — test DB URL (defaults to localhost:5432/test_cla)

### Alembic
- `alembic.ini` — migration config (sqlalchemy.url overridden by compose env)

### Pytest
- `pytest.ini` — excludes e2e by default, markers defined for e2e

### Docker
- `Dockerfile` — builds container with migrations, runs API on 8000
- `compose.yaml` — three services: api, migrate (alembic upgrade), db (postgres)

## Common Workflows

### Adding a Validation Rule
1. Add rule logic to `Validator.validate()` in `services/validator.py`
2. Write unit test in `tests/services/test_validator.py` with sample data
3. Update `metrics/validation_accuracy/validation_rules.md` with rule description
4. Run `pytest tests/services/test_validator.py -v` to verify

### Adding a FHIR Field
1. Update Pydantic model in `/domain/fhir/r4/`
2. Update normalizer in `services/normalizer.py` to map parsed data to new field
3. Write test in `tests/services/test_normalizer.py`
4. Run local demo to verify end-to-end

### Adding a Database Table
1. Add SQLAlchemy model in `persistence/models/`
2. Generate migration: `alembic revision --autogenerate -m "add X table"`
3. Update repository in `persistence/repositories/` if needed
4. Add tests for new model/repository
5. Test locally: `docker compose down -v && docker compose up --build`

### Debugging a Failed Ingestion
1. Check API response for ingestion ID
2. Query `processing_event` table for that ingestion: status transitions, timestamps, error messages
3. If validation failed, check row-level errors in API response (detailed per-row diagnostics)
4. Check `raw_data` table for uploaded bytes if needed

## Performance Considerations

- **N+1 queries:** Use SQLAlchemy eager loading (joinedload) in repositories
- **Validation:** Collects all errors in one pass (doesn't fail on first error)
- **Normalization:** Deterministic, idempotent; safe to retry
- **Throughput:** Current: 333.8 files/min (80% improvement via batching, eager loading)

See `metrics/performance/README.md` for benchmark details and profiling results.

## Testing Coverage

- **Unit + Integration:** 94% average, 95% median (business logic and repositories)
- **E2E:** Separate from unit tests (marked `@pytest.mark.e2e`)
- **Key areas:** validation rules, normalization, idempotency, conflict detection

## Roadmap

- AI enrichment: controlled LLM augmentation (RAG, schema verification, provenance) — implemented
- Patient messaging: clinician-gated LLM-drafted patient summaries — implemented (demo-send only)
- Durability: replace in-process FastAPI background tasks with distributed workers (Celery, etc.)
- De-identification boundary: swap the `ai_generation_job` correlation table for Redis + TTL,
  and move AI drafting to an async worker (the `correlation_id` round-trip is the seam)

## Documentation

- `README.md` — project overview, screenshots, setup instructions
- `supporting_docs/database_design.md` — schema, ERDs, table relationships
- `metrics/validation_accuracy/validation_rules.md` — detailed validation logic
- `api_contracts/raw_csv_api_contract.md` — POST endpoint spec
- `api_contracts/read_api_contract.md` — GET endpoint spec
- `app/domain/fhir/version_nomenclature.md` — FHIR versioning notes
