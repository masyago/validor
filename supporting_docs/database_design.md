# Database Design 

## Tables 
Models defined in SQLAlchemy, used a migration tool Alembic to create the relations
in Postgresql.
 
### ERD

#### Core Data Pipeline
Demonstrates data flow from a clinical lab analyzer to normalized results.
<img src="supporting_docs/diagrams/database/data_pipeline.png" width="500">



#### AI and Provenance
Shows tables and relationships related to AI and provenance.

<img src="supporting_docs/diagrams/database/erd_cla_ai_provenance_v1.png" width="500">


Polymorphic associations (processing_event, ai_annotation, vector_store) are intentionally represented as (type, id) pairs and omitted from the core ERD for
clarity.

### Immutable (source of truth)

* `ingestion`
   
* `raw_data`
    * one-to-one relationship with `ingestion` on key `ingestion.ingestion_id`.
     Both sides are mandatory
    


### Staged Data

Parsed, not normalized

* `panel`
    * one-to-many relationship with `ingestion` on key `ingestion.ingestion_id` ("one" side)
* `test`
    * one-to-many relationship with `panel` on key `panel.panel_id` ("one" side)

### Normalized Data

FHIR-like normalized and validated data

* `diagnostic_report`
* `observation`
 Normalization. Normalization is a deterministic, idempotent transformation from validated canonical domain records into FHIR-shaped entities, adding transform/version metadata and enforcing representation-level invariants; it does not re-validate raw ingestion, but it does validate the produced normalized artifact (and any serialized FHIR JSON) and records provenance via processing events.

 Status: "FINAL". Only one status. Because a normalized artifact is only meaningful if it’s complete and internally consistent. Failure is a pipeline concern, not a persisted clinical artifact.
 
### AI Augmentation

* `ai_annotation`
* `vector_store`
* `document`

### Processing Log 

Append-only operational history
* `processing_event`

## version 0