# API Contract: Read Ingestion Data 

This document defines the API contract for retrieving status and data associated with uploaded canonical lab analyzer CSVs. The data can be retrieved by ingestion_id and by patient_id (pseudonymous internal identifier).

---

## Common Error Responses

### `404 Not Found`
Indicates that path resource was not found. Specifically, either `ingestion_id`
or `patient_id` (depending on the endpoint) was not found.

**Body:**

### Example of response:

```json
{
  "ingestion_id": "a7b1c3d4-e5f6-7890-1234-567890abcdef",
  "detail": "Item not found"
}
```

```json
{
  "patient_id": "PAT-a3842643-c0b1-4b4a-9df2-e3911ec563d1",
  "detail": "Item not found"
}
```

### `422 Unprocessable Entity`
Format validation error. For example, when `ingestion_id` is not a valid UUID.

**Body:**

#### Response shape
FastAPI validation error shape. Example:

```json
{
  "detail": [
    {
      "loc": ["path", "ingestion_id"],
      "msg": "value is not a valid uuid",
      "type": "uuid_parsing"
    }
  ]
}
```


## `GET /v1/ingestions/{ingestion_id}`

### Description
The endpoint returns status and metadata for specified ingestion_id.

### URL Parameters
`ingestion_id`: UUID. Required parameter.

### Responses

#### `200 OK`
Indicates that record with `ingestion_id` is found. Returns status of the
ingestion, timestamp when API received the raw data, and error details. 

**Content-Type:** `application/json`

**Body:**

```json
{
  "ingestion_id": "a7b1c3d4-e5f6-7890-1234-567890abcdef",
  "status": "COMPLETED",
  "api_received_at": "2026-01-12T14:35:10.123Z",
  "error_code": null,
  "error_detail": null
}
```

### Error responses
See Common Error Responses (404, 422)


## `GET /v1/ingestions/{ingestion_id}/processing-events`

### Description
Returns an ordered list of processing/provenance events for the specified `ingestion_id`.
These events include stage-level pipeline transitions (parse/validation/normalization/FHIR) as well as ingestion acceptance and idempotency-related events.

This endpoint is intended for operational visibility (e.g., CLI demo output) and is the canonical HTTP way to derive per-stage status without directly querying the database.

### URL Parameters
`ingestion_id`: UUID. Required parameter.

### Responses

#### `200 OK`
Returns a list of processing events associated with `ingestion_id`.
The list MAY be empty (e.g., the ingestion exists but no events have been emitted yet).

**Content-Type:** `application/json`

**Body:**

```json
[
  {
    "event_id": "4c64c0c2-1b07-4b2b-8bd7-80e3c02ad1c4",
    "ingestion_id": "a7b1c3d4-e5f6-7890-1234-567890abcdef",
    "occurred_at": "2026-01-12T14:35:10.130Z",
    "event_type": "INGESTION_ACCEPTED",
    "actor": "ingestion-api",
    "severity": "INFO",
    "message": "Ingestion accepted",
    "details": {
      "instrument_id": "CANONICAL_CHEM_ANALYZER_V1",
      "run_id": "20260112_001"
    }
  },
  {
    "event_id": "86afc5d7-15b8-4e8a-a3a4-8adbd44ddc55",
    "ingestion_id": "a7b1c3d4-e5f6-7890-1234-567890abcdef",
    "occurred_at": "2026-01-12T14:35:11.205Z",
    "event_type": "PARSE_SUCCEEDED",
    "actor": "parser",
    "severity": "INFO",
    "message": null,
    "details": {
      "row_count": 18
    }
  }
]
```

### Error responses
See Common Error Responses (404, 422)

## `GET /v1/ingestions/{ingestion_id}/ai_annotation`

### Description
Retrieves persisted AI annotation rows for the specified `ingestion_id`.

This endpoint exposes the output of the controlled AI enrichment stage. A row may represent either:
- an accepted annotation whose `content_json` matches the internal annotation schema
- a rejected annotation audit payload when an LLM response was received but failed schema validation

The list MAY be empty. For example, the ingestion may exist but AI enrichment may have been skipped, may have failed before an annotation was produced, or may not yet have persisted an annotation row.

### URL Parameters
`ingestion_id`: UUID. Required parameter.

### Responses

#### `200 OK`
Returns a list of AI annotation rows associated with `ingestion_id`.

**Content-Type:** `application/json`

**Body:**

```json
[
  {
    "ai_annotation_id": "1f2f3a4b-5c6d-7e8f-9012-34567890abcd",
    "ingestion_id": "a7b1c3d4-e5f6-7890-1234-567890abcdef",
    "annotation_type": "anomaly_flag",
    "content_json": {
      "annotation_type": "anomaly_flag",
      "secondary_types": ["followup_suggestion"],
      "summary": "Lipid abnormalities were detected and reviewed.",
      "analyte_findings": [
        {
          "analyte_code": "TC",
          "description": "Total cholesterol is elevated above the reference range.",
          "trend_direction": "increasing",
          "confidence": 0.86
        }
      ],
      "requires_review": true,
      "review_priority": "urgent"
    },
    "provider": "amazon_bedrock",
    "model_id": "anthropic.claude-3-5-haiku-20241022-v1:0",
    "prompt_version": "v1.0.0",
    "temperature": "0.0",
    "content_schema_version": "v1.0.0",
    "input_hash": "7f6f8e4f9bc5c1f9f0f8d6c8ab1234567890abcdef1234567890abcdef1234",
    "created_at": "2026-01-12T14:35:31.123Z",
    "validation_status": "ACCEPTED",
    "validated_at": "2026-01-12T14:35:31.123Z",
    "rejection_reason": null
  }
]
```

### Error responses
See Common Error Responses (404, 422)

## `GET /v1/ingestions/{ingestion_id}/diagnostic-reports?include_json=1`

### Description
Retrieves metadata and (optional) resource JSON for diagnostic reports associated with specified `ingestion_id`.

### URL Parameters
`ingestion_id`: UUID. Required parameter.

### Query Parameters
`include_json` is an optional parameter. It indicates whether to include 
DiagnosticReport resource JSON (`include_json=1`) or not (`include_json=0`, 
default).

### Responses

#### `200 OK`
Returns a list of diagnostic reports associated with `ingestion_id`. The list MAY be empty.

**Content-Type:** `application/json`

**Body:**

```json
[
  {
    "diagnostic_report_id": "6f9a6b1f-4c2c-4a40-a08c-5d94b6a6d0d1",
    "patient_id": "PAT-a3842643-c0b1-4b4a-9df2-e3911ec563d1",
    "panel_code": "LIPID",
    "effective_at": "2026-01-12T14:35:10.123Z",
    "normalized_at": "2026-01-12T14:35:30.123Z",
    "resource_json": null,
    "status": "final"
  },
  {
    "diagnostic_report_id": "d1e8a2c3-12ab-4cde-9f01-23456789abcd",
    "patient_id": "PAT-a3842643-c0b1-4b4a-9df2-e3911ec563d1",
    "panel_code": "BMP",
    "effective_at": "2026-01-12T14:35:10.123Z",
    "normalized_at": "2026-01-12T14:35:30.153Z",
    "resource_json": null,
    "status": "final"
  }
]
```

If `include_json=1`, `resource_json` is a FHIR DiagnosticReport JSON object
(schema-defined by this service). Example (truncated):

```json
{
  "resourceType": "DiagnosticReport",
  "id": "6f9a6b1f-4c2c-4a40-a08c-5d94b6a6d0d1",
  "status": "final"
}
```
### Error responses
See Common Error Responses (404, 422)

## `GET /v1/ingestions/{ingestion_id}/observations?include_json=1&limit=...&offset=...`

### Description
Retrieves metadata and (optional) resource JSON for observations associated
with specified `ingestion_id`.

### URL Parameters
`ingestion_id`: UUID. Required parameter.

### Query Parameters
`include_json` is an optional parameter. It indicates whether to include 
Observation resource JSON (`include_json=1`) or not (`include_json=0`, 
default).

`limit` and `offset` are optional parameters. `limit` specifies number of 
results to display, while `offset` indicates number of records to skip from
beginning of the results. Default values: `limit=10` and `offset=0`.

### Responses
#### `200 OK`
Returns a list of observations associated with `ingestion_id`. The list MAY be empty.

**Content-Type:** `application/json`

**Body:**

```json
[
  {
    "observation_id": "ea18a367-828d-4cc8-8086-3ffcd0d0cf5d",
    "diagnostic_report_id": "6f9a6b1f-4c2c-4a40-a08c-5d94b6a6d0d1",
    "patient_id": "PAT-a3842643-c0b1-4b4a-9df2-e3911ec563d1",
    "code": "LDL",
    "display": "Low-Density Lipoprotein",
    "effective_at": "2026-01-12T14:35:10.123Z",
    "normalized_at": "2026-01-12T14:35:30.153Z",
    "value_num": 52.0,
    "value_text": null,
    "comparator": null,
    "unit": "mg/dL",
    "ref_low_num": 0.0,
    "ref_high_num": 100.0,
    "flag_analyzer_interpretation": null,
    "flag_system_interpretation": null,
    "discrepancy": null,
    "resource_json": null,
    "status": "final"
  }
]
```

If `include_json=1`, `resource_json` is a FHIR Observation JSON object (schema-defined by this service). Example (truncated):

```json
{
  "resourceType": "Observation",
  "id": "ea18a367-828d-4cc8-8086-3ffcd0d0cf5d",
  "status": "final"
}
```

### Error responses
See Common Error Responses (404, 422)

## `GET /v1/patients/{patient_id}/diagnostic-reports?include_json=1&limit=...&offset=...`

### Description
Retrieves metadata and (optional) resource JSON for diagnostic reports associated
with specified `patient_id`.

### URL Parameters
`patient_id`: string. Required parameter. pseudonymous internal identifier.

### Query Parameters
`include_json` is an optional parameter. It indicates whether to include DiagnosticReport resource JSON (`include_json=1`) or not (`include_json=0`, default).

`limit` and `offset` are optional parameters. `limit` specifies number of results to return, while `offset` indicates number of records to skip from the beginning of the results. Default values: `limit=10` and `offset=0`.

### Responses

#### `200 OK`
Returns a list of diagnostic reports for `patient_id`. The list MAY be empty.

**Content-Type:** `application/json`

**Body:**

```json
[
  {
    "diagnostic_report_id": "6f9a6b1f-4c2c-4a40-a08c-5d94b6a6d0d1",
    "patient_id": "PAT-a3842643-c0b1-4b4a-9df2-e3911ec563d1",
    "panel_code": "LIPID",
    "effective_at": "2026-01-12T14:35:10.123Z",
    "normalized_at": "2026-01-12T14:35:30.123Z",
    "resource_json": null,
    "status": "final"
  }
]
```

If `include_json=1`, `resource_json` is a FHIR DiagnosticReport JSON object (schema-defined by this service). Example (truncated):

```json
{
  "resourceType": "DiagnosticReport",
  "id": "6f9a6b1f-4c2c-4a40-a08c-5d94b6a6d0d1",
  "status": "final"
}
```
### Error responses
See Common Error Responses (404, 422)

## `GET /v1/patients/{patient_id}/observations?include_json=1&limit=...&offset=...`

### Description
Retrieves metadata and (optional) resource JSON for observations associated
with specified `patient_id`.

### URL Parameters
`patient_id`: string. Required parameter. Pseudonymous internal identifier.

### Query Parameters
`include_json` is an optional parameter. It indicates whether to include Observation resource JSON (`include_json=1`) or not (`include_json=0`, default).

`limit` and `offset` are optional parameters. `limit` specifies number of results to return, while `offset` indicates number of records to skip from the beginning of the results. Default values: `limit=10` and `offset=0`.

### Responses

#### `200 OK`
Returns a list of observations for `patient_id`. The list MAY be empty.

**Content-Type:** `application/json`

**Body:**

```json
[
  {
    "observation_id": "ea18a367-828d-4cc8-8086-3ffcd0d0cf5d",
    "diagnostic_report_id": "6f9a6b1f-4c2c-4a40-a08c-5d94b6a6d0d1",
    "patient_id": "PAT-a3842643-c0b1-4b4a-9df2-e3911ec563d1",
    "code": "LDL",
    "display": "Low-Density Lipoprotein",
    "effective_at": "2026-01-12T14:35:10.123Z",
    "normalized_at": "2026-01-12T14:35:30.153Z",
    "value_num": 52.0,
    "value_text": null,
    "comparator": null,
    "unit": "mg/dL",
    "ref_low_num": 0.0,
    "ref_high_num": 100.0,
    "flag_analyzer_interpretation": null,
    "flag_system_interpretation": null,
    "discrepancy": null,
    "resource_json": null,
    "status": "final"
  }
]
```

If `include_json=1`, `resource_json` is a FHIR Observation JSON object (schema-defined by this service). Example (truncated):

```json
{
  "resourceType": "Observation",
  "id": "ea18a367-828d-4cc8-8086-3ffcd0d0cf5d",
  "status": "final"
}
```
