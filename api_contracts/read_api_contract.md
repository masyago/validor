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

## `GET /v1/ingestions/{ingestion_id}/patient_message`

### Description
Returns the single **active** patient message for the specified `ingestion_id`.

A patient message is a plain-language, clinician-gated summary of the ingestion's
result set drafted by a controlled LLM call. "Active" means the message is not
`SUPERSEDED` or `REJECTED` (at most one active message exists per ingestion).

The synthetic "To:" line (`patient_given_name`/`patient_family_name`/`patient_email`)
is applied at render time from the `patient` row and is never baked into
`draft_content_json`.

If no active message exists (e.g. drafting was skipped, failed before persisting,
or every message was superseded/rejected), the endpoint returns `404`.

### URL Parameters
`ingestion_id`: UUID. Required parameter.

### Responses

#### `200 OK`

**Content-Type:** `application/json`

**Body:**

```json
{
  "patient_message_id": "8f1c2d3e-4a5b-6c7d-8e9f-0123456789ab",
  "ingestion_id": "a7b1c3d4-e5f6-7890-1234-567890abcdef",
  "patient_id": "PAT-a3842643-c0b1-4b4a-9df2-e3911ec563d1",
  "patient_given_name": "Jordan",
  "patient_family_name": "Rivera",
  "patient_email": "jordan.rivera@example.test",
  "draft_content_json": {
    "subject": "Blood test results",
    "opening": "Your recent blood test results are now available. Here is a summary of the key findings.",
    "normal_summary": "Most results were within normal ranges, including blood sugar, potassium, and several liver and kidney markers.",
    "abnormal_findings": [
      {
        "title": "Cholesterol (total cholesterol)",
        "analyte_codes": ["TC"],
        "explanation": "Total cholesterol measures the fat in your blood. Your level is a little above the usual range and will be monitored; follow-up is recommended."
      }
    ],
    "recommendation": "We recommend scheduling a follow-up appointment to discuss these findings and determine whether additional tests are needed."
  },
  "final_content_json": null,
  "content_schema_version": "v1.1.0",
  "correlation_id": "3b2a1c0d-9e8f-7a6b-5c4d-3e2f1a0b9c8d",
  "generation_event_id": "4c64c0c2-1b07-4b2b-8bd7-80e3c02ad1c4",
  "provider": "amazon_bedrock",
  "model_id": "anthropic.claude-3-5-haiku-20241022-v1:0",
  "prompt_version": "v1.1.0",
  "temperature": "0.0",
  "input_hash": "7f6f8e4f9bc5c1f9f0f8d6c8ab1234567890abcdef1234567890abcdef1234",
  "retrieved_refs_json": [],
  "created_at": "2026-01-12T14:35:32.123Z",
  "validation_status": "ACCEPTED",
  "validated_at": "2026-01-12T14:35:32.123Z",
  "validation_error": null,
  "review_status": "PENDING_REVIEW",
  "reviewed_by": null,
  "approved_by": null,
  "reviewed_at": null,
  "approved_at": null,
  "sent_at": null,
  "review_note": null,
  "superseded_by": null
}
```

`validation_status` is the machine gate (`PENDING | ACCEPTED | REJECTED`).
`review_status` is the human clinical gate (`DRAFT | PENDING_REVIEW | APPROVED |
CHANGES_REQUESTED | REJECTED | SENT | SUPERSEDED`). A draft can only advance to
`PENDING_REVIEW` once `validation_status = ACCEPTED`.

### Error responses
See Common Error Responses (404, 422)

## `POST /v1/patient_messages/{patient_message_id}/approve`

### Description
Records a clinician's approval of a patient message. Optionally accepts
clinician-edited content; if `final_content_json` is omitted, the draft is
approved as-is. Sets `review_status = APPROVED`, `approved_by`, and `approved_at`.

### URL Parameters
`patient_message_id`: UUID. Required parameter.

### Request Body

```json
{
  "approved_by": "dr.smith",
  "final_content_json": {
    "subject": "Blood test results",
    "opening": "Edited opening text.",
    "normal_summary": "Most results were within normal ranges.",
    "abnormal_findings": [],
    "recommendation": "Please schedule a follow-up appointment."
  }
}
```

`final_content_json` is optional.

### Responses

#### `200 OK`
Returns the updated patient message (same shape as
`GET .../patient_message`).

#### `409 Conflict`
Returned when the transition is illegal for the message's current state (for
example, approving a message that is not in a reviewable state, or whose machine
gate is not `ACCEPTED`).

**Body:**

```json
{ "detail": "Cannot approve message in state REJECTED" }
```

### Error responses
See Common Error Responses (404, 422); plus `409` above.

## `POST /v1/patient_messages/{patient_message_id}/request_changes`

### Description
Records that the clinician wants changes before approval. Sets
`review_status = CHANGES_REQUESTED`, `reviewed_by`, `reviewed_at`, and
`review_note`.

### URL Parameters
`patient_message_id`: UUID. Required parameter.

### Request Body

```json
{
  "reviewed_by": "dr.smith",
  "note": "Please soften the wording around the cholesterol result."
}
```

`note` is optional.

### Responses

#### `200 OK`
Returns the updated patient message.

#### `409 Conflict`
Returned when the transition is illegal for the current state.

### Error responses
See Common Error Responses (404, 422); plus `409` above.

## `POST /v1/patient_messages/{patient_message_id}/reject`

### Description
Rejects the patient message. Sets `review_status = REJECTED`, `reviewed_by`,
`reviewed_at`, and `review_note`. A rejected message is no longer "active".

### URL Parameters
`patient_message_id`: UUID. Required parameter.

### Request Body

```json
{
  "reviewed_by": "dr.smith",
  "note": "Not appropriate to send for this result set."
}
```

`note` is optional.

### Responses

#### `200 OK`
Returns the updated patient message.

#### `409 Conflict`
Returned when the transition is illegal for the current state.

### Error responses
See Common Error Responses (404, 422); plus `409` above.

## `POST /v1/patient_messages/{patient_message_id}/send`

### Description
**Demo-send.** Flips an `APPROVED` message to `review_status = SENT` and stamps
`sent_at`, and emits a `MESSAGE_SENT` processing event targeting the message.
There is **no external delivery** and no patient-facing surface — this exists to
demonstrate the end of the review workflow.

### URL Parameters
`patient_message_id`: UUID. Required parameter.

### Request Body
None.

### Responses

#### `200 OK`
Returns the updated patient message with `review_status = SENT` and `sent_at`
populated.

#### `409 Conflict`
Returned when the message is not `APPROVED` (only an approved message can be
sent).

### Error responses
See Common Error Responses (404, 422); plus `409` above.

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
