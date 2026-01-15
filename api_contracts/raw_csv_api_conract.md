# API Contract: Raw CSV Ingestion

This document defines the API contract for ingesting raw CSV data from
canonical laboratory analyzers.

---

## `POST /v1/ingestions`

### Summary

Accepts a raw CSV file from a canonical lab analyzer, along with metadata.
The API performs checks whether required fields presents, file is not empty, and
content_sha256 matches.

### Description

This endpoint is the primary entry point for new lab data. It is designed to be
called by a middleware or uploader service that monitors lab instruments for 
new data exports. The endpoint immediately accepts the data and returns a 
`202 Accepted` response, providing the client with an `ingestion_id` to track 
the status of the processing pipeline.

Each uploaded CSV file is treated as a single, atomic ingestion event.
Uniqueness of a run is defined as combination of instrument_id and run_id.

### Request

**Content-Type:** `multipart/form-data`

**Form Fields:**

| Name             | Type   | Required | Description                                                                                             | Example                                  |
| ---------------- | ------ | -------- | ------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `file`           | File   | Yes      | The raw CSV file exported from the lab analyzer.                                                        | `results_20260112.csv`                   |
| `uploader_id`      | string | Yes      | Identifier for the middleware or uploader service sending the data.                                     | `uploader-agent-001`                   |
| `spec_version`   | string | Yes      | The version of the ingestion schema the CSV file adheres to.                                            | `analyzer_csv_v1`                        |
| `instrument_id`  | string | Yes      | The unique identifier of the physical lab instrument that generated the data.                           | `CHEM-ANALYZER-XYZ-789`                  |
| `run_id`         | string | Yes      | A unique identifier for the specific analysis run on the instrument.                                    | `RUN-20260112-1430-A`                    |
| `content_sha256` | string | No      | The SHA256 hash of the `file` content, used for an integrity check upon receipt.                        | `e3b0c44298fc1c149afbf4c8996fb924...`     |
| `uploader_received_at`    | string | Yes      | The ISO 8601 timestamp (UTC) when the file was received by the uploader from the instrument's export location. | `2026-01-12T14:30:05Z`                   |

Note on hash optional field `content_sha256`:
* If content_sha256 is provided: API recomputes and returns 422 CONTENT_HASH_MISMATCH on mismatch.
* If content_sha256 is omitted: API skips comparison. Server still computes and stores
  a server generated hash sever_sha256.


### Responses

### `200 OK`
Indicates that the same run (combination of `instrument_id`and `run_id`) exists
and has identical content.

**Content-Type:** `application/json`

**Location:** /v1/ingestions/a7b1c3d4-e5f6-7890-1234-567890abcdef

**Body:**

```json
{
  "existing_ingestion_id": "a7b1c3d4-e5f6-7890-1234-567890abcdef",
  "message": "The run was already submitted."

}
```

#### `202 Accepted`

Indicates that the request has been successfully received and queued for 
processing.

**Content-Type:** `application/json`

**Location:** /v1/ingestions/a7b1c3d4-e5f6-7890-1234-567890abcdef

**Body:**

```json
{
  "ingestion_id": "a7b1c3d4-e5f6-7890-1234-567890abcdef",
  "status": "PROCESSING",
  "api_received_at": "2026-01-12T14:35:10.123Z",
  "message": "Ingestion request received and is being processed."
}
```

#### `409 Conflict`

Returned when conflict occurs:
    - Existing ingestion (`instrument_id` and `run_id`) has
     `server_sha256`. 
    - New upload has with the same `instrument_id` and `run_id` produce different
      `server_sha256_new`
    - `server_sha256_new` differs from `server_sha256`


**Content-Type:** `application/json`

**Body:**

```json
{
    "code": "RUN_ID_CONTENT_MISMATCH",
    "retryable": false,
    "existing_ingestion_id": "a7b1c3d4-e5f6-7890-1234-567890abcdef",
    "conflict_key": {
        "instrument_id": "CHEM-ANALYZER-XYZ-789",
        "run_id": "RUN-20260112-1430-A"
    },
    "hashes": {
        "existing": "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
        "submitted": "7509e5bda0c762d2bac7f90d758b5b2263fa01ccbc542ab5e3df163be08e6ca9"
    },
    "message": "An ingestion already exists for the run (instrument_id, run_id) but server-produced hash differs"
}
```

#### `413 Payload Too Large`

File exceeds size limit.

**Content-Type:** `application/json`

**Body:**

```json
{
    "code": "PAYLOAD_TOO_LARGE",
    "retryable": false,
    "max_bytes": 1000000,
    "message": "File exceeds size limit."
}
```

#### `415 Unsupported Media Type`

Media type is not `multipart/form-data`

**Content-Type:** `application/json`

**Body:**

```json
{
    "code": "UNSUPPORTED_MEDIA_TYPE",
    "retryable": false,
    "expected": "multipart/form-data",
    "received": "application/json",
    "message": "Media type is not `multipart/form-data`."
}
```

#### `422 Unprocessable Entity`

Validation error. The error can occur due to incorrect or missing metadata 
(missing required fields, incorrect data types, bad data formatting) or hash 
mismatch (if hash provided).
CSV column/row validation errors will be available at
`GET /v1/ingestions/{id}/errors`.


**Content-Type:** `application/json`

**Body:**

Missing field error

```json
{
  "code": "VALIDATION_ERROR",
  "retryable": false,
  "errors": [
      { "field": "instrument_id", "message": "field required" }
      ],
  "message": "Validation error."
}
```
Hash mismatch error

```json
{
  "code": "CONTENT_HASH_MISMATCH",
  "retryable": false,
  "message": "Content integrity check failed."
}
```

### Authentication

Authentication between uploader and API is intentionally omitted in this
project; in production this boundary would be secured via service-to-service 
authentication (e.g., mTLS or signed tokens) and network isolation.