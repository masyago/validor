Shared pytest fixtures:
- [already in parent conftest] db_session (Postgres test_cla): begin transaction per test + rollback.
- seed_ingestion(), seed_panel(), seed_test() helpers.
- freeze_time (e.g., freezegun) so normalized_at is stable.
- fetch_events(ingestion_id) helper to query processing_event by ingestion_id ordered by occurred_at.
Optional: monkeypatch_serializer to force Phase 2 failures deterministically.

Phase 1 (relational) tests
P1.1 Happy path (1 panel, N tests) & (2 panels, N tests - use multi-panel case only for happy path)
Given: - ingestion with 1 panel + e.g. 3 tests
       - add variant with 2 panels with 3 tests each

one numeric + in-range
one numeric + out-of-range (produces discrepancy when analyzer flag mismatches)
one text-only (no result_value_num, has result_raw)
When: run job
Then:
returns (ok=True, norm_errors=[], json_failures=[]) if Phase 2 also succeeds (see Phase 2 tests)
diagnostic_report row exists for the panel with correct mappings and resource_json eventually set (after Phase 2)
observation rows exist for tests with correct mappings and diagnostic_report_id populated
Phase 1 event emitted: NORMALIZATION_STARTED, NORMALIZATION_RELATIONAL_SUCCEEDED

P1.2 All-or-nothing validation failure (missing required field)
Given: at least 1 panel/test missing a required field (e.g., panel.patient_id is None)
When: run job
Then:
returns ok=False and norm_errors includes the specific model/field/message
no rows inserted into diagnostic_report/observation for that ingestion
events include: NORMALIZATION_STARTED, NORMALIZATION_RELATIONAL_FAILED, NORMALIZATION_FAILED


P1.3 Empty ingestion (no panels) - TODO: decide expected behavior
Given: ingestion_id exists (or not) but panel_repo.get_by_ingestion_id returns empty
When: run job
Then: 
returns ok=False, errors with Normalization error describing that panels don't exist.
no rows inserted into diagnostic_report/observation for that ingestion
events include: NORMALIZATION_STARTED, NORMALIZATION_RELATIONAL_FAILED, NORMALIZATION_FAILED


P1.4 Idempotency (re-run same ingestion)
Given: successful first run
When: run job again for same ingestion_id
Then:
counts in diagnostic_report and observation unchanged (no duplicates)
verify Phase 1 “created” counts (from event details) are 0 on second run
resource_json behavior: should remain stable

P1.5 Concurrency safety (UPSERT race)
Given: one ingestion seeded
When: run run_for_ingestion_id concurrently in two separate sessions (threads/processes)
Then:
no unique constraint errors
final DB state has exactly 1 DR per panel and 1 Obs per test
events: assert no crash + correct rows.


Phase 2 (FHIR JSON) tests

P2.1 Happy path writes JSON to both tables
Given: a normal ingestion (same as P1.1 with 1 Panel)
When: run job
Then:
diagnostic_report.resource_json is not null
every observation.resource_json is not null
events include: FHIR_JSON_GENERATION_SUCCEEDED, NORMALIZATION_SUCCEEDED
json_failures returned is empty

P2.2 Partial JSON failure → overall success with warnings
Given: seed ingestion, but monkeypatch serializer to throw for one observation:
e.g. patch R4ObsDrV1Serializer.make_observation to raise for a specific observation_id
When: run job
Then:
job returns ok=True, json_failures contains 1 entry
that observation keeps resource_json is NULL; others are written
events include: FHIR_JSON_GENERATION_FAILED with severity=WARN, and NORMALIZATION_SUCCEEDED_WITH_WARNINGS (also WARN)


P2.3 Determinism/idempotency of JSON
Given: successful run with frozen time and stable test ordering
When: run job twice
Then: for each DR/Obs, resource_json is semantically equal between runs.
Recommendation for assertions:
compare parsed JSON objects (Python dict/list), not raw strings
for DiagnosticReport.result, compare as sorted list by "reference" or compare as a set.
Retry behavior tests (effective and fast)
Right now retries are hard-coded (max_attempts = 3) and no backoff is shown, so tests can still be fast if you simulate a “fails once then succeeds” transient error.

R1 Phase 1 retries on retryable DB error
Method: monkeypatch a repo method used in Phase 1 (e.g., panel_repo.get_by_ingestion_id) to raise OperationalError on first call, then behave normally.
Assert: job succeeds and emits RELATIONAL_SUCCEEDED with retry_attempts > 1.

R2 Phase 2 retries on retryable DB error
Monkeypatch dr_repo.update_resource_json (or session.commit) to raise OperationalError once.
Assert: job eventually succeeds; emits FHIR_JSON_GENERATION_SUCCEEDED with attempts > 1.


Provenance event assertions:
For each scenario, assert:
* the set of ProcessingEventType values present for the ingestion
* severities (WARN for Phase 2 failure paths)
* event dedupe: optionally assert (ingestion_id, event_type, dedupe_key) unique (should be, given ux_processing_event_dedupe)
* Because occurred_at ordering can vary slightly, prefer asserting presence + key details, not strict sequence, unless you explicitly ORDER BY occurred_at, event_id.

