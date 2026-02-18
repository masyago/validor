1. Phase 1
- success (happy path)
  - all rows from Panel and Test pass normalization. No normalization errors
    returned.
  - no errors when persisting.

  - variants:
     - rows variants:
        - include with and without flag discrepancies
        - omit some optional fields

    - expected Provenance events:
      - NORMALIZATION_STARTED
      - NORMALIZATION_RELATIONAL_SUCCEEDED

    - expected rows:
      - in DiagnosticReport with the same ingestion_id as input
      - in Observation the same ingestion_id as input


- failures
    - missed required fields
        - In output: check for specific normalization errors. (model, field, message:"required field missing)
        - 
    - no panels that match ingestion_id
    - retries exceeded number of attempts
    - some other error (connection etc)

- expected Provenance events:
    - NORMALIZATION_STARTED
    - NORMALIZATION_RELATIONAL_FAILED
    - NORMALIZATION_FAILED

2. Phase 2
- success (happy path)
    - dr and obs with ingestion_id exist, all JSON resources generated and added to tables
    


- failures
 - number of retries exceeded limit
 - if Phase 2 fails, overall normalization is successful (with warnings)
