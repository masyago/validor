# CSV Validation rules

* List of CSV columns:

    * `run_id`: required
    * `sample_id`: optional
    * `patient_id`: required
    * `panel_code`: required
    * `test_code`: required
    * `test_name`: optional
    * `analyte_type`: optional
    * `result`: required
    * `units`: optional
    * `reference_range_low`: optional
    * `reference_range_high`: optional
    * `flag`: optional
    * `collection_timestamp`: required
    * `instrument_id`: required

* All required fields must be present. Required fields:
    * `run_id`
    * `patient_id`
    * `panel_code`
    * `test_code`
    * `result`
    * `collection_timestamp`
    * `instrument_id`

* Structure within CSV
    * one `patient_id` per CSV

* Data format
    * `patient_id`: string of format 'PAT-<uuid>'
    * `sample_id`: string of format 'SAM-<uuid>'
    * `result`: str, can contain comparators, numbers, letters (and other characters). Numeric can be integer, decimal but cannot be negative. Either numeric value or text must be present, both - ok. 
    * `collection_timestamp`: string - ISO 8601 timestamp 
    * If present and after converted to numeric, `reference_range_low` cannot be greater than
    `reference_range_high`

* Test files. 30 files total:
    * 6 valid files: small, medium, large sizes. 2 of each
    * 16 files with a single defect. Variants of `valid_small_01.csv`
        * 3 files: same defect in the beginning, middle, and end of the file
        * 
    * 4 files with many defects. Variants of the small valid file
    * 4 larger files and many defects

* How to process the files and record results:
    * run uploader for the folder `metrics/validation_accuracy/fixed_csv_v1`
     `uv run python -m csv_uploader.csv_uploader --watch-dir metrics/validation_accuracy/fixed_csv_v1 --once --keep-files --stability-delay-seconds 0 --wait-for-terminal`
    * run script that extracts results from the DB `metrics/record_validation_accuracy_results.py`
     `uv run python -m metrics.record_validation_accuracy_results --dir metrics/validation_accuracy/fixed_csv_v1 --out metrics/validation_accuracy/validation_results_20260327.csv`
