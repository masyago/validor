# CSV generator description

CSV generator produces canonical clinical analyzer output in CSV format.
The generator is provided with:
* `csv_uploader/analytes.yaml`: a set of panels and analytes (lab tests)
* `csv_uploader/generation_profiles.yaml`: a set of valid and invalid profiles
for the CSV files
* `csv_uploader/config.yaml`

## CSV columns

`run_id`: required

`sample_id`: optional. Mostly one per CSV

`patient_id`: required. One per CSV

`panel_code`: required

`test_code`: required

`test_name`: optional

`analyte_type`: optional

`result`: required

`units`: optional

`reference_range_low`: optional

`reference_range_high`: optional

`flag`: optional

`collection_timestamp`: required. One timestamp per
 panel (not per individual test)
 
`instrument_id`: required, provided by `csv_uploader/config.yaml`





