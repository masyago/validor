The document describes how validation accuracy was measured for the service.

Total of 30 files were generated and submitted to the service. It was a mix of 
valid and intentionally invalid files with variety of sizes and injected defects.
Detailed description of every file and defects was documented prior to submitting
files ot the service. 

## Files
The files stored in directory `metrics/validation_accuracy/fixed_csv_v1`.

Valid files: 
* 2 small (108 rows each)
* 2 medium (1080 rows each)
* 2 large (10800 rows each)

Invalid files with a single defect:
* 16 small

Invalid files with multiple (3 to 150) defects:
* 4 small
* 2 medium
* 2 large

## Execution
* File generation
    * Valid files small, medium, and large were generated using a python script
      `csv_uploader/csv_generator.py`.  
    * Defects were added to the files manually to cover wide range of possible
      invalid data.
* Fresh docker containers were started (to ensure database reset)
* Files were uploaded to the service using the uploader 
  `csv_uploader/csv_uploader.py`
* Script was run to extract the results from the database:
 `metrics/validation_accuracy/record_validation_accuracy_results.py`

## Results
* 24 of of 24 invalid files and 6 out of 6 valid files were correctly 
identified and processed by the service. For rows with defects, recall was 
99.5%, precision 100.0% with 49,896 rows ingested. 

* One file, `invalid_medium_02.csv`, was excluded from the analysis due to human 
error: description of row defects didn't align with the actual file.


