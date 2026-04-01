The document describes how to run 2 type of demos:
1. CLI demo
2. Website demo

# CLI Demo

* In terminal A, initialize docker containers. It will start API and database
  containers and migrate schemas. It can take a few seconds.
    `docker compose up --build`

* In terminal B, run demo file. You will see summary of generated CSV file, 
  the upload to API, API response, and status for each stage of the data pipeline, 
  along with the final status for the ingestion.
    `uv run python csv_uploader/cli_demo.py --once`

# Website Demo

