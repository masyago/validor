## Local Demo Workflow (Simulator + Uploader)

The analyzer CSV generator and CSV uploader are intentionally kept **outside**
the ingestion service. They simulate external systems in the pipeline:

- `csv_uploader/csv_generator.py`: a canonical analyzer producing CSV exports
- `csv_uploader/csv_uploader.py`: a middleware uploader POSTing exports to the API

### Run in three terminals

1) Start the API:
```sh
uv run fastapi dev app/main.py --port 8000
```

2) Start the uploader watcher:
```sh
uv run python -m csv_uploader.csv_uploader
```

3) Generate a CSV export (one file):
```sh
uv run python -m csv_uploader.csv_generator
```


