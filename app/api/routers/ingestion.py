from fastapi import FastAPI

app = FastAPI()


@app.post("/v1/ingestions")
def ingest_csv_file(request: IngestionRequest):
    return (
        response  # response is defined via Pydantic model Ingestion response
    )
