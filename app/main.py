from fastapi import FastAPI
from app.api.routers.ingestion import router as api_router

app = FastAPI()

app.include_router(api_router, prefix="/v1")


@app.get("/")
def main():
    return {"message": "Hello from clinical-lab-analyzer!"}


if __name__ == "__main__":
    main()
