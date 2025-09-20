from fastapi import FastAPI
from optimizer_service.api.endpoints import router as api_router

app = FastAPI(
    title="Data Lakehouse Optimization Service",
    description="An intelligent service for optimizing the performance of a Data Lakehouse using LLM.",
    version="0.1.0"
)

app.include_router(api_router, prefix="/api", tags=["Optimization"])

@app.get("/")
def read_root():
    return {"message": "Welcome to the Optimization Service API!"}