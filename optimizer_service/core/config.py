import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    REDIS_URL: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    CELERY_BROKER_URL: str = os.environ.get("CELERY_BROKER_URL", REDIS_URL)
    CELERY_RESULT_BACKEND: str = os.environ.get("CELERY_RESULT_BACKEND", REDIS_URL)
    GEMMA_API_KEY: str = os.environ.get("GEMMA_API_KEY", "YOUR_GEMMA_API_KEY_HERE")
    LLM_PROVIDER: str = os.environ.get("LLM_PROVIDER", "gemma")
    VLLM_HOST: str = os.environ.get("VLLM_HOST", "localhost")
    VLLM_PORT: int = int(os.environ.get("VLLM_PORT", "8001"))
    LLAMA_HOST: str = os.environ.get("LLAMA_HOST", "localhost")
    LLAMA_PORT: int = int(os.environ.get("LLAMA_PORT", "8001"))


settings = Settings()