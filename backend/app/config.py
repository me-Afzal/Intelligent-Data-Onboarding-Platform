"""
Application configuration via Pydantic Settings.

All fields can be overridden with CODEACE_-prefixed environment variables
(e.g. CODEACE_SECRET_KEY). get_settings() is cached so the .env file is
only read once per process, and the required directories are created on
first access.
"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Codeace Intelligent Data Onboarding API"
    # Must be changed to a long random value in production.
    secret_key: str = "changeme-replace-with-a-long-random-secret-in-production"
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/0"
    # Celery results use a separate Redis DB to avoid key collisions with job state.
    celery_result_backend: str = "redis://localhost:6379/1"
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen2.5:7b"
    database_path: Path = Path("data/ecommerce.duckdb")
    upload_dir: Path = Path("uploads")
    # Rows per pandas chunk during CSV ingestion; tune for available memory.
    chunk_size: int = 100_000
    # Hard cap applied to every AI-generated SELECT to prevent runaway results.
    max_query_rows: int = 1_000

    class Config:
        env_file = ".env"
        env_prefix = "CODEACE_"


@lru_cache
def get_settings() -> Settings:
    """Return the singleton Settings instance, reading .env and env vars once.

    lru_cache ensures the Settings object (and directory creation) only runs
    once per process. Call get_settings.cache_clear() in tests to reset state.
    """
    settings = Settings()
    # Create storage directories lazily so the app starts cleanly in any CWD.
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    settings.upload_dir.mkdir(parents=True, exist_ok=True)
    return settings
