from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Codeace Intelligent Data Onboarding API"
    secret_key: str = "changeme-replace-with-a-long-random-secret-in-production"
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen2.5:7b"
    database_path: Path = Path("data/ecommerce.duckdb")
    upload_dir: Path = Path("uploads")
    chunk_size: int = 100_000
    max_query_rows: int = 1_000

    class Config:
        env_file = ".env"
        env_prefix = "CODEACE_"


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    settings.upload_dir.mkdir(parents=True, exist_ok=True)
    return settings
