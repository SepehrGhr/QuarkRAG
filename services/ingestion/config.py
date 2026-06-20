from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

_env_file = Path(__file__).resolve().parent.parent.parent / ".env"

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://user:password@localhost:5432/dbname"
    KAFKA_BROKER_URL: str = "kafka:9092"
    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://jaeger:4317"
    SERVICE_NAME: str = "ingestion-service"
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=(str(_env_file), ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()

