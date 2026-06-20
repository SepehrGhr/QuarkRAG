from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

_env_file = Path(__file__).resolve().parent.parent.parent / ".env"

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://user:password@localhost:5432/dbname"
    KAFKA_BROKER_URL: str = "kafka:9092"
    QDRANT_HOST: str = "qdrant"
    QDRANT_PORT: int = 6333
    REDIS_URL: str = "redis://redis:6379/0"
    
    EMBEDDING_PROVIDER: str = "openai"  # "local" or "openai"
    OPENAI_API_KEY: str = "your-openai-key-here"
    
    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://jaeger:4317"
    SERVICE_NAME: str = "embedding-service"
    LOG_LEVEL: str = "INFO"
    
    DLQ_TOPIC: str = "dlq"
    RAW_TOPIC: str = "docs.raw"
    DELETE_TOPIC: str = "docs.delete"
    EMBEDDED_TOPIC: str = "docs.embedded"
    COLLECTION_NAME: str = "quarkrag_documents"

    model_config = SettingsConfigDict(
        env_file=(str(_env_file), ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()

