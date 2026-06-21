from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

_env_file = Path(__file__).resolve().parent.parent.parent / ".env"

class Settings(BaseSettings):
    KAFKA_BROKER_URL: str = "kafka:9092"
    QDRANT_HOST: str = "qdrant"
    QDRANT_PORT: int = 6333
    REDIS_URL: str = "redis://redis:6379/0"
    LLM_PROVIDER_SERVICE_URL: str = "http://llm-provider-service:8000"
    
    EMBEDDING_PROVIDER: str = "openai"  # "local", "openai", or "ollama"
    OPENAI_API_KEY: str = "your-openai-key-here"
    OPENAI_API_BASE: str = "https://api.openai.com/v1"
    OPENAI_EMBEDDING_MODEL_NAME: str = "text-embedding-3-small"
    OPENAI_EMBEDDING_DIMENSION: int = 1536
    OLLAMA_URL: str = "http://host.docker.internal:11434"
    OLLAMA_EMBEDDING_MODEL_NAME: str = "nomic-embed-text"
    OLLAMA_EMBEDDING_DIMENSION: int = 768
    
    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://jaeger:4317"
    SERVICE_NAME: str = "query-service"
    LOG_LEVEL: str = "INFO"
    
    QUERY_EVENTS_TOPIC: str = "query.events"
    COLLECTION_NAME: str = "quarkrag_documents"

    model_config = SettingsConfigDict(
        env_file=(str(_env_file), ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()

