from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    KAFKA_BROKER_URL: str = "kafka:9092"
    QDRANT_HOST: str = "qdrant"
    QDRANT_PORT: int = 6333
    REDIS_URL: str = "redis://redis:6379/0"
    LLM_PROVIDER_SERVICE_URL: str = "http://llm-provider-service:8000"
    
    EMBEDDING_PROVIDER: str = "local"  # "local" or "openai"
    OPENAI_API_KEY: str = "your-openai-key-here"
    
    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://jaeger:4317"
    SERVICE_NAME: str = "query-service"
    LOG_LEVEL: str = "INFO"
    
    QUERY_EVENTS_TOPIC: str = "query.events"
    COLLECTION_NAME: str = "quarkrag_documents"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
