from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

_env_file = Path(__file__).resolve().parent.parent.parent / ".env"

class Settings(BaseSettings):
    OPENAI_API_KEY: str = "your-openai-key-here"
    OPENAI_MODEL_NAME: str = "gpt-4o-mini"
    OPENAI_API_BASE: str = "https://api.openai.com/v1"
    
    OLLAMA_URL: str = "http://ollama:11434"
    OLLAMA_MODEL_NAME: str = "llama3"
    
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = 5
    CIRCUIT_BREAKER_RESET_TIMEOUT: int = 30
    
    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://jaeger:4317"
    SERVICE_NAME: str = "llm-provider-service"
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=(str(_env_file), ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()

