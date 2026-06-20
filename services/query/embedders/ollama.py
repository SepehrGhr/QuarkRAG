from openai import AsyncOpenAI
from services.query.embedders.base import BaseEmbedder
from services.query.config import settings
from services.query.logging_config import logger

class OllamaEmbedder(BaseEmbedder):
    def __init__(self):
        logger.info(
            "Initializing OllamaEmbedder in query-service",
            model_name=settings.OLLAMA_EMBEDDING_MODEL_NAME,
            url=settings.OLLAMA_URL
        )
        # Note: Ollama's OpenAI-compatible API endpoint is at base_url/v1
        base_url = f"{settings.OLLAMA_URL.rstrip('/')}/v1"
        self.client = AsyncOpenAI(base_url=base_url, api_key="ollama")

    async def embed_text(self, text: str) -> list[float]:
        response = await self.client.embeddings.create(
            input=[text],
            model=self.model_name
        )
        return response.data[0].embedding

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        response = await self.client.embeddings.create(
            input=texts,
            model=self.model_name
        )
        return [item.embedding for item in response.data]

    @property
    def dimension(self) -> int:
        return settings.OLLAMA_EMBEDDING_DIMENSION

    @property
    def model_name(self) -> str:
        return settings.OLLAMA_EMBEDDING_MODEL_NAME
