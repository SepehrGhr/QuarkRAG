from openai import AsyncOpenAI
from services.embedding.embedders.base import BaseEmbedder
from services.embedding.config import settings
from services.embedding.logging_config import logger

class OpenAIEmbedder(BaseEmbedder):
    def __init__(self):
        logger.info("Initializing OpenAIEmbedder with text-embedding-3-small model")
        self.client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)

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
        return 1536

    @property
    def model_name(self) -> str:
        return "text-embedding-3-small"
