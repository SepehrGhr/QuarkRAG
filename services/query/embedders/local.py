import asyncio
from sentence_transformers import SentenceTransformer
from services.query.embedders.base import BaseEmbedder
from services.query.logging_config import logger

class LocalEmbedder(BaseEmbedder):
    def __init__(self):
        logger.info("Initializing LocalEmbedder with all-MiniLM-L6-v2 model in query-service")
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("LocalEmbedder model loaded successfully in query-service")

    async def embed_text(self, text: str) -> list[float]:
        return await asyncio.to_thread(self._embed_sync, text)

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return await asyncio.to_thread(self._embed_batch_sync, texts)

    def _embed_sync(self, text: str) -> list[float]:
        return self.model.encode(text).tolist()

    def _embed_batch_sync(self, texts: list[str]) -> list[list[float]]:
        return self.model.encode(texts).tolist()

    @property
    def dimension(self) -> int:
        return 384

    @property
    def model_name(self) -> str:
        return "all-MiniLM-L6-v2"
