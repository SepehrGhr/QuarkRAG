import asyncio
# from sentence_transformers import SentenceTransformer
from services.embedding.embedders.base import BaseEmbedder
from services.embedding.logging_config import logger

class LocalEmbedder(BaseEmbedder):
    def __init__(self):
        logger.error("LocalEmbedder is disabled to reduce image weight and memory usage")
        raise NotImplementedError("Local embedding model is disabled. Please configure EMBEDDING_PROVIDER=openai.")

    async def embed_text(self, text: str) -> list[float]:
        raise NotImplementedError()

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        raise NotImplementedError()

    def _embed_sync(self, text: str) -> list[float]:
        raise NotImplementedError()

    def _embed_batch_sync(self, texts: list[str]) -> list[list[float]]:
        raise NotImplementedError()

    @property
    def dimension(self) -> int:
        return 384

    @property
    def model_name(self) -> str:
        return "all-MiniLM-L6-v2"
