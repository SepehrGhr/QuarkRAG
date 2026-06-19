from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models as qmodels
from services.embedding.config import settings
from services.embedding.logging_config import logger
from services.embedding.embedders import get_embedder

_qdrant_client = None

def get_qdrant_client():
    global _qdrant_client
    if _qdrant_client is None:
        _qdrant_client = AsyncQdrantClient(
            host=settings.QDRANT_HOST,
            port=settings.QDRANT_PORT
        )
    return _qdrant_client

async def init_qdrant_collection():
    client = get_qdrant_client()
    embedder = get_embedder()
    collection_name = settings.COLLECTION_NAME
    
    try:
        collections = await client.get_collections()
        exists = any(c.name == collection_name for c in collections.collections)
    except Exception as e:
        logger.exception("Failed to connect to Qdrant or retrieve collections")
        raise

    if not exists:
        logger.info(
            "Creating Qdrant collection",
            collection_name=collection_name,
            dimension=embedder.dimension,
            provider=settings.EMBEDDING_PROVIDER,
            model=embedder.model_name
        )
        try:
            await client.create_collection(
                collection_name=collection_name,
                vectors_config=qmodels.VectorParams(
                    size=embedder.dimension,
                    distance=qmodels.Distance.COSINE
                ),
                metadata={
                    "embedding_provider": settings.EMBEDDING_PROVIDER,
                    "model_name": embedder.model_name
                }
            )
            logger.info("Qdrant collection created successfully", collection_name=collection_name)
        except Exception as e:
            logger.exception("Failed to create Qdrant collection", collection_name=collection_name)
            raise
    else:
        logger.info("Qdrant collection already exists", collection_name=collection_name)
        try:
            info = await client.get_collection(collection_name)
            meta = info.config.metadata or {}
            stored_provider = meta.get("embedding_provider")
            stored_model = meta.get("model_name")
            
            if stored_provider != settings.EMBEDDING_PROVIDER or stored_model != embedder.model_name:
                logger.error(
                    "Embedding consistency mismatch detected in existing Qdrant collection!",
                    stored_provider=stored_provider,
                    current_provider=settings.EMBEDDING_PROVIDER,
                    stored_model=stored_model,
                    current_model=embedder.model_name
                )
                raise RuntimeError("Embedding consistency mismatch in Qdrant collection metadata")
        except Exception as e:
            logger.exception("Error checking collection consistency", collection_name=collection_name)
            raise
