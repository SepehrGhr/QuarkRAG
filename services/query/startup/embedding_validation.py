import sys
from qdrant_client import AsyncQdrantClient
from services.query.config import settings
from services.query.logging_config import logger
from services.query.embedders import get_embedder

async def validate_embedding_consistency():
    logger.info("Running embedding consistency startup validation")
    client = AsyncQdrantClient(host=settings.QDRANT_HOST, port=settings.QDRANT_PORT)
    embedder = get_embedder()
    collection_name = settings.COLLECTION_NAME
    
    try:
        collections = await client.get_collections()
        exists = any(c.name == collection_name for c in collections.collections)
        
        if not exists:
            logger.warning("Qdrant collection does not exist yet. Skipping consistency check.", collection_name=collection_name)
            return

        info = await client.get_collection(collection_name)
        meta = info.config.metadata or {}
        stored_provider = meta.get("embedding_provider")
        stored_model = meta.get("model_name")
        
        logger.info(
            "Found Qdrant collection metadata",
            stored_provider=stored_provider,
            stored_model=stored_model
        )
        
        if stored_provider != settings.EMBEDDING_PROVIDER or stored_model != embedder.model_name:
            logger.critical(
                "FATAL: Embedding consistency mismatch! The Qdrant collection is configured with different embedding parameters.",
                stored_provider=stored_provider,
                current_provider=settings.EMBEDDING_PROVIDER,
                stored_model=stored_model,
                current_model=embedder.model_name
            )
            sys.exit(1)
            
        logger.info("Embedding consistency validation succeeded")
    except Exception:
        logger.exception("Failed to validate embedding consistency during startup")
        sys.exit(1)
