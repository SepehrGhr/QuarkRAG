from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models as qmodels
from services.query.config import settings
from services.query.logging_config import logger

_qdrant_client = None

def get_qdrant_client():
    global _qdrant_client
    if _qdrant_client is None:
        _qdrant_client = AsyncQdrantClient(
            host=settings.QDRANT_HOST,
            port=settings.QDRANT_PORT
        )
    return _qdrant_client

async def search_similar_chunks(query_vector: list[float], namespace: str, top_k: int) -> list[dict]:
    client = get_qdrant_client()
    collection_name = settings.COLLECTION_NAME
    
    try:
        search_result = await client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            query_filter=qmodels.Filter(
                must=[
                    qmodels.FieldCondition(
                        key="namespace",
                        match=qmodels.MatchValue(value=namespace)
                    )
                ]
            ),
            limit=top_k
        )
        
        logger.info("Qdrant ANN search completed", namespace=namespace, results_count=len(search_result))
        
        chunks = []
        for hit in search_result:
            if hit.payload:
                chunks.append({
                    "id": hit.id,
                    "text": hit.payload.get("text", ""),
                    "score": hit.score,
                    "chunk_index": hit.payload.get("chunk_index"),
                    "document_id": hit.payload.get("document_id")
                })
        return chunks
    except Exception:
        logger.exception("Failed to search Qdrant", namespace=namespace)
        raise
