import uuid
from qdrant_client.http import models as qmodels
from services.embedding.qdrant.client import get_qdrant_client
from services.embedding.config import settings
from services.embedding.logging_config import logger

async def upsert_vector(
    document_id: str,
    chunk_index: int,
    text: str,
    vector: list[float],
    namespace: str
):
    client = get_qdrant_client()
    collection_name = settings.COLLECTION_NAME
    
    # Deterministic point UUID for idempotency
    point_id = str(uuid.uuid5(uuid.UUID(document_id), f"chunk_{chunk_index}"))
    
    point = qmodels.PointStruct(
        id=point_id,
        vector=vector,
        payload={
            "document_id": document_id,
            "chunk_index": chunk_index,
            "text": text,
            "namespace": namespace
        }
    )
    
    try:
        await client.upsert(
            collection_name=collection_name,
            points=[point]
        )
        logger.debug("Vector upserted to Qdrant", point_id=point_id, document_id=document_id, chunk_index=chunk_index)
    except Exception:
        logger.exception("Failed to upsert vector to Qdrant", document_id=document_id, chunk_index=chunk_index)
        raise
