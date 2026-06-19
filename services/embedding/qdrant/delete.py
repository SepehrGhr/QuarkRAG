from qdrant_client.http import models as qmodels
from services.embedding.qdrant.client import get_qdrant_client
from services.embedding.config import settings
from services.embedding.logging_config import logger

async def delete_vectors_by_document(document_id: str, namespace: str):
    client = get_qdrant_client()
    collection_name = settings.COLLECTION_NAME
    
    try:
        await client.delete(
            collection_name=collection_name,
            points_selector=qmodels.FilterSelector(
                filter=qmodels.Filter(
                    must=[
                        qmodels.FieldCondition(
                            key="document_id",
                            match=qmodels.MatchValue(value=document_id)
                        ),
                        qmodels.FieldCondition(
                            key="namespace",
                            match=qmodels.MatchValue(value=namespace)
                        )
                    ]
                )
            )
        )
        logger.info("Deleted vectors from Qdrant", document_id=document_id, namespace=namespace)
    except Exception as e:
        logger.exception("Failed to delete vectors from Qdrant", document_id=document_id, namespace=namespace)
        raise
