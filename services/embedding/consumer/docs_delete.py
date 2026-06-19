import json
from aiokafka import AIOKafkaConsumer
from services.embedding.config import settings
from services.embedding.logging_config import logger
from services.embedding.qdrant.delete import delete_vectors_by_document

async def run_delete_consumer():
    consumer = AIOKafkaConsumer(
        settings.DELETE_TOPIC,
        bootstrap_servers=settings.KAFKA_BROKER_URL,
        group_id="embedding-delete-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest"
    )
    
    await consumer.start()
    logger.info("Kafka docs.delete consumer started")
    
    try:
        async for msg in consumer:
            msg_val = msg.value
            document_id = msg_val.get("document_id")
            namespace = msg_val.get("namespace")
            
            logger.info("Received delete event for document", document_id=document_id, namespace=namespace)
            if document_id and namespace:
                try:
                    await delete_vectors_by_document(document_id, namespace)
                except Exception as e:
                    logger.exception("Failed to delete vectors for document from Qdrant", document_id=document_id)
    finally:
        await consumer.stop()
