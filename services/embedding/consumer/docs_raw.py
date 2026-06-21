import json
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from tenacity import retry, stop_after_attempt, wait_exponential
from services.embedding.config import settings
from services.embedding.logging_config import logger
from services.embedding.embedders import get_embedder
from services.embedding.qdrant.upsert import upsert_vector
from services.embedding.database import update_document_status
from services.embedding.kafka.producer import kafka_producer

redis_client = None

async def get_redis():
    global redis_client
    if redis_client is None:
        redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    return redis_client

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
async def process_message_with_retry(msg_val: dict):
    document_id = msg_val["document_id"]
    chunk_index = msg_val["chunk_index"]
    text = msg_val["text"]
    namespace = msg_val["namespace"]
    total_chunks = msg_val["total_chunks"]
    
    redis = await get_redis()
    started_key = f"doc:{document_id}:started"
    
    is_first = await redis.set(started_key, "1", ex=3600, nx=True)
    if is_first:
        await update_document_status(
            document_id=document_id,
            status="embedding",
            embedding_provider=settings.EMBEDDING_PROVIDER
        )
    
    embedder = get_embedder()
    vector = await embedder.embed_text(text)
    
    await upsert_vector(
        document_id=document_id,
        chunk_index=chunk_index,
        text=text,
        vector=vector,
        namespace=namespace
    )
    
    counter_key = f"doc:{document_id}:processed_chunks"
    processed_count = await redis.incr(counter_key)
    await redis.expire(counter_key, 3600)
    
    logger.info(
        "Processed chunk successfully",
        document_id=document_id,
        chunk_index=chunk_index,
        processed=processed_count,
        total=total_chunks
    )
    
    if processed_count == total_chunks:
        await update_document_status(document_id=document_id, status="ready")
        
        await kafka_producer.send_message(
            settings.EMBEDDED_TOPIC,
            key=document_id,
            value={
                "document_id": document_id,
                "namespace": namespace,
                "chunk_count": total_chunks,
                "status": "ready"
            }
        )
        
        await redis.delete(started_key, counter_key)

async def handle_failed_message(msg_val: dict, exception: Exception):
    document_id = msg_val.get("document_id")
    logger.error("Failed to process message after retries, sending to DLQ", document_id=document_id, error=str(exception))
    
    if document_id:
        try:
            await update_document_status(document_id=document_id, status="failed")
        except Exception as db_err:
            logger.error("Failed to update status to failed in DB", error=str(db_err))
            
    try:
        dlq_payload = {
            "original_message": msg_val,
            "error": str(exception),
            "service": settings.SERVICE_NAME
        }
        await kafka_producer.send_message(
            settings.DLQ_TOPIC,
            key=document_id or "unknown",
            value=dlq_payload
        )
    except Exception as k_err:
        logger.exception("Failed to publish message to DLQ", error=str(k_err))

async def run_raw_consumer():
    consumer = AIOKafkaConsumer(
        settings.RAW_TOPIC,
        bootstrap_servers=settings.KAFKA_BROKER_URL,
        group_id="embedding-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest"
    )
    
    await consumer.start()
    logger.info("Kafka docs.raw consumer started")
    
    try:
        async for msg in consumer:
            msg_val = msg.value
            try:
                await process_message_with_retry(msg_val)
            except Exception as e:
                logger.exception("Persistent processing error in raw consumer", document_id=msg_val.get("document_id"))
                await handle_failed_message(msg_val, e)
    finally:
        await consumer.stop()
