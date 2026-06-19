import hashlib
import json
import redis.asyncio as aioredis
from services.query.config import settings
from services.query.logging_config import logger

_redis_client = None

def get_redis_client():
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis_client

def generate_cache_key(question: str, namespace: str, top_k: int) -> str:
    raw_key = f"{question}:{namespace}:{top_k}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

async def get_cached_answer(question: str, namespace: str, top_k: int) -> str:
    redis = get_redis_client()
    key = generate_cache_key(question, namespace, top_k)
    try:
        answer = await redis.get(key)
        if answer:
            logger.info("Redis cache hit", key=key)
            return answer
        logger.info("Redis cache miss", key=key)
        return None
    except Exception as e:
        logger.error("Failed to read from Redis cache", error=str(e))
        return None

async def set_cached_answer(question: str, namespace: str, top_k: int, answer: str, ttl: int = 3600):
    redis = get_redis_client()
    key = generate_cache_key(question, namespace, top_k)
    try:
        await redis.setex(key, ttl, answer)
        logger.info("Saved answer to Redis cache", key=key)
    except Exception as e:
        logger.error("Failed to write to Redis cache", error=str(e))
