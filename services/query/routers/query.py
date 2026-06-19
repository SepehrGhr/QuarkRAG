import time
import httpx
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException
from prometheus_client import Counter, Histogram
from services.query.config import settings
from services.query.logging_config import logger
from services.query.embedders import get_embedder
from services.query.cache.redis_cache import get_cached_answer, set_cached_answer
from services.query.search.qdrant_search import search_similar_chunks
from services.query.kafka.producer import kafka_producer

router = APIRouter(tags=["query"])

class QueryRequest(BaseModel):
    question: str
    namespace: str = "default"
    top_k: int = 5

class QueryResponse(BaseModel):
    answer: str
    cache_hit: bool

QUERY_DURATION = Histogram(
    "quarkrag_query_duration_seconds",
    "Time spent processing queries in seconds",
    ["cache_hit"]
)
CACHE_HIT_TOTAL = Counter(
    "quarkrag_cache_hit_total",
    "Total number of cache hits"
)
CACHE_MISS_TOTAL = Counter(
    "quarkrag_cache_miss_total",
    "Total number of cache misses"
)

@router.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    start_time = time.perf_counter()
    logger.info("Received query request", question=request.question, namespace=request.namespace)
    
    cached_answer = await get_cached_answer(request.question, request.namespace, request.top_k)
    if cached_answer:
        CACHE_HIT_TOTAL.inc()
        duration = time.perf_counter() - start_time
        QUERY_DURATION.labels(cache_hit="true").observe(duration)
        
        try:
            await kafka_producer.send_message(
                settings.QUERY_EVENTS_TOPIC,
                key=request.namespace,
                value={
                    "question": request.question,
                    "namespace": request.namespace,
                    "top_k": request.top_k,
                    "cache_hit": True,
                    "duration_seconds": duration,
                    "results_count": 0
                }
            )
        except Exception as ke:
            logger.error("Failed to publish query event to Kafka", error=str(ke))
            
        return QueryResponse(answer=cached_answer, cache_hit=True)
        
    CACHE_MISS_TOTAL.inc()
    
    try:
        embedder = get_embedder()
        query_vector = await embedder.embed_text(request.question)
    except Exception as e:
        logger.exception("Failed to generate query embedding")
        raise HTTPException(status_code=500, detail=f"Embedding generation failed: {str(e)}")
        
    try:
        chunks = await search_similar_chunks(query_vector, request.namespace, request.top_k)
    except Exception as e:
        logger.exception("Qdrant search failed")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
        
    context_chunks = [c["text"] for c in chunks]
    llm_payload = {
        "question": request.question,
        "context": context_chunks,
        "namespace": request.namespace
    }
    
    llm_url = f"{settings.LLM_PROVIDER_SERVICE_URL}/generate"
    try:
        async with httpx.AsyncClient() as client:
            logger.info("Calling LLM provider service", url=llm_url)
            llm_response = await client.post(llm_url, json=llm_payload, timeout=30.0)
            
        if llm_response.status_code != 200:
            logger.error("LLM provider returned failure code", status_code=llm_response.status_code, response=llm_response.text)
            raise HTTPException(status_code=502, detail="LLM provider service failed")
            
        answer = llm_response.json()["answer"]
    except httpx.RequestError as re:
        logger.exception("HTTP connection to LLM provider service failed")
        raise HTTPException(status_code=502, detail=f"LLM provider connection failed: {str(re)}")
    except Exception as e:
        logger.exception("LLM generation process failed")
        raise HTTPException(status_code=500, detail=f"LLM call failed: {str(e)}")
        
    await set_cached_answer(request.question, request.namespace, request.top_k, answer)
    
    duration = time.perf_counter() - start_time
    QUERY_DURATION.labels(cache_hit="false").observe(duration)
    
    try:
        await kafka_producer.send_message(
            settings.QUERY_EVENTS_TOPIC,
            key=request.namespace,
            value={
                "question": request.question,
                "namespace": request.namespace,
                "top_k": request.top_k,
                "cache_hit": False,
                "duration_seconds": duration,
                "results_count": len(chunks)
            }
        )
    except Exception as ke:
        logger.error("Failed to publish query event to Kafka", error=str(ke))
        
    logger.info("Query successfully processed", duration_seconds=duration)
    return QueryResponse(answer=answer, cache_hit=False)
