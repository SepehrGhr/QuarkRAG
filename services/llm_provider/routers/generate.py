import time
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from prometheus_client import Gauge, Histogram
from services.llm_provider.circuit_breaker.breaker import CircuitBreaker
from services.llm_provider.providers.openai import OpenAIProvider
from services.llm_provider.providers.ollama import OllamaProvider
from services.llm_provider.logging_config import logger

router = APIRouter(tags=["generate"])

class GenerateRequest(BaseModel):
    question: str
    context: list[str]
    namespace: str = "default"

class GenerateResponse(BaseModel):
    answer: str
    provider: str

# Instantiate breaker and providers
breaker = CircuitBreaker(failure_threshold=5, reset_timeout=30)
openai_provider = OpenAIProvider()
ollama_provider = OllamaProvider()

# Metrics definition
BREAKER_STATE_GAUGE = Gauge(
    "quarkrag_circuit_breaker_state",
    "Circuit breaker state: 0=closed, 1=open, 2=half-open",
    ["provider"]
)
ACTIVE_PROVIDER_GAUGE = Gauge(
    "quarkrag_llm_provider_active",
    "Active LLM provider serving requests: 1=active, 0=inactive",
    ["provider"]
)
REQUEST_DURATION_HISTOGRAM = Histogram(
    "quarkrag_llm_request_duration_seconds",
    "LLM generation request duration in seconds",
    ["provider", "status"]
)

@router.post("/generate", response_model=GenerateResponse)
async def generate_response(request: GenerateRequest):
    current_state = await breaker.get_state()
    BREAKER_STATE_GAUGE.labels(provider="openai").set(current_state.value)
    
    target = await breaker.before_call()
    
    if target == "primary":
        logger.info("Routing request to primary LLM provider (OpenAI)")
        ACTIVE_PROVIDER_GAUGE.labels(provider="openai").set(1)
        ACTIVE_PROVIDER_GAUGE.labels(provider="ollama").set(0)
        
        start = time.perf_counter()
        try:
            answer = await openai_provider.generate(request.question, request.context)
            await breaker.record_success()
            duration = time.perf_counter() - start
            REQUEST_DURATION_HISTOGRAM.labels(provider="openai", status="success").observe(duration)
            return GenerateResponse(answer=answer, provider="openai")
        except Exception:
            logger.exception("Primary provider failed")
            await breaker.record_failure()
            duration = time.perf_counter() - start
            REQUEST_DURATION_HISTOGRAM.labels(provider="openai", status="failure").observe(duration)
            
            # Execute immediate fallback to Ollama
            logger.info("Executing immediate fallback to Ollama due to primary failure")
            ACTIVE_PROVIDER_GAUGE.labels(provider="openai").set(0)
            ACTIVE_PROVIDER_GAUGE.labels(provider="ollama").set(1)
            
            start_fallback = time.perf_counter()
            try:
                answer = await ollama_provider.generate(request.question, request.context)
                duration_fallback = time.perf_counter() - start_fallback
                REQUEST_DURATION_HISTOGRAM.labels(provider="ollama", status="success").observe(duration_fallback)
                return GenerateResponse(answer=answer, provider="ollama")
            except Exception:
                logger.exception("Fallback provider failed as well")
                duration_fallback = time.perf_counter() - start_fallback
                REQUEST_DURATION_HISTOGRAM.labels(provider="ollama", status="failure").observe(duration_fallback)
                raise HTTPException(status_code=502, detail="Both primary and fallback LLM providers failed")
                
    else:  # fallback
        logger.info("Routing request directly to fallback provider (Ollama) due to open circuit breaker")
        ACTIVE_PROVIDER_GAUGE.labels(provider="openai").set(0)
        ACTIVE_PROVIDER_GAUGE.labels(provider="ollama").set(1)
        
        start = time.perf_counter()
        try:
            answer = await ollama_provider.generate(request.question, request.context)
            duration = time.perf_counter() - start
            REQUEST_DURATION_HISTOGRAM.labels(provider="ollama", status="success").observe(duration)
            return GenerateResponse(answer=answer, provider="ollama")
        except Exception:
            logger.exception("Fallback provider failed")
            duration = time.perf_counter() - start
            REQUEST_DURATION_HISTOGRAM.labels(provider="ollama", status="failure").observe(duration)
            raise HTTPException(status_code=502, detail="Fallback LLM provider failed")
