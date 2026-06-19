import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from services.embedding.config import settings
from services.embedding.logging_config import setup_logging, logger
from services.embedding.qdrant.client import get_qdrant_client, init_qdrant_collection
from services.embedding.embedders import get_embedder
from services.embedding.database import engine
from services.embedding.kafka.producer import kafka_producer
from services.embedding.consumer.docs_raw import run_raw_consumer
from services.embedding.consumer.docs_delete import run_delete_consumer

# OTEL Setup
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

setup_logging()

consumer_tasks = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting embedding-service")
    
    # 1. Initialize OTel
    provider = TracerProvider()
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT, insecure=True))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    
    # 2. Trigger embedder loading (downloads/loads model)
    logger.info("Pre-loading embedder model...")
    get_embedder()
    logger.info("Embedder model loaded successfully")
    
    # 3. Initialize Qdrant collection & metadata
    await init_qdrant_collection()
    
    # 4. Start Kafka Publisher
    await kafka_producer.start()
    
    # 5. Start consumers as background tasks
    loop = asyncio.get_running_loop()
    raw_task = loop.create_task(run_raw_consumer())
    delete_task = loop.create_task(run_delete_consumer())
    consumer_tasks.extend([raw_task, delete_task])
    
    yield
    logger.info("Stopping embedding-service")
    
    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)
    
    await kafka_producer.stop()
    await engine.dispose()

app = FastAPI(
    title=settings.SERVICE_NAME,
    lifespan=lifespan
)

FastAPIInstrumentor.instrument_app(app)

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/readiness")
async def readiness():
    qdrant_ok = False
    model_ok = False
    
    try:
        client = get_qdrant_client()
        await client.get_collections()
        qdrant_ok = True
    except Exception as e:
        logger.error("Readiness check Qdrant connection failed", error=str(e))
        
    try:
        embedder = get_embedder()
        model_ok = embedder is not None
    except Exception as e:
        logger.error("Readiness check embedder loading failed", error=str(e))
        
    if qdrant_ok and model_ok:
        return {"status": "ready"}
    else:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not ready",
                "qdrant": "up" if qdrant_ok else "down",
                "model": "loaded" if model_ok else "not loaded"
            }
        )
