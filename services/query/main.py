from contextlib import asynccontextmanager
from fastapi import FastAPI
from prometheus_client import make_asgi_app
from services.query.config import settings
from services.query.logging_config import setup_logging, logger
from services.query.startup.embedding_validation import validate_embedding_consistency
from services.query.embedders import get_embedder
from services.query.kafka.producer import kafka_producer
from services.query.routers import query

# OTEL Setup
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

setup_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting query-service")
    
    provider = TracerProvider()
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT, insecure=True))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    
    get_embedder()
    
    await validate_embedding_consistency()
    
    await kafka_producer.start()
    
    yield
    logger.info("Stopping query-service")
    await kafka_producer.stop()

app = FastAPI(
    title=settings.SERVICE_NAME,
    lifespan=lifespan
)

FastAPIInstrumentor.instrument_app(app)

app.include_router(query.router)

app.mount("/metrics", make_asgi_app())

@app.get("/health")
async def health():
    return {"status": "healthy"}
