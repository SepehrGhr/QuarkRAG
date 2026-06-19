from contextlib import asynccontextmanager
from fastapi import FastAPI
from services.ingestion.config import settings
from services.ingestion.logging_config import setup_logging, logger
from services.ingestion.database import engine
from services.ingestion.kafka.producer import kafka_producer
from services.ingestion.routers import documents
from services.ingestion.exceptions import setup_exception_handlers

# Setup OTEL
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

# Initialize logging before FastAPI
setup_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting ingestion-service")
    
    # Setup OpenTelemetry
    provider = TracerProvider()
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT, insecure=True))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    
    # Start Kafka Producer
    await kafka_producer.start()
    
    yield
    logger.info("Stopping ingestion-service")
    await kafka_producer.stop()
    await engine.dispose()

app = FastAPI(
    title=settings.SERVICE_NAME,
    lifespan=lifespan
)

setup_exception_handlers(app)

app.include_router(documents.router)

# OpenTelemetry Instrumentation
FastAPIInstrumentor.instrument_app(app)

@app.get("/health")
async def health():
    db_ok = False
    kafka_ok = False
    
    try:
        from sqlalchemy.sql import text
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception as e:
        logger.error("Health check database connection failed", error=str(e))
        
    if kafka_producer.producer is not None:
        kafka_ok = True
        
    status = "healthy" if db_ok and kafka_ok else "unhealthy"
    
    return {
        "status": status,
        "database": "up" if db_ok else "down",
        "kafka": "up" if kafka_ok else "down"
    }
