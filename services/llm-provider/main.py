from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app
from services.llm_provider.config import settings
from services.llm_provider.logging_config import setup_logging, logger
from services.llm_provider.routers import generate

# OTEL Setup
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

setup_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting llm-provider-service")
    
    provider = TracerProvider()
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT, insecure=True))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    
    yield
    logger.info("Stopping llm-provider-service")

app = FastAPI(
    title=settings.SERVICE_NAME,
    lifespan=lifespan
)

# OpenTelemetry Instrumentation
FastAPIInstrumentor.instrument_app(app)

app.include_router(generate.router)

app.mount("/metrics", make_asgi_app())

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception in llm-provider-service", error=str(exc))
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

@app.get("/health")
async def health():
    return {"status": "healthy"}
