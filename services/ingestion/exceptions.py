from fastapi import Request, FastAPI
from fastapi.responses import JSONResponse
from services.ingestion.logging_config import logger

class IngestionException(Exception):
    def __init__(self, message: str, status_code: int = 400):
        self.message = message
        self.status_code = status_code
        super().__init__(message)

class DocumentNotFoundException(IngestionException):
    def __init__(self, document_id: str):
        super().__init__(f"Document with ID {document_id} not found", status_code=404)

def setup_exception_handlers(app: FastAPI):
    @app.exception_handler(IngestionException)
    async def ingestion_exception_handler(request: Request, exc: IngestionException):
        logger.error("Ingestion error occurred", error=exc.message, path=request.url.path)
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.message}
        )

    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception):
        logger.exception("Unhandled exception occurred", error=str(exc), path=request.url.path)
        return JSONResponse(
            status_code=500,
            content={"detail": "An unexpected error occurred."}
        )
