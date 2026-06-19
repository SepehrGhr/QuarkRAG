from pydantic import BaseModel
from datetime import datetime
from uuid import UUID
from typing import Optional
from services.ingestion.models.document import DocumentStatus

class DocumentResponse(BaseModel):
    id: UUID
    filename: str
    namespace: str
    chunk_count: Optional[int] = None
    chunking_strategy: Optional[str] = None
    embedding_provider: Optional[str] = None
    status: DocumentStatus
    uploaded_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class DocumentListResponse(BaseModel):
    documents: list[DocumentResponse]
