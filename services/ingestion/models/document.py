import uuid
import enum
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import UUID
from services.ingestion.database import Base

class DocumentStatus(str, enum.Enum):
    UPLOADED = "uploaded"
    CHUNKING = "chunking"
    EMBEDDING = "embedding"
    READY = "ready"
    FAILED = "failed"

class Document(Base):
    __tablename__ = "documents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    filename = Column(String, nullable=False)
    namespace = Column(String, nullable=False, default="default")
    chunk_count = Column(Integer, nullable=True)
    chunking_strategy = Column(String, nullable=True)
    embedding_provider = Column(String, nullable=True)
    status = Column(SQLEnum(DocumentStatus), nullable=False, default=DocumentStatus.UPLOADED)
    uploaded_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
