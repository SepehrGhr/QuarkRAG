from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.sql import text
from services.embedding.config import settings
from services.embedding.logging_config import logger

engine = create_async_engine(settings.DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def update_document_status(document_id: str, status: str, chunk_count: int = None, embedding_provider: str = None):
    async with AsyncSessionLocal() as session:
        try:
            # Update status using raw SQL to decouple from specific ORM setup
            query = "UPDATE documents SET status = :status, updated_at = NOW()"
            params = {"status": status, "document_id": document_id}
            
            if chunk_count is not None:
                query += ", chunk_count = :chunk_count"
                params["chunk_count"] = chunk_count
            if embedding_provider is not None:
                query += ", embedding_provider = :embedding_provider"
                params["embedding_provider"] = embedding_provider
                
            query += " WHERE id = :document_id"
            
            await session.execute(text(query), params)
            await session.commit()
            logger.info("Updated document status in DB", document_id=document_id, status=status)
        except Exception as e:
            await session.rollback()
            logger.exception("Failed to update document status in DB", document_id=document_id, status=status)
            raise
