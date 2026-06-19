import uuid
from typing import Optional
from fastapi import APIRouter, Depends, UploadFile, File, Form, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from services.ingestion.database import get_db
from services.ingestion.models.document import Document, DocumentStatus
from services.ingestion.schemas.document import DocumentResponse, DocumentListResponse
from services.ingestion.exceptions import DocumentNotFoundException, IngestionException
from services.ingestion.chunking.recursive import split_text_recursive
from services.ingestion.chunking.markdown import split_text_markdown
from services.ingestion.kafka.producer import kafka_producer
from services.ingestion.logging_config import logger

router = APIRouter(prefix="/documents", tags=["documents"])

@router.post("/upload", response_model=DocumentResponse, status_code=201)
async def upload_document(
    file: UploadFile = File(...),
    namespace: str = Form("default"),
    chunking_strategy: str = Form("recursive"),
    db: AsyncSession = Depends(get_db)
):
    logger.info("Received document upload request", filename=file.filename, namespace=namespace, strategy=chunking_strategy)
    
    try:
        content_bytes = await file.read()
        content = content_bytes.decode("utf-8")
    except UnicodeDecodeError as e:
        raise IngestionException(f"Only UTF-8 encoded text files are supported: {str(e)}")
    except Exception as e:
        raise IngestionException(f"Failed to read file: {str(e)}")

    db_doc = Document(
        filename=file.filename,
        namespace=namespace,
        chunking_strategy=chunking_strategy,
        status=DocumentStatus.UPLOADED
    )
    db.add(db_doc)
    await db.commit()
    await db.refresh(db_doc)

    db_doc.status = DocumentStatus.CHUNKING
    await db.commit()

    try:
        if chunking_strategy == "recursive":
            chunks = split_text_recursive(content)
        elif chunking_strategy == "markdown":
            chunks = split_text_markdown(content)
        else:
            db_doc.status = DocumentStatus.FAILED
            await db.commit()
            raise IngestionException(f"Unsupported chunking strategy: {chunking_strategy}")

        db_doc.chunk_count = len(chunks)
        await db.commit()
        await db.refresh(db_doc)
    except Exception as e:
        db_doc.status = DocumentStatus.FAILED
        await db.commit()
        raise IngestionException(f"Chunking failed: {str(e)}")

    try:
        for idx, chunk_text in enumerate(chunks):
            payload = {
                "document_id": str(db_doc.id),
                "chunk_index": idx,
                "text": chunk_text,
                "namespace": namespace,
                "total_chunks": len(chunks)
            }
            await kafka_producer.send_message("docs.raw", key=str(db_doc.id), value=payload)
    except Exception as e:
        db_doc.status = DocumentStatus.FAILED
        await db.commit()
        logger.exception("Failed to publish chunks to Kafka", document_id=str(db_doc.id))
        raise IngestionException(f"Failed to publish chunks to queue: {str(e)}", status_code=500)

    logger.info("Document chunked and published to Kafka", document_id=str(db_doc.id), chunk_count=len(chunks))
    return db_doc

@router.get("", response_model=DocumentListResponse)
async def list_documents(
    namespace: Optional[str] = Query(None),
    status: Optional[DocumentStatus] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    query = select(Document)
    if namespace:
        query = query.filter(Document.namespace == namespace)
    if status:
        query = query.filter(Document.status == status)
    
    result = await db.execute(query)
    documents = result.scalars().all()
    return {"documents": documents}

@router.get("/{document_id}", response_model=DocumentResponse)
async def get_document(
    document_id: uuid.UUID,
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(select(Document).filter(Document.id == document_id))
    doc = result.scalar_one_or_none()
    if not doc:
        raise DocumentNotFoundException(str(document_id))
    return doc

@router.delete("/{document_id}", status_code=204)
async def delete_document(
    document_id: uuid.UUID,
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(select(Document).filter(Document.id == document_id))
    doc = result.scalar_one_or_none()
    if not doc:
        raise DocumentNotFoundException(str(document_id))
    
    namespace = doc.namespace
    await db.delete(doc)
    await db.commit()

    try:
        payload = {
            "document_id": str(document_id),
            "namespace": namespace
        }
        await kafka_producer.send_message("docs.delete", key=str(document_id), value=payload)
        logger.info("Published document deletion request", document_id=str(document_id))
    except Exception as e:
        logger.exception("Failed to publish delete event to Kafka", document_id=str(document_id))

    return None
