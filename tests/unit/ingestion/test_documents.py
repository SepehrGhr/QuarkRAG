import pytest
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient
from services.ingestion.main import app
from services.ingestion.database import get_db
from services.ingestion.models.document import Document, DocumentStatus
from sqlalchemy.ext.asyncio import AsyncSession

@pytest.fixture
def mock_db():
    session = AsyncMock(spec=AsyncSession)
    
    async def mock_refresh(obj):
        if not getattr(obj, "id", None):
            obj.id = uuid.uuid4()
        if not getattr(obj, "uploaded_at", None):
            obj.uploaded_at = datetime.now(timezone.utc)
        if not getattr(obj, "updated_at", None):
            obj.updated_at = datetime.now(timezone.utc)
            
    session.refresh.side_effect = mock_refresh
    return session

@pytest.fixture
def client(mock_db):
    app.dependency_overrides[get_db] = lambda: mock_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()

def test_upload_document(client, mock_db):
    with patch("services.ingestion.routers.documents.split_text_recursive", return_value=["chunk1", "chunk2"]):
        mock_db.commit = AsyncMock()

        response = client.post(
            "/documents/upload",
            data={"namespace": "test-ns", "chunking_strategy": "recursive"},
            files={"file": ("test.txt", b"Hello testing world!", "text/plain")}
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["filename"] == "test.txt"
        assert data["namespace"] == "test-ns"
        assert data["chunking_strategy"] == "recursive"
        assert data["chunk_count"] == 2
        assert data["status"] == "chunking"

def test_get_document_not_found(client, mock_db):
    mock_db.execute = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute.return_value = mock_result

    doc_id = uuid.uuid4()
    response = client.get(f"/documents/{doc_id}")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

def test_get_document_success(client, mock_db):
    mock_db.execute = AsyncMock()
    mock_result = MagicMock()
    doc_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    mock_doc = Document(
        id=doc_id,
        filename="test.txt",
        namespace="default",
        chunking_strategy="recursive",
        status=DocumentStatus.ready,
        uploaded_at=now,
        updated_at=now
    )
    mock_result.scalar_one_or_none.return_value = mock_doc
    mock_db.execute.return_value = mock_result

    response = client.get(f"/documents/{doc_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == str(doc_id)
    assert data["filename"] == "test.txt"
    assert data["status"] == "ready"

def test_list_documents(client, mock_db):
    mock_db.execute = AsyncMock()
    mock_result = MagicMock()
    now = datetime.now(timezone.utc)
    doc1 = Document(
        id=uuid.uuid4(),
        filename="doc1.txt",
        namespace="ns1",
        chunking_strategy="recursive",
        status=DocumentStatus.ready,
        uploaded_at=now,
        updated_at=now
    )
    doc2 = Document(
        id=uuid.uuid4(),
        filename="doc2.txt",
        namespace="ns1",
        chunking_strategy="recursive",
        status=DocumentStatus.uploaded,
        uploaded_at=now,
        updated_at=now
    )
    
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = [doc1, doc2]
    mock_result.scalars.return_value = mock_scalars
    mock_db.execute.return_value = mock_result

    response = client.get("/documents?namespace=ns1")
    assert response.status_code == 200
    data = response.json()
    assert "documents" in data
    assert len(data["documents"]) == 2
    assert data["documents"][0]["filename"] == "doc1.txt"

def test_delete_document(client, mock_db):
    with patch("services.ingestion.routers.documents.kafka_producer.send_message", new_callable=AsyncMock) as mock_send:
        mock_db.execute = AsyncMock()
        mock_result = MagicMock()
        doc_id = uuid.uuid4()
        now = datetime.now(timezone.utc)
        mock_doc = Document(
            id=doc_id,
            filename="test.txt",
            namespace="default",
            chunking_strategy="recursive",
            status=DocumentStatus.ready,
            uploaded_at=now,
            updated_at=now
        )
        mock_result.scalar_one_or_none.return_value = mock_doc
        mock_db.execute.return_value = mock_result
        mock_db.delete = AsyncMock()
        mock_db.commit = AsyncMock()

        response = client.delete(f"/documents/{doc_id}")
        assert response.status_code == 204
        
        mock_db.delete.assert_called_once_with(mock_doc)
        mock_db.commit.assert_called()
        mock_send.assert_called_once()
        args, kwargs = mock_send.call_args
        assert args[0] == "docs.delete"
