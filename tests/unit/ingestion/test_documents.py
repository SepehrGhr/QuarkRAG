import pytest
import uuid
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient
from services.ingestion.main import app
from services.ingestion.database import get_db
from services.ingestion.models.document import Document, DocumentStatus
from sqlalchemy.ext.asyncio import AsyncSession

@pytest.fixture
def mock_db():
    session = AsyncMock(spec=AsyncSession)
    return session

@pytest.fixture
def client(mock_db):
    app.dependency_overrides[get_db] = lambda: mock_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()

def test_upload_document(client, mock_db):
    with patch("services.ingestion.routers.documents.split_text_recursive", return_value=["chunk1", "chunk2"]) as mock_split:
        mock_db.commit = AsyncMock()
        mock_db.refresh = AsyncMock()

        # Mocking the Document database model initialization and attributes
        # Since we pass it to database, mock_db.add is called.
        # We need mock_db.commit and refresh to execute without error.
        
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
    # Mock result to return None
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute.return_value = mock_result

    doc_id = uuid.uuid4()
    response = client.get(f"/documents/{doc_id}")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]
