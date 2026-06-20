import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, AsyncMock

# Import app inside the test or use a fixture to avoid side effects during import
from services.embedding.main import app

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_readiness_ready(client):
    with patch("services.embedding.main.get_qdrant_client") as mock_get_qdrant, \
         patch("services.embedding.main.get_embedder") as mock_get_embedder:
        
        mock_qdrant = AsyncMock()
        mock_qdrant.get_collections = AsyncMock()
        
        mock_get_qdrant.return_value = mock_qdrant
        mock_get_embedder.return_value = MagicMock()
        
        # We need to run the async test correctly since readiness is async
        # TestClient handles async routes seamlessly
        response = client.get("/readiness")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}

def test_readiness_not_ready(client):
    with patch("services.embedding.main.get_qdrant_client") as mock_get_qdrant, \
         patch("services.embedding.main.get_embedder") as mock_get_embedder:
        
        mock_get_qdrant.side_effect = Exception("Qdrant down")
        mock_get_embedder.side_effect = Exception("Model down")
        
        response = client.get("/readiness")
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "not ready"
        assert data["qdrant"] == "down"
        assert data["model"] == "not loaded"
