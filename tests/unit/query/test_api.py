import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock

from services.query.main import app

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

def test_query_cache_hit(client):
    with patch("services.query.routers.query.get_cached_answer", new_callable=AsyncMock) as mock_get_cache, \
         patch("services.query.routers.query.kafka_producer.send_message", new_callable=AsyncMock) as mock_send:
        
        mock_get_cache.return_value = "Cached answer"
        
        response = client.post(
            "/query",
            json={"question": "What is life?", "namespace": "default", "top_k": 3}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["answer"] == "Cached answer"
        assert data["cache_hit"] is True
        
        mock_get_cache.assert_called_once_with("What is life?", "default", 3)
        mock_send.assert_called_once()
        args, kwargs = mock_send.call_args
        assert kwargs["value"]["cache_hit"] is True

def test_query_cache_miss_success(client):
    with patch("services.query.routers.query.get_cached_answer", new_callable=AsyncMock) as mock_get_cache, \
         patch("services.query.routers.query.get_embedder") as mock_get_embedder, \
         patch("services.query.routers.query.search_similar_chunks", new_callable=AsyncMock) as mock_search, \
         patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_http_post, \
         patch("services.query.routers.query.set_cached_answer", new_callable=AsyncMock) as mock_set_cache, \
         patch("services.query.routers.query.kafka_producer.send_message", new_callable=AsyncMock) as mock_send:
        
        mock_get_cache.return_value = None
        
        mock_embedder = AsyncMock()
        mock_embedder.embed_text.return_value = [0.1, 0.2]
        mock_get_embedder.return_value = mock_embedder
        
        mock_search.return_value = [{"text": "chunk 1", "score": 0.9}]
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"answer": "LLM generated answer"}
        mock_http_post.return_value = mock_response
        
        response = client.post(
            "/query",
            json={"question": "What is life?", "namespace": "default"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["answer"] == "LLM generated answer"
        assert data["cache_hit"] is False
        
        mock_embedder.embed_text.assert_called_once_with("What is life?")
        mock_search.assert_called_once()
        mock_http_post.assert_called_once()
        mock_set_cache.assert_called_once_with("What is life?", "default", 5, "LLM generated answer")
        mock_send.assert_called_once()

def test_query_llm_failure(client):
    with patch("services.query.routers.query.get_cached_answer", new_callable=AsyncMock) as mock_get_cache, \
         patch("services.query.routers.query.get_embedder") as mock_get_embedder, \
         patch("services.query.routers.query.search_similar_chunks", new_callable=AsyncMock) as mock_search, \
         patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_http_post:
        
        mock_get_cache.return_value = None
        mock_embedder = AsyncMock()
        mock_embedder.embed_text.return_value = [0.1]
        mock_get_embedder.return_value = mock_embedder
        mock_search.return_value = [{"text": "chunk 1"}]
        
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_http_post.return_value = mock_response
        
        response = client.post(
            "/query",
            json={"question": "fail", "namespace": "default"}
        )
        
        assert response.status_code == 502
