import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

from services.llm_provider.main import app

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

def test_generate_primary_success(client):
    with patch("services.llm_provider.routers.generate.openai_provider.generate", new_callable=AsyncMock) as mock_openai, \
         patch("services.llm_provider.routers.generate.ollama_provider.generate", new_callable=AsyncMock) as mock_ollama, \
         patch("services.llm_provider.routers.generate.breaker.before_call", new_callable=AsyncMock) as mock_breaker_before, \
         patch("services.llm_provider.routers.generate.breaker.record_success", new_callable=AsyncMock) as mock_breaker_success:
        
        mock_breaker_before.return_value = "primary"
        mock_openai.return_value = "Primary answer"
        
        response = client.post(
            "/generate",
            json={"question": "Test?", "context": ["ctx"]}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["answer"] == "Primary answer"
        assert data["provider"] == "openai"
        mock_openai.assert_called_once_with("Test?", ["ctx"])
        mock_breaker_success.assert_called_once()
        mock_ollama.assert_not_called()

def test_generate_primary_failure_fallback_success(client):
    with patch("services.llm_provider.routers.generate.openai_provider.generate", new_callable=AsyncMock) as mock_openai, \
         patch("services.llm_provider.routers.generate.ollama_provider.generate", new_callable=AsyncMock) as mock_ollama, \
         patch("services.llm_provider.routers.generate.breaker.before_call", new_callable=AsyncMock) as mock_breaker_before, \
         patch("services.llm_provider.routers.generate.breaker.record_failure", new_callable=AsyncMock) as mock_breaker_failure:
        
        mock_breaker_before.return_value = "primary"
        mock_openai.side_effect = Exception("OpenAI down")
        mock_ollama.return_value = "Fallback answer"
        
        response = client.post(
            "/generate",
            json={"question": "Test?", "context": ["ctx"]}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["answer"] == "Fallback answer"
        assert data["provider"] == "ollama"
        mock_openai.assert_called_once_with("Test?", ["ctx"])
        mock_breaker_failure.assert_called_once()
        mock_ollama.assert_called_once_with("Test?", ["ctx"])

def test_generate_circuit_breaker_open(client):
    with patch("services.llm_provider.routers.generate.openai_provider.generate", new_callable=AsyncMock) as mock_openai, \
         patch("services.llm_provider.routers.generate.ollama_provider.generate", new_callable=AsyncMock) as mock_ollama, \
         patch("services.llm_provider.routers.generate.breaker.before_call", new_callable=AsyncMock) as mock_breaker_before:
        
        mock_breaker_before.return_value = "fallback"
        mock_ollama.return_value = "Fallback only answer"
        
        response = client.post(
            "/generate",
            json={"question": "Test?", "context": ["ctx"]}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["answer"] == "Fallback only answer"
        assert data["provider"] == "ollama"
        mock_openai.assert_not_called()
        mock_ollama.assert_called_once_with("Test?", ["ctx"])

def test_generate_all_providers_fail(client):
    with patch("services.llm_provider.routers.generate.openai_provider.generate", new_callable=AsyncMock) as mock_openai, \
         patch("services.llm_provider.routers.generate.ollama_provider.generate", new_callable=AsyncMock) as mock_ollama, \
         patch("services.llm_provider.routers.generate.breaker.before_call", new_callable=AsyncMock) as mock_breaker_before:
        
        mock_breaker_before.return_value = "primary"
        mock_openai.side_effect = Exception("OpenAI down")
        mock_ollama.side_effect = Exception("Ollama down")
        
        response = client.post(
            "/generate",
            json={"question": "Test?", "context": ["ctx"]}
        )
        
        assert response.status_code == 502
