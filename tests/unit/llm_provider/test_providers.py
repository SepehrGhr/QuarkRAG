import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from services.llm_provider.providers.openai import OpenAIProvider
from services.llm_provider.providers.ollama import OllamaProvider

@pytest.mark.asyncio
async def test_openai_provider_generates():
    with patch("services.llm_provider.providers.openai.AsyncOpenAI") as mock_openai:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "OpenAI Answer"
        
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)
        mock_openai.return_value = mock_client
        
        provider = OpenAIProvider()
        ans = await provider.generate("question", ["chunk1"])
        assert ans == "OpenAI Answer"

@pytest.mark.asyncio
async def test_ollama_provider_generates():
    with patch("httpx.AsyncClient.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": {"content": "Ollama Answer"}}
        mock_post.return_value = mock_response
        
        provider = OllamaProvider()
        ans = await provider.generate("question", ["chunk1"])
        assert ans == "Ollama Answer"
