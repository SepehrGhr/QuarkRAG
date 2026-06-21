import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from services.embedding.embedders.local import LocalEmbedder
from services.embedding.embedders.openai import OpenAIEmbedder

def test_local_embedder_disabled():
    with pytest.raises(NotImplementedError):
        LocalEmbedder()

@pytest.mark.asyncio
async def test_openai_embedder_dimension():
    with patch("services.embedding.embedders.openai.AsyncOpenAI") as mock_openai, \
         patch("services.embedding.embedders.openai.settings.OPENAI_EMBEDDING_DIMENSION", 1536), \
         patch("services.embedding.embedders.openai.settings.OPENAI_EMBEDDING_MODEL_NAME", "text-embedding-3-small"):
        
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [MagicMock(embedding=[0.2] * 1536)]
        
        async_create = AsyncMock(return_value=mock_response)
        mock_client.embeddings.create = async_create
        mock_openai.return_value = mock_client
        
        embedder = OpenAIEmbedder()
        assert embedder.dimension == 1536
        assert embedder.model_name == "text-embedding-3-small"
        
        embedding = await embedder.embed_text("test")
        assert len(embedding) == 1536
        assert embedding[0] == 0.2
