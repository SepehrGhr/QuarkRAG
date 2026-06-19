import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from services.embedding.embedders.local import LocalEmbedder
from services.embedding.embedders.openai import OpenAIEmbedder

def test_local_embedder_dimension():
    with patch("services.embedding.embedders.local.SentenceTransformer") as mock_transformer:
        mock_model = MagicMock()
        mock_model.encode.return_value.tolist.return_value = [0.1] * 384
        mock_transformer.return_value = mock_model
        
        embedder = LocalEmbedder()
        assert embedder.dimension == 384
        assert embedder.model_name == "all-MiniLM-L6-v2"

@pytest.mark.asyncio
async def test_openai_embedder_dimension():
    with patch("services.embedding.embedders.openai.AsyncOpenAI") as mock_openai:
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
