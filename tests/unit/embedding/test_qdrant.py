import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from services.embedding.qdrant.client import init_qdrant_collection

@pytest.mark.asyncio
async def test_init_qdrant_collection_creates_new():
    mock_client = AsyncMock()
    mock_collections_res = MagicMock()
    mock_collections_res.collections = []
    mock_client.get_collections.return_value = mock_collections_res
    
    mock_embedder = MagicMock()
    mock_embedder.dimension = 384
    mock_embedder.model_name = "all-MiniLM-L6-v2"
    
    with patch("services.embedding.qdrant.client.get_qdrant_client", return_value=mock_client), \
         patch("services.embedding.qdrant.client.get_embedder", return_value=mock_embedder):
        
        await init_qdrant_collection()
        
        mock_client.create_collection.assert_called_once()
        args, kwargs = mock_client.create_collection.call_args
        assert kwargs["metadata"]["embedding_provider"] == "local"

@pytest.mark.asyncio
async def test_init_qdrant_collection_consistency_mismatch():
    mock_client = AsyncMock()
    mock_collections_res = MagicMock()
    mock_collection = MagicMock()
    mock_collection.name = "quarkrag_documents"
    mock_collections_res.collections = [mock_collection]
    mock_client.get_collections.return_value = mock_collections_res
    
    mock_info = MagicMock()
    mock_info.config.metadata = {"embedding_provider": "openai", "model_name": "text-embedding-3-small"}
    mock_client.get_collection.return_value = mock_info
    
    mock_embedder = MagicMock()
    mock_embedder.dimension = 384
    mock_embedder.model_name = "all-MiniLM-L6-v2"
    
    with patch("services.embedding.qdrant.client.get_qdrant_client", return_value=mock_client), \
         patch("services.embedding.qdrant.client.get_embedder", return_value=mock_embedder):
        
        with pytest.raises(RuntimeError) as excinfo:
            await init_qdrant_collection()
        assert "consistency mismatch" in str(excinfo.value)
