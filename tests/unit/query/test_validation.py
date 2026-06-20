import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from services.query.startup.embedding_validation import validate_embedding_consistency

@pytest.mark.asyncio
async def test_validate_embedding_consistency_success():
    mock_client = AsyncMock()
    mock_collections_res = MagicMock()
    mock_collection = MagicMock()
    mock_collection.name = "quarkrag_documents"
    mock_collections_res.collections = [mock_collection]
    mock_client.get_collections.return_value = mock_collections_res
    
    mock_info = MagicMock()
    mock_info.config.metadata = {"embedding_provider": "local", "model_name": "all-MiniLM-L6-v2"}
    mock_client.get_collection.return_value = mock_info
    
    mock_embedder = MagicMock()
    mock_embedder.model_name = "all-MiniLM-L6-v2"
    
    with patch("services.query.startup.embedding_validation.AsyncQdrantClient", return_value=mock_client), \
         patch("services.query.startup.embedding_validation.get_embedder", return_value=mock_embedder), \
         patch("services.query.startup.embedding_validation.settings.EMBEDDING_PROVIDER", "local"):
        
        await validate_embedding_consistency()

@pytest.mark.asyncio
async def test_validate_embedding_consistency_mismatch_exits():
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
    mock_embedder.model_name = "all-MiniLM-L6-v2"
    
    with patch("services.query.startup.embedding_validation.AsyncQdrantClient", return_value=mock_client), \
         patch("services.query.startup.embedding_validation.get_embedder", return_value=mock_embedder):
        
        with pytest.raises(SystemExit) as excinfo:
            await validate_embedding_consistency()
        assert excinfo.value.code == 1
