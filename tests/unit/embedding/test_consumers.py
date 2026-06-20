import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from services.embedding.consumer.docs_raw import process_message_with_retry, handle_failed_message
from services.embedding.consumer.docs_delete import run_delete_consumer

@pytest.mark.asyncio
async def test_process_message_first_chunk():
    msg = {
        "document_id": "doc123",
        "chunk_index": 0,
        "text": "test content",
        "namespace": "default",
        "total_chunks": 2
    }
    
    mock_redis = AsyncMock()
    mock_redis.set.return_value = True  # is_first = True
    mock_redis.incr.return_value = 1    # processed_count = 1
    
    with patch("services.embedding.consumer.docs_raw.get_redis", return_value=mock_redis), \
         patch("services.embedding.consumer.docs_raw.update_document_status") as mock_update_status, \
         patch("services.embedding.consumer.docs_raw.get_embedder") as mock_get_embedder, \
         patch("services.embedding.consumer.docs_raw.upsert_vector") as mock_upsert, \
         patch("services.embedding.consumer.docs_raw.kafka_producer.send_message") as mock_send:
        
        mock_embedder = AsyncMock()
        mock_embedder.embed_text.return_value = [0.1, 0.2]
        mock_get_embedder.return_value = mock_embedder
        
        await process_message_with_retry(msg)
        
        mock_update_status.assert_called_once()
        mock_upsert.assert_called_once_with(
            document_id="doc123",
            chunk_index=0,
            text="test content",
            vector=[0.1, 0.2],
            namespace="default"
        )
        mock_send.assert_not_called()

@pytest.mark.asyncio
async def test_process_message_last_chunk():
    msg = {
        "document_id": "doc123",
        "chunk_index": 1,
        "text": "test content 2",
        "namespace": "default",
        "total_chunks": 2
    }
    
    mock_redis = AsyncMock()
    mock_redis.set.return_value = False  # is_first = False
    mock_redis.incr.return_value = 2     # processed_count = 2 (last chunk)
    
    with patch("services.embedding.consumer.docs_raw.get_redis", return_value=mock_redis), \
         patch("services.embedding.consumer.docs_raw.update_document_status") as mock_update_status, \
         patch("services.embedding.consumer.docs_raw.get_embedder") as mock_get_embedder, \
         patch("services.embedding.consumer.docs_raw.upsert_vector") as mock_upsert, \
         patch("services.embedding.consumer.docs_raw.kafka_producer.send_message") as mock_send:
        
        mock_embedder = AsyncMock()
        mock_embedder.embed_text.return_value = [0.3, 0.4]
        mock_get_embedder.return_value = mock_embedder
        
        await process_message_with_retry(msg)
        
        # update_status should be called with "ready" because it's the last chunk
        mock_update_status.assert_called_once_with(document_id="doc123", status="ready")
        mock_upsert.assert_called_once()
        mock_send.assert_called_once()

@pytest.mark.asyncio
async def test_handle_failed_message():
    msg = {"document_id": "doc123", "text": "foo"}
    err = Exception("Something went wrong")
    
    with patch("services.embedding.consumer.docs_raw.update_document_status") as mock_update, \
         patch("services.embedding.consumer.docs_raw.kafka_producer.send_message") as mock_send:
         
        await handle_failed_message(msg, err)
        
        mock_update.assert_called_once_with(document_id="doc123", status="failed")
        mock_send.assert_called_once()

@pytest.mark.asyncio
async def test_run_delete_consumer_processes_message():
    # Mock AIOKafkaConsumer acting as an async iterator
    mock_consumer = AsyncMock()
    mock_msg = MagicMock()
    mock_msg.value = {"document_id": "doc123", "namespace": "default"}
    
    mock_consumer.__aiter__.return_value = [mock_msg]
    
    with patch("services.embedding.consumer.docs_delete.AIOKafkaConsumer", return_value=mock_consumer), \
         patch("services.embedding.consumer.docs_delete.delete_vectors_by_document") as mock_delete:
        
        await run_delete_consumer()
        
        mock_consumer.start.assert_called_once()
        mock_delete.assert_called_once_with("doc123", "default")
        mock_consumer.stop.assert_called_once()
