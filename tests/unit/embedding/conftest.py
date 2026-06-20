import pytest
from unittest.mock import AsyncMock, patch

@pytest.fixture(autouse=True)
def mock_embedding_deps():
    import services.embedding.qdrant.client as qdrant_client_mod
    import services.embedding.embedders as embedders_mod
    qdrant_client_mod._qdrant_client = None
    embedders_mod._embedder = None
    
    with patch("services.embedding.kafka.producer.kafka_producer.start", new_callable=AsyncMock), \
         patch("services.embedding.kafka.producer.kafka_producer.stop", new_callable=AsyncMock), \
         patch("services.embedding.kafka.producer.kafka_producer.send_message", new_callable=AsyncMock), \
         patch("services.embedding.main.OTLPSpanExporter"):
        yield
        
    qdrant_client_mod._qdrant_client = None
    embedders_mod._embedder = None
