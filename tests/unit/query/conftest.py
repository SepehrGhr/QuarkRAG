import pytest
from unittest.mock import AsyncMock, patch

@pytest.fixture(autouse=True)
def mock_query_deps():
    import services.query.search.qdrant_search as qdrant_search_mod
    import services.query.embedders as embedders_mod
    qdrant_search_mod._qdrant_client = None
    embedders_mod._embedder = None
    
    with patch("services.query.kafka.producer.kafka_producer.start", new_callable=AsyncMock), \
         patch("services.query.kafka.producer.kafka_producer.stop", new_callable=AsyncMock), \
         patch("services.query.kafka.producer.kafka_producer.send_message", new_callable=AsyncMock), \
         patch("services.query.main.OTLPSpanExporter"):
        yield
        
    qdrant_search_mod._qdrant_client = None
    embedders_mod._embedder = None
