import pytest
from unittest.mock import AsyncMock, patch

@pytest.fixture(autouse=True)
def mock_query_deps():
    with patch("services.query.kafka.producer.kafka_producer.start", new_callable=AsyncMock), \
         patch("services.query.kafka.producer.kafka_producer.stop", new_callable=AsyncMock), \
         patch("services.query.kafka.producer.kafka_producer.send_message", new_callable=AsyncMock), \
         patch("services.query.main.OTLPSpanExporter"):
        yield
