import pytest
from unittest.mock import AsyncMock, patch

@pytest.fixture(autouse=True)
def mock_external_services():
    with patch("services.ingestion.kafka.producer.kafka_producer.start", new_callable=AsyncMock) as mock_start, \
         patch("services.ingestion.kafka.producer.kafka_producer.stop", new_callable=AsyncMock) as mock_stop, \
         patch("services.ingestion.kafka.producer.kafka_producer.send_message", new_callable=AsyncMock) as mock_send, \
         patch("services.ingestion.main.OTLPSpanExporter"):
        yield {
            "start": mock_start,
            "stop": mock_stop,
            "send": mock_send
        }
