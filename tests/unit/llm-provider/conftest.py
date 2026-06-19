import pytest
from unittest.mock import AsyncMock, patch

@pytest.fixture(autouse=True)
def mock_llm_deps():
    with patch("services.llm_provider.main.OTLPSpanExporter"):
        yield
