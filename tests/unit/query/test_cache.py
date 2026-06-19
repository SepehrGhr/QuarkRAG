import pytest
from unittest.mock import AsyncMock, patch
from services.query.cache.redis_cache import generate_cache_key, get_cached_answer, set_cached_answer

def test_cache_key_generation():
    key1 = generate_cache_key("test question", "default", 5)
    key2 = generate_cache_key("test question", "default", 5)
    key3 = generate_cache_key("other question", "default", 5)
    
    assert key1 == key2
    assert key1 != key3
    assert len(key1) == 64

@pytest.mark.asyncio
async def test_cache_hit_and_miss():
    mock_redis = AsyncMock()
    mock_redis.get.return_value = "cached response"
    
    with patch("services.query.cache.redis_cache.get_redis_client", return_value=mock_redis):
        ans = await get_cached_answer("q", "ns", 5)
        assert ans == "cached response"
        mock_redis.get.assert_called_once()
        
        mock_redis.get.return_value = None
        ans_miss = await get_cached_answer("q2", "ns", 5)
        assert ans_miss is None

@pytest.mark.asyncio
async def test_cache_set():
    mock_redis = AsyncMock()
    
    with patch("services.query.cache.redis_cache.get_redis_client", return_value=mock_redis):
        await set_cached_answer("q", "ns", 5, "answer", ttl=60)
        mock_redis.setex.assert_called_once()
