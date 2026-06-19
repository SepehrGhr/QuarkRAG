import pytest
import time
import asyncio
from unittest.mock import patch
from services.llm_provider.circuit_breaker.breaker import CircuitBreaker, CircuitBreakerState

@pytest.mark.asyncio
async def test_circuit_breaker_transitions():
    cb = CircuitBreaker(failure_threshold=3, reset_timeout=2)
    
    assert cb.state == CircuitBreakerState.CLOSED
    
    await cb.record_failure()
    assert cb.state == CircuitBreakerState.CLOSED
    await cb.record_failure()
    assert cb.state == CircuitBreakerState.CLOSED
    await cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    
    target = await cb.before_call()
    assert target == "fallback"
    
    await asyncio.sleep(2.1)
    
    assert await cb.get_state() == CircuitBreakerState.HALF_OPEN
    
    await cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    
    await asyncio.sleep(2.1)
    assert await cb.get_state() == CircuitBreakerState.HALF_OPEN
    
    await cb.record_success()
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb.failure_count == 0

@pytest.mark.asyncio
async def test_circuit_breaker_concurrent_probing():
    cb = CircuitBreaker(failure_threshold=1, reset_timeout=1)
    await cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    
    await asyncio.sleep(1.1)
    
    target1 = await cb.before_call()
    assert target1 == "primary"
    assert cb.is_probing is True
    
    target2 = await cb.before_call()
    assert target2 == "fallback"
