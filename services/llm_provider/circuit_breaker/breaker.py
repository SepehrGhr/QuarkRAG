import time
import asyncio
from services.llm_provider.circuit_breaker.state import CircuitBreakerState
from services.llm_provider.logging_config import logger

class CircuitBreaker:
    def __init__(self, failure_threshold=5, reset_timeout=30):
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.last_failure_time = None
        self.reset_timeout = reset_timeout
        self.is_probing = False
        self._lock = asyncio.Lock()

    async def get_state(self) -> CircuitBreakerState:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.reset_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.is_probing = False
                    logger.info("Circuit breaker transitioned to HALF_OPEN (timeout elapsed)")
            return self.state

    async def before_call(self) -> str:
        """Decide call destination: 'primary' or 'fallback'."""
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.reset_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.is_probing = False
                    logger.info("Circuit breaker transitioned to HALF_OPEN (timeout elapsed)")
            
            if self.state == CircuitBreakerState.CLOSED:
                return "primary"
            elif self.state == CircuitBreakerState.HALF_OPEN:
                if not self.is_probing:
                    self.is_probing = True
                    logger.info("Circuit breaker HALF_OPEN: selecting primary for probe call")
                    return "primary"
                else:
                    logger.info("Circuit breaker HALF_OPEN: probe in progress, routing to fallback")
                    return "fallback"
            else:  # OPEN
                return "fallback"

    async def record_success(self):
        async with self._lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                logger.info("Circuit breaker probe succeeded. Transitioning HALF_OPEN -> CLOSED")
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None
            self.is_probing = False

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            self.is_probing = False
            
            if self.state == CircuitBreakerState.CLOSED:
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(
                        "Circuit breaker failure threshold reached. Transitioning CLOSED -> OPEN",
                        failure_count=self.failure_count,
                        threshold=self.failure_threshold
                    )
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                logger.warning("Circuit breaker probe failed. Transitioning HALF_OPEN -> OPEN")
