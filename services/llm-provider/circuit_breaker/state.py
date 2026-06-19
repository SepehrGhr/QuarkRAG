from enum import Enum

class CircuitBreakerState(Enum):
    CLOSED = 0      # Normal: all calls go to primary provider
    OPEN = 1        # Failing: all calls fail fast or route to fallback
    HALF_OPEN = 2   # Probing: one test call allowed to check recovery
