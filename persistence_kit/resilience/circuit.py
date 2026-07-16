from __future__ import annotations

import time
from enum import Enum


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when an operation is short-circuited because the circuit is open."""


class CircuitBreaker:
    """Generic in-process circuit breaker for any external operation.

    Domain-agnostic: it only counts successes and failures. The caller decides
    what counts as a failure (an exception, a 5xx status, a slow response, a DB
    error) and calls :meth:`record_failure` / :meth:`record_success`, gating each
    attempt on :meth:`allow` (or :meth:`guard`).

    After ``failure_threshold`` consecutive failures the circuit opens and calls
    are rejected for ``recovery_timeout`` seconds. It then moves to half-open and
    lets ``half_open_max_calls`` probe calls through: a success closes it, a
    failure re-opens it.

    Not synchronized; intended for a single event loop per breaker instance.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 1,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._half_open_calls = 0

    @property
    def state(self) -> CircuitState:
        return self._state

    def allow(self) -> bool:
        if self._state is CircuitState.OPEN:
            if time.monotonic() - self._opened_at >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
            else:
                return False
        if self._state is CircuitState.HALF_OPEN:
            if self._half_open_calls >= self.half_open_max_calls:
                return False
            self._half_open_calls += 1
        return True

    def guard(self, message: str = "Circuito abierto; operacion corto-circuitada.") -> None:
        if not self.allow():
            raise CircuitOpenError(message)

    def record_success(self) -> None:
        self._failures = 0
        self._half_open_calls = 0
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        if self._state is CircuitState.HALF_OPEN:
            self._trip()
            return
        self._failures += 1
        if self._failures >= self.failure_threshold:
            self._trip()

    def _trip(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        self._half_open_calls = 0
