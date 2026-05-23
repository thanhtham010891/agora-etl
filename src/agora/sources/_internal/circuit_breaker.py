"""
agora/sources/circuit_breaker.py
=================================
``AsyncCircuitBreaker`` — async-native circuit breaker for HTTPSource.

Prevents cascading failures by temporarily blocking requests to a
failing upstream service (OPEN state), then probing (HALF_OPEN),
and recovering (CLOSED) automatically.

States
------
    CLOSED    — normal operation, requests flow through
    OPEN      — failing, requests are rejected immediately
    HALF_OPEN — probing: one request allowed through to test recovery

Usage (standalone)::

    cb = AsyncCircuitBreaker(name="external-api", failure_threshold=5)

    try:
        result = await cb.call(my_async_fn, arg1, arg2)
    except CircuitBreakerOpenError:
        # Handle gracefully — skip item, log, etc.
        ...

Usage (built-in to HTTPSource)::

    class MyExtractor(HTTPSource):
        def __init__(self):
            super().__init__(
                ...,
                circuit_breaker=CircuitBreakerConfig(
                    failure_threshold=5,
                    timeout_seconds=60,
                    success_threshold=2,
                ),
            )
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar

import logstruct

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logstruct.getLogger(__name__)
T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal — requests flow through
    OPEN = "open"  # Failing — requests rejected immediately
    HALF_OPEN = "half_open"  # Probing — one request allowed through


class CircuitBreakerOpenError(Exception):
    """Raised when the circuit breaker is OPEN and rejects a request."""

    def __init__(self, name: str, retry_in: float) -> None:
        super().__init__(f"Circuit breaker '{name}' is OPEN. Retry in {retry_in:.0f}s.")
        self.name = name
        self.retry_in = retry_in


# Backward-compatible alias for existing imports.
CircuitBreakerOpen = CircuitBreakerOpenError


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """Tuning parameters for ``AsyncCircuitBreaker``.

    Attributes
    ----------
    failure_threshold:
        Number of consecutive failures before opening the circuit.
    timeout_seconds:
        How long the circuit stays OPEN before moving to HALF_OPEN.
    success_threshold:
        Number of consecutive successes in HALF_OPEN before closing.
    """

    failure_threshold: int = 5
    timeout_seconds: float = 60.0
    success_threshold: int = 2


class AsyncCircuitBreaker:
    """Async circuit breaker — thread-safe via ``asyncio.Lock``.

    Parameters
    ----------
    name:
        Identifier used in log messages (e.g. ``"external-api"``).
    config:
        Tuning parameters.  Defaults to ``CircuitBreakerConfig()``.
    """

    def __init__(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None,
    ) -> None:
        self.name = name
        self._cfg = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    async def call(self, fn: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T:
        """Call ``fn`` with circuit breaker protection.

        Parameters
        ----------
        fn:
            Async callable to execute.
        *args, **kwargs:
            Forwarded to ``fn``.

        Raises
        ------
        CircuitBreakerOpen
            If the circuit is OPEN and the timeout has not elapsed.
        Exception
            Any exception raised by ``fn`` (also recorded as a failure).
        """
        async with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._set_state(CircuitState.HALF_OPEN)
                else:
                    raise CircuitBreakerOpenError(self.name, self._time_until_retry())

        try:
            result = await fn(*args, **kwargs)
            async with self._lock:
                self._on_success()
            return result
        except CircuitBreakerOpenError:
            raise
        except Exception:
            async with self._lock:
                self._on_failure()
            raise

    async def reset(self) -> None:
        """Manually reset to CLOSED state."""
        async with self._lock:
            logger.info("circuit_breaker_reset", name=self.name)
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def is_open(self) -> bool:
        return self._state == CircuitState.OPEN

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _on_success(self) -> None:
        self._failure_count = 0
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self._cfg.success_threshold:
                self._set_state(CircuitState.CLOSED)

    def _on_failure(self) -> None:
        self._failure_count += 1
        self._success_count = 0
        self._last_failure_time = time.monotonic()

        if self._state == CircuitState.CLOSED:
            if self._failure_count >= self._cfg.failure_threshold:
                self._set_state(CircuitState.OPEN)
        elif self._state == CircuitState.HALF_OPEN:
            self._set_state(CircuitState.OPEN)

    def _should_attempt_reset(self) -> bool:
        if self._last_failure_time is None:
            return True
        return time.monotonic() - self._last_failure_time >= self._cfg.timeout_seconds

    def _time_until_retry(self) -> float:
        if self._last_failure_time is None:
            return 0.0
        elapsed = time.monotonic() - self._last_failure_time
        return max(0.0, self._cfg.timeout_seconds - elapsed)

    def _set_state(self, new_state: CircuitState) -> None:
        if new_state == self._state:
            return
        logger.info(
            "circuit_breaker_state_change",
            name=self.name,
            old=self._state.value,
            new=new_state.value,
            failures=self._failure_count,
        )
        self._state = new_state
        if new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
