"""
tests/sources/test_circuit_breaker.py
=======================================
Tests for AsyncCircuitBreaker state machine.
No external services required.
"""

from __future__ import annotations

import pytest

from agora.sources._internal.circuit_breaker import (
    AsyncCircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpen,
    CircuitState,
)


async def _ok() -> str:
    return "ok"


async def _fail() -> None:
    raise ValueError("boom")


class TestCircuitBreakerStateMachine:
    def _make_cb(
        self, threshold: int = 3, timeout: float = 60.0, success: int = 2
    ) -> AsyncCircuitBreaker:
        return AsyncCircuitBreaker(
            name="test",
            config=CircuitBreakerConfig(
                failure_threshold=threshold,
                timeout_seconds=timeout,
                success_threshold=success,
            ),
        )

    async def test_initial_state_is_closed(self) -> None:
        cb = self._make_cb()
        assert cb.state == CircuitState.CLOSED
        assert not cb.is_open

    async def test_success_stays_closed(self) -> None:
        cb = self._make_cb()
        result = await cb.call(_ok)
        assert result == "ok"
        assert cb.state == CircuitState.CLOSED

    async def test_opens_after_threshold_failures(self) -> None:
        cb = self._make_cb(threshold=3)
        for _ in range(3):
            with pytest.raises(ValueError):
                await cb.call(_fail)
        assert cb.state == CircuitState.OPEN
        assert cb.is_open

    async def test_open_raises_circuit_breaker_open(self) -> None:
        cb = self._make_cb(threshold=1)
        with pytest.raises(ValueError):
            await cb.call(_fail)
        assert cb.is_open
        with pytest.raises(CircuitBreakerOpen):
            await cb.call(_ok)

    async def test_circuit_breaker_open_has_name_and_retry_in(self) -> None:
        cb = self._make_cb(threshold=1)
        with pytest.raises(ValueError):
            await cb.call(_fail)
        try:
            await cb.call(_ok)
        except CircuitBreakerOpen as exc:
            assert exc.name == "test"
            assert exc.retry_in > 0

    async def test_manual_reset_closes_circuit(self) -> None:
        cb = self._make_cb(threshold=1)
        with pytest.raises(ValueError):
            await cb.call(_fail)
        assert cb.is_open
        await cb.reset()
        assert cb.state == CircuitState.CLOSED

    async def test_half_open_after_timeout(self) -> None:
        """After timeout expires, circuit moves to HALF_OPEN on next call attempt."""
        import time

        cb = self._make_cb(threshold=1, timeout=0.0)
        with pytest.raises(ValueError):
            await cb.call(_fail)
        assert cb.is_open
        # timeout=0.0 means it should immediately allow a probe
        # Manually bump last_failure_time to simulate timeout passing
        cb._last_failure_time = time.monotonic() - 999
        # Next call should transition to HALF_OPEN and allow through
        result = await cb.call(_ok)
        assert result == "ok"

    async def test_half_open_success_closes_circuit(self) -> None:
        """Two successes in HALF_OPEN (success_threshold=2) should close circuit."""
        import time

        cb = self._make_cb(threshold=1, timeout=0.0, success=2)
        with pytest.raises(ValueError):
            await cb.call(_fail)
        cb._last_failure_time = time.monotonic() - 999
        # First success — moves to HALF_OPEN, doesn't close yet
        await cb.call(_ok)
        # Second success — should close
        await cb.call(_ok)
        assert cb.state == CircuitState.CLOSED

    async def test_half_open_failure_reopens_circuit(self) -> None:
        import time

        cb = self._make_cb(threshold=1, timeout=0.0)
        with pytest.raises(ValueError):
            await cb.call(_fail)
        cb._last_failure_time = time.monotonic() - 999
        with pytest.raises(ValueError):
            await cb.call(_fail)
        assert cb.state == CircuitState.OPEN
