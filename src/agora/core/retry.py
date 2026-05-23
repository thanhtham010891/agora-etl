"""agora/core/retry.py
=======================
Shared retry policy and async retry helper for external operations.

This layer is intentionally small:
- ``RetryPolicy`` describes *when* and *how* to retry
- ``retry_async`` executes an async operation under that policy

It is meant for source/sink I/O operations, not record-level middleware
retries (see ``RetryMiddleware`` for that use case).
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy(Generic[T]):
    """Policy describing retry behavior for an async operation."""

    max_attempts: int = 3
    initial_backoff_s: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_s: float = 60.0
    jitter_ratio: float = 0.0
    retry_exceptions: tuple[type[Exception], ...] = ()
    retry_if: Callable[[Exception], bool] | None = None

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.initial_backoff_s < 0:
            raise ValueError("initial_backoff_s must be >= 0")
        if self.backoff_multiplier < 1:
            raise ValueError("backoff_multiplier must be >= 1")
        if self.max_backoff_s < 0:
            raise ValueError("max_backoff_s must be >= 0")
        if self.jitter_ratio < 0:
            raise ValueError("jitter_ratio must be >= 0")

    def should_retry(self, exc: Exception, *, attempt: int) -> bool:
        """Return whether *exc* should be retried after *attempt*."""
        if attempt >= self.max_attempts:
            return False
        if not isinstance(exc, self.retry_exceptions):
            return False
        return self.retry_if(exc) if self.retry_if is not None else True

    def backoff_for(self, *, attempt: int) -> float:
        """Compute the retry delay after *attempt*."""
        delay = min(
            self.initial_backoff_s * (self.backoff_multiplier ** max(attempt - 1, 0)),
            self.max_backoff_s,
        )
        if self.jitter_ratio <= 0 or delay <= 0:
            return delay

        jitter = delay * self.jitter_ratio
        return max(0.0, random.uniform(delay - jitter, delay + jitter))


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy[T],
    on_retry: Callable[[int, Exception, float], Awaitable[None] | None] | None = None,
) -> T:
    """Run *operation* under *policy* until it succeeds or exhausts retries."""

    attempt = 1
    while True:
        try:
            return await operation()
        except Exception as exc:
            if not policy.should_retry(exc, attempt=attempt):
                raise

            delay = policy.backoff_for(attempt=attempt)
            if on_retry is not None:
                maybe_result = on_retry(attempt, exc, delay)
                if asyncio.iscoroutine(maybe_result):
                    await maybe_result
            if delay > 0:
                await asyncio.sleep(delay)
            attempt += 1
