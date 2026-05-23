from __future__ import annotations

import pytest

from agora.core.retry import RetryPolicy, retry_async


@pytest.mark.asyncio
async def test_retry_async_retries_then_succeeds() -> None:
    attempts: list[int] = []
    retry_events: list[tuple[int, float]] = []

    async def operation() -> str:
        attempts.append(len(attempts) + 1)
        if len(attempts) < 3:
            raise RuntimeError("try again")
        return "ok"

    result = await retry_async(
        operation,
        policy=RetryPolicy[str](
            max_attempts=3,
            initial_backoff_s=0.0,
            retry_exceptions=(RuntimeError,),
        ),
        on_retry=lambda attempt, exc, delay: retry_events.append((attempt, delay)),
    )

    assert result == "ok"
    assert attempts == [1, 2, 3]
    assert retry_events == [(1, 0.0), (2, 0.0)]


@pytest.mark.asyncio
async def test_retry_async_does_not_retry_non_retryable_error() -> None:
    attempts = 0

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError("no retry")

    with pytest.raises(ValueError, match="no retry"):
        await retry_async(
            operation,
            policy=RetryPolicy[None](
                max_attempts=3,
                initial_backoff_s=0.0,
                retry_exceptions=(RuntimeError,),
            ),
        )

    assert attempts == 1
