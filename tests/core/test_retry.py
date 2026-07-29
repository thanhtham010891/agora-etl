from __future__ import annotations

import pytest

from agora.core.failures import AlertSeverity, FailureClassification, FailureDecision
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


@pytest.mark.asyncio
async def test_retry_async_uses_failure_decision_and_preserves_it_on_error() -> None:
    attempts = 0
    decision = FailureDecision(
        classification=FailureClassification.CONNECTIVITY,
        retryable=True,
        dlq_eligible=False,
        alert_severity=AlertSeverity.WARNING,
    )

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("backend unavailable")

    with pytest.raises(RuntimeError) as raised:
        await retry_async(
            operation,
            policy=RetryPolicy[None](
                max_attempts=2,
                initial_backoff_s=0.0,
                failure_classifier=lambda exc: decision,
            ),
        )

    assert attempts == 2
    assert raised.value.failure_decision is decision  # type: ignore[attr-defined]
