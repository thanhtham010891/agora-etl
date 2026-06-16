from __future__ import annotations

import pytest

from agora.ai.governance import (
    AIBudgetExceeded,
    AIBudgetPolicy,
    AICostCatalog,
    AICostRate,
)
from agora.ai.providers.base import CompletionResponse
from agora.middlewares.ai.base import AIMiddleware


class _CountingProvider:
    model = "governed-model"

    def __init__(
        self,
        *,
        input_tokens: int = 1,
        output_tokens: int = 1,
        response_model: str | None = None,
    ) -> None:
        self.calls = 0
        self._input_tokens = input_tokens
        self._output_tokens = output_tokens
        self._response_model = response_model

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        response_format: object = None,
    ) -> CompletionResponse:
        del prompt, system, temperature, max_tokens, response_format
        self.calls += 1
        return CompletionResponse(
            content="{}",
            model=self._response_model or self.model,
            input_tokens=self._input_tokens,
            output_tokens=self._output_tokens,
        )


class _ProbeMiddleware(AIMiddleware[dict[str, object]]):
    async def complete(self, prompt: str, *, max_tokens: int = 4096) -> CompletionResponse:
        return await self._cached_complete(prompt, max_tokens=max_tokens)

    async def process(
        self,
        record: dict[str, object],
        ctx: object,
    ) -> dict[str, object] | None:
        del ctx
        return record


@pytest.mark.asyncio
async def test_budget_preflight_rejects_before_provider_call() -> None:
    provider = _CountingProvider()
    middleware = _ProbeMiddleware(
        provider,
        budget_policy=AIBudgetPolicy(max_output_tokens=10),
    )

    with pytest.raises(AIBudgetExceeded, match="estimated output_tokens"):
        await middleware.complete("hello", max_tokens=11)

    assert provider.calls == 0


@pytest.mark.asyncio
async def test_budget_postflight_rejects_provider_usage() -> None:
    provider = _CountingProvider(input_tokens=10, output_tokens=1)
    middleware = _ProbeMiddleware(
        provider,
        budget_policy=AIBudgetPolicy(max_input_tokens=5),
    )

    with pytest.raises(AIBudgetExceeded, match="input_tokens=10"):
        await middleware.complete("a", max_tokens=1)

    assert provider.calls == 1


@pytest.mark.asyncio
async def test_cost_guard_requires_catalog_when_enabled() -> None:
    provider = _CountingProvider()
    middleware = _ProbeMiddleware(
        provider,
        budget_policy=AIBudgetPolicy(max_cost_usd=0.01),
    )

    with pytest.raises(AIBudgetExceeded, match="requires an AICostCatalog"):
        await middleware.complete("hello", max_tokens=1)

    assert provider.calls == 0


@pytest.mark.asyncio
async def test_cost_guard_fails_closed_for_missing_model_price() -> None:
    provider = _CountingProvider()
    middleware = _ProbeMiddleware(
        provider,
        budget_policy=AIBudgetPolicy(max_cost_usd=0.01),
        cost_catalog=AICostCatalog(rates={}),
    )

    with pytest.raises(AIBudgetExceeded, match="no rate was found"):
        await middleware.complete("hello", max_tokens=1)

    assert provider.calls == 0


@pytest.mark.asyncio
async def test_cost_guard_uses_catalog_for_preflight_estimate() -> None:
    provider = _CountingProvider()
    middleware = _ProbeMiddleware(
        provider,
        budget_policy=AIBudgetPolicy(max_cost_usd=5.0),
        cost_catalog=AICostCatalog(
            rates={
                "governed-model": AICostRate(
                    input_per_1k_usd=0.0,
                    output_per_1k_usd=10.0,
                )
            }
        ),
    )

    with pytest.raises(AIBudgetExceeded, match="estimated cost_usd"):
        await middleware.complete("hello", max_tokens=1000)

    assert provider.calls == 0


@pytest.mark.asyncio
async def test_run_scoped_budget_accumulates_usage() -> None:
    provider = _CountingProvider(input_tokens=3, output_tokens=2)
    middleware = _ProbeMiddleware(
        provider,
        budget_policy=AIBudgetPolicy(max_total_tokens=9, scope="run"),
    )

    await middleware.complete("a", max_tokens=1)
    with pytest.raises(AIBudgetExceeded, match="total_tokens=10"):
        await middleware.complete("a", max_tokens=1)

    assert provider.calls == 2


@pytest.mark.asyncio
async def test_run_scoped_budget_resets_on_start() -> None:
    provider = _CountingProvider(input_tokens=3, output_tokens=2)
    middleware = _ProbeMiddleware(
        provider,
        budget_policy=AIBudgetPolicy(max_total_tokens=9, scope="run"),
    )

    await middleware.complete("a", max_tokens=1)
    await middleware.on_start(object())  # type: ignore[arg-type]
    await middleware.complete("a", max_tokens=1)

    assert provider.calls == 2
