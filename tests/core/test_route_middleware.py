from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from agora.core.middleware import RouteMiddleware


class _CountingMiddleware:
    name = "counter"

    def __init__(self) -> None:
        self.start_calls = 0
        self.stop_calls = 0

    async def on_start(self, ctx) -> None:
        del ctx
        self.start_calls += 1

    async def on_stop(self, ctx) -> None:
        del ctx
        self.stop_calls += 1

    async def process(self, record, ctx):
        del ctx
        return record


@pytest.mark.asyncio
async def test_route_middleware_dedupes_shared_lifecycle_instances() -> None:
    shared = _CountingMiddleware()
    middleware = (
        RouteMiddleware(lambda record: record, name="router")
        .route("a", shared)
        .route("b", shared)
        .default(shared)
    )

    await middleware.on_start(MagicMock())
    await middleware.on_stop(MagicMock())

    assert shared.start_calls == 1
    assert shared.stop_calls == 1
