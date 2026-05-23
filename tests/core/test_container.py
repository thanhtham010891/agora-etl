from __future__ import annotations

from dataclasses import dataclass

import pytest

from agora.ai import ai_provider_registry
from agora.ai.cache import StateBackendLLMCache
from agora.ai.providers.base import CompletionResponse, EmbeddingResponse
from agora.core.component_factory import config_component_factory
from agora.middlewares.ai.enrich import AIEnrichMiddleware


@dataclass
class _FakeProvider:
    model: str = "fake-model"

    async def complete(self, prompt: str, **kwargs) -> CompletionResponse:
        return CompletionResponse(content=f"ok:{prompt}", model=self.model)

    async def embed(self, text: str) -> EmbeddingResponse:
        return EmbeddingResponse(embedding=[0.0, 1.0], model=self.model)

    async def embed_batch(self, texts: list[str]) -> list[EmbeddingResponse]:
        return [EmbeddingResponse(embedding=[0.0, 1.0], model=self.model) for _ in texts]


@pytest.mark.asyncio
async def test_build_ai_middleware_component_resolves_provider_and_backend_cache(tmp_path) -> None:
    provider_type = "_test_fake_ai_provider_for_container"
    ai_provider_registry.register_factory(provider_type, _FakeProvider)

    middleware = config_component_factory.build_middleware_component(
        {
            "type": "ai_enrich",
            "provider": {"type": provider_type},
            "prompt_template": "hello {name}",
            "cache": {
                "backend": {
                    "type": "sqlite",
                    "path": tmp_path / "ai-cache.db",
                }
            },
        }
    )

    assert isinstance(middleware, AIEnrichMiddleware)
    assert isinstance(middleware._provider, _FakeProvider)
    assert isinstance(middleware._cache, StateBackendLLMCache)

    await middleware._cache.set("k", "v")
    assert await middleware._cache.get("k") == "v"
    await middleware._cache.close()
