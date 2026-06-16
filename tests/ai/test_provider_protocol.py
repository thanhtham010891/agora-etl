"""
tests/ai/test_provider_protocol.py
=====================================
Tests for AI provider capability protocols and response dataclasses.
No LLM API calls — uses a local stub provider.
"""

from __future__ import annotations

import pytest

from agora.ai.providers.base import (
    AIProvider,
    CompletionProvider,
    CompletionResponse,
    EmbeddingProvider,
    EmbeddingResponse,
    require_completion_provider,
    require_embedding_provider,
)

# ======================================================================
# Stub provider — satisfies AIProvider protocol without any real SDK
# ======================================================================


class StubProvider:
    """Minimal provider for testing — returns fixed responses."""

    model: str = "stub-v1"

    async def complete(
        self,
        prompt: str,
        *,
        system=None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        response_format=None,
    ) -> CompletionResponse:
        return CompletionResponse(
            content=f"echo:{prompt}",
            model=self.model,
            input_tokens=len(prompt),
            output_tokens=5,
        )

    async def embed(self, text: str) -> EmbeddingResponse:
        # Return a simple deterministic embedding
        embedding = [float(ord(c)) for c in text[:8]]
        embedding += [0.0] * (8 - len(embedding))
        return EmbeddingResponse(embedding=embedding, model=self.model, input_tokens=len(text))

    async def embed_batch(self, texts: list[str]) -> list[EmbeddingResponse]:
        return [await self.embed(t) for t in texts]


class CompletionOnlyProvider:
    """Minimal completion-only provider for contract tests."""

    model: str = "completion-only-v1"

    async def complete(
        self,
        prompt: str,
        *,
        system=None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        response_format=None,
    ) -> CompletionResponse:
        return CompletionResponse(content=f"done:{prompt}", model=self.model)


class EmbeddingOnlyProvider:
    """Minimal embedding-only provider for contract tests."""

    model: str = "embedding-only-v1"

    async def embed(self, text: str) -> EmbeddingResponse:
        return EmbeddingResponse(embedding=[float(len(text)), 1.0], model=self.model)

    async def embed_batch(self, texts: list[str]) -> list[EmbeddingResponse]:
        return [await self.embed(text) for text in texts]


class DeclaredNoEmbeddingProvider:
    """Provider with legacy embedding methods that explicitly disables embeddings."""

    model: str = "declared-no-embedding-v1"
    supports_embeddings = False

    async def embed(self, text: str) -> EmbeddingResponse:
        return EmbeddingResponse(embedding=[float(len(text))], model=self.model)

    async def embed_batch(self, texts: list[str]) -> list[EmbeddingResponse]:
        return [await self.embed(text) for text in texts]


# ======================================================================
# Protocol compliance
# ======================================================================


class TestAIProviderProtocol:
    def test_stub_satisfies_protocol(self) -> None:
        provider = StubProvider()
        assert isinstance(provider, AIProvider)
        assert isinstance(provider, CompletionProvider)
        assert isinstance(provider, EmbeddingProvider)

    def test_completion_only_provider_satisfies_completion_protocols(self) -> None:
        provider = CompletionOnlyProvider()
        assert isinstance(provider, AIProvider)
        assert isinstance(provider, CompletionProvider)
        assert not isinstance(provider, EmbeddingProvider)

    def test_embedding_only_provider_satisfies_embedding_protocol(self) -> None:
        provider = EmbeddingOnlyProvider()
        assert not isinstance(provider, AIProvider)
        assert not isinstance(provider, CompletionProvider)
        assert isinstance(provider, EmbeddingProvider)

    def test_non_provider_fails_isinstance(self) -> None:
        class NotAProvider:
            pass

        assert not isinstance(NotAProvider(), AIProvider)
        assert not isinstance(NotAProvider(), CompletionProvider)
        assert not isinstance(NotAProvider(), EmbeddingProvider)

    async def test_complete_returns_completion_response(self) -> None:
        provider = StubProvider()
        resp = await provider.complete("hello")
        assert isinstance(resp, CompletionResponse)
        assert resp.content == "echo:hello"
        assert resp.model == "stub-v1"

    async def test_embed_returns_embedding_response(self) -> None:
        provider = StubProvider()
        resp = await provider.embed("test")
        assert isinstance(resp, EmbeddingResponse)
        assert len(resp.embedding) == 8

    async def test_embed_batch_returns_list(self) -> None:
        provider = StubProvider()
        responses = await provider.embed_batch(["a", "b", "c"])
        assert len(responses) == 3
        assert all(isinstance(r, EmbeddingResponse) for r in responses)

    def test_require_completion_provider_accepts_completion_only_provider(self) -> None:
        provider = CompletionOnlyProvider()
        resolved = require_completion_provider(provider, consumer="AIEnrichMiddleware")
        assert resolved is provider

    def test_require_completion_provider_rejects_embedding_only_provider(self) -> None:
        with pytest.raises(
            TypeError,
            match="AIEnrichMiddleware requires a completion-capable provider with complete\\(\\)",
        ):
            require_completion_provider(
                EmbeddingOnlyProvider(),
                consumer="AIEnrichMiddleware",
            )

    def test_require_embedding_provider_accepts_embedding_only_provider(self) -> None:
        provider = EmbeddingOnlyProvider()
        resolved = require_embedding_provider(provider, consumer="EmbeddingStore")
        assert resolved is provider

    def test_require_embedding_provider_rejects_declared_no_embedding_provider(self) -> None:
        with pytest.raises(TypeError, match="supports_embeddings=False"):
            require_embedding_provider(
                DeclaredNoEmbeddingProvider(),
                consumer="EmbeddingStore",
            )

    def test_require_embedding_provider_rejects_completion_only_provider(self) -> None:
        with pytest.raises(
            TypeError,
            match=(
                "EmbeddingStore requires an embedding-capable provider with "
                "embed\\(\\) and embed_batch\\(\\)"
            ),
        ):
            require_embedding_provider(
                CompletionOnlyProvider(),
                consumer="EmbeddingStore",
            )


# ======================================================================
# CompletionResponse
# ======================================================================


class TestCompletionResponse:
    def test_total_tokens(self) -> None:
        resp = CompletionResponse(
            content="hello",
            model="test",
            input_tokens=10,
            output_tokens=5,
        )
        assert resp.total_tokens == 15

    def test_frozen(self) -> None:
        resp = CompletionResponse(content="x", model="y")
        with pytest.raises(AttributeError):
            resp.content = "z"  # type: ignore[misc]

    def test_default_token_counts(self) -> None:
        resp = CompletionResponse(content="x", model="y")
        assert resp.input_tokens == 0
        assert resp.output_tokens == 0
        assert resp.total_tokens == 0


# ======================================================================
# EmbeddingResponse
# ======================================================================


class TestEmbeddingResponse:
    def test_dim_property(self) -> None:
        resp = EmbeddingResponse(embedding=[0.1, 0.2, 0.3], model="test")
        assert resp.dim == 3

    def test_empty_embedding(self) -> None:
        resp = EmbeddingResponse(embedding=[], model="test")
        assert resp.dim == 0

    def test_frozen(self) -> None:
        resp = EmbeddingResponse(embedding=[1.0], model="test")
        with pytest.raises(AttributeError):
            resp.model = "other"  # type: ignore[misc]


def test_completion_only_provider_works_for_completion_middleware() -> None:
    from agora.middlewares.ai.enrich import AIEnrichMiddleware

    middleware = AIEnrichMiddleware(
        provider=CompletionOnlyProvider(),
        prompt_template="hello {name}",
    )

    assert middleware.name == "ai_enrich"


def test_embedding_only_provider_is_rejected_for_completion_middleware() -> None:
    from agora.middlewares.ai.enrich import AIEnrichMiddleware

    with pytest.raises(
        TypeError,
        match="AIEnrichMiddleware requires a completion-capable provider with complete\\(\\)",
    ):
        AIEnrichMiddleware(
            provider=EmbeddingOnlyProvider(),
            prompt_template="hello {name}",
        )


def test_completion_only_provider_works_for_llm_classify_mode() -> None:
    from agora.middlewares.ai.classify import AIClassifyMiddleware

    middleware = AIClassifyMiddleware(
        provider=CompletionOnlyProvider(),
        source_fields=["name"],
        categories=["restaurant", "hotel"],
    )

    assert middleware.name == "ai_classify"


def test_completion_only_provider_is_rejected_for_embedding_classify_mode() -> None:
    from agora.middlewares.ai.classify import AIClassifyMiddleware

    with pytest.raises(
        TypeError,
        match=(
            "AIClassifyMiddleware\\(use_embeddings=True\\) requires an "
            "embedding-capable provider with embed\\(\\) and embed_batch\\(\\)"
        ),
    ):
        AIClassifyMiddleware(
            provider=CompletionOnlyProvider(),
            source_fields=["name"],
            categories=["restaurant", "hotel"],
            use_embeddings=True,
        )


def test_embedding_only_provider_works_for_embedding_classify_mode() -> None:
    from agora.middlewares.ai.classify import AIClassifyMiddleware

    middleware = AIClassifyMiddleware(
        provider=EmbeddingOnlyProvider(),
        source_fields=["name"],
        categories=["restaurant", "hotel"],
        use_embeddings=True,
    )

    assert middleware.name == "ai_classify"


@pytest.mark.asyncio
async def test_embedding_only_provider_works_for_embedding_store() -> None:
    from agora.middlewares.dedup.stores.embedding import EmbeddingStore

    store = EmbeddingStore(provider=EmbeddingOnlyProvider(), similarity_threshold=0.99)
    assert await store.mark_if_new("hello") is True
    assert await store.mark_if_new("hello") is False


def test_completion_only_provider_is_rejected_for_embedding_store() -> None:
    from agora.middlewares.dedup.stores.embedding import EmbeddingStore

    with pytest.raises(
        TypeError,
        match=(
            "EmbeddingStore requires an embedding-capable provider with "
            "embed\\(\\) and embed_batch\\(\\)"
        ),
    ):
        EmbeddingStore(provider=CompletionOnlyProvider())
