"""
tests/ai/test_provider_protocol.py
=====================================
Tests for AIProvider protocol and response dataclasses.
No LLM API calls — uses a local stub provider.
"""

from __future__ import annotations

import pytest

from agora.ai.providers.base import AIProvider, CompletionResponse, EmbeddingResponse

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


# ======================================================================
# Protocol compliance
# ======================================================================


class TestAIProviderProtocol:
    def test_stub_satisfies_protocol(self) -> None:
        provider = StubProvider()
        assert isinstance(provider, AIProvider)

    def test_non_provider_fails_isinstance(self) -> None:
        class NotAProvider:
            pass

        assert not isinstance(NotAProvider(), AIProvider)

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
