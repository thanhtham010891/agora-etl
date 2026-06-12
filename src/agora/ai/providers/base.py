"""
agora/ai/providers/base.py
===========================
Provider protocols for Agora AI integrations.

Design decisions
----------------
- Protocols (not ABCs): providers can be implemented externally without
  subclassing agora. Duck-typing works as long as the required capability
  methods are present.
- ``CompletionResponse`` / ``EmbeddingResponse`` are plain dataclasses —
  no framework lock-in, easy to mock in tests.
- Temperature defaults to 0.0: ETL enrichment requires deterministic,
  reproducible outputs. Users can override per-call.
- ``response_format``: pass a Pydantic model to request structured JSON
  output. Providers that support JSON mode (Gemini, OpenAI) will use it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

if TYPE_CHECKING:
    from pydantic import BaseModel


# ======================================================================
# Response value objects
# ======================================================================


@dataclass(frozen=True, slots=True)
class CompletionResponse:
    """LLM text completion result."""

    content: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens


@dataclass(frozen=True, slots=True)
class EmbeddingResponse:
    """Vector embedding result."""

    embedding: list[float]
    model: str
    input_tokens: int = 0

    @property
    def dim(self) -> int:
        return len(self.embedding)


# ======================================================================
# Provider capability protocols
# ======================================================================


@runtime_checkable
class CompletionProvider(Protocol):
    """Structural protocol for completion-capable providers.

    Completion-only providers are valid here. This is the capability required
    by the LLM-style AI middlewares.

    Parameters accepted by ``complete``
    ------------------------------------
    prompt:
        User-facing message / task description.
    system:
        Optional system instruction (role, output format, constraints).
    temperature:
        Sampling temperature. Default 0.0 for deterministic ETL output.
    max_tokens:
        Hard limit on output length.
    response_format:
        Pydantic model class → provider will return JSON matching schema.
    """

    @property
    def model(self) -> str:
        """Canonical model identifier (e.g. 'gemini-2.0-flash')."""
        ...

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        response_format: type[BaseModel] | None = None,
    ) -> CompletionResponse:
        """Run a single completion and return structured result."""
        ...


@runtime_checkable
class EmbeddingProvider(Protocol):
    """Structural protocol for embedding-capable providers.

    This is the capability required by embedding-based classification and
    semantic dedup stores.

    Parameters accepted by ``embed``
    ---------------------------------
    text:
        Text to embed. Chunking is the caller's responsibility.
    """

    @property
    def model(self) -> str:
        """Canonical model identifier for embedding calls."""
        ...

    async def embed(self, text: str) -> EmbeddingResponse:
        """Embed a single text string into a vector."""
        ...

    async def embed_batch(self, texts: list[str]) -> list[EmbeddingResponse]:
        """Embed multiple strings.  Implementations should batch API calls."""
        ...


@runtime_checkable
class AIProvider(CompletionProvider, Protocol):
    """Backward-compatible alias for completion-capable AI providers.

    Historically ``AIProvider`` implied both completion and embedding support.
    In `0.3.x`, Agora treats completion and embedding as separate capabilities.
    ``AIProvider`` remains as the completion-facing compatibility name used by
    the existing AI middleware surface.
    """


def _describe_provider_capabilities(provider: object) -> str:
    capabilities: list[str] = []
    if hasattr(provider, "complete"):
        capabilities.append("complete()")
    if hasattr(provider, "embed"):
        capabilities.append("embed()")
    if hasattr(provider, "embed_batch"):
        capabilities.append("embed_batch()")
    if not capabilities:
        return "no recognized AI capability methods"
    return ", ".join(capabilities)


def require_completion_provider(
    provider: object,
    *,
    consumer: str,
) -> CompletionProvider:
    """Return *provider* as a completion provider or raise a clear error."""
    if hasattr(provider, "complete"):
        return cast("CompletionProvider", provider)
    capability_summary = _describe_provider_capabilities(provider)
    raise TypeError(
        f"{consumer} requires a completion-capable provider with complete(). "
        f"Got {type(provider).__name__} exposing {capability_summary}."
    )


def require_embedding_provider(
    provider: object,
    *,
    consumer: str,
) -> EmbeddingProvider:
    """Return *provider* as an embedding provider or raise a clear error."""
    if hasattr(provider, "embed") and hasattr(provider, "embed_batch"):
        return cast("EmbeddingProvider", provider)
    capability_summary = _describe_provider_capabilities(provider)
    raise TypeError(
        f"{consumer} requires an embedding-capable provider with embed() and "
        f"embed_batch(). Got {type(provider).__name__} exposing {capability_summary}."
    )
