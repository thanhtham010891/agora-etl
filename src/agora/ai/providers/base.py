"""
agora/ai/providers/base.py
===========================
``AIProvider`` — structural protocol for all LLM/embedding providers.

Design decisions
----------------
- Protocol (not ABC): providers can be implemented externally without
  subclassing agora. Duck-typing works: any object with ``complete`` and
  ``embed`` methods is a valid provider.
- ``CompletionResponse`` / ``EmbeddingResponse`` are plain dataclasses —
  no framework lock-in, easy to mock in tests.
- Temperature defaults to 0.0: ETL enrichment requires deterministic,
  reproducible outputs. Users can override per-call.
- ``response_format``: pass a Pydantic model to request structured JSON
  output. Providers that support JSON mode (Gemini, OpenAI) will use it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

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
# AIProvider protocol
# ======================================================================


@runtime_checkable
class AIProvider(Protocol):
    """Structural protocol for LLM/embedding providers.

    Providers must implement both completion and embedding operations.
    Implementations live in sub-modules (gemini, openai, anthropic).

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

    Parameters accepted by ``embed``
    ---------------------------------
    text:
        Text to embed.  Chunking is the caller's responsibility.
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

    async def embed(self, text: str) -> EmbeddingResponse:
        """Embed a single text string into a vector."""
        ...

    async def embed_batch(self, texts: list[str]) -> list[EmbeddingResponse]:
        """Embed multiple strings.  Implementations should batch API calls."""
        ...
