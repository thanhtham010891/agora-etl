"""
agora/middlewares/ai/enrich.py
===============================
``AIEnrichMiddleware`` — enrich records with LLM-generated fields.

Calls the provider once per record and merges the JSON response back
into the record.  Works with dicts, Pydantic models, and dataclasses.

Usage::

    pipeline.pipe(
        AIEnrichMiddleware(
            provider=GeminiProvider(),
            prompt_template=\"\"\"
            POI data: {name}, {address}, {category}
            Task: Return JSON with:
            - summary: 1-sentence Vietnamese description
            - tags: list[str] of category tags
            - price_level: "budget" | "mid" | "premium"
            \"\"\",
            output_fields=["summary", "tags", "price_level"],
            cache=SQLiteLLMCache(".cache/enrich.db"),
        )
    )

Prompt template
---------------
Uses ``str.format_map`` — reference any field on the record by name:
``{name}``, ``{address}``, ``{rating}`` etc.

For records that are dicts, all keys are available.
For Pydantic models, all fields from ``model_dump()`` are available.

Output merging
--------------
When ``output_fields`` is set, only those fields are merged.
When ``output_fields=None``, ALL keys from the LLM response are merged.

Records are never mutated — a shallow copy is made before merging.
Pydantic records are re-validated after enrichment if ``revalidate=True``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

import logstruct

from agora.middlewares.ai.base import AIMiddleware, OnError
from agora.utils.records import merge_into_record

if TYPE_CHECKING:
    from agora.ai.cache import LLMCache
    from agora.ai.providers.base import AIProvider
    from agora.core.context import PipelineContext

T = TypeVar("T")

logger = logstruct.getLogger(__name__)


class AIEnrichMiddleware(AIMiddleware[T], Generic[T]):
    """Enrich records with LLM-generated fields.

    Parameters
    ----------
    provider:
        Any ``AIProvider`` (Gemini, OpenAI, Anthropic).
    prompt_template:
        Prompt string with ``{field_name}`` placeholders referencing
        record fields.  The LLM must return a JSON object.
    output_fields:
        Whitelist of keys to extract from the LLM JSON response.
        ``None`` = merge all keys returned by the LLM.
    system:
        Optional system instruction (output format, language, constraints).
    temperature:
        Sampling temperature.  Default 0.0 for deterministic enrichment.
    max_tokens:
        Max tokens for LLM response.
    revalidate:
        Re-run Pydantic validation after enrichment (for model records).
    cache:
        Optional ``LLMCache`` to skip repeated calls on identical records.
    cache_ttl:
        Cache entry lifetime in seconds.
    on_error:
        ``"passthrough"`` — return original on failure (default).
        ``"drop"`` — drop the record.
        ``"raise"`` — re-raise the exception.
    """

    name = "ai_enrich"

    def __init__(
        self,
        provider: AIProvider,
        prompt_template: str,
        *,
        output_fields: list[str] | None = None,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 1024,
        revalidate: bool = False,
        cache: LLMCache | None = None,
        cache_ttl: int = 86_400,
        on_error: OnError = "passthrough",
    ) -> None:
        super().__init__(provider, cache=cache, cache_ttl=cache_ttl, on_error=on_error)
        self._prompt_template = prompt_template
        self._output_fields = output_fields
        self._system = system
        self._temperature = temperature
        self._max_tokens = max_tokens
        self._revalidate = revalidate

    async def process(self, record: T, ctx: PipelineContext) -> T | None:
        try:
            prompt = self._render_prompt(self._prompt_template, record)
            response = await self._cached_complete(
                prompt,
                system=self._system,
                temperature=self._temperature,
                max_tokens=self._max_tokens,
                ctx=ctx,
            )
            fields = self._parse_json(response.content)

            if self._output_fields is not None:
                fields = {k: v for k, v in fields.items() if k in self._output_fields}

            return self._merge(record, fields)

        except Exception as exc:
            return await self._handle_error(exc, record, ctx)

    def _merge(self, record: T, fields: dict) -> T:
        """Merge *fields* into *record* without mutation."""
        return merge_into_record(record, fields)
