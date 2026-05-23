"""
agora/middlewares/ai/classify.py
==================================
``AIClassifyMiddleware`` — assign a category to each record.

Two modes
---------
``use_embeddings=False`` (default, LLM mode):
    Sends the record fields to the LLM with the list of categories.
    More expensive, but understands nuance and context.

``use_embeddings=True`` (embedding mode):
    Embeds both the record text and each category label, then assigns
    the category with the highest cosine similarity.
    Faster and cheaper — no LLM call per record.
    Requires a provider that implements ``embed()``.

Usage::

    # LLM mode
    pipeline.pipe(
        AIClassifyMiddleware(
            provider=GeminiProvider(),
            source_fields=["name", "description"],
            categories=["restaurant", "hotel", "attraction", "cafe", "spa"],
            output_field="ai_category",
        )
    )

    # Embedding mode (cheaper)
    pipeline.pipe(
        AIClassifyMiddleware(
            provider=OpenAIProvider(),
            source_fields=["name", "description"],
            categories=["restaurant", "hotel", "attraction", "cafe", "spa"],
            output_field="ai_category",
            use_embeddings=True,
        )
    )
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Generic, TypeVar

import logstruct

from agora.middlewares.ai.base import AIMiddleware, OnError
from agora.utils.math import cosine_similarity as _cosine_similarity
from agora.utils.records import merge_into_record

if TYPE_CHECKING:
    from agora.ai.cache import LLMCache
    from agora.ai.providers.base import AIProvider
    from agora.core.context import PipelineContext

T = TypeVar("T")

logger = logstruct.getLogger(__name__)


# _cosine_similarity imported from agora.utils.math


class AIClassifyMiddleware(AIMiddleware[T], Generic[T]):
    """Assign one of the given categories to each record.

    Parameters
    ----------
    provider:
        Any ``AIProvider``.
    source_fields:
        Record fields whose values are concatenated to form the input text.
    categories:
        Exhaustive list of valid category labels.
    output_field:
        Name of the field to write the assigned category into.
    use_embeddings:
        ``True`` = use cosine similarity over embeddings (faster, cheaper).
        ``False`` = use LLM completion (slower, smarter).
    confidence_field:
        If set, also write the confidence score (cosine sim or LLM-returned)
        into this field.
    cache / cache_ttl / on_error:
        See ``AIMiddleware``.
    """

    name = "ai_classify"

    def __init__(
        self,
        provider: AIProvider,
        source_fields: list[str],
        categories: list[str],
        *,
        output_field: str = "category",
        use_embeddings: bool = False,
        confidence_field: str | None = None,
        cache: LLMCache | None = None,
        cache_ttl: int = 86_400,
        on_error: OnError = "passthrough",
    ) -> None:
        super().__init__(provider, cache=cache, cache_ttl=cache_ttl, on_error=on_error)
        if not categories:
            raise ValueError("AIClassifyMiddleware requires at least one category")
        if not source_fields:
            raise ValueError("AIClassifyMiddleware requires at least one source_field")
        self._source_fields = source_fields
        self._categories = categories
        self._output_field = output_field
        self._use_embeddings = use_embeddings
        self._confidence_field = confidence_field

        # Pre-computed category embeddings (populated lazily on first process)
        self._category_embeddings: list[list[float]] | None = None
        self._embeddings_lock = asyncio.Lock()  # initialized eagerly to avoid lazy-init race

    # ------------------------------------------------------------------ #
    # Embedding mode helpers                                               #
    # ------------------------------------------------------------------ #

    async def _ensure_category_embeddings(self) -> list[list[float]]:
        async with self._embeddings_lock:
            if self._category_embeddings is None:
                responses = await self._provider.embed_batch(self._categories)
                self._category_embeddings = [r.embedding for r in responses]
                logger.debug(
                    "ai_classify_embeddings_ready",
                    categories=self._categories,
                    dim=len(self._category_embeddings[0]),
                )
        return self._category_embeddings

    async def _classify_with_embeddings(self, text: str) -> tuple[str, float]:
        category_embeddings = await self._ensure_category_embeddings()
        record_response = await self._provider.embed(text)
        record_embedding = record_response.embedding

        similarities = [
            _cosine_similarity(record_embedding, cat_emb) for cat_emb in category_embeddings
        ]
        best_idx = max(range(len(similarities)), key=lambda i: similarities[i])
        return self._categories[best_idx], similarities[best_idx]

    # ------------------------------------------------------------------ #
    # LLM mode helpers                                                     #
    # ------------------------------------------------------------------ #

    async def _classify_with_llm(
        self, text: str, ctx: PipelineContext | None = None
    ) -> tuple[str, float]:
        categories_str = ", ".join(f'"{c}"' for c in self._categories)
        prompt = (
            f"Classify the following into exactly one of these categories: {categories_str}.\n\n"
            f"Text: {text}\n\n"
            f'Return JSON: {{"category": "<chosen>", "confidence": <0.0-1.0>}}'
        )
        response = await self._cached_complete(prompt, temperature=0.0, max_tokens=128, ctx=ctx)
        data = self._parse_json(response.content)
        category = data.get("category", self._categories[0])
        confidence = float(data.get("confidence", 1.0))

        if category not in self._categories:
            logger.warning(
                "ai_classify_unknown_category",
                returned=category,
                valid=self._categories,
            )
            category = self._categories[0]
            confidence = 0.0

        return category, confidence

    # ------------------------------------------------------------------ #
    # Source text builder                                                  #
    # ------------------------------------------------------------------ #

    def _build_text(self, record: T) -> str:
        parts: list[str] = []
        for field in self._source_fields:
            if isinstance(record, dict):
                value = record.get(field, "")
            else:
                value = getattr(record, field, "")
            if value:
                parts.append(str(value))
        return " | ".join(parts)

    # ------------------------------------------------------------------ #
    # process                                                              #
    # ------------------------------------------------------------------ #

    async def process(self, record: T, ctx: PipelineContext) -> T | None:
        try:
            text = self._build_text(record)
            if not text.strip():
                return record

            if self._use_embeddings:
                category, confidence = await self._classify_with_embeddings(text)
            else:
                category, confidence = await self._classify_with_llm(text, ctx=ctx)

            # Track category distribution
            ai_m = ctx.metrics.middleware(self.name).ai
            ai_m.category_counts[category] = ai_m.category_counts.get(category, 0) + 1

            updates: dict = {self._output_field: category}
            if self._confidence_field:
                updates[self._confidence_field] = round(confidence, 4)

            return merge_into_record(record, updates)

        except Exception as exc:
            return await self._handle_error(exc, record, ctx)
