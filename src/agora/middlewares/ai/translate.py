"""
agora/middlewares/ai/translate.py
===================================
``AITranslateMiddleware`` — translate record fields into a target language.

Supports single-record and batch translation.  Batch mode amortises the
per-call overhead when multiple records share the same translation needs.

Usage::

    pipeline.pipe(
        AITranslateMiddleware(
            provider=GeminiProvider(),
            fields=["name", "description"],
            target_lang="vi",            # Vietnamese
            source_lang="auto",          # auto-detect
            output_prefix="vi_",         # write to vi_name, vi_description
            cache=SQLiteLLMCache(".cache/translate.db"),
        )
    )

    # In-place (overwrite original fields)
    AITranslateMiddleware(
        provider=GeminiProvider(),
        fields=["description"],
        target_lang="vi",
        output_prefix="",               # default: overwrite
    )

Field naming
------------
- ``output_prefix=""`` (default): translated value overwrites the source field.
- ``output_prefix="vi_"``: translated value is written to a new field
  (e.g. ``vi_description``), keeping the original intact.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Generic, TypeVar

import logstruct

from agora.middlewares.ai.base import AIMiddleware, OnError
from agora.utils.records import merge_into_record

if TYPE_CHECKING:
    from agora.ai.cache import LLMCache
    from agora.ai.governance import AIBudgetPolicy, AICostCatalog
    from agora.ai.providers.base import CompletionProvider
    from agora.core.context import PipelineContext

T = TypeVar("T")

logger = logstruct.getLogger(__name__)


class AITranslateMiddleware(AIMiddleware[T], Generic[T]):
    """Translate specified record fields to a target language.

    Parameters
    ----------
    provider:
        Any completion-capable provider.
    fields:
        Names of the fields to translate.
    target_lang:
        Target language code or name (e.g. ``"vi"``, ``"Vietnamese"``,
        ``"en"``, ``"French"``).
    source_lang:
        Source language.  ``"auto"`` = detect automatically (default).
    output_prefix:
        Prefix for translated field names.  Default ``""`` (overwrite).
    cache / cache_ttl / on_error:
        See ``AIMiddleware``.
    """

    name = "ai_translate"

    def __init__(
        self,
        provider: CompletionProvider,
        fields: list[str],
        target_lang: str,
        *,
        source_lang: str = "auto",
        output_prefix: str = "",
        max_tokens: int = 2048,
        cache: LLMCache | None = None,
        cache_ttl: int = 86_400 * 7,  # translations are stable — cache for 7 days
        on_error: OnError = OnError.PASSTHROUGH,
        budget_policy: AIBudgetPolicy | None = None,
        cost_catalog: AICostCatalog | None = None,
    ) -> None:
        super().__init__(
            provider,
            cache=cache,
            cache_ttl=cache_ttl,
            on_error=on_error,
            budget_policy=budget_policy,
            cost_catalog=cost_catalog,
        )
        if not fields:
            raise ValueError("AITranslateMiddleware requires at least one field")
        self._fields = fields
        self._target_lang = target_lang
        self._source_lang = source_lang
        self._output_prefix = output_prefix
        self._max_tokens = max_tokens

    def _get_field_value(self, record: T, field: str) -> str:
        if isinstance(record, dict):
            return str(record.get(field, "") or "")
        return str(getattr(record, field, "") or "")

    def _build_prompt(self, texts: dict[str, str]) -> str:
        """Build a single prompt that translates all fields at once."""
        source_note = f"from {self._source_lang} " if self._source_lang != "auto" else ""
        fields_json = json.dumps(texts, ensure_ascii=False)
        return (
            f"Translate the following JSON fields {source_note}to {self._target_lang}.\n"
            f"Return a JSON object with the same keys and translated values.\n"
            f"Do NOT translate field names, only values. Do NOT add markdown.\n\n"
            f"Input: {fields_json}"
        )

    async def process(self, record: T, ctx: PipelineContext) -> T | None:
        try:
            # Gather fields with non-empty values
            to_translate = {
                field: value
                for field in self._fields
                if (value := self._get_field_value(record, field))
            }

            if not to_translate:
                return record

            prompt = self._build_prompt(to_translate)
            response = await self._cached_complete(
                prompt,
                temperature=0.1,  # slight creativity for natural translation
                max_tokens=self._max_tokens,
                ctx=ctx,
            )

            translated = self._parse_json(response.content)

            # Build output field mapping
            updates = {
                f"{self._output_prefix}{field}": translated[field]
                for field in to_translate
                if field in translated
            }

            return merge_into_record(record, updates)

        except Exception as exc:
            return await self._handle_error(exc, record, ctx)
