"""
agora/middlewares/ai/extract.py
================================
``AIExtractMiddleware`` — extract structured fields from unstructured text.

Reads one field from the record (the "source field"), sends it to the LLM
with per-field extraction instructions, and merges the result back.

Usage::

    pipeline.pipe(
        AIExtractMiddleware(
            provider=GeminiProvider(),
            source_field="raw_description",
            extract={
                "opening_hours": "Extract as dict {day_of_week: hours_string}",
                "price_min_vnd":  "Minimum price in VND as integer, null if not found",
                "price_max_vnd":  "Maximum price in VND as integer, null if not found",
                "amenities":      "List of all mentioned amenities",
                "phone":          "Phone number as string, null if not found",
            },
            cache=SQLiteLLMCache(".cache/extract.db"),
        )
    )

The LLM is instructed to return a single JSON object with the listed keys.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

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

_SYSTEM_EXTRACT = """\
You are a structured data extractor.
Given the input text, extract the requested fields and return a single JSON object.
Return null for any field you cannot find in the text.
Do NOT add extra fields. Do NOT add markdown.
"""


class AIExtractMiddleware(AIMiddleware[T], Generic[T]):
    """Extract structured fields from an unstructured text field.

    Parameters
    ----------
    provider:
        Any completion-capable provider.
    source_field:
        Name of the record field containing the unstructured text.
    extract:
        Mapping of ``output_field_name → extraction_instruction``.
        The instruction tells the LLM what to extract and the expected type.
    system:
        Override default system prompt.  Default: generic extractor prompt.
    max_tokens:
        Max tokens for the LLM response.
    cache / cache_ttl / on_error:
        See ``AIMiddleware``.
    """

    name = "ai_extract"

    def __init__(
        self,
        provider: CompletionProvider,
        source_field: str,
        extract: dict[str, str],
        *,
        system: str | None = None,
        max_tokens: int = 1024,
        cache: LLMCache | None = None,
        cache_ttl: int = 86_400,
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
        if not extract:
            raise ValueError("AIExtractMiddleware requires at least one entry in 'extract'")
        self._source_field = source_field
        self._extract = extract
        self._system = system or _SYSTEM_EXTRACT
        self._max_tokens = max_tokens

    def _build_prompt(self, text: str) -> str:
        fields_spec = "\n".join(
            f'  "{field}": {instruction}' for field, instruction in self._extract.items()
        )
        return (
            f"Extract the following fields from the text below.\n\n"
            f"Fields to extract:\n{fields_spec}\n\n"
            f"Text:\n{text}"
        )

    def _get_source(self, record: T) -> str:
        if isinstance(record, dict):
            value = record.get(self._source_field, "")
        else:
            value = getattr(record, self._source_field, "")
        return str(value) if value is not None else ""

    async def process(self, record: T, ctx: PipelineContext) -> T | None:
        try:
            source_text = self._get_source(record)
            if not source_text.strip():
                logger.debug(
                    "ai_extract_empty_source",
                    field=self._source_field,
                )
                return record

            prompt = self._build_prompt(source_text)
            response = await self._cached_complete(
                prompt,
                system=self._system,
                temperature=0.0,
                max_tokens=self._max_tokens,
                ctx=ctx,
            )

            extracted = self._parse_json(response.content)
            # Keep only expected fields, filter nulls
            cleaned = {k: v for k, v in extracted.items() if k in self._extract}

            return self._merge_into(record, cleaned)

        except Exception as exc:
            return await self._handle_error(exc, record, ctx)

    def _merge_into(self, record: T, fields: dict[str, Any]) -> T:
        return merge_into_record(record, fields)
