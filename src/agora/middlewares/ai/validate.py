"""
agora/middlewares/ai/validate.py
==================================
``AIValidateMiddleware`` — LLM-powered data quality gate.

Sends each record to the LLM for quality assessment.  The LLM returns
a structured verdict: valid/invalid, issues list, and confidence score.

Records below the confidence threshold or explicitly marked invalid are
handled according to ``on_invalid``:
- ``"flag"``       — add a ``_validation`` field to the record (default)
- ``"drop"``       — return None (remove from pipeline)
- ``"raise"``      — raise ``DataQualityError``

Usage::

    pipeline.pipe(
        AIValidateMiddleware(
            provider=GeminiProvider(),
            criteria=\"\"\"
            Validate this POI record for quality issues:
            - Name must be a real place name (not spam/test data)
            - Coordinates must be in Vietnam (lat 8-24, lon 102-110)
            - Rating must be between 1.0 and 5.0 if present
            - Reviews should be in Vietnamese or English
            \"\"\",
            min_confidence=0.85,
            on_invalid="flag",
        )
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Literal, TypeVar

import logstruct

from agora.middlewares.ai.base import AIMiddleware, OnError
from agora.utils.records import merge_into_record

if TYPE_CHECKING:
    from agora.ai.cache import LLMCache
    from agora.ai.providers.base import AIProvider
    from agora.core.context import PipelineContext

T = TypeVar("T")

logger = logstruct.getLogger(__name__)

OnInvalid = Literal["flag", "drop", "raise"]

_SYSTEM_VALIDATE = """\
You are a data quality auditor for an ETL pipeline.
Evaluate the provided record and return a JSON object with exactly these keys:
  "valid":      boolean — true if the record passes quality checks
  "issues":     list[str] — list of specific quality problems found (empty list if none)
  "confidence": float 0.0-1.0 — your confidence in this assessment
Do NOT add markdown, explanations, or extra keys.
"""


class DataQualityError(Exception):
    """Raised when a record fails quality validation and ``on_invalid='raise'``."""

    def __init__(self, record: object, issues: list[str]) -> None:
        self.record = record
        self.issues = issues
        super().__init__(f"Data quality failure: {issues}")


class AIValidateMiddleware(AIMiddleware[T], Generic[T]):
    """LLM-based data quality gate.

    Parameters
    ----------
    provider:
        Any ``AIProvider``.
    criteria:
        Human-readable quality criteria for the LLM to evaluate.
        Be specific about what constitutes valid vs invalid data.
    min_confidence:
        Records where the LLM confidence < this threshold are flagged/dropped.
    flag_field:
        Field name for validation metadata (only used when ``on_invalid='flag'``).
    on_invalid:
        ``"flag"``  — write validation result into ``flag_field``, continue.
        ``"drop"``  — return None (record is removed from pipeline).
        ``"raise"`` — raise ``DataQualityError``.
    system:
        Override default system prompt.
    cache / cache_ttl / on_error:
        See ``AIMiddleware``.
    """

    name = "ai_validate"

    def __init__(
        self,
        provider: AIProvider,
        criteria: str,
        *,
        min_confidence: float = 0.8,
        flag_field: str = "_ai_validation",
        on_invalid: OnInvalid = "flag",
        system: str | None = None,
        cache: LLMCache | None = None,
        cache_ttl: int = 86_400,
        on_error: OnError = "passthrough",
    ) -> None:
        super().__init__(provider, cache=cache, cache_ttl=cache_ttl, on_error=on_error)
        self._criteria = criteria.strip()
        self._min_confidence = min_confidence
        self._flag_field = flag_field
        self._on_invalid = on_invalid
        self._system = system or _SYSTEM_VALIDATE

    def _build_prompt(self, record: T) -> str:
        if isinstance(record, dict):
            record_str = str(record)
        elif hasattr(record, "model_dump"):
            record_str = str(record.model_dump())
        else:
            record_str = str(record)

        return f"Quality criteria:\n{self._criteria}\n\nRecord to evaluate:\n{record_str}"

    async def process(self, record: T, ctx: PipelineContext) -> T | None:
        try:
            prompt = self._build_prompt(record)
            response = await self._cached_complete(
                prompt,
                system=self._system,
                temperature=0.0,
                max_tokens=512,
                ctx=ctx,
            )
            verdict = self._parse_json(response.content)

            is_valid: bool = bool(verdict.get("valid", True))
            issues: list[str] = list(verdict.get("issues", []))
            confidence: float = float(verdict.get("confidence", 1.0))

            # Treat low-confidence verdicts as invalid
            is_valid = is_valid and confidence >= self._min_confidence

            # Track quality metrics
            ai_m = ctx.metrics.middleware(self.name).ai
            if is_valid:
                ai_m.validation_pass += 1
            else:
                ai_m.validation_fail += 1

            if not is_valid:
                logger.info(
                    "ai_validate_invalid",
                    issues=issues,
                    confidence=confidence,
                    policy=self._on_invalid,
                )
                if self._on_invalid == "drop":
                    return None
                if self._on_invalid == "raise":
                    raise DataQualityError(record, issues)
                # flag: attach validation metadata and continue
                validation_data = {
                    "valid": False,
                    "issues": issues,
                    "confidence": confidence,
                }
                return self._attach_flag(record, validation_data)

            return record

        except DataQualityError:
            raise
        except Exception as exc:
            return await self._handle_error(exc, record, ctx)

    def _attach_flag(self, record: T, data: dict) -> T:
        return merge_into_record(record, {self._flag_field: data})
