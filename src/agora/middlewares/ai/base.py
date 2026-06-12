"""
agora/middlewares/ai/base.py
=============================
``AIMiddleware`` — abstract base for all LLM-powered middlewares.

Provides:
- Transparent LLM response caching (cache-aside pattern)
- Token usage logging via logstruct
- Prompt rendering via Python str.format_map (no extra deps)
- ``on_stop`` that closes cache connections cleanly

Subclasses only need to implement ``process()``.

Design notes
------------
- ``_cached_complete`` handles the full read-through cache logic so
  subclasses never think about caching.
- ``_render_prompt`` uses ``str.format_map`` with a dict view of the
  record — works with dicts, Pydantic models (via __dict__), dataclasses.
- ``on_error_policy`` is ``"passthrough"`` by default: AI enrichment
  failures should never block the main pipeline. Override per-middleware.
"""

from __future__ import annotations

import json
import string
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.ai.cache import LLMCache, make_cache_key
from agora.core.middleware import Middleware
from agora.core.types import OnError as _OnError

if TYPE_CHECKING:
    from pydantic import BaseModel

    from agora.ai.providers.base import CompletionResponse
    from agora.core.context import PipelineContext

T = TypeVar("T")
U = TypeVar("U")

logger = logstruct.getLogger(__name__)
OnError = _OnError


class _SafeFormatter(string.Formatter):
    """Injection-safe string formatter for prompt templates.

    - Missing keys are left as-is (e.g. ``{ctx}`` → ``"{ctx}"``) — no ``KeyError``.
    - Attribute/index access (``{obj.attr}``, ``{obj[key]}``) is blocked to prevent
      data exfiltration via format string exploitation.
    """

    def get_value(self, key: int | str, args: Any, kwargs: Any) -> Any:
        if isinstance(key, str):
            return kwargs.get(key, f"{{{key}}}")
        return super().get_value(key, args, kwargs)

    def get_field(self, field_name: str, args: Any, kwargs: Any) -> tuple[Any, str]:
        # Block attribute access (obj.attr) and index access (obj[key])
        if "." in field_name or "[" in field_name:
            logger.warning(
                "prompt_template_blocked_access",
                field=field_name,
                reason="attribute/index access in prompt templates is not allowed",
            )
            return f"{{{field_name}}}", field_name
        return super().get_field(field_name, args, kwargs)  # type: ignore[no-any-return]


_SAFE_FORMATTER = _SafeFormatter()


class AIMiddleware(Middleware[T, T], Generic[T]):
    """Abstract base for AI-powered middlewares.

    Parameters
    ----------
    provider:
        Any completion-capable provider (GeminiProvider, OpenAIProvider,
        AnthropicProvider, etc.).
    cache:
        Optional response cache.  Highly recommended for production to
        avoid redundant LLM calls on reruns.
    cache_ttl:
        Cache entry lifetime in seconds.  Default: 24 hours.
    on_error:
        Behaviour when the LLM call or post-processing fails:
        - ``"passthrough"`` — return the original record unchanged (safe default)
        - ``"drop"``        — return None (drop the record)
        - ``"raise"``       — re-raise the exception (stops the pipeline)
    """

    def __init__(
        self,
        provider: object,
        *,
        cache: LLMCache | None = None,
        cache_ttl: int = 86_400,
        on_error: OnError = OnError.PASSTHROUGH,
        require_completion: bool = True,
    ) -> None:
        if require_completion:
            from agora.ai.providers.base import require_completion_provider

            require_completion_provider(provider, consumer=type(self).__name__)
        self._provider = provider
        self._cache = cache
        self._cache_ttl = cache_ttl
        self._on_error = on_error

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def on_stop(self, ctx: PipelineContext) -> None:
        if self._cache is not None:
            await self._cache.close()

    # ------------------------------------------------------------------ #
    # Helpers for subclasses                                               #
    # ------------------------------------------------------------------ #

    async def _cached_complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        response_format: type[BaseModel] | None = None,
        ctx: PipelineContext | None = None,
    ) -> CompletionResponse:
        """Call the provider with transparent cache-aside logic.

        Automatically records token usage, cache hits/misses into
        ``ctx.metrics.middleware(name).ai`` when *ctx* is provided.
        """
        cache_kwargs: dict[str, Any] = {
            "system": system,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "response_format": f"{response_format.__module__}.{response_format.__qualname__}"
            if response_format
            else None,
        }
        cache_key = make_cache_key(prompt, cache_kwargs)

        if self._cache is not None:
            cached = await self._cache.get(cache_key)
            if cached is not None:
                logger.debug("llm_cache_hit", middleware=self.name)
                if ctx is not None:
                    ctx.metrics.middleware(self.name).ai.cache_hits += 1
                from agora.ai.providers.base import CompletionResponse

                return CompletionResponse(content=cached, model="cached")

        if ctx is not None:
            ctx.metrics.middleware(self.name).ai.cache_misses += 1

        from agora.ai.providers.base import require_completion_provider

        provider = require_completion_provider(self._provider, consumer=type(self).__name__)
        response = await provider.complete(
            prompt,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format=response_format,
        )

        logger.debug(
            "llm_complete",
            middleware=self.name,
            model=response.model,
            input_tokens=response.input_tokens,
            output_tokens=response.output_tokens,
        )

        if ctx is not None:
            ai_m = ctx.metrics.middleware(self.name).ai
            ai_m.llm_calls += 1
            ai_m.input_tokens += response.input_tokens
            ai_m.output_tokens += response.output_tokens

        if self._cache is not None:
            await self._cache.set(cache_key, response.content, self._cache_ttl)

        return response

    def _render_prompt(self, template: str, record: T) -> str:
        """Render *template* with record fields — injection-safe via ``_SafeFormatter``.

        Supports dicts, Pydantic models, and dataclasses transparently.
        Missing template variables are left as-is (no ``KeyError``).
        Extra record keys that are not in the template are silently ignored.
        Attribute/index access in placeholders (e.g. ``{obj.attr}``) is blocked.
        """
        if isinstance(record, dict):
            data: dict[str, Any] = record
        elif hasattr(record, "model_dump"):
            data = record.model_dump()
        elif hasattr(record, "__dict__"):
            data = record.__dict__
        else:
            data = {"record": str(record)}
        return _SAFE_FORMATTER.format(template, **data)

    def _parse_json(self, text: str) -> dict[str, Any]:
        """Parse JSON from LLM response, stripping markdown code fences."""
        stripped = text.strip()
        if stripped.startswith("```"):
            lines = stripped.splitlines()
            stripped = "\n".join(lines[1:-1]) if len(lines) > 2 else stripped
        return json.loads(stripped)  # type: ignore[no-any-return]

    async def _handle_error(
        self,
        exc: Exception,
        record: T,
        ctx: PipelineContext,
    ) -> T | None:
        ctx.log.warning(
            "ai_middleware_error",
            middleware=self.name,
            error=str(exc),
            policy=self._on_error,
        )
        if self._on_error == "raise":
            raise exc
        if self._on_error == "drop":
            return None
        return record  # passthrough

    @abstractmethod
    async def process(self, record: T, ctx: PipelineContext) -> T | None: ...
