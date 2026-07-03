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
from agora.ai.governance import (
    AIBudgetExceeded,
    AIBudgetPolicy,
    AIBudgetUsage,
    AICostCatalog,
    estimate_prompt_tokens,
)
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
        budget_policy: AIBudgetPolicy | None = None,
        cost_catalog: AICostCatalog | None = None,
    ) -> None:
        if require_completion:
            from agora.ai.providers.base import require_completion_provider

            require_completion_provider(provider, consumer=type(self).__name__)
        self._provider = provider
        self._cache = cache
        self._cache_ttl = cache_ttl
        self._on_error = on_error
        self._budget_policy = budget_policy
        self._cost_catalog = cost_catalog
        self._budget_usage = AIBudgetUsage()

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def on_start(self, ctx: PipelineContext) -> None:
        del ctx
        if self._budget_policy is not None and self._budget_policy.scope == "run":
            self._budget_usage = AIBudgetUsage()

    async def on_stop(self, ctx: PipelineContext) -> None:
        del ctx
        if self._cache is not None:
            await self._cache.close()

    @property
    def provider(self) -> object:
        """Return the configured AI provider instance."""
        return self._provider

    @property
    def cache(self) -> LLMCache | None:
        """Return the configured response cache, if any."""
        return self._cache

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
            "model": self._provider_cache_identity(),
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
        model = self._provider_cache_identity()
        self._enforce_budget_preflight(
            model=model,
            prompt=prompt,
            system=system,
            max_tokens=max_tokens,
        )
        response = await provider.complete(
            prompt,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format=response_format,
        )
        self._record_budget_usage(response)

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

    def _enforce_budget_preflight(
        self,
        *,
        model: str,
        prompt: str,
        system: str | None,
        max_tokens: int,
    ) -> None:
        policy = self._budget_policy
        if policy is None:
            return
        input_tokens = estimate_prompt_tokens(system, prompt)
        output_tokens = max(max_tokens, 0)
        self._check_budget(
            policy,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            estimated=True,
        )

    def _record_budget_usage(self, response: CompletionResponse) -> None:
        policy = self._budget_policy
        if policy is None:
            return
        cost_usd = self._estimate_cost_usd(
            response.model,
            input_tokens=response.input_tokens,
            output_tokens=response.output_tokens,
        )
        self._check_budget(
            policy,
            model=response.model,
            input_tokens=response.input_tokens,
            output_tokens=response.output_tokens,
            cost_usd=cost_usd,
            estimated=False,
        )
        if policy.scope == "run":
            self._budget_usage.input_tokens += response.input_tokens
            self._budget_usage.output_tokens += response.output_tokens
            self._budget_usage.cost_usd += cost_usd

    def _check_budget(
        self,
        policy: AIBudgetPolicy,
        *,
        model: str,
        input_tokens: int,
        output_tokens: int,
        estimated: bool,
        cost_usd: float | None = None,
    ) -> None:
        prefix = "estimated " if estimated else ""
        effective_input = input_tokens
        effective_output = output_tokens
        effective_total = input_tokens + output_tokens
        effective_cost = (
            self._estimate_cost_usd(model, input_tokens=input_tokens, output_tokens=output_tokens)
            if cost_usd is None
            else cost_usd
        )
        if policy.scope == "run":
            effective_input += self._budget_usage.input_tokens
            effective_output += self._budget_usage.output_tokens
            effective_total += self._budget_usage.total_tokens
            effective_cost += self._budget_usage.cost_usd

        if policy.max_input_tokens is not None and effective_input > policy.max_input_tokens:
            raise AIBudgetExceeded(
                f"AI budget exceeded: {prefix}input_tokens={effective_input} "
                f"> max_input_tokens={policy.max_input_tokens}."
            )
        if policy.max_output_tokens is not None and effective_output > policy.max_output_tokens:
            raise AIBudgetExceeded(
                f"AI budget exceeded: {prefix}output_tokens={effective_output} "
                f"> max_output_tokens={policy.max_output_tokens}."
            )
        if policy.max_total_tokens is not None and effective_total > policy.max_total_tokens:
            raise AIBudgetExceeded(
                f"AI budget exceeded: {prefix}total_tokens={effective_total} "
                f"> max_total_tokens={policy.max_total_tokens}."
            )
        if policy.max_cost_usd is not None and effective_cost > policy.max_cost_usd:
            raise AIBudgetExceeded(
                f"AI budget exceeded: {prefix}cost_usd={effective_cost:.6f} "
                f"> max_cost_usd={policy.max_cost_usd:.6f}."
            )

    def _estimate_cost_usd(
        self,
        model: str,
        *,
        input_tokens: int,
        output_tokens: int,
    ) -> float:
        if self._budget_policy is None or self._budget_policy.max_cost_usd is None:
            return 0.0
        if self._cost_catalog is None:
            raise AIBudgetExceeded(
                "AI cost guard requires an AICostCatalog when max_cost_usd is set."
            )
        return self._cost_catalog.estimate_usd(
            model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

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

    def render_prompt(self, template: str, record: T) -> str:
        """Public wrapper around Agora's safe prompt renderer."""
        return self._render_prompt(template, record)

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

    def _provider_cache_identity(self) -> str:
        model = getattr(self._provider, "model", None)
        if isinstance(model, str) and model:
            return model
        private_model = getattr(self._provider, "_model", None)
        if isinstance(private_model, str) and private_model:
            return private_model
        return type(self._provider).__qualname__

    @abstractmethod
    async def process(self, record: T, ctx: PipelineContext) -> T | None: ...
