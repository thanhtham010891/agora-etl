"""AI-specific observability metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AIMiddlewareMetrics:
    """AI-specific metrics for a single AI middleware stage."""

    llm_calls: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    errors: int = 0
    validation_pass: int = 0
    validation_fail: int = 0
    category_counts: dict[str, int] = field(default_factory=dict)

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @property
    def cache_hit_rate(self) -> float:
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "llm_calls": self.llm_calls,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate": round(self.cache_hit_rate, 4),
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "errors": self.errors,
            "validation_pass": self.validation_pass,
            "validation_fail": self.validation_fail,
            "category_counts": self.category_counts,
        }


@dataclass
class AIMetrics:
    """Aggregated AI metrics across all AI middlewares in a pipeline run."""

    total_llm_calls: int = 0
    total_cache_hits: int = 0
    total_cache_misses: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_errors: int = 0
    total_validation_pass: int = 0
    total_validation_fail: int = 0

    @property
    def total_tokens(self) -> int:
        return self.total_input_tokens + self.total_output_tokens

    @property
    def cache_hit_rate(self) -> float:
        total = self.total_cache_hits + self.total_cache_misses
        return self.total_cache_hits / total if total > 0 else 0.0

    @property
    def validation_pass_rate(self) -> float:
        total = self.total_validation_pass + self.total_validation_fail
        return self.total_validation_pass / total if total > 0 else 0.0

    def absorb(self, mw: AIMiddlewareMetrics) -> None:
        self.total_llm_calls += mw.llm_calls
        self.total_cache_hits += mw.cache_hits
        self.total_cache_misses += mw.cache_misses
        self.total_input_tokens += mw.input_tokens
        self.total_output_tokens += mw.output_tokens
        self.total_errors += mw.errors
        self.total_validation_pass += mw.validation_pass
        self.total_validation_fail += mw.validation_fail

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_llm_calls": self.total_llm_calls,
            "total_cache_hits": self.total_cache_hits,
            "total_cache_misses": self.total_cache_misses,
            "cache_hit_rate": round(self.cache_hit_rate, 4),
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_tokens": self.total_tokens,
            "total_errors": self.total_errors,
            "validation_pass_rate": round(self.validation_pass_rate, 4),
        }
