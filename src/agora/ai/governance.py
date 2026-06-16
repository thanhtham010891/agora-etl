"""AI budget and cost governance helpers."""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Mapping


class AIBudgetExceededError(RuntimeError):
    """Raised when an AI call would exceed the configured budget policy."""


AIBudgetExceeded = AIBudgetExceededError


@dataclass(frozen=True, slots=True)
class AICostRate:
    """Per-1K-token price for one model."""

    input_per_1k_usd: float
    output_per_1k_usd: float

    def estimate_usd(self, *, input_tokens: int, output_tokens: int) -> float:
        return (max(input_tokens, 0) / 1000.0) * self.input_per_1k_usd + (
            max(output_tokens, 0) / 1000.0
        ) * self.output_per_1k_usd


@dataclass(frozen=True, slots=True)
class AICostCatalog:
    """Model price catalog used by ``AIBudgetPolicy`` cost guards."""

    rates: Mapping[str, AICostRate]

    def rate_for(self, model: str) -> AICostRate:
        try:
            return self.rates[model]
        except KeyError as exc:
            raise AIBudgetExceeded(
                f"AI cost guard requires a price for model {model!r}, but no rate was found."
            ) from exc

    def estimate_usd(self, model: str, *, input_tokens: int, output_tokens: int) -> float:
        return self.rate_for(model).estimate_usd(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )


@dataclass(frozen=True, slots=True)
class AIBudgetPolicy:
    """Token/cost budget for completion-backed AI middleware calls."""

    max_input_tokens: int | None = None
    max_output_tokens: int | None = None
    max_total_tokens: int | None = None
    max_cost_usd: float | None = None
    scope: Literal["record", "run"] = "record"

    def __post_init__(self) -> None:
        for name, value in (
            ("max_input_tokens", self.max_input_tokens),
            ("max_output_tokens", self.max_output_tokens),
            ("max_total_tokens", self.max_total_tokens),
        ):
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative when provided.")
        if self.max_cost_usd is not None and self.max_cost_usd < 0:
            raise ValueError("max_cost_usd must be non-negative when provided.")
        if self.scope not in {"record", "run"}:
            raise ValueError("scope must be 'record' or 'run'.")


@dataclass(slots=True)
class AIBudgetUsage:
    """Mutable run-scoped budget usage tracked by one middleware instance."""

    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens


def estimate_prompt_tokens(*parts: str | None) -> int:
    """Return a deterministic rough token estimate for preflight checks."""

    text = "\n".join(part for part in parts if part)
    if not text:
        return 0
    return max(1, ceil(len(text) / 4))


__all__ = [
    "AIBudgetExceeded",
    "AIBudgetExceededError",
    "AIBudgetPolicy",
    "AIBudgetUsage",
    "AICostCatalog",
    "AICostRate",
    "estimate_prompt_tokens",
]
