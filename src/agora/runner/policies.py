"""Policies used by scheduler-style runners."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@runtime_checkable
class BackoffPolicy(Protocol):
    """Compute how long a scheduler should wait after consecutive failures."""

    def next_delay(self, consecutive_errors: int) -> float:
        """Return the delay in seconds for the current error streak."""


@dataclass(frozen=True, slots=True)
class ExponentialBackoffPolicy:
    """Exponential scheduler backoff with an upper bound."""

    base_delay_seconds: float = 60.0
    max_delay_seconds: float = 600.0

    def next_delay(self, consecutive_errors: int) -> float:
        """Return the bounded exponential delay for the error streak."""
        if consecutive_errors <= 0:
            return 0.0
        return min(
            self.base_delay_seconds * (2 ** (consecutive_errors - 1)),
            self.max_delay_seconds,
        )


__all__ = ["BackoffPolicy", "ExponentialBackoffPolicy"]
