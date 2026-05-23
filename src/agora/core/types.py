"""
agora/core/types.py
===================
Core type variables, type aliases, and structural protocols used
throughout the agora framework.

These are the "vocabulary" types — every other module imports from here.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, TypeVar

# ======================================================================
# Generic TypeVars
# ======================================================================

T = TypeVar("T")  # Input record type (source emits T)
U = TypeVar("U")  # Output record type (middleware produces U)
K = TypeVar("K")  # Key type (used in dedup, routing)
P = TypeVar("P")  # Plugin type (used in Registry)

# ======================================================================
# Type aliases
# ======================================================================

# A row dict passed to a SQL sink (column → value)
SqlRow = dict[str, Any]

# A routing key string (e.g. DataSource.value, topic name)
SourceKey = str

# Factory type for plugin registries
PluginFactory = Callable[..., Any]


# ======================================================================
# Enums
# ======================================================================


class OnError(StrEnum):
    """Error-handling policy for middlewares.

    Backward-compatible with plain strings because ``StrEnum`` members
    compare equal to their string values::

        >>> OnError.PASSTHROUGH == "passthrough"
        True
    """

    PASSTHROUGH = "passthrough"
    DROP = "drop"
    RAISE = "raise"
    LOG = "log"  # used by ValidateMiddleware — semantics: log then drop


class DLQFailurePolicy(StrEnum):
    """Policy for failures while writing to the DLQ sink."""

    LOG_ONLY = "log_only"
    RAISE = "raise"


class CheckpointFailurePolicy(StrEnum):
    """Policy for checkpoint-store failures."""

    FAIL_CLOSED = "fail_closed"
    LOG_AND_CONTINUE = "log_and_continue"


class SinkFailurePolicy(StrEnum):
    """Policy for sink delivery failures after middleware processing."""

    FAIL_CLOSED = "fail_closed"
    LOG_AND_CONTINUE = "log_and_continue"


class SourceRecordFailurePolicy(StrEnum):
    """Policy for source-side record decode/deserialize failures."""

    FAIL_CLOSED = "fail_closed"
    LOG_AND_CONTINUE = "log_and_continue"


class DedupStoreFailurePolicy(StrEnum):
    """Policy for dedup-store failures inside ``DedupMiddleware``."""

    FAIL_CLOSED = "fail_closed"
    FAIL_OPEN = "fail_open"


# ======================================================================
# Backpressure config
# ======================================================================


@dataclass(frozen=True, slots=True)
class Backpressure:
    """Adaptive backpressure configuration for pipeline runtime."""

    min_buffer_size: int = 1
    max_buffer_size: int | None = None
    scale_up_step: int = 1
    scale_down_step: int = 1
    writer_slow_ms: float = 25.0
    checkpoint_slow_ms: float = 10.0

    @classmethod
    def adaptive(
        cls,
        *,
        min_buffer_size: int = 1,
        max_buffer_size: int | None = None,
        scale_up_step: int = 1,
        scale_down_step: int = 1,
        writer_slow_ms: float = 25.0,
        checkpoint_slow_ms: float = 10.0,
    ) -> Backpressure:
        return cls(
            min_buffer_size=min_buffer_size,
            max_buffer_size=max_buffer_size,
            scale_up_step=scale_up_step,
            scale_down_step=scale_down_step,
            writer_slow_ms=writer_slow_ms,
            checkpoint_slow_ms=checkpoint_slow_ms,
        )
