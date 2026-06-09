"""Runtime delivery value objects and outcomes."""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Callable

    from agora.core.context import PipelineContext
    from agora.core.middleware import MiddlewareFailure
    from agora.core.runtime._delivery_state import CheckpointState


@dataclass(slots=True)
class SourceQueueError:
    exc: Exception


@dataclass(slots=True)
class SourceRecord:
    raw: Any
    checkpoint: Any = None
    on_success: Callable[[], Awaitable[None]] | None = None


@dataclass(slots=True)
class PendingWrite:
    processed: Any
    raw: Any
    checkpoint: Any = None
    on_success: Callable[[], Awaitable[None]] | None = None


@dataclass(slots=True)
class ProcessedSourceRecord:
    source_record: SourceRecord
    result: Any | None
    failure: MiddlewareFailure | None = None


class RecordDeliveryError(RuntimeError):
    """Raised when sink delivery must fail the pipeline."""

    def __init__(self, exc: Exception) -> None:
        super().__init__(str(exc))
        self.original = exc


class CommitOutcome(ABC):  # noqa: B024
    """Abstract base for typed delivery outcomes."""

    __slots__ = ()


@dataclass(slots=True)
class CheckpointedOutcome(CommitOutcome):
    """Base for outcomes that carry a checkpoint and optional hook."""

    checkpoint: Any
    on_success: Callable[[], Awaitable[None]] | None = None


@dataclass(slots=True)
class Written(CheckpointedOutcome):
    """Record was durably written to the sink."""


@dataclass(slots=True)
class Dropped(CheckpointedOutcome):
    """Record was filtered or had no sink route."""


@dataclass(slots=True)
class ErroredRouted(CheckpointedOutcome):
    """Record failed but was successfully routed to the DLQ."""


@dataclass(slots=True)
class ErroredUnrouted(CommitOutcome):
    """Record failed and could not be routed to the DLQ."""

    exc: Exception


@dataclass(slots=True)
class RunState:
    """Mutable execution state shared by runtime helpers."""

    ctx: PipelineContext
    checkpoint_state: CheckpointState
    pending_writes: list[PendingWrite]
    processed_count: int = 0
    pending_write_batch_size: int = 1
    pending_write_flush_interval_s: float | None = None
    pending_write_notify: asyncio.Event | None = None
    pending_write_stop: asyncio.Event | None = None
    pending_write_flushed: asyncio.Event | None = None
    pending_write_owner_task: asyncio.Task[None] | None = None
    pending_write_error: BaseException | None = None
