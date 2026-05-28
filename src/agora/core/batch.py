"""
agora/core/batch.py
===================
Batch-native execution protocols for Agora 0.2.0.

These protocols are additive — existing ``BaseSource``, ``Middleware``, and
``BaseSink`` subclasses continue to work unchanged on the per-record path.
Batch protocols are opt-in: implement them to participate in the batch lane.

Public surface:
- ``BatchableSource[T]``   — source that can emit native batches
- ``BatchMiddleware[T, U]`` — middleware that processes a whole batch at once
- ``BatchProcessResult``   — result of ``MiddlewareChain.process_batch()``
- ``BatchFailure``         — carries the failed batch and exception
- ``is_batch_capable_source()`` — runtime detection helper
- ``is_arrow_native_sink()``    — detects Arrow-native write path
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeGuard, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agora.core.checkpoint import CheckpointValue
    from agora.core.context import PipelineContext

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
U = TypeVar("U")


# ======================================================================
# BatchableSource protocol
# ======================================================================


@runtime_checkable
class BatchableSource(Protocol[T_co]):
    """Protocol for sources that can emit native batches.

    Implement this alongside ``BaseSource`` to participate in the batch
    execution lane.  The runtime calls ``stream_batches()`` instead of
    ``stream()`` when this protocol is detected.

    The ``supports_batch_emit`` flag must be ``True`` — the runtime checks
    it explicitly so that a source that accidentally satisfies the structural
    protocol is not silently routed to the batch lane.
    """

    source_name: str
    supports_batch_emit: bool

    def stream_batches(self) -> AsyncGenerator[Any, None]:
        """Yield batches of records.

        Each yielded value is either a ``list[T]`` or a ``pa.RecordBatch``
        (for Arrow-native sources like ``ParquetSource``).  The runtime
        handles both shapes.
        """
        ...

    def current_checkpoint(self) -> CheckpointValue:
        """Return the current checkpoint value after the last emitted batch."""
        ...


def is_batch_capable_source(source: object) -> TypeGuard[BatchableSource[Any]]:
    """Return True when *source* explicitly supports batch emission."""
    return isinstance(source, BatchableSource) and bool(
        getattr(source, "supports_batch_emit", False)
    )


# ======================================================================
# BatchMiddleware ABC
# ======================================================================


class BatchMiddleware(ABC, Generic[T, U]):
    """Abstract base for middlewares that process a whole batch at once.

    Implement ``process_batch()`` to transform a list of records in one call.
    The runtime passes the entire batch and expects a list of the same length
    back, where ``None`` entries indicate dropped records.

    Failure policy (Option A): if ``process_batch()`` raises, the entire
    batch is treated as failed — all records are routed to the DLQ (if
    configured) or the run aborts.  There is no per-record fallback.
    """

    name: str = "batch_middleware"

    @abstractmethod
    async def process_batch(
        self,
        records: list[T],
        ctx: PipelineContext,
    ) -> list[U | None]:
        """Process *records* and return one result per input record.

        Return ``None`` at position *i* to drop ``records[i]``.
        The returned list must have the same length as *records*.
        Raise to fail the entire batch.
        """

    async def on_start(self, ctx: PipelineContext) -> None:
        """Called once before the pipeline starts streaming."""

    async def on_stop(self, ctx: PipelineContext) -> None:
        """Called once after the pipeline finishes (or on error)."""


# ======================================================================
# BatchProcessResult and BatchFailure
# ======================================================================


@dataclass(frozen=True, slots=True)
class BatchFailure:
    """Carries a failed batch and the exception that caused the failure."""

    batch: list[Any]
    exception: Exception
    middleware: str = "unknown"


@dataclass(frozen=True, slots=True)
class BatchProcessResult:
    """Result of ``MiddlewareChain.process_batch()``.

    ``results`` is a list of the same length as the input batch.
    ``None`` entries are records that were dropped by a middleware.
    ``failure`` is set when a ``BatchMiddleware`` raised — in that case
    ``results`` is empty and the entire batch should be DLQ-routed.
    """

    results: list[Any | None] = field(default_factory=list)
    failure: BatchFailure | None = None

    @property
    def ok(self) -> bool:
        return self.failure is None


# ======================================================================
# Arrow-native middleware
# ======================================================================


class ArrowBatchMiddleware(ABC):
    """Middleware that transforms a ``pa.RecordBatch`` in place, staying columnar.

    Implement ``process_arrow_batch(batch, ctx) -> pa.RecordBatch``. Return a
    batch with fewer rows to filter rows out; return a zero-row batch to drop
    the whole batch. The runtime never materialises Python row objects for a
    chain made entirely of ``ArrowBatchMiddleware`` stages — the batch flows
    columnar from an Arrow-native source to an Arrow-native sink.

    Use only for vectorisable transforms (arithmetic, comparison, cast, string
    ops, fill-null, ...). Arbitrary per-row Python logic belongs on a regular
    ``Middleware`` (per-record lane), not here.
    """

    name: str = "arrow_batch_middleware"

    @abstractmethod
    async def process_arrow_batch(self, batch: Any, ctx: PipelineContext) -> Any:
        """Transform *batch* (a ``pa.RecordBatch``) and return a ``pa.RecordBatch``."""

    async def on_start(self, ctx: PipelineContext) -> None:  # noqa: B027
        """Called once before the pipeline starts streaming."""

    async def on_stop(self, ctx: PipelineContext) -> None:  # noqa: B027
        """Called once after the pipeline finishes (or on error)."""

    async def on_error(self, record: Any, exc: Exception, ctx: PipelineContext) -> None:  # noqa: B027
        """No-op — prevents AttributeError when used in a mixed chain."""

    async def process(self, record: Any, ctx: PipelineContext) -> Any:
        """Pass-through for per-record path (mixed chain fallback).

        ArrowBatchMiddleware cannot operate on individual Python records.
        When placed in a mixed chain, the runtime skips it in process_batch()
        but process_range() may still call process(). Return the record unchanged.
        """
        return record


def is_arrow_batch_middleware(obj: object) -> TypeGuard[ArrowBatchMiddleware]:
    """Return True when *obj* is an Arrow-native batch middleware."""
    return isinstance(obj, ArrowBatchMiddleware)


# ======================================================================
# Arrow-native sink detection
# ======================================================================


@runtime_checkable
class ArrowNativeSink(Protocol):
    """Protocol for sinks that can consume ``pa.RecordBatch`` directly.

    When both the source and sink are Arrow-native and no middleware
    transforms the batch, the runtime uses the shortest path:
    ``RecordBatch → write_arrow_batch()`` with zero Python object
    allocation per row.
    """

    async def write_arrow_batch(self, batch: Any) -> None:
        """Write a ``pa.RecordBatch`` directly."""
        ...


def is_arrow_native_sink(sink: object) -> TypeGuard[ArrowNativeSink]:
    """Return True when *sink* exposes a native Arrow write path."""
    return isinstance(sink, ArrowNativeSink) and callable(getattr(sink, "write_arrow_batch", None))


__all__ = [
    "ArrowBatchMiddleware",
    "ArrowNativeSink",
    "BatchFailure",
    "BatchMiddleware",
    "BatchProcessResult",
    "BatchableSource",
    "is_arrow_batch_middleware",
    "is_arrow_native_sink",
    "is_batch_capable_source",
]
