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

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeGuard, TypeVar, runtime_checkable

from agora.core.data_plane import DataPlane
from agora.core.source import source_data_plane_spec

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

    Prefer implementing ``data_plane_spec()`` returning a non-row plane.
    The older ``supports_batch_emit`` flag remains accepted in 0.3.x as a
    compatibility bridge.
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
    return (
        callable(getattr(source, "stream_batches", None))
        and callable(getattr(source, "current_checkpoint", None))
        and source_data_plane_spec(source).emitted_plane != DataPlane.PYTHON_ROWS
    )


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

    async def process(self, record: T, ctx: PipelineContext) -> U | None:
        """Per-record fallback for linear lane (non-batch source).

        Wraps the single record in a list, calls process_batch, and returns
        the first result. This allows BatchMiddleware to work with any source,
        not just batch-emit sources.
        """
        results = await self.process_batch([record], ctx)
        return results[0] if results else None

    async def on_error(self, record: T, exc: Exception, ctx: PipelineContext) -> None:
        """Called when process() raises on the linear lane. Default: log and continue."""
        import logstruct

        logstruct.getLogger(__name__).exception(
            "batch_middleware_error",
            middleware=self.name,
            error=str(exc),
        )

    async def apply_in_batch(
        self,
        current: list[Any],
        ctx: PipelineContext,
        chain: Any,
        idx: int,
    ) -> BatchProcessResult | list[Any]:
        """Double-dispatch hook: handle self in MiddlewareChain.process_batch().

        Returns a BatchProcessResult on failure, or the updated current list on success.
        """
        non_none = [r for r in current if r is not None]
        t0 = time.monotonic()
        m_metrics = ctx.metrics.middleware(self.name)
        m_metrics.records_in += len(current)
        try:
            with ctx.trace_span(
                "middleware.process_batch",
                middleware=self.name,
                batch_size=len(non_none),
            ):
                batch_results = await self.process_batch(non_none, ctx)
        except Exception as exc:
            m_metrics.records_errored += len(non_none)
            m_metrics.total_time_ms += (time.monotonic() - t0) * 1000
            ctx.log.exception(
                "batch_middleware_error",
                middleware=self.name,
                batch_size=len(non_none),
            )
            return BatchProcessResult(
                results=[],
                failure=BatchFailure(batch=non_none, exception=exc, middleware=self.name),
            )
        finally:
            m_metrics.total_time_ms += (time.monotonic() - t0) * 1000

        if len(batch_results) != len(non_none):
            raise RuntimeError(
                f"BatchMiddleware '{self.name}' returned {len(batch_results)} "
                f"results for {len(non_none)} inputs — lengths must match."
            )

        result_iter = iter(batch_results)
        updated = [next(result_iter) if r is not None else None for r in current]
        dropped = sum(1 for r in updated if r is None) - sum(1 for r in current if r is None)
        m_metrics.records_dropped += max(0, dropped)
        m_metrics.records_out += sum(1 for r in updated if r is not None)
        return updated


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
        """Compatibility shim for non-Arrow execution paths.

        ArrowBatchMiddleware cannot operate on individual Python records.
        The runtime planner now rejects mixed Arrow/Python-row chains up front,
        but this pass-through keeps older internal call sites from exploding if
        they still reach the per-record hook.
        """
        return record

    async def apply_in_batch(
        self,
        current: list[Any],
        ctx: PipelineContext,
        chain: Any,
        idx: int,
    ) -> BatchProcessResult | list[Any]:
        """Compatibility shim for legacy callers outside the validated Arrow lane."""
        return current


def is_arrow_batch_middleware(obj: object) -> TypeGuard[ArrowBatchMiddleware]:
    """Return True when *obj* is an Arrow-native batch middleware."""
    return isinstance(obj, ArrowBatchMiddleware)


# ======================================================================
# Arrow-native sink detection
# ======================================================================


@runtime_checkable
class ArrowNativeSink(Protocol):
    """Protocol for sinks that can consume ``pa.RecordBatch`` directly.

    When both the source and sink expose this contract and no middleware
    transforms the batch, the runtime may hand the original ``RecordBatch``
    to ``write_arrow_batch()`` at the sink boundary.

    Individual sinks may still materialize rows internally after accepting
    the Arrow batch. That is a sink-local implementation detail, distinct
    from whether the writer/runtime stayed on the Arrow batch contract.
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
