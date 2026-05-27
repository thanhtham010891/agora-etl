"""
agora/core/source.py
====================
Abstract async source — the entry point of every agora pipeline.

A source emits records of type T via an async generator.  Sources can
represent anything:
  - Kafka topics          (KafkaSource)
  - JSONL / Parquet files (FileSource)
  - HTTP polling          (HttpPollingSource)
  - In-memory iterables   (IterableSource — useful for tests)

Implementing a custom source
-----------------------------
1. Subclass ``BaseSource[T]``.
2. Implement ``stream()`` as an ``AsyncGenerator``.
3. Override ``close()`` to release resources if needed.

Example::

    class MySource(BaseSource[MyRecord]):
        async def stream(self) -> AsyncGenerator[MyRecord, None]:
            for item in fetch_items():
                yield MyRecord.from_dict(item)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeGuard, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable
    from types import TracebackType

    from agora.core.checkpoint import Checkpoint, CheckpointValue

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


@dataclass(frozen=True, slots=True)
class SourceRuntimeMetrics:
    """Typed source-side counters surfaced in the pipeline summary."""

    record_error_count: int = 0
    record_drop_count: int = 0

    @classmethod
    def from_mapping(cls, counters: dict[str, int] | None) -> SourceRuntimeMetrics:
        counters = counters or {}
        return cls(
            record_error_count=int(counters.get("record_error_count", 0)),
            record_drop_count=int(counters.get("record_drop_count", 0)),
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "record_error_count": self.record_error_count,
            "record_drop_count": self.record_drop_count,
        }


@runtime_checkable
class PrefetchCapableSource(Protocol[T_co]):
    """Capability protocol for sources that support bounded prefetch."""

    source_name: str
    supports_prefetch: bool
    prefetch_limit: int

    def stream(self) -> AsyncGenerator[T, None]:
        """Yield records asynchronously."""
        ...


@runtime_checkable
class RuntimeMetricsSource(Protocol):
    """Capability protocol for sources that expose runtime counters."""

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        """Return typed source-side runtime metrics."""
        ...


@runtime_checkable
class DeliveryHookSource(Protocol):
    """Capability protocol for sources that expose post-delivery hooks."""

    def delivery_success_callback(self) -> Callable[[], Awaitable[None]] | None:
        """Return a callback to run after the current record is handled successfully."""
        ...


def is_prefetch_capable(source: object) -> TypeGuard[PrefetchCapableSource[Any]]:
    """Return True when *source* explicitly enables prefetch support."""
    return isinstance(source, PrefetchCapableSource) and bool(
        getattr(source, "supports_prefetch", False)
    )


def prefetch_limit_for(source: object) -> int:
    """Return the effective prefetch limit for *source*."""
    if not is_prefetch_capable(source):
        return 0
    return max(0, int(source.prefetch_limit))


def source_runtime_metrics(source: object) -> SourceRuntimeMetrics:
    """Return typed runtime metrics for *source* when supported."""
    if isinstance(source, RuntimeMetricsSource):
        return source.runtime_metrics()
    return SourceRuntimeMetrics()


def source_delivery_success_callback(
    source: object,
) -> Callable[[], Awaitable[None]] | None:
    """Return the current post-delivery hook for *source* when supported."""
    if isinstance(source, DeliveryHookSource):
        return source.delivery_success_callback()
    return None


class SourceRecordError(RuntimeError):
    """Record-scoped source failure that the runtime can DLQ precisely."""

    def __init__(
        self,
        exc: Exception,
        *,
        record: Any,
        checkpoint: CheckpointValue = None,
        source: str | None = None,
        stage: str = "source_record",
    ) -> None:
        super().__init__(str(exc))
        self.original = exc
        self.record = record
        self.checkpoint = checkpoint
        self.source = source
        self.stage = stage


class BaseSource(ABC, Generic[T]):
    """Abstract async source.

    Subclasses must implement ``stream()`` which yields records of type T.
    The source lifecycle (open → stream → close) is managed automatically
    by the pipeline runner via the async context manager protocol.
    """

    # Subclasses may override for display in logs / CLI.
    source_name: str = "source"
    supports_prefetch: bool = False
    prefetch_limit: int = 0
    supports_checkpoint: bool = False

    # ------------------------------------------------------------------ #
    # Abstract interface                                                   #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def stream(self) -> AsyncGenerator[T, None]:
        """Yield records asynchronously.

        The generator must be finite (raises StopAsyncIteration) or
        honour the ``max_records`` contract imposed by the pipeline runner.
        """

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def open(self) -> None:
        """Called once before streaming starts.  Override to set up connections."""

    async def close(self) -> None:
        """Called once after streaming ends.  Override to release resources."""

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        """Configure the source to resume from *checkpoint* if supported.

        Only called if the source implements CheckpointableSource protocol.
        Default implementation does nothing (no checkpoint support).
        """

    def current_checkpoint(self) -> CheckpointValue:
        """Return the current progress marker for checkpointing or DLQ metadata.

        Only called if the source implements CheckpointableSource protocol.
        Default implementation returns None (no checkpoint support).

        Returns:
            Serializable checkpoint value (dict, str, int, or None).
        """
        return None

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        """Return typed source-side counters for dropped/error records.

        Subclasses should override this method going forward. Older
        subclasses that still override ``runtime_counters()`` continue to
        work via the compatibility bridge below.
        """
        if type(self).runtime_counters is not BaseSource.runtime_counters:
            return SourceRuntimeMetrics.from_mapping(self.runtime_counters())
        return SourceRuntimeMetrics()

    def runtime_counters(self) -> dict[str, int]:
        """Compatibility shim for older source implementations.

        Newer sources should override ``runtime_metrics()`` instead of this
        stringly-typed mapping API.
        """
        return {}

    # ------------------------------------------------------------------ #
    # Async context manager                                                #
    # ------------------------------------------------------------------ #

    async def __aenter__(self) -> BaseSource[T]:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()


# ======================================================================
# IterableSource — wraps any sync/async iterable (great for tests)
# ======================================================================


class IterableSource(BaseSource[T]):
    """Emit records from an in-memory iterable.

    Primarily useful for unit tests — no I/O, no setup::

        source = IterableSource([record_a, record_b])
        async for record in source.stream():
            ...
    """

    source_name = "iterable"

    def __init__(self, records: Any) -> None:
        # Accept list, tuple, generator, or any iterable
        self._records = records

    async def stream(self) -> AsyncGenerator[T, None]:
        for record in self._records:
            yield record
