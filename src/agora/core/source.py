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

import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeGuard, TypeVar, runtime_checkable

from agora.core.data_plane import DataPlane, SourceDataPlaneSpec

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator
    from types import TracebackType

    from agora.core.checkpoint import Checkpoint, CheckpointValue

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
_WARNED_LEGACY_SOURCE_TYPES: set[type[object]] = set()


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


def source_data_plane_spec(source: object) -> SourceDataPlaneSpec:
    """Return the emitted data plane for *source*."""
    if isinstance(source, BaseSource):
        return source.data_plane_spec()

    advertised = getattr(source, "data_plane_spec", None)
    if callable(advertised):
        spec = advertised()
        if not isinstance(spec, SourceDataPlaneSpec):
            raise TypeError("data_plane_spec() must return SourceDataPlaneSpec")
        return spec

    return _source_data_plane_spec_from_legacy_flags(source, warn=True)


def _source_data_plane_spec_from_legacy_flags(
    source: object,
    *,
    warn: bool,
) -> SourceDataPlaneSpec:
    """Compatibility bridge for older source bool flags."""
    supports_batch_emit = bool(getattr(source, "supports_batch_emit", False))
    emits_arrow_batches = bool(getattr(source, "emits_arrow_batches", False))
    if warn and (supports_batch_emit or emits_arrow_batches):
        source_type = type(source)
        if source_type not in _WARNED_LEGACY_SOURCE_TYPES:
            _WARNED_LEGACY_SOURCE_TYPES.add(source_type)
            warnings.warn(
                f"{source_type.__name__} uses legacy source data-plane bool flags; "
                "override data_plane_spec() returning SourceDataPlaneSpec instead. "
                "Legacy flags remain supported in 0.3.x and are planned for removal in 0.4.0.",
                DeprecationWarning,
                stacklevel=3,
            )
    emitted_plane = DataPlane.PYTHON_ROWS
    if emits_arrow_batches:
        emitted_plane = DataPlane.ARROW_BATCHES
    elif supports_batch_emit:
        emitted_plane = DataPlane.PYTHON_BATCHES
    return SourceDataPlaneSpec(
        source_name=str(getattr(source, "source_name", type(source).__name__)),
        emitted_plane=emitted_plane,
        supports_batch_emit=supports_batch_emit,
        emits_arrow_batches=emits_arrow_batches,
    )


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

        The generator must be finite (raises StopAsyncIteration) unless the
        caller wraps the source in ``source.limit(n)`` for bounded execution.
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
            warnings.warn(
                f"{type(self).__name__} overrides runtime_counters(); this is deprecated. "
                "Override runtime_metrics() returning SourceRuntimeMetrics instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return SourceRuntimeMetrics.from_mapping(self.runtime_counters())
        return SourceRuntimeMetrics()

    def runtime_counters(self) -> dict[str, int]:
        """Deprecated compatibility shim for older source implementations.

        .. deprecated::
            Override ``runtime_metrics()`` (returning ``SourceRuntimeMetrics``)
            instead of this stringly-typed mapping API.
        """
        return {}

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        """Return the source-side data-plane contract used by runtime planning."""
        return _source_data_plane_spec_from_legacy_flags(self, warn=True)

    @property
    def emitted_data_plane(self) -> DataPlane:
        """Convenience alias for the source's emitted plane."""
        return self.data_plane_spec().emitted_plane

    def limit(self, max_records: int | None) -> BaseSource[T]:
        """Return a source wrapper that emits at most *max_records* records."""
        if max_records is None:
            return self
        if max_records < 0:
            raise ValueError(f"max_records must be >= 0, got {max_records}")
        return LimitedSource(self, max_records=max_records)

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


def _slice_emitted_batch(batch: Any, count: int) -> Any:
    """Trim one emitted batch to *count* rows while preserving its shape when possible."""
    if count < 0:
        raise ValueError(f"count must be >= 0, got {count}")
    if hasattr(batch, "slice"):
        return batch.slice(0, count)
    return batch[:count]


class LimitedSource(BaseSource[T]):
    """Source wrapper that caps total emitted records before the runtime sees them."""

    source_name = "limited_source"

    def __init__(self, source: BaseSource[T], *, max_records: int) -> None:
        if max_records < 0:
            raise ValueError(f"max_records must be >= 0, got {max_records}")
        self._source = source
        self._max_records = max_records
        self.source_name = source.source_name
        self.supports_prefetch = bool(getattr(source, "supports_prefetch", False))
        upstream_prefetch = int(getattr(source, "prefetch_limit", 0))
        if upstream_prefetch > 0:
            self.prefetch_limit = min(upstream_prefetch, max_records)
        else:
            self.prefetch_limit = upstream_prefetch
        self.supports_checkpoint = bool(getattr(source, "supports_checkpoint", False))
        self.supports_rust_prefetch = bool(getattr(source, "supports_rust_prefetch", False))

    def limit(self, max_records: int | None) -> BaseSource[T]:
        if max_records is None:
            return self
        if max_records < 0:
            raise ValueError(f"max_records must be >= 0, got {max_records}")
        return LimitedSource(self._source, max_records=min(self._max_records, max_records))

    async def open(self) -> None:
        await self._source.open()

    async def close(self) -> None:
        await self._source.close()

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        await self._source.prepare_resume(checkpoint)

    def current_checkpoint(self) -> CheckpointValue:
        return self._source.current_checkpoint()

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return self._source.runtime_metrics()

    def runtime_counters(self) -> dict[str, int]:
        return self._source.runtime_counters()

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        upstream = self._source.data_plane_spec()
        return replace(upstream, source_name=self.source_name)

    def delivery_success_callback(self) -> Callable[[], Awaitable[None]] | None:
        if isinstance(self._source, DeliveryHookSource):
            return self._source.delivery_success_callback()
        return None

    async def stream(self) -> AsyncGenerator[T, None]:
        remaining = self._max_records
        if remaining <= 0:
            return
        async for record in self._source.stream():
            if remaining <= 0:
                break
            yield record
            remaining -= 1

    async def stream_batches(self) -> AsyncGenerator[Any, None]:
        if self.data_plane_spec().emitted_plane == DataPlane.PYTHON_ROWS:
            raise RuntimeError(
                f"Source '{self.source_name}' does not support batch emission; stream_batches() "
                "is unavailable on this limited wrapper."
            )
        remaining = self._max_records
        if remaining <= 0:
            return
        async for batch in self._source.stream_batches():  # type: ignore[attr-defined]
            if remaining <= 0:
                break
            batch_size = len(batch)
            if batch_size <= remaining:
                yield batch
                remaining -= batch_size
                continue
            yield _slice_emitted_batch(batch, remaining)
            break

    def stream_sync_batches(self) -> Iterator[Any]:
        upstream = getattr(self._source, "stream_sync_batches", None)
        if not callable(upstream):
            raise TypeError(f"Source '{self.source_name}' does not expose stream_sync_batches().")
        remaining = self._max_records
        if remaining <= 0:
            return
        for item in upstream():
            if remaining <= 0:
                break
            yield item
            remaining -= 1
