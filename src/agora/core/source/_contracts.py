"""Source capability contracts and helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeGuard, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping

T_co = TypeVar("T_co", covariant=True)


@dataclass(frozen=True, slots=True)
class SourceRuntimeMetrics:
    """Typed source-side counters surfaced in the pipeline summary."""

    record_error_count: int = 0
    record_drop_count: int = 0
    arrow_batch_count: int = 0
    arrow_max_batch_rows: int = 0
    arrow_read_time_ms: float = 0.0
    arrow_batch_materialize_time_ms: float = 0.0
    arrow_total_load_time_ms: float = 0.0
    arrow_resolved_read_block_size: int = 0

    @classmethod
    def from_mapping(cls, counters: Mapping[str, int | float] | None) -> SourceRuntimeMetrics:
        counters = counters or {}
        return cls(
            record_error_count=int(counters.get("record_error_count", 0)),
            record_drop_count=int(counters.get("record_drop_count", 0)),
            arrow_batch_count=int(counters.get("arrow_batch_count", 0)),
            arrow_max_batch_rows=int(counters.get("arrow_max_batch_rows", 0)),
            arrow_read_time_ms=float(counters.get("arrow_read_time_ms", 0.0)),
            arrow_batch_materialize_time_ms=float(
                counters.get("arrow_batch_materialize_time_ms", 0.0)
            ),
            arrow_total_load_time_ms=float(counters.get("arrow_total_load_time_ms", 0.0)),
            arrow_resolved_read_block_size=int(counters.get("arrow_resolved_read_block_size", 0)),
        )

    def to_dict(self) -> dict[str, int | float]:
        metrics: dict[str, int | float] = {
            "record_error_count": self.record_error_count,
            "record_drop_count": self.record_drop_count,
        }
        if self.arrow_batch_count:
            metrics["arrow_batch_count"] = self.arrow_batch_count
        if self.arrow_max_batch_rows:
            metrics["arrow_max_batch_rows"] = self.arrow_max_batch_rows
        if self.arrow_read_time_ms:
            metrics["arrow_read_time_ms"] = self.arrow_read_time_ms
        if self.arrow_batch_materialize_time_ms:
            metrics["arrow_batch_materialize_time_ms"] = self.arrow_batch_materialize_time_ms
        if self.arrow_total_load_time_ms:
            metrics["arrow_total_load_time_ms"] = self.arrow_total_load_time_ms
        if self.arrow_resolved_read_block_size:
            metrics["arrow_resolved_read_block_size"] = self.arrow_resolved_read_block_size
        return metrics


@runtime_checkable
class PrefetchCapableSource(Protocol[T_co]):
    """Capability protocol for sources that support bounded prefetch."""

    source_name: str
    supports_prefetch: bool
    prefetch_limit: int

    def stream(self) -> AsyncGenerator[T_co, None]: ...


@runtime_checkable
class RuntimeMetricsSource(Protocol):
    """Capability protocol for sources that expose runtime counters."""

    def runtime_metrics(self) -> SourceRuntimeMetrics: ...


@runtime_checkable
class DeliveryHookSource(Protocol):
    """Capability protocol for sources that expose post-delivery hooks."""

    def delivery_success_callback(self) -> Callable[[], Awaitable[None]] | None: ...


def is_prefetch_capable(source: object) -> TypeGuard[PrefetchCapableSource[Any]]:
    """Return True when *source* explicitly enables prefetch support."""
    return bool(getattr(source, "supports_prefetch", False)) and callable(
        getattr(source, "stream", None)
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


def source_has_delivery_success_callback(source: object) -> bool:
    """Return True when *source* exposes a delivery-success callback hook."""
    return isinstance(source, DeliveryHookSource)


def source_delivery_success_callback(
    source: object,
) -> Callable[[], Awaitable[None]] | None:
    """Return the current post-delivery hook for *source* when supported."""
    if isinstance(source, DeliveryHookSource):
        return source.delivery_success_callback()
    return None
