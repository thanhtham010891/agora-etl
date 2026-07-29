"""In-memory and limited source wrappers."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any, TypeVar

from agora.core.data_plane import DataPlane, SourceDataPlaneSpec
from agora.core.source._base import BaseSource
from agora.core.source._contracts import (
    SourceRuntimeMetrics,
    source_delivery_success_callback,
    source_has_delivery_success_callback,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator

    from agora.core.checkpoint import Checkpoint, CheckpointValue, SourceIdentity

T = TypeVar("T")


class IterableSource(BaseSource[T]):
    """Emit records from an in-memory iterable."""

    source_name = "iterable"

    def __init__(self, records: Any) -> None:
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

    def checkpoint_source_identity(self) -> SourceIdentity | None:
        """Preserve an upstream source identity through a limit wrapper."""
        from agora.core.checkpoint import checkpoint_source_identity

        return checkpoint_source_identity(self._source)

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return self._source.runtime_metrics()

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        upstream = self._source.data_plane_spec()
        return replace(upstream, source_name=self.source_name)

    def delivery_success_callback(self) -> Callable[[], Awaitable[None]] | None:
        if source_has_delivery_success_callback(self._source):
            return source_delivery_success_callback(self._source)
        return None

    async def stream(self) -> AsyncGenerator[T, None]:
        remaining = self._max_records
        if remaining <= 0:
            return
        upstream = self._source.stream().__aiter__()
        while remaining > 0:
            try:
                record = await anext(upstream)
            except StopAsyncIteration:
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
        upstream = self._source.stream_batches().__aiter__()  # type: ignore[attr-defined]
        while remaining > 0:
            try:
                batch = await anext(upstream)
            except StopAsyncIteration:
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
        iterator = iter(upstream())
        while remaining > 0:
            try:
                item = next(iterator)
            except StopIteration:
                break
            yield item
            remaining -= 1
