"""Base sink contract for Agora."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from agora.core.data_plane import DataPlane, SinkDataPlaneSpec
from agora.core.sink._support import (
    SinkCapabilities,
    normalized_sink_capabilities,
    sink_data_plane_spec,
    warn_legacy_sink_flags_once,
)

if TYPE_CHECKING:
    from types import TracebackType

T = TypeVar("T")


class BaseSink(ABC, Generic[T]):
    """Abstract async sink."""

    sink_name: str = "sink"
    batch_writable_native: bool = False
    arrow_passthrough_native: bool = False
    parallel_writes_safe: bool = False
    ordered_writes_required: bool = True
    accepted_data_planes: tuple[DataPlane, ...] = ()
    native_data_planes: tuple[DataPlane, ...] = ()

    @abstractmethod
    async def write(self, record: T) -> None:
        """Persist a single record."""

    async def open(self) -> None:
        """Called once before the pipeline loop starts."""

    def bind_context(self, ctx: Any) -> None:
        """Attach run-scoped context when a sink needs pipeline metadata."""

    def sink_capabilities(self) -> SinkCapabilities:
        """Execution hints used by writer/runtime strategy selection."""
        accepted_planes = self.accepted_data_planes
        native_planes = self.native_data_planes
        batch_writable_native = DataPlane.PYTHON_BATCHES in native_planes
        arrow_passthrough_native = DataPlane.ARROW_BATCHES in native_planes
        if not accepted_planes and not native_planes:
            batch_writable_native = self.batch_writable_native
            arrow_passthrough_native = self.arrow_passthrough_native
            if batch_writable_native or arrow_passthrough_native:
                warn_legacy_sink_flags_once(self)
        if not batch_writable_native and type(self).write_batch is not BaseSink.write_batch:
            batch_writable_native = True
        return normalized_sink_capabilities(
            SinkCapabilities(
                batch_writable_native=batch_writable_native,
                arrow_passthrough_native=arrow_passthrough_native,
                parallel_writes_safe=self.parallel_writes_safe,
                ordered_writes_required=self.ordered_writes_required,
                accepted_data_planes=accepted_planes,
                native_data_planes=native_planes,
            ),
            batch_native=batch_writable_native,
            arrow_native=arrow_passthrough_native,
        )

    def data_plane_spec(self) -> SinkDataPlaneSpec:
        """Return the sink-side data-plane contract used by runtime planning."""
        return sink_data_plane_spec(self)

    async def write_batch(self, records: list[T]) -> None:
        """Persist a batch of records."""
        for record in records:
            await self.write(record)

    async def flush(self) -> None:
        """Flush any buffered data."""

    async def close(self) -> None:
        """Flush and release all resources."""
        await self.flush()

    async def __aenter__(self) -> BaseSink[T]:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()
