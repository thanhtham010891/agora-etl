"""Base source abstraction."""

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

from agora.core.source._contracts import SourceRuntimeMetrics
from agora.core.source._data_plane import source_data_plane_spec_from_legacy_flags

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from types import TracebackType

    from agora.core.checkpoint import Checkpoint, CheckpointValue
    from agora.core.data_plane import DataPlane, SourceDataPlaneSpec

T = TypeVar("T")


class BaseSource(ABC, Generic[T]):
    """Abstract async source."""

    source_name: str = "source"
    supports_prefetch: bool = False
    prefetch_limit: int = 0
    supports_checkpoint: bool = False

    @abstractmethod
    def stream(self) -> AsyncGenerator[T, None]:
        """Yield records asynchronously."""

    async def open(self) -> None:
        """Called once before streaming starts."""

    async def close(self) -> None:
        """Called once after streaming ends."""

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        """Configure the source to resume from *checkpoint* if supported."""

    def current_checkpoint(self) -> CheckpointValue:
        """Return the current progress marker for checkpointing or DLQ metadata."""
        return None

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        """Return typed source-side counters for dropped/error records."""
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
        """Deprecated compatibility shim for older source implementations."""
        return {}

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        """Return the source-side data-plane contract used by runtime planning."""
        return source_data_plane_spec_from_legacy_flags(self, warn=True)

    @property
    def emitted_data_plane(self) -> DataPlane:
        """Convenience alias for the source's emitted plane."""
        return self.data_plane_spec().emitted_plane

    def limit(self, max_records: int | None) -> BaseSource[T]:
        """Return a source wrapper that emits at most *max_records* records."""
        from agora.core.source._wrappers import LimitedSource

        if max_records is None:
            return self
        if max_records < 0:
            raise ValueError(f"max_records must be >= 0, got {max_records}")
        return LimitedSource(self, max_records=max_records)

    async def __aenter__(self) -> BaseSource[T]:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        del exc_type, exc_val, exc_tb
        await self.close()
