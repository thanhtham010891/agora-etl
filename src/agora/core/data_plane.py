"""Shared execution data-plane vocabulary for source -> middleware -> sink flow."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable


class DataPlane(StrEnum):
    """Logical shape of data as it crosses runtime boundaries."""

    PYTHON_ROWS = "python_rows"
    PYTHON_BATCHES = "python_batches"
    ARROW_BATCHES = "arrow_batches"


def ordered_unique_planes(planes: Iterable[DataPlane]) -> tuple[DataPlane, ...]:
    """Return stable unique planes in encounter order."""
    ordered: list[DataPlane] = []
    seen: set[DataPlane] = set()
    for plane in planes:
        if plane in seen:
            continue
        ordered.append(plane)
        seen.add(plane)
    return tuple(ordered)


@dataclass(frozen=True, slots=True)
class SourceDataPlaneSpec:
    """Advertised data-plane contract for a source."""

    source_name: str
    emitted_plane: DataPlane
    supports_batch_emit: bool
    emits_arrow_batches: bool


@dataclass(frozen=True, slots=True)
class SinkDataPlaneSpec:
    """Advertised write contract for one sink-like target."""

    sink_name: str
    accepted_planes: tuple[DataPlane, ...]
    native_planes: tuple[DataPlane, ...]

    def selected_plane_for(self, upstream: DataPlane) -> DataPlane:
        """Return the plane this sink will actually receive for *upstream*."""
        if upstream in self.native_planes:
            return upstream
        if upstream == DataPlane.ARROW_BATCHES and DataPlane.PYTHON_BATCHES in self.native_planes:
            return DataPlane.PYTHON_BATCHES
        return DataPlane.PYTHON_ROWS

    def downgraded_from(self, upstream: DataPlane) -> bool:
        """Return True when the sink cannot stay on *upstream* natively."""
        return self.selected_plane_for(upstream) != upstream
