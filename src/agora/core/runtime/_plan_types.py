"""Runtime plan value objects for Agora pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from agora.core.data_plane import DataPlane, SourceDataPlaneSpec

if TYPE_CHECKING:
    from agora.core.middleware import MiddlewareModeSpec


class RuntimeLane(StrEnum):
    """Execution lane selected for a prepared pipeline run."""

    LINEAR = "linear"
    BUFFERED = "buffered"
    BATCH = "batch"


@dataclass(frozen=True, slots=True)
class BufferedStageSpec:
    """Runtime-selected buffered middleware stage."""

    index: int
    middleware: Any
    name: str
    concurrency: int


@dataclass(frozen=True, slots=True)
class MiddlewareExecutionPlan:
    """Derived middleware data-plane decisions for a single runtime plan."""

    stages: tuple[MiddlewareModeSpec, ...] = field(default_factory=tuple)
    input_data_plane: DataPlane = DataPlane.PYTHON_ROWS
    output_data_plane: DataPlane = DataPlane.PYTHON_ROWS
    materializes_arrow_to_rows: bool = False
    materialization_reason: str | None = None


@dataclass(frozen=True, slots=True)
class WriterSinkPlan:
    """Resolved sink-level write mode behind a writer."""

    sink_name: str
    accepted_data_planes: tuple[DataPlane, ...]
    native_data_planes: tuple[DataPlane, ...]
    selected_data_plane: DataPlane
    downgraded_from_input: bool = False
    selection_reason: str = ""


@dataclass(frozen=True, slots=True)
class WriterExecutionPlan:
    """Derived writer-side data-plane and write-path decisions."""

    batch_size: int
    input_data_plane: DataPlane = DataPlane.PYTHON_ROWS
    input_data_plane_reason: str = ""
    direct_flush_eligible: bool = False
    arrow_fast_path: bool = False
    arrow_chain: bool = False
    sink_plans: tuple[WriterSinkPlan, ...] = field(default_factory=tuple)

    @property
    def downgraded_sink_count(self) -> int:
        return sum(1 for sink in self.sink_plans if sink.downgraded_from_input)


@dataclass(frozen=True, slots=True)
class RuntimePlan:
    """Immutable execution plan built once before pipeline run starts."""

    lane: RuntimeLane
    lane_reason: str
    source_name: str
    batch_source: bool
    has_delivery_hooks: bool
    source: SourceDataPlaneSpec = field(
        default_factory=lambda: SourceDataPlaneSpec(
            source_name="source",
            emitted_plane=DataPlane.PYTHON_ROWS,
            supports_batch_emit=False,
            emits_arrow_batches=False,
        )
    )
    middleware: MiddlewareExecutionPlan = field(default_factory=MiddlewareExecutionPlan)
    buffered_stages: tuple[BufferedStageSpec, ...] = field(default_factory=tuple)
    writer: WriterExecutionPlan = field(default_factory=lambda: WriterExecutionPlan(batch_size=1))

    @property
    def uses_buffered_lane(self) -> bool:
        return self.lane == RuntimeLane.BUFFERED

    @property
    def uses_batch_lane(self) -> bool:
        return self.lane == RuntimeLane.BATCH
