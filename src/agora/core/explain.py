"""Human-friendly runtime planning summaries for pre-run inspection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.data_plane import DataPlane

if TYPE_CHECKING:
    from agora.core.runtime import RuntimePlan


@dataclass(frozen=True, slots=True)
class MiddlewareStageExplain:
    """One middleware stage as rendered by ``Pipeline.explain()``."""

    index: int
    name: str
    data_plane: DataPlane

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "name": self.name,
            "data_plane": self.data_plane.value,
        }


@dataclass(frozen=True, slots=True)
class SinkWriteExplain:
    """Resolved per-sink write mode behind the writer."""

    sink_name: str
    accepted_data_planes: tuple[DataPlane, ...]
    native_data_planes: tuple[DataPlane, ...]
    selected_data_plane: DataPlane
    downgraded_from_input: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "sink_name": self.sink_name,
            "accepted_data_planes": [plane.value for plane in self.accepted_data_planes],
            "native_data_planes": [plane.value for plane in self.native_data_planes],
            "selected_data_plane": self.selected_data_plane.value,
            "downgraded_from_input": self.downgraded_from_input,
        }


@dataclass(frozen=True, slots=True)
class PipelineExplain:
    """Stable, serializable summary of the runtime plan before execution."""

    pipeline_id: str
    source_name: str
    source_limit: int | None
    planned_lane: str
    batch_source: bool
    direct_flush_eligible: bool
    arrow_chain_eligible: bool
    arrow_fast_path_eligible: bool
    source_data_plane: DataPlane
    middleware_input_data_plane: DataPlane
    middleware_output_data_plane: DataPlane
    middleware_materializes_arrow_to_rows: bool
    middleware_matrix: tuple[MiddlewareStageExplain, ...]
    writer_input_data_plane: DataPlane
    sink_downgrade_count: int
    sinks: tuple[SinkWriteExplain, ...]

    @classmethod
    def from_runtime_plan(
        cls,
        *,
        pipeline_id: str,
        plan: RuntimePlan,
        source_limit: int | None = None,
    ) -> PipelineExplain:
        return cls(
            pipeline_id=pipeline_id,
            source_name=plan.source_name,
            source_limit=source_limit,
            planned_lane=plan.lane.value,
            batch_source=plan.batch_source,
            direct_flush_eligible=plan.writer.direct_flush_eligible,
            arrow_chain_eligible=plan.writer.arrow_chain,
            arrow_fast_path_eligible=plan.writer.arrow_fast_path,
            source_data_plane=plan.source.emitted_plane,
            middleware_input_data_plane=plan.middleware.input_data_plane,
            middleware_output_data_plane=plan.middleware.output_data_plane,
            middleware_materializes_arrow_to_rows=plan.middleware.materializes_arrow_to_rows,
            middleware_matrix=tuple(
                MiddlewareStageExplain(
                    index=stage.index,
                    name=stage.name,
                    data_plane=DataPlane(stage.data_plane.value),
                )
                for stage in plan.middleware.stages
            ),
            writer_input_data_plane=plan.writer.input_data_plane,
            sink_downgrade_count=plan.writer.downgraded_sink_count,
            sinks=tuple(
                SinkWriteExplain(
                    sink_name=sink.sink_name,
                    accepted_data_planes=sink.accepted_data_planes,
                    native_data_planes=sink.native_data_planes,
                    selected_data_plane=sink.selected_data_plane,
                    downgraded_from_input=sink.downgraded_from_input,
                )
                for sink in plan.writer.sink_plans
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "source_name": self.source_name,
            "source_limit": self.source_limit,
            "planned_lane": self.planned_lane,
            "batch_source": self.batch_source,
            "direct_flush_eligible": self.direct_flush_eligible,
            "arrow_chain_eligible": self.arrow_chain_eligible,
            "arrow_fast_path_eligible": self.arrow_fast_path_eligible,
            "source_data_plane": self.source_data_plane.value,
            "middleware_input_data_plane": self.middleware_input_data_plane.value,
            "middleware_output_data_plane": self.middleware_output_data_plane.value,
            "middleware_materializes_arrow_to_rows": self.middleware_materializes_arrow_to_rows,
            "middleware_matrix": [stage.to_dict() for stage in self.middleware_matrix],
            "writer_input_data_plane": self.writer_input_data_plane.value,
            "sink_downgrade_count": self.sink_downgrade_count,
            "sinks": [sink.to_dict() for sink in self.sinks],
        }

    def __str__(self) -> str:
        parts = [
            f"pipeline={self.pipeline_id!r}",
            f"lane={self.planned_lane}",
            f"source_plane={self.source_data_plane.value}",
            f"writer_plane={self.writer_input_data_plane.value}",
        ]
        if self.source_limit is not None:
            parts.append(f"source_limit={self.source_limit}")
        if self.arrow_chain_eligible:
            parts.append("arrow_chain=on")
        if self.arrow_fast_path_eligible:
            parts.append("arrow_fast_path=on")
        if self.middleware_materializes_arrow_to_rows:
            parts.append("middleware_materialization=on")
        if self.sink_downgrade_count > 0:
            parts.append(f"sink_downgrades={self.sink_downgrade_count}")

        middleware = "<empty>"
        if self.middleware_matrix:
            middleware = " -> ".join(
                f"{stage.name}[{stage.data_plane.value}]" for stage in self.middleware_matrix
            )
        sinks = "<none>"
        if self.sinks:
            sinks = ", ".join(
                f"{sink.sink_name}:{sink.selected_data_plane.value}"
                f"{' (downgraded)' if sink.downgraded_from_input else ''}"
                for sink in self.sinks
            )
        return f"PipelineExplain({', '.join(parts)}, middleware={middleware}, sinks={sinks})"
