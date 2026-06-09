"""Builders that map runtime plans into explain models."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agora.core.data_plane import DataPlane
from agora.core.explain._models import MiddlewareStageExplain, PipelineExplain, SinkWriteExplain

if TYPE_CHECKING:
    from agora.core.runtime import RuntimePlan


def build_pipeline_explain(
    *,
    pipeline_id: str,
    plan: RuntimePlan,
    source_limit: int | None = None,
) -> PipelineExplain:
    """Build a stable explain summary from a resolved runtime plan."""
    return PipelineExplain(
        pipeline_id=pipeline_id,
        source_name=plan.source_name,
        source_limit=source_limit,
        planned_lane=plan.lane.value,
        lane_reason=plan.lane_reason,
        batch_source=plan.batch_source,
        direct_flush_eligible=plan.writer.direct_flush_eligible,
        arrow_chain_eligible=plan.writer.arrow_chain,
        arrow_fast_path_eligible=plan.writer.arrow_fast_path,
        source_data_plane=plan.source.emitted_plane,
        middleware_input_data_plane=plan.middleware.input_data_plane,
        middleware_output_data_plane=plan.middleware.output_data_plane,
        middleware_materializes_arrow_to_rows=plan.middleware.materializes_arrow_to_rows,
        middleware_materialization_reason=plan.middleware.materialization_reason,
        middleware_matrix=tuple(
            MiddlewareStageExplain(
                index=stage.index,
                name=stage.name,
                data_plane=DataPlane(stage.data_plane.value),
            )
            for stage in plan.middleware.stages
        ),
        writer_input_data_plane=plan.writer.input_data_plane,
        writer_input_data_plane_reason=plan.writer.input_data_plane_reason,
        sink_downgrade_count=plan.writer.downgraded_sink_count,
        sinks=tuple(
            SinkWriteExplain(
                sink_name=sink.sink_name,
                accepted_data_planes=sink.accepted_data_planes,
                native_data_planes=sink.native_data_planes,
                selected_data_plane=sink.selected_data_plane,
                downgraded_from_input=sink.downgraded_from_input,
                selection_reason=sink.selection_reason,
            )
            for sink in plan.writer.sink_plans
        ),
    )
