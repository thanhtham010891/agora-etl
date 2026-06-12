"""Serialization helpers for explain models."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora.core.explain._models import (
        AccelerationExplain,
        MiddlewareStageExplain,
        PipelineExplain,
        SinkWriteExplain,
    )


def acceleration_explain_to_dict(acceleration: AccelerationExplain) -> dict[str, Any]:
    """Serialize acceleration decisions."""
    return {
        "mode": acceleration.mode,
        "profile": acceleration.profile,
        "profile_settings": dict(acceleration.profile_settings),
        "available": acceleration.available,
        "package_version": acceleration.package_version,
        "compatible": acceleration.compatible,
        "active_capabilities": list(acceleration.active_capabilities),
        "inactive_capabilities": dict(acceleration.inactive_capabilities),
        "source_prefetch_eligible": acceleration.source_prefetch_eligible,
        "source_prefetch_active": acceleration.source_prefetch_active,
        "source_prefetch_inactive_reason": acceleration.source_prefetch_inactive_reason,
        "direct_flush_eligible": acceleration.direct_flush_eligible,
        "direct_flush_inactive_reason": acceleration.direct_flush_inactive_reason,
        "expected_row_materialization_points": list(
            acceleration.expected_row_materialization_points
        ),
    }


def middleware_stage_to_dict(stage: MiddlewareStageExplain) -> dict[str, Any]:
    """Serialize one middleware explain row."""
    return {
        "index": stage.index,
        "name": stage.name,
        "data_plane": stage.data_plane.value,
    }


def sink_write_to_dict(sink: SinkWriteExplain) -> dict[str, Any]:
    """Serialize one sink write-mode explain row."""
    return {
        "sink_name": sink.sink_name,
        "accepted_data_planes": [plane.value for plane in sink.accepted_data_planes],
        "native_data_planes": [plane.value for plane in sink.native_data_planes],
        "selected_data_plane": sink.selected_data_plane.value,
        "downgraded_from_input": sink.downgraded_from_input,
        "selection_reason": sink.selection_reason,
    }


def pipeline_explain_to_dict(explain: PipelineExplain) -> dict[str, Any]:
    """Serialize the full pipeline explain summary."""
    return {
        "pipeline_id": explain.pipeline_id,
        "source_name": explain.source_name,
        "source_limit": explain.source_limit,
        "planned_lane": explain.planned_lane,
        "lane_reason": explain.lane_reason,
        "batch_source": explain.batch_source,
        "direct_flush_eligible": explain.direct_flush_eligible,
        "arrow_chain_eligible": explain.arrow_chain_eligible,
        "arrow_fast_path_eligible": explain.arrow_fast_path_eligible,
        "source_data_plane": explain.source_data_plane.value,
        "middleware_input_data_plane": explain.middleware_input_data_plane.value,
        "middleware_output_data_plane": explain.middleware_output_data_plane.value,
        "middleware_materializes_arrow_to_rows": explain.middleware_materializes_arrow_to_rows,
        "middleware_materialization_reason": explain.middleware_materialization_reason,
        "middleware_matrix": [stage.to_dict() for stage in explain.middleware_matrix],
        "writer_input_data_plane": explain.writer_input_data_plane.value,
        "writer_input_data_plane_reason": explain.writer_input_data_plane_reason,
        "sink_downgrade_count": explain.sink_downgrade_count,
        "sinks": [sink.to_dict() for sink in explain.sinks],
        "acceleration": explain.acceleration.to_dict(),
    }
