"""String rendering helpers for explain summaries."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agora.core.explain._models import PipelineExplain


def render_pipeline_explain(explain: PipelineExplain) -> str:
    """Render a compact human-readable explain summary."""
    parts = [
        f"pipeline={explain.pipeline_id!r}",
        f"lane={explain.planned_lane}",
        f"source_plane={explain.source_data_plane.value}",
        f"writer_plane={explain.writer_input_data_plane.value}",
    ]
    parts.append(f"lane_reason={explain.lane_reason!r}")
    if explain.source_limit is not None:
        parts.append(f"source_limit={explain.source_limit}")
    if explain.arrow_chain_eligible:
        parts.append("arrow_chain=on")
    if explain.arrow_fast_path_eligible:
        parts.append("arrow_fast_path=on")
    if explain.middleware_materializes_arrow_to_rows:
        parts.append("middleware_materialization=on")
    if explain.middleware_materialization_reason is not None:
        parts.append(f"middleware_reason={explain.middleware_materialization_reason!r}")
    parts.append(f"writer_reason={explain.writer_input_data_plane_reason!r}")
    if explain.sink_downgrade_count > 0:
        parts.append(f"sink_downgrades={explain.sink_downgrade_count}")
    acceleration = explain.acceleration
    parts.append(
        "acceleration="
        f"{acceleration.mode}/{'available' if acceleration.available else 'unavailable'}"
    )
    parts.append(f"performance_profile={acceleration.profile}")
    if acceleration.profile_settings:
        rendered_settings = ",".join(
            f"{key}={value}" for key, value in acceleration.profile_settings.items()
        )
        parts.append(f"profile_settings={rendered_settings}")
    if acceleration.package_version is not None:
        parts.append(f"agora_etl_rs={acceleration.package_version}")
    parts.append(f"agora_etl_rs_compatible={acceleration.compatible}")
    if acceleration.active_capabilities:
        parts.append(f"rust_paths={','.join(acceleration.active_capabilities)}")
    if acceleration.source_prefetch_active:
        parts.append("source_prefetch=rust")
    elif acceleration.source_prefetch_inactive_reason:
        parts.append(f"source_prefetch_reason={acceleration.source_prefetch_inactive_reason!r}")
    if acceleration.direct_flush_inactive_reason:
        parts.append(f"direct_flush_reason={acceleration.direct_flush_inactive_reason!r}")
    if acceleration.expected_row_materialization_points:
        parts.append(
            f"row_materialization={'; '.join(acceleration.expected_row_materialization_points)!r}"
        )

    middleware = "<empty>"
    if explain.middleware_matrix:
        middleware = " -> ".join(
            f"{stage.name}[{stage.data_plane.value}]" for stage in explain.middleware_matrix
        )

    sinks = "<none>"
    if explain.sinks:
        sinks = ", ".join(
            f"{sink.sink_name}:{sink.selected_data_plane.value}"
            f"{' (downgraded)' if sink.downgraded_from_input else ''}"
            f" [{sink.selection_reason}]"
            for sink in explain.sinks
        )

    return f"PipelineExplain({', '.join(parts)}, middleware={middleware}, sinks={sinks})"
