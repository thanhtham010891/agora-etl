"""Builders that map runtime plans into explain models."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agora.core._pipeline_support import resolved_performance_profile_settings
from agora.core.acceleration import (
    AccelerationCapability,
    AccelerationMode,
    AccelerationStatus,
    acceleration_status,
    normalize_acceleration_mode,
)
from agora.core.data_plane import DataPlane
from agora.core.explain._models import (
    AccelerationExplain,
    MiddlewareStageExplain,
    PipelineExplain,
    SinkWriteExplain,
)
from agora.core.types import DeliveryConfig

if TYPE_CHECKING:
    from agora.core.runtime import RuntimePlan


def build_pipeline_explain(
    *,
    pipeline_id: str,
    plan: RuntimePlan,
    source_limit: int | None = None,
    config: DeliveryConfig | None = None,
) -> PipelineExplain:
    """Build a stable explain summary from a resolved runtime plan."""
    config = config or DeliveryConfig()
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
        acceleration=_build_acceleration_explain(plan=plan, config=config),
    )


def _build_acceleration_explain(
    *,
    plan: RuntimePlan,
    config: DeliveryConfig,
) -> AccelerationExplain:
    status = acceleration_status(config.acceleration_mode)
    active = tuple(sorted(capability.value for capability in status.capabilities if status.enabled))
    inactive = _inactive_capability_reasons(status)
    source_prefetch_active, source_prefetch_reason = _source_prefetch_state(
        plan=plan,
        config=config,
        status=status,
    )
    materialization_points: list[str] = []
    if plan.middleware.materializes_arrow_to_rows:
        materialization_points.append(
            plan.middleware.materialization_reason
            or "middleware materializes Arrow batches to rows"
        )
    if (
        plan.middleware.output_data_plane == DataPlane.ARROW_BATCHES
        and plan.writer.input_data_plane != DataPlane.ARROW_BATCHES
        and plan.writer.input_data_plane_reason
    ):
        materialization_points.append(plan.writer.input_data_plane_reason)
    elif plan.writer.input_data_plane == DataPlane.ARROW_BATCHES:
        for sink in plan.writer.sink_plans:
            if sink.downgraded_from_input:
                materialization_points.append(f"{sink.sink_name}: {sink.selection_reason}")
    direct_flush_reason: str | None = None
    if not plan.writer.direct_flush_eligible:
        direct_flush_reason = _direct_flush_inactive_reason(plan=plan, config=config, status=status)
    return AccelerationExplain(
        mode=status.mode.value,
        profile=config.performance_profile,
        profile_settings=resolved_performance_profile_settings(
            config,
            source_prefetch_limit=plan.source_prefetch_limit,
        ).to_dict(),
        available=status.enabled,
        package_version=status.version,
        compatible=status.compatible,
        active_capabilities=active,
        inactive_capabilities=inactive,
        source_prefetch_eligible=plan.source_supports_rust_prefetch
        and plan.source_has_sync_prefetch_path,
        source_prefetch_active=source_prefetch_active,
        source_prefetch_inactive_reason=source_prefetch_reason,
        direct_flush_eligible=plan.writer.direct_flush_eligible,
        direct_flush_inactive_reason=direct_flush_reason,
        expected_row_materialization_points=tuple(materialization_points),
    )


def _inactive_capability_reasons(status: AccelerationStatus) -> dict[str, str]:
    reason = status.reason
    inactive: dict[str, str] = {}
    for capability in AccelerationCapability:
        if status.supports(capability):
            continue
        inactive[capability.value] = reason or "capability is not exposed by agora-etl-rs"
    return inactive


def _direct_flush_inactive_reason(
    *,
    plan: RuntimePlan,
    config: DeliveryConfig,
    status: AccelerationStatus,
) -> str:
    if plan.lane.value != "linear":
        return f"direct flush only applies to the linear lane (planned lane: {plan.lane.value})"
    if config.batch_size <= 1:
        return "writer batch size is 1"
    if config.batch_flush_interval_ms is not None and config.batch_flush_interval_ms > 0:
        return "batch flush interval requires the pending-write owner path"
    if normalize_acceleration_mode(config.acceleration_mode) == AccelerationMode.OFF:
        return "acceleration is disabled"
    if not status.enabled:
        return status.reason or "Rust acceleration unavailable"
    return "writer shape is not safe for direct flush"


def _source_prefetch_state(
    *,
    plan: RuntimePlan,
    config: DeliveryConfig,
    status: AccelerationStatus,
) -> tuple[bool, str | None]:
    if normalize_acceleration_mode(config.acceleration_mode) == AccelerationMode.OFF:
        return False, "acceleration is disabled"
    if not status.enabled:
        return False, status.reason or "Rust acceleration unavailable"
    if not plan.source_supports_rust_prefetch:
        return False, "source has no sync prefetch path"
    if not plan.source_has_sync_prefetch_path:
        return False, "async-only source"
    if plan.uses_buffered_lane:
        return True, None
    if config.performance_profile == "throughput":
        return True, None
    return False, "benchmark gate not enabled for that lane"
