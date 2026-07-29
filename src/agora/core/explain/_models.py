"""Serializable explain models for pre-run pipeline inspection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora.core.data_plane import DataPlane
    from agora.core.delivery import DeliveryCapability
    from agora.core.runtime import RuntimePlan
    from agora.core.types import DeliveryConfig


@dataclass(frozen=True, slots=True)
class AccelerationExplain:
    """Acceleration decisions shown by ``Pipeline.explain()``."""

    mode: str
    profile: str
    profile_settings: dict[str, Any]
    available: bool
    package_version: str | None
    compatible: bool
    active_capabilities: tuple[str, ...]
    inactive_capabilities: dict[str, str]
    source_prefetch_eligible: bool
    source_prefetch_active: bool
    source_prefetch_inactive_reason: str | None
    direct_flush_eligible: bool
    direct_flush_inactive_reason: str | None
    expected_row_materialization_points: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        from agora.core.explain._serialization import acceleration_explain_to_dict

        return acceleration_explain_to_dict(self)


@dataclass(frozen=True, slots=True)
class MiddlewareStageExplain:
    """One middleware stage as rendered by ``Pipeline.explain()``."""

    index: int
    name: str
    data_plane: DataPlane

    def to_dict(self) -> dict[str, Any]:
        from agora.core.explain._serialization import middleware_stage_to_dict

        return middleware_stage_to_dict(self)


@dataclass(frozen=True, slots=True)
class SinkWriteExplain:
    """Resolved per-sink write mode behind the writer."""

    sink_name: str
    accepted_data_planes: tuple[DataPlane, ...]
    native_data_planes: tuple[DataPlane, ...]
    selected_data_plane: DataPlane
    downgraded_from_input: bool = False
    selection_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        from agora.core.explain._serialization import sink_write_to_dict

        return sink_write_to_dict(self)


@dataclass(frozen=True, slots=True)
class PipelineExplain:
    """Stable, serializable summary of the runtime plan before execution."""

    pipeline_id: str
    source_name: str
    source_limit: int | None
    planned_lane: str
    lane_reason: str
    batch_source: bool
    direct_flush_eligible: bool
    arrow_chain_eligible: bool
    arrow_fast_path_eligible: bool
    source_data_plane: DataPlane
    middleware_input_data_plane: DataPlane
    middleware_output_data_plane: DataPlane
    middleware_materializes_arrow_to_rows: bool
    middleware_materialization_reason: str | None
    middleware_matrix: tuple[MiddlewareStageExplain, ...]
    writer_input_data_plane: DataPlane
    writer_input_data_plane_reason: str
    sink_downgrade_count: int
    sinks: tuple[SinkWriteExplain, ...]
    acceleration: AccelerationExplain
    delivery: DeliveryCapability

    @classmethod
    def from_runtime_plan(
        cls,
        *,
        pipeline_id: str,
        plan: RuntimePlan,
        source_limit: int | None = None,
        config: DeliveryConfig | None = None,
        source: object | None = None,
        writer: object | None = None,
    ) -> PipelineExplain:
        from agora.core.explain._builders import build_pipeline_explain

        return build_pipeline_explain(
            pipeline_id=pipeline_id,
            plan=plan,
            source_limit=source_limit,
            config=config,
            source=source,
            writer=writer,
        )

    def to_dict(self) -> dict[str, Any]:
        from agora.core.explain._serialization import pipeline_explain_to_dict

        return pipeline_explain_to_dict(self)

    def __str__(self) -> str:
        from agora.core.explain._render import render_pipeline_explain

        return render_pipeline_explain(self)
