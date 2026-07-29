"""Runtime pressure and execution-plane metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RuntimeMetrics:
    """Runtime pressure signals captured during a pipeline run."""

    execution_lane: str = ""
    source_data_plane: str = ""
    writer_input_data_plane: str = ""
    acceleration_mode: str = "auto"
    acceleration_profile: str = "balanced"
    acceleration_profile_settings: dict[str, Any] = field(default_factory=dict)
    acceleration_available: bool = False
    acceleration_package_version: str = ""
    acceleration_compatible: bool = False
    acceleration_capabilities: tuple[str, ...] = ()
    rust_checkpoint_state_active: bool = False
    rust_metrics_accumulator_active: bool = False
    rust_linear_batch_buffer_active: bool = False
    rust_record_buffer_active: bool = False
    direct_flush_eligible: bool = False
    direct_flush_active: bool = False
    direct_flush_inactive_reason: str = ""
    arrow_fast_path_active: bool = False
    arrow_chain_active: bool = False
    writer_downgraded_sink_count: int = 0
    source_prefetch_enabled: bool = False
    source_prefetch_limit: int = 0
    source_prefetch_block_count: int = 0
    source_prefetch_max_depth: int = 0
    rust_prefetch_active: bool = False
    rust_prefetch_inactive_reason: str = ""
    rust_prefetch_wait_count: int = 0
    rust_prefetch_batch_drain_count: int = 0
    rust_prefetch_push_batch_count: int = 0
    source_record_error_count: int = 0
    source_record_drop_count: int = 0
    source_arrow_batch_count: int = 0
    source_arrow_max_batch_rows: int = 0
    source_arrow_read_time_ms: float = 0.0
    source_arrow_batch_materialize_time_ms: float = 0.0
    source_arrow_total_load_time_ms: float = 0.0
    source_arrow_read_block_size_bytes: int = 0
    buffered_stage_limit: int = 0
    buffered_stage_max_in_flight: int = 0
    buffered_stage_drain_count: int = 0
    process_batch_stage_limit: int = 0
    process_batch_stage_max_in_flight: int = 0
    process_batch_stage_drain_count: int = 0
    adaptive_backpressure_enabled: bool = False
    adaptive_backpressure_min_limit: int = 0
    adaptive_backpressure_max_limit: int = 0
    adaptive_backpressure_scale_up_count: int = 0
    adaptive_backpressure_scale_down_count: int = 0
    checkpoint_enabled: bool = False
    checkpoint_save_count: int = 0
    checkpoint_save_max_batch_size: int = 0
    checkpoint_save_time_ms: float = 0.0
    checkpoint_failure_count: int = 0
    dlq_failure_count: int = 0
    failure_classification_counts: dict[str, int] = field(default_factory=dict)
    failure_alert_severity_counts: dict[str, int] = field(default_factory=dict)
    last_failure_decision: dict[str, Any] | None = None
    writer_flush_count: int = 0
    writer_flush_max_batch_size: int = 0
    writer_flush_time_ms: float = 0.0
    csv_arrow_native_batch_count: int = 0
    csv_arrow_native_row_count: int = 0
    csv_arrow_native_rendered_bytes: int = 0
    csv_arrow_native_serialize_time_ms: float = 0.0
    csv_arrow_native_buffer_copy_time_ms: float = 0.0
    csv_arrow_native_boundary_write_time_ms: float = 0.0
    csv_arrow_native_rust_boundary_batch_count: int = 0
    csv_arrow_rust_import_time_ms: float = 0.0
    csv_arrow_rust_file_open_time_ms: float = 0.0
    csv_arrow_rust_metadata_time_ms: float = 0.0
    csv_arrow_rust_writer_build_time_ms: float = 0.0
    csv_arrow_rust_header_render_time_ms: float = 0.0
    csv_arrow_rust_column_build_time_ms: float = 0.0
    csv_arrow_rust_row_render_time_ms: float = 0.0
    csv_arrow_rust_writer_write_time_ms: float = 0.0
    csv_arrow_rust_file_flush_time_ms: float = 0.0
    csv_arrow_downgrade_batch_count: int = 0
    csv_arrow_downgrade_row_count: int = 0
    csv_arrow_downgrade_fallback_time_ms: float = 0.0

    def copy(self) -> RuntimeMetrics:
        from dataclasses import replace

        return replace(
            self,
            failure_classification_counts=dict(self.failure_classification_counts),
            failure_alert_severity_counts=dict(self.failure_alert_severity_counts),
            last_failure_decision=(
                None if self.last_failure_decision is None else dict(self.last_failure_decision)
            ),
        )

    def record_failure_decision(self, decision: dict[str, Any]) -> None:
        """Record the decision that governed retry, DLQ, and alert behavior."""
        classification = str(decision["classification"])
        severity = str(decision["alert_severity"])
        self.failure_classification_counts[classification] = (
            self.failure_classification_counts.get(classification, 0) + 1
        )
        self.failure_alert_severity_counts[severity] = (
            self.failure_alert_severity_counts.get(severity, 0) + 1
        )
        self.last_failure_decision = dict(decision)

    def to_dict(
        self,
    ) -> dict[str, int | bool | float | str | tuple[str, ...] | dict[str, Any] | None]:
        return {
            "execution_lane": self.execution_lane,
            "source_data_plane": self.source_data_plane,
            "writer_input_data_plane": self.writer_input_data_plane,
            "acceleration_mode": self.acceleration_mode,
            "acceleration_profile": self.acceleration_profile,
            "acceleration_profile_settings": dict(self.acceleration_profile_settings),
            "acceleration_available": self.acceleration_available,
            "acceleration_package_version": self.acceleration_package_version,
            "acceleration_compatible": self.acceleration_compatible,
            "acceleration_capabilities": self.acceleration_capabilities,
            "rust_checkpoint_state_active": self.rust_checkpoint_state_active,
            "rust_metrics_accumulator_active": self.rust_metrics_accumulator_active,
            "rust_linear_batch_buffer_active": self.rust_linear_batch_buffer_active,
            "rust_record_buffer_active": self.rust_record_buffer_active,
            "direct_flush_eligible": self.direct_flush_eligible,
            "direct_flush_active": self.direct_flush_active,
            "direct_flush_inactive_reason": self.direct_flush_inactive_reason,
            "arrow_fast_path_active": self.arrow_fast_path_active,
            "arrow_chain_active": self.arrow_chain_active,
            "writer_downgraded_sink_count": self.writer_downgraded_sink_count,
            "source_prefetch_enabled": self.source_prefetch_enabled,
            "source_prefetch_limit": self.source_prefetch_limit,
            "source_prefetch_block_count": self.source_prefetch_block_count,
            "source_prefetch_max_depth": self.source_prefetch_max_depth,
            "rust_prefetch_active": self.rust_prefetch_active,
            "rust_prefetch_inactive_reason": self.rust_prefetch_inactive_reason,
            "rust_prefetch_wait_count": self.rust_prefetch_wait_count,
            "rust_prefetch_batch_drain_count": self.rust_prefetch_batch_drain_count,
            "rust_prefetch_push_batch_count": self.rust_prefetch_push_batch_count,
            "source_record_error_count": self.source_record_error_count,
            "source_record_drop_count": self.source_record_drop_count,
            "source_arrow_batch_count": self.source_arrow_batch_count,
            "source_arrow_max_batch_rows": self.source_arrow_max_batch_rows,
            "source_arrow_read_time_ms": self.source_arrow_read_time_ms,
            "source_arrow_batch_materialize_time_ms": self.source_arrow_batch_materialize_time_ms,
            "source_arrow_total_load_time_ms": self.source_arrow_total_load_time_ms,
            "source_arrow_read_block_size_bytes": self.source_arrow_read_block_size_bytes,
            "buffered_stage_limit": self.buffered_stage_limit,
            "buffered_stage_max_in_flight": self.buffered_stage_max_in_flight,
            "buffered_stage_drain_count": self.buffered_stage_drain_count,
            "process_batch_stage_limit": self.process_batch_stage_limit,
            "process_batch_stage_max_in_flight": self.process_batch_stage_max_in_flight,
            "process_batch_stage_drain_count": self.process_batch_stage_drain_count,
            "adaptive_backpressure_enabled": self.adaptive_backpressure_enabled,
            "adaptive_backpressure_min_limit": self.adaptive_backpressure_min_limit,
            "adaptive_backpressure_max_limit": self.adaptive_backpressure_max_limit,
            "adaptive_backpressure_scale_up_count": self.adaptive_backpressure_scale_up_count,
            "adaptive_backpressure_scale_down_count": self.adaptive_backpressure_scale_down_count,
            "checkpoint_enabled": self.checkpoint_enabled,
            "checkpoint_save_count": self.checkpoint_save_count,
            "checkpoint_save_max_batch_size": self.checkpoint_save_max_batch_size,
            "checkpoint_save_time_ms": self.checkpoint_save_time_ms,
            "checkpoint_failure_count": self.checkpoint_failure_count,
            "dlq_failure_count": self.dlq_failure_count,
            "failure_classification_counts": dict(self.failure_classification_counts),
            "failure_alert_severity_counts": dict(self.failure_alert_severity_counts),
            "last_failure_decision": (
                None if self.last_failure_decision is None else dict(self.last_failure_decision)
            ),
            "writer_flush_count": self.writer_flush_count,
            "writer_flush_max_batch_size": self.writer_flush_max_batch_size,
            "writer_flush_time_ms": self.writer_flush_time_ms,
            "csv_arrow_native_batch_count": self.csv_arrow_native_batch_count,
            "csv_arrow_native_row_count": self.csv_arrow_native_row_count,
            "csv_arrow_native_rendered_bytes": self.csv_arrow_native_rendered_bytes,
            "csv_arrow_native_serialize_time_ms": self.csv_arrow_native_serialize_time_ms,
            "csv_arrow_native_buffer_copy_time_ms": self.csv_arrow_native_buffer_copy_time_ms,
            "csv_arrow_native_boundary_write_time_ms": self.csv_arrow_native_boundary_write_time_ms,
            "csv_arrow_native_rust_boundary_batch_count": (
                self.csv_arrow_native_rust_boundary_batch_count
            ),
            "csv_arrow_rust_import_time_ms": self.csv_arrow_rust_import_time_ms,
            "csv_arrow_rust_file_open_time_ms": self.csv_arrow_rust_file_open_time_ms,
            "csv_arrow_rust_metadata_time_ms": self.csv_arrow_rust_metadata_time_ms,
            "csv_arrow_rust_writer_build_time_ms": self.csv_arrow_rust_writer_build_time_ms,
            "csv_arrow_rust_header_render_time_ms": self.csv_arrow_rust_header_render_time_ms,
            "csv_arrow_rust_column_build_time_ms": self.csv_arrow_rust_column_build_time_ms,
            "csv_arrow_rust_row_render_time_ms": self.csv_arrow_rust_row_render_time_ms,
            "csv_arrow_rust_writer_write_time_ms": self.csv_arrow_rust_writer_write_time_ms,
            "csv_arrow_rust_file_flush_time_ms": self.csv_arrow_rust_file_flush_time_ms,
            "csv_arrow_downgrade_batch_count": self.csv_arrow_downgrade_batch_count,
            "csv_arrow_downgrade_row_count": self.csv_arrow_downgrade_row_count,
            "csv_arrow_downgrade_fallback_time_ms": self.csv_arrow_downgrade_fallback_time_ms,
        }
