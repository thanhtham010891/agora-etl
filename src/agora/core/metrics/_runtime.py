"""Runtime pressure and execution-plane metrics."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RuntimeMetrics:
    """Runtime pressure signals captured during a pipeline run."""

    execution_lane: str = ""
    source_data_plane: str = ""
    writer_input_data_plane: str = ""
    direct_flush_active: bool = False
    arrow_fast_path_active: bool = False
    arrow_chain_active: bool = False
    writer_downgraded_sink_count: int = 0
    source_prefetch_enabled: bool = False
    source_prefetch_limit: int = 0
    source_prefetch_block_count: int = 0
    source_prefetch_max_depth: int = 0
    rust_prefetch_active: bool = False
    rust_prefetch_wait_count: int = 0
    rust_prefetch_batch_drain_count: int = 0
    rust_prefetch_push_batch_count: int = 0
    source_record_error_count: int = 0
    source_record_drop_count: int = 0
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
    writer_flush_count: int = 0
    writer_flush_max_batch_size: int = 0
    writer_flush_time_ms: float = 0.0
    csv_arrow_native_batch_count: int = 0
    csv_arrow_native_row_count: int = 0
    csv_arrow_downgrade_batch_count: int = 0
    csv_arrow_downgrade_row_count: int = 0

    def copy(self) -> RuntimeMetrics:
        from dataclasses import replace

        return replace(self)

    def to_dict(self) -> dict[str, int | bool | float | str]:
        return {
            "execution_lane": self.execution_lane,
            "source_data_plane": self.source_data_plane,
            "writer_input_data_plane": self.writer_input_data_plane,
            "direct_flush_active": self.direct_flush_active,
            "arrow_fast_path_active": self.arrow_fast_path_active,
            "arrow_chain_active": self.arrow_chain_active,
            "writer_downgraded_sink_count": self.writer_downgraded_sink_count,
            "source_prefetch_enabled": self.source_prefetch_enabled,
            "source_prefetch_limit": self.source_prefetch_limit,
            "source_prefetch_block_count": self.source_prefetch_block_count,
            "source_prefetch_max_depth": self.source_prefetch_max_depth,
            "rust_prefetch_active": self.rust_prefetch_active,
            "rust_prefetch_wait_count": self.rust_prefetch_wait_count,
            "rust_prefetch_batch_drain_count": self.rust_prefetch_batch_drain_count,
            "rust_prefetch_push_batch_count": self.rust_prefetch_push_batch_count,
            "source_record_error_count": self.source_record_error_count,
            "source_record_drop_count": self.source_record_drop_count,
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
            "writer_flush_count": self.writer_flush_count,
            "writer_flush_max_batch_size": self.writer_flush_max_batch_size,
            "writer_flush_time_ms": self.writer_flush_time_ms,
            "csv_arrow_native_batch_count": self.csv_arrow_native_batch_count,
            "csv_arrow_native_row_count": self.csv_arrow_native_row_count,
            "csv_arrow_downgrade_batch_count": self.csv_arrow_downgrade_batch_count,
            "csv_arrow_downgrade_row_count": self.csv_arrow_downgrade_row_count,
        }
