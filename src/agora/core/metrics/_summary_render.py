"""String rendering helpers for run summaries."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agora.core.metrics._summary import PipelineRunSummary


def render_pipeline_run_summary(summary: PipelineRunSummary) -> str:
    """Render a compact run summary for logs and REPL output."""
    parts = [
        f"consumed={summary.records_consumed}",
        f"written={summary.records_written}",
        f"dropped={summary.records_dropped}",
        f"errors={summary.records_errored}",
        f"elapsed={summary.elapsed_seconds:.1f}s",
    ]
    runtime = summary.runtime
    if runtime.execution_lane:
        parts.append(f"lane={runtime.execution_lane}")
    if runtime.source_data_plane:
        parts.append(f"source_plane={runtime.source_data_plane}")
    if runtime.writer_input_data_plane:
        parts.append(f"writer_plane={runtime.writer_input_data_plane}")
    if runtime.acceleration_mode:
        parts.append(f"acceleration={runtime.acceleration_mode}")
    if runtime.acceleration_package_version:
        parts.append(f"agora_etl_rs={runtime.acceleration_package_version}")
    if runtime.rust_checkpoint_state_active:
        parts.append("rust_checkpoint_state=on")
    if runtime.rust_metrics_accumulator_active:
        parts.append("rust_metrics_accumulator=on")
    if runtime.rust_linear_batch_buffer_active:
        parts.append("rust_linear_batch_buffer=on")
    if runtime.rust_record_buffer_active:
        parts.append("rust_record_buffer=on")
    if runtime.direct_flush_active:
        parts.append("direct_flush=on")
    elif runtime.direct_flush_inactive_reason:
        parts.append(f"direct_flush_reason={runtime.direct_flush_inactive_reason}")
    if runtime.arrow_chain_active:
        parts.append("arrow_chain=on")
    if runtime.arrow_fast_path_active:
        parts.append("arrow_fast_path=on")
    if runtime.writer_downgraded_sink_count > 0:
        parts.append(f"sink_downgrades={runtime.writer_downgraded_sink_count}")
    if runtime.csv_arrow_native_batch_count > 0 or runtime.csv_arrow_native_row_count > 0:
        parts.append(
            "csv_arrow_native="
            f"{runtime.csv_arrow_native_batch_count}b/{runtime.csv_arrow_native_row_count}r"
        )
    if runtime.csv_arrow_downgrade_batch_count > 0 or runtime.csv_arrow_downgrade_row_count > 0:
        parts.append(
            "csv_arrow_downgraded="
            f"{runtime.csv_arrow_downgrade_batch_count}b/{runtime.csv_arrow_downgrade_row_count}r"
        )
    if summary.ai is not None:
        parts.append(f"llm_calls={summary.ai.total_llm_calls}")
        parts.append(f"tokens={summary.ai.total_tokens}")
    return f"PipelineRunSummary({', '.join(parts)})"
