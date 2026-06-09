"""Support helpers for Prometheus text rendering."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

RUNTIME_GAUGES: tuple[tuple[str, str], ...] = (
    ("direct_flush_active", "direct_flush_active"),
    ("arrow_fast_path_active", "arrow_fast_path_active"),
    ("arrow_chain_active", "arrow_chain_active"),
    ("source_prefetch_limit", "source_prefetch_limit"),
    ("source_prefetch_max_depth", "source_prefetch_max_depth"),
    ("rust_prefetch_active", "rust_prefetch_active"),
    ("rust_prefetch_wait_count", "rust_prefetch_wait_count"),
    ("rust_prefetch_batch_drain_count", "rust_prefetch_batch_drain_count"),
    ("rust_prefetch_push_batch_count", "rust_prefetch_push_batch_count"),
    ("buffered_stage_limit", "buffered_stage_limit"),
    ("buffered_stage_max_in_flight", "buffered_stage_max_in_flight"),
    ("process_batch_stage_limit", "process_batch_stage_limit"),
    ("process_batch_stage_max_in_flight", "process_batch_stage_max_in_flight"),
    ("process_batch_stage_drain_count", "process_batch_stage_drain_count"),
    ("checkpoint_save_time_ms", "checkpoint_save_time_ms"),
    ("writer_flush_time_ms", "writer_flush_time_ms"),
    ("writer_flush_max_batch_size", "writer_flush_max_batch_size"),
    ("checkpoint_save_max_batch_size", "checkpoint_save_max_batch_size"),
    ("adaptive_backpressure_min_limit", "adaptive_backpressure_min_limit"),
    ("adaptive_backpressure_max_limit", "adaptive_backpressure_max_limit"),
    ("csv_arrow_native_batch_count", "csv_arrow_native_batch_count"),
    ("csv_arrow_native_row_count", "csv_arrow_native_row_count"),
    ("csv_arrow_downgrade_batch_count", "csv_arrow_downgrade_batch_count"),
    ("csv_arrow_downgrade_row_count", "csv_arrow_downgrade_row_count"),
)


def exporter_content_type() -> str:
    return _CONTENT_TYPE


def escape_label_value(value: str) -> str:
    """Escape a Prometheus label value per the text format spec."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def append_metric_header(lines: list[str], *, help_text: str, metric_type: str, name: str) -> None:
    lines.extend(
        [
            f"# HELP {name} {help_text}",
            f"# TYPE {name} {metric_type}",
        ]
    )


def render_runtime_signal_lines(
    *,
    metric_name: str,
    pipeline_id: str,
    runtime: object,
) -> list[str]:
    """Render gauge lines for the configured runtime signals."""
    escaped_pipeline = escape_label_value(pipeline_id)
    lines: list[str] = []
    for signal, attr_name in RUNTIME_GAUGES:
        value = getattr(runtime, attr_name)
        if isinstance(value, bool):
            value = int(value)
        lines.append(f'{metric_name}{{pipeline_id="{escaped_pipeline}",signal="{signal}"}} {value}')
    return lines


def render_runtime_lane_line(
    *,
    metric_name: str,
    pipeline_id: str,
    lane: str,
) -> str:
    escaped_pipeline = escape_label_value(pipeline_id)
    escaped_lane = escape_label_value(lane)
    return f'{metric_name}{{pipeline_id="{escaped_pipeline}",lane="{escaped_lane}"}} 1'


def render_scrape_time_line() -> str:
    return f"# scrape_time {time.time():.3f}"


def extend_lines(lines: list[str], values: Iterable[str]) -> None:
    lines.extend(values)
