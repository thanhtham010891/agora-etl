"""Built-in zero-dependency Prometheus exporter."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agora.metrics.exporters._prometheus_support import (
    append_metric_header,
    escape_label_value,
    exporter_content_type,
    extend_lines,
    render_runtime_lane_line,
    render_runtime_signal_lines,
    render_scrape_time_line,
)

if TYPE_CHECKING:
    from agora.metrics.collector import MetricsCollector


class PrometheusTextExporter:
    """Zero-dependency Prometheus text exporter."""

    def __init__(self, collector: MetricsCollector, namespace: str = "agora") -> None:
        self._collector = collector
        self._ns = namespace

    @property
    def content_type(self) -> str:
        return exporter_content_type()

    def render(self) -> str:
        ns = self._ns
        stats_by_pipeline = self._collector.all()
        lines: list[str] = []

        append_metric_header(
            lines,
            help_text="Registered pipelines known to the worker",
            metric_type="gauge",
            name=f"{ns}_pipeline_registered",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            schedule = escape_label_value(stats.schedule or "")
            lines.append(
                f'{ns}_pipeline_registered{{pipeline_id="{epid}",schedule="{schedule}"}} 1'
            )

        append_metric_header(
            lines,
            help_text="Whether the pipeline currently has an active run",
            metric_type="gauge",
            name=f"{ns}_pipeline_running",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            lines.append(f'{ns}_pipeline_running{{pipeline_id="{epid}"}} {int(stats.is_running)}')

        append_metric_header(
            lines,
            help_text="Duration of the current active run",
            metric_type="gauge",
            name=f"{ns}_pipeline_live_run_duration_seconds",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            lines.append(
                f'{ns}_pipeline_live_run_duration_seconds{{pipeline_id="{epid}"}} '
                f"{stats.active_run_duration_s:.6f}"
            )

        append_metric_header(
            lines,
            help_text="Records consumed per second in the active run",
            metric_type="gauge",
            name=f"{ns}_pipeline_live_throughput_rps",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            lines.append(
                f'{ns}_pipeline_live_throughput_rps{{pipeline_id="{epid}"}} '
                f"{stats.active_run_throughput_rps:.6f}"
            )

        append_metric_header(
            lines,
            help_text="Current active-run record counters",
            metric_type="gauge",
            name=f"{ns}_pipeline_live_records",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            for outcome, value in [
                ("consumed", stats.live_records_consumed),
                ("written", stats.live_records_written),
                ("dropped", stats.live_records_dropped),
                ("errored", stats.live_records_errored),
                ("pending", stats.live_records_pending),
            ]:
                label = f'{{pipeline_id="{epid}",outcome="{outcome}"}}'
                lines.append(f"{ns}_pipeline_live_records{label} {value}")

        append_metric_header(
            lines,
            help_text="Total pipeline runs",
            metric_type="counter",
            name=f"{ns}_pipeline_runs_total",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            lines.append(
                f'{ns}_pipeline_runs_total{{pipeline_id="{epid}",status="success"}} {stats.successful_runs}'
            )
            lines.append(
                f'{ns}_pipeline_runs_total{{pipeline_id="{epid}",status="failure"}} {stats.failed_runs}'
            )

        append_metric_header(
            lines,
            help_text="Total records processed",
            metric_type="counter",
            name=f"{ns}_pipeline_records_total",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            for outcome, value in [
                ("consumed", stats.total_records_consumed),
                ("written", stats.total_records_written),
                ("dropped", stats.total_records_dropped),
                ("errored", stats.total_records_errored),
            ]:
                lines.append(
                    f'{ns}_pipeline_records_total{{pipeline_id="{epid}",outcome="{outcome}"}} {value}'
                )

        append_metric_header(
            lines,
            help_text="Success rate (0.0-1.0)",
            metric_type="gauge",
            name=f"{ns}_pipeline_success_rate",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            lines.append(
                f'{ns}_pipeline_success_rate{{pipeline_id="{epid}"}} {stats.success_rate:.4f}'
            )

        append_metric_header(
            lines,
            help_text="Duration of the last completed run",
            metric_type="gauge",
            name=f"{ns}_pipeline_last_run_duration_seconds",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            lines.append(
                f'{ns}_pipeline_last_run_duration_seconds{{pipeline_id="{epid}"}} '
                f"{stats.last_run_duration_s:.6f}"
            )

        append_metric_header(
            lines,
            help_text="Records consumed per second in the last completed run",
            metric_type="gauge",
            name=f"{ns}_pipeline_last_run_throughput_rps",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            lines.append(
                f'{ns}_pipeline_last_run_throughput_rps{{pipeline_id="{epid}"}} '
                f"{stats.last_run_throughput_rps:.6f}"
            )

        append_metric_header(
            lines,
            help_text="Cumulative runtime-side observability counters",
            metric_type="counter",
            name=f"{ns}_pipeline_runtime_events_total",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            runtime_totals = [
                ("source_prefetch_block", stats.runtime.total_source_prefetch_block_count),
                ("rust_prefetch_run", stats.runtime.total_rust_prefetch_runs),
                ("rust_prefetch_wait", stats.runtime.total_rust_prefetch_wait_count),
                ("rust_prefetch_batch_drain", stats.runtime.total_rust_prefetch_batch_drain_count),
                ("rust_prefetch_push_batch", stats.runtime.total_rust_prefetch_push_batch_count),
                ("checkpoint_save", stats.runtime.total_checkpoint_save_count),
                ("checkpoint_failure", stats.runtime.total_checkpoint_failure_count),
                ("dlq_failure", stats.runtime.total_dlq_failure_count),
                ("writer_flush", stats.runtime.total_writer_flush_count),
                ("adaptive_scale_up", stats.runtime.total_adaptive_scale_up_count),
                ("adaptive_scale_down", stats.runtime.total_adaptive_scale_down_count),
                ("csv_arrow_native_batch", stats.runtime.total_csv_arrow_native_batch_count),
                ("csv_arrow_native_row", stats.runtime.total_csv_arrow_native_row_count),
                ("csv_arrow_downgrade_batch", stats.runtime.total_csv_arrow_downgrade_batch_count),
                ("csv_arrow_downgrade_row", stats.runtime.total_csv_arrow_downgrade_row_count),
            ]
            for event, value in runtime_totals:
                lines.append(
                    f'{ns}_pipeline_runtime_events_total{{pipeline_id="{epid}",event="{event}"}} {value}'
                )

        append_metric_header(
            lines,
            help_text="Last-run runtime gauges",
            metric_type="gauge",
            name=f"{ns}_pipeline_runtime_last",
        )
        for pid, stats in stats_by_pipeline.items():
            runtime = stats.runtime.last_runtime
            if runtime is None:
                continue
            extend_lines(
                lines,
                render_runtime_signal_lines(
                    metric_name=f"{ns}_pipeline_runtime_last",
                    pipeline_id=pid,
                    runtime=runtime,
                ),
            )

        append_metric_header(
            lines,
            help_text="Last-run execution lane marker",
            metric_type="gauge",
            name=f"{ns}_pipeline_runtime_lane_last",
        )
        for pid, stats in stats_by_pipeline.items():
            runtime = stats.runtime.last_runtime
            if runtime is None or not runtime.execution_lane:
                continue
            lines.append(
                render_runtime_lane_line(
                    metric_name=f"{ns}_pipeline_runtime_lane_last",
                    pipeline_id=pid,
                    lane=runtime.execution_lane,
                )
            )

        append_metric_header(
            lines,
            help_text="Current-run runtime gauges",
            metric_type="gauge",
            name=f"{ns}_pipeline_runtime_current",
        )
        for pid, stats in stats_by_pipeline.items():
            runtime = stats.live_runtime
            if runtime is None:
                continue
            extend_lines(
                lines,
                render_runtime_signal_lines(
                    metric_name=f"{ns}_pipeline_runtime_current",
                    pipeline_id=pid,
                    runtime=runtime,
                ),
            )

        append_metric_header(
            lines,
            help_text="Current-run execution lane marker",
            metric_type="gauge",
            name=f"{ns}_pipeline_runtime_lane_current",
        )
        for pid, stats in stats_by_pipeline.items():
            runtime = stats.live_runtime
            if runtime is None or not runtime.execution_lane:
                continue
            lines.append(
                render_runtime_lane_line(
                    metric_name=f"{ns}_pipeline_runtime_lane_current",
                    pipeline_id=pid,
                    lane=runtime.execution_lane,
                )
            )

        append_metric_header(
            lines,
            help_text="Cumulative middleware record counters",
            metric_type="counter",
            name=f"{ns}_pipeline_middleware_records_total",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            for middleware_name, middleware in stats.middlewares.items():
                emw = escape_label_value(middleware_name)
                for outcome, value in [
                    ("in", middleware.total_records_in),
                    ("out", middleware.total_records_out),
                    ("dropped", middleware.total_records_dropped),
                    ("errored", middleware.total_records_errored),
                ]:
                    lines.append(
                        f'{ns}_pipeline_middleware_records_total{{pipeline_id="{epid}",middleware="{emw}",outcome="{outcome}"}} {value}'
                    )

        append_metric_header(
            lines,
            help_text="Cumulative middleware processing time in milliseconds",
            metric_type="counter",
            name=f"{ns}_pipeline_middleware_time_ms_total",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            for middleware_name, middleware in stats.middlewares.items():
                emw = escape_label_value(middleware_name)
                lines.append(
                    f'{ns}_pipeline_middleware_time_ms_total{{pipeline_id="{epid}",middleware="{emw}"}} '
                    f"{middleware.total_time_ms:.6f}"
                )

        append_metric_header(
            lines,
            help_text="Last-run average middleware processing time in milliseconds",
            metric_type="gauge",
            name=f"{ns}_pipeline_middleware_last_avg_time_ms",
        )
        for pid, stats in stats_by_pipeline.items():
            epid = escape_label_value(pid)
            for middleware_name, middleware in stats.middlewares.items():
                emw = escape_label_value(middleware_name)
                lines.append(
                    f'{ns}_pipeline_middleware_last_avg_time_ms{{pipeline_id="{epid}",middleware="{emw}"}} '
                    f"{middleware.last_avg_time_ms:.6f}"
                )

        append_metric_header(
            lines,
            help_text="Slowest middleware average latency in the last completed run",
            metric_type="gauge",
            name=f"{ns}_pipeline_slowest_middleware_last_avg_time_ms",
        )
        for pid, stats in stats_by_pipeline.items():
            if stats.last_slowest_middleware is None:
                continue
            epid = escape_label_value(pid)
            emw = escape_label_value(stats.last_slowest_middleware)
            lines.append(
                f'{ns}_pipeline_slowest_middleware_last_avg_time_ms{{pipeline_id="{epid}",middleware="{emw}"}} '
                f"{stats.last_slowest_middleware_avg_time_ms:.6f}"
            )

        append_metric_header(
            lines,
            help_text="Worker process uptime",
            metric_type="gauge",
            name=f"{ns}_process_uptime_seconds",
        )
        health = self._collector.to_health_dict()
        lines.append(f"{ns}_process_uptime_seconds {health['uptime_seconds']}")
        lines.append(render_scrape_time_line())
        lines.append("")
        return "\n".join(lines)
