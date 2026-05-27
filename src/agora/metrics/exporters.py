"""Metrics exporter registry and built-in Prometheus-compatible fallback."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from agora.core.registry import Registry

if TYPE_CHECKING:
    from agora.metrics.collector import MetricsCollector

_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _escape_label_value(value: str) -> str:
    """Escape a Prometheus label value per the text format spec."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


@runtime_checkable
class MetricsExporter(Protocol):
    @property
    def content_type(self) -> str: ...

    def render(self) -> str: ...


class PrometheusTextExporter:
    """Zero-dependency Prometheus text exporter."""

    def __init__(self, collector: MetricsCollector, namespace: str = "agora") -> None:
        self._collector = collector
        self._ns = namespace

    @property
    def content_type(self) -> str:
        return _CONTENT_TYPE

    def render(self) -> str:
        ns = self._ns
        stats_by_pipeline = self._collector.all()
        lines: list[str] = [
            f"# HELP {ns}_pipeline_runs_total Total pipeline runs",
            f"# TYPE {ns}_pipeline_runs_total counter",
        ]
        for pid, stats in stats_by_pipeline.items():
            epid = _escape_label_value(pid)
            label = f'{{pipeline_id="{epid}",status="success"}}'
            lines.append(f"{ns}_pipeline_runs_total{label} {stats.successful_runs}")
            label = f'{{pipeline_id="{epid}",status="failure"}}'
            lines.append(f"{ns}_pipeline_runs_total{label} {stats.failed_runs}")

        lines += [
            f"# HELP {ns}_pipeline_records_total Total records processed",
            f"# TYPE {ns}_pipeline_records_total counter",
        ]
        for pid, stats in stats_by_pipeline.items():
            epid = _escape_label_value(pid)
            for outcome, val in [
                ("consumed", stats.total_records_consumed),
                ("written", stats.total_records_written),
                ("dropped", stats.total_records_dropped),
                ("errored", stats.total_records_errored),
            ]:
                label = f'{{pipeline_id="{epid}",outcome="{outcome}"}}'
                lines.append(f"{ns}_pipeline_records_total{label} {val}")

        lines += [
            f"# HELP {ns}_pipeline_success_rate Success rate (0.0-1.0)",
            f"# TYPE {ns}_pipeline_success_rate gauge",
        ]
        for pid, stats in stats_by_pipeline.items():
            epid = _escape_label_value(pid)
            lines.append(
                f'{ns}_pipeline_success_rate{{pipeline_id="{epid}"}} {stats.success_rate:.4f}'
            )

        lines += [
            f"# HELP {ns}_pipeline_last_run_duration_seconds Duration of the last completed run",
            f"# TYPE {ns}_pipeline_last_run_duration_seconds gauge",
        ]
        for pid, stats in stats_by_pipeline.items():
            epid = _escape_label_value(pid)
            lines.append(
                f'{ns}_pipeline_last_run_duration_seconds{{pipeline_id="{epid}"}} '
                f"{stats.last_run_duration_s:.6f}"
            )

        lines += [
            f"# HELP {ns}_pipeline_last_run_throughput_rps Records consumed per second in the last completed run",
            f"# TYPE {ns}_pipeline_last_run_throughput_rps gauge",
        ]
        for pid, stats in stats_by_pipeline.items():
            epid = _escape_label_value(pid)
            lines.append(
                f'{ns}_pipeline_last_run_throughput_rps{{pipeline_id="{epid}"}} '
                f"{stats.last_run_throughput_rps:.6f}"
            )

        lines += [
            f"# HELP {ns}_pipeline_runtime_events_total Cumulative runtime-side observability counters",
            f"# TYPE {ns}_pipeline_runtime_events_total counter",
        ]
        for pid, stats in stats_by_pipeline.items():
            epid = _escape_label_value(pid)
            runtime_totals = [
                ("source_prefetch_block", stats.runtime.total_source_prefetch_block_count),
                ("checkpoint_save", stats.runtime.total_checkpoint_save_count),
                ("checkpoint_failure", stats.runtime.total_checkpoint_failure_count),
                ("dlq_failure", stats.runtime.total_dlq_failure_count),
                ("writer_flush", stats.runtime.total_writer_flush_count),
                ("adaptive_scale_up", stats.runtime.total_adaptive_scale_up_count),
                ("adaptive_scale_down", stats.runtime.total_adaptive_scale_down_count),
            ]
            for event, value in runtime_totals:
                lines.append(
                    f'{ns}_pipeline_runtime_events_total{{pipeline_id="{epid}",event="{event}"}} {value}'
                )

        lines += [
            f"# HELP {ns}_pipeline_runtime_last Last-run runtime gauges",
            f"# TYPE {ns}_pipeline_runtime_last gauge",
        ]
        runtime_gauges = [
            ("source_prefetch_limit", "source_prefetch_limit"),
            ("source_prefetch_max_depth", "source_prefetch_max_depth"),
            ("buffered_stage_limit", "buffered_stage_limit"),
            ("buffered_stage_max_in_flight", "buffered_stage_max_in_flight"),
            ("checkpoint_save_time_ms", "checkpoint_save_time_ms"),
            ("writer_flush_time_ms", "writer_flush_time_ms"),
            ("writer_flush_max_batch_size", "writer_flush_max_batch_size"),
            ("checkpoint_save_max_batch_size", "checkpoint_save_max_batch_size"),
            ("adaptive_backpressure_min_limit", "adaptive_backpressure_min_limit"),
            ("adaptive_backpressure_max_limit", "adaptive_backpressure_max_limit"),
        ]
        for pid, stats in stats_by_pipeline.items():
            epid = _escape_label_value(pid)
            runtime = stats.runtime.last_runtime
            if runtime is None:
                continue
            for signal, attr_name in runtime_gauges:
                lines.append(
                    f'{ns}_pipeline_runtime_last{{pipeline_id="{epid}",signal="{signal}"}} '
                    f"{getattr(runtime, attr_name)}"
                )

        lines += [
            f"# HELP {ns}_pipeline_middleware_records_total Cumulative middleware record counters",
            f"# TYPE {ns}_pipeline_middleware_records_total counter",
        ]
        for pid, stats in stats_by_pipeline.items():
            epid = _escape_label_value(pid)
            for middleware_name, middleware in stats.middlewares.items():
                emw = _escape_label_value(middleware_name)
                for outcome, value in [
                    ("in", middleware.total_records_in),
                    ("out", middleware.total_records_out),
                    ("dropped", middleware.total_records_dropped),
                    ("errored", middleware.total_records_errored),
                ]:
                    lines.append(
                        f'{ns}_pipeline_middleware_records_total{{pipeline_id="{epid}",middleware="{emw}",outcome="{outcome}"}} {value}'
                    )

        lines += [
            f"# HELP {ns}_pipeline_middleware_time_ms_total Cumulative middleware processing time in milliseconds",
            f"# TYPE {ns}_pipeline_middleware_time_ms_total counter",
        ]
        for pid, stats in stats_by_pipeline.items():
            epid = _escape_label_value(pid)
            for middleware_name, middleware in stats.middlewares.items():
                emw = _escape_label_value(middleware_name)
                lines.append(
                    f'{ns}_pipeline_middleware_time_ms_total{{pipeline_id="{epid}",middleware="{emw}"}} '
                    f"{middleware.total_time_ms:.6f}"
                )

        lines += [
            f"# HELP {ns}_pipeline_middleware_last_avg_time_ms Last-run average middleware processing time in milliseconds",
            f"# TYPE {ns}_pipeline_middleware_last_avg_time_ms gauge",
        ]
        for pid, stats in stats_by_pipeline.items():
            epid = _escape_label_value(pid)
            for middleware_name, middleware in stats.middlewares.items():
                emw = _escape_label_value(middleware_name)
                lines.append(
                    f'{ns}_pipeline_middleware_last_avg_time_ms{{pipeline_id="{epid}",middleware="{emw}"}} '
                    f"{middleware.last_avg_time_ms:.6f}"
                )

        lines += [
            f"# HELP {ns}_pipeline_slowest_middleware_last_avg_time_ms Slowest middleware average latency in the last completed run",
            f"# TYPE {ns}_pipeline_slowest_middleware_last_avg_time_ms gauge",
        ]
        for pid, stats in stats_by_pipeline.items():
            if stats.last_slowest_middleware is None:
                continue
            epid = _escape_label_value(pid)
            emw = _escape_label_value(stats.last_slowest_middleware)
            lines.append(
                f'{ns}_pipeline_slowest_middleware_last_avg_time_ms{{pipeline_id="{epid}",middleware="{emw}"}} '
                f"{stats.last_slowest_middleware_avg_time_ms:.6f}"
            )

        lines += [
            f"# HELP {ns}_process_uptime_seconds Worker process uptime",
            f"# TYPE {ns}_process_uptime_seconds gauge",
        ]
        health = self._collector.to_health_dict()
        lines.append(f"{ns}_process_uptime_seconds {health['uptime_seconds']}")
        lines.append(f"# scrape_time {time.time():.3f}")
        lines.append("")
        return "\n".join(lines)


metrics_exporter_registry: Registry[type[MetricsExporter]] = Registry(name="metrics_exporter")
metrics_exporter_registry.register_factory("prometheus", PrometheusTextExporter)  # type: ignore[arg-type]
metrics_exporter_registry.load_entrypoints("agora.metrics.exporters")

__all__ = ["MetricsExporter", "PrometheusTextExporter", "metrics_exporter_registry"]
