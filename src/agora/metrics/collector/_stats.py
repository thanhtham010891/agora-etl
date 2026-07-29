"""Collector-side cumulative stats models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora.core.metrics import MiddlewareMetrics, RuntimeMetrics


@dataclass
class RuntimeRunStats:
    """Observability snapshot derived from pipeline runtime metrics."""

    total_source_prefetch_block_count: int = 0
    total_rust_prefetch_runs: int = 0
    total_rust_prefetch_wait_count: int = 0
    total_rust_prefetch_batch_drain_count: int = 0
    total_rust_prefetch_push_batch_count: int = 0
    total_checkpoint_save_count: int = 0
    total_checkpoint_failure_count: int = 0
    total_dlq_failure_count: int = 0
    total_failure_classification_counts: dict[str, int] = field(default_factory=dict)
    total_failure_alert_severity_counts: dict[str, int] = field(default_factory=dict)
    total_writer_flush_count: int = 0
    total_adaptive_scale_up_count: int = 0
    total_adaptive_scale_down_count: int = 0
    total_csv_arrow_native_batch_count: int = 0
    total_csv_arrow_native_row_count: int = 0
    total_csv_arrow_downgrade_batch_count: int = 0
    total_csv_arrow_downgrade_row_count: int = 0
    last_runtime: RuntimeMetrics | None = None

    def absorb(self, runtime: RuntimeMetrics) -> None:
        self.total_source_prefetch_block_count += runtime.source_prefetch_block_count
        self.total_rust_prefetch_runs += int(runtime.rust_prefetch_active)
        self.total_rust_prefetch_wait_count += runtime.rust_prefetch_wait_count
        self.total_rust_prefetch_batch_drain_count += runtime.rust_prefetch_batch_drain_count
        self.total_rust_prefetch_push_batch_count += runtime.rust_prefetch_push_batch_count
        self.total_checkpoint_save_count += runtime.checkpoint_save_count
        self.total_checkpoint_failure_count += runtime.checkpoint_failure_count
        self.total_dlq_failure_count += runtime.dlq_failure_count
        for classification, count in runtime.failure_classification_counts.items():
            self.total_failure_classification_counts[classification] = (
                self.total_failure_classification_counts.get(classification, 0) + count
            )
        for severity, count in runtime.failure_alert_severity_counts.items():
            self.total_failure_alert_severity_counts[severity] = (
                self.total_failure_alert_severity_counts.get(severity, 0) + count
            )
        self.total_writer_flush_count += runtime.writer_flush_count
        self.total_adaptive_scale_up_count += runtime.adaptive_backpressure_scale_up_count
        self.total_adaptive_scale_down_count += runtime.adaptive_backpressure_scale_down_count
        self.total_csv_arrow_native_batch_count += runtime.csv_arrow_native_batch_count
        self.total_csv_arrow_native_row_count += runtime.csv_arrow_native_row_count
        self.total_csv_arrow_downgrade_batch_count += runtime.csv_arrow_downgrade_batch_count
        self.total_csv_arrow_downgrade_row_count += runtime.csv_arrow_downgrade_row_count
        self.last_runtime = runtime.copy()

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "total_source_prefetch_block_count": self.total_source_prefetch_block_count,
            "total_rust_prefetch_runs": self.total_rust_prefetch_runs,
            "total_rust_prefetch_wait_count": self.total_rust_prefetch_wait_count,
            "total_rust_prefetch_batch_drain_count": self.total_rust_prefetch_batch_drain_count,
            "total_rust_prefetch_push_batch_count": self.total_rust_prefetch_push_batch_count,
            "total_checkpoint_save_count": self.total_checkpoint_save_count,
            "total_checkpoint_failure_count": self.total_checkpoint_failure_count,
            "total_dlq_failure_count": self.total_dlq_failure_count,
            "total_failure_classification_counts": dict(self.total_failure_classification_counts),
            "total_failure_alert_severity_counts": dict(self.total_failure_alert_severity_counts),
            "total_writer_flush_count": self.total_writer_flush_count,
            "total_adaptive_scale_up_count": self.total_adaptive_scale_up_count,
            "total_adaptive_scale_down_count": self.total_adaptive_scale_down_count,
            "total_csv_arrow_native_batch_count": self.total_csv_arrow_native_batch_count,
            "total_csv_arrow_native_row_count": self.total_csv_arrow_native_row_count,
            "total_csv_arrow_downgrade_batch_count": self.total_csv_arrow_downgrade_batch_count,
            "total_csv_arrow_downgrade_row_count": self.total_csv_arrow_downgrade_row_count,
        }
        if self.last_runtime is not None:
            payload["last_run"] = self.last_runtime.to_dict()
        return payload


@dataclass
class MiddlewareRunStats:
    """Cumulative and last-run metrics for one middleware stage."""

    name: str
    total_records_in: int = 0
    total_records_out: int = 0
    total_records_dropped: int = 0
    total_records_errored: int = 0
    total_time_ms: float = 0.0
    last_records_in: int = 0
    last_records_out: int = 0
    last_records_dropped: int = 0
    last_records_errored: int = 0
    last_total_time_ms: float = 0.0
    last_avg_time_ms: float = 0.0

    def absorb(self, metrics: MiddlewareMetrics) -> None:
        self.total_records_in += metrics.records_in
        self.total_records_out += metrics.records_out
        self.total_records_dropped += metrics.records_dropped
        self.total_records_errored += metrics.records_errored
        self.total_time_ms += metrics.total_time_ms
        self.last_records_in = metrics.records_in
        self.last_records_out = metrics.records_out
        self.last_records_dropped = metrics.records_dropped
        self.last_records_errored = metrics.records_errored
        self.last_total_time_ms = metrics.total_time_ms
        self.last_avg_time_ms = metrics.avg_time_ms

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "total_records_in": self.total_records_in,
            "total_records_out": self.total_records_out,
            "total_records_dropped": self.total_records_dropped,
            "total_records_errored": self.total_records_errored,
            "total_time_ms": round(self.total_time_ms, 3),
            "last_records_in": self.last_records_in,
            "last_records_out": self.last_records_out,
            "last_records_dropped": self.last_records_dropped,
            "last_records_errored": self.last_records_errored,
            "last_total_time_ms": round(self.last_total_time_ms, 3),
            "last_avg_time_ms": round(self.last_avg_time_ms, 3),
        }


@dataclass
class AIRunStats:
    """Cumulative AI metrics across all runs of a single pipeline."""

    total_llm_calls: int = 0
    total_cache_hits: int = 0
    total_cache_misses: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_errors: int = 0
    total_validation_pass: int = 0
    total_validation_fail: int = 0

    @property
    def total_tokens(self) -> int:
        return self.total_input_tokens + self.total_output_tokens

    @property
    def cache_hit_rate(self) -> float:
        total = self.total_cache_hits + self.total_cache_misses
        return self.total_cache_hits / total if total > 0 else 0.0

    @property
    def validation_pass_rate(self) -> float:
        total = self.total_validation_pass + self.total_validation_fail
        return self.total_validation_pass / total if total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_llm_calls": self.total_llm_calls,
            "total_cache_hits": self.total_cache_hits,
            "total_cache_misses": self.total_cache_misses,
            "cache_hit_rate": round(self.cache_hit_rate, 4),
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_tokens": self.total_tokens,
            "total_errors": self.total_errors,
            "validation_pass_rate": round(self.validation_pass_rate, 4),
        }


@dataclass
class PipelineStats:
    """Cumulative statistics for a single pipeline."""

    pipeline_id: str
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    total_records_consumed: int = 0
    total_records_written: int = 0
    total_records_dropped: int = 0
    total_records_errored: int = 0
    last_run_at: datetime | None = None
    last_run_duration_s: float = 0.0
    last_run_throughput_rps: float = 0.0
    last_error: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    schedule: str | None = None
    is_running: bool = False
    active_run_id: str | None = None
    active_run_started_at: datetime | None = None
    active_run_duration_s: float = 0.0
    active_run_throughput_rps: float = 0.0
    live_records_consumed: int = 0
    live_records_written: int = 0
    live_records_dropped: int = 0
    live_records_errored: int = 0
    live_runtime: RuntimeMetrics | None = None
    last_live_at: datetime | None = None
    ai: AIRunStats | None = None
    runtime: RuntimeRunStats = field(default_factory=RuntimeRunStats)
    middlewares: dict[str, MiddlewareRunStats] = field(default_factory=dict)
    last_slowest_middleware: str | None = None
    last_slowest_middleware_avg_time_ms: float = 0.0
    last_slowest_middleware_total_time_ms: float = 0.0

    @property
    def success_rate(self) -> float:
        if self.total_runs == 0:
            return 1.0
        return self.successful_runs / self.total_runs

    @property
    def uptime_seconds(self) -> float:
        return (datetime.now(UTC) - self.started_at).total_seconds()

    @property
    def status(self) -> str:
        if self.is_running:
            return "running"
        if self.total_runs == 0:
            return "idle"
        if self.failed_runs == 0:
            return "ok"
        if self.success_rate >= 0.5:
            return "degraded"
        return "failing"

    @property
    def live_records_pending(self) -> int:
        return max(
            self.live_records_consumed
            - self.live_records_written
            - self.live_records_dropped
            - self.live_records_errored,
            0,
        )

    def clear_live_run(self) -> None:
        self.is_running = False
        self.active_run_id = None
        self.active_run_started_at = None
        self.active_run_duration_s = 0.0
        self.active_run_throughput_rps = 0.0
        self.live_records_consumed = 0
        self.live_records_written = 0
        self.live_records_dropped = 0
        self.live_records_errored = 0
        self.live_runtime = None
        self.last_live_at = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "pipeline_id": self.pipeline_id,
            "status": self.status,
            "schedule": self.schedule,
            "total_runs": self.total_runs,
            "successful_runs": self.successful_runs,
            "failed_runs": self.failed_runs,
            "success_rate": round(self.success_rate, 4),
            "total_records_consumed": self.total_records_consumed,
            "total_records_written": self.total_records_written,
            "total_records_dropped": self.total_records_dropped,
            "total_records_errored": self.total_records_errored,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "last_run_duration_s": round(self.last_run_duration_s, 3),
            "last_run_throughput_rps": round(self.last_run_throughput_rps, 3),
            "last_error": self.last_error,
            "uptime_seconds": round(self.uptime_seconds, 1),
            "lifecycle": {
                "state": "running" if self.is_running else "idle",
                "running": self.is_running,
                "active_run_id": self.active_run_id,
                "active_run_started_at": (
                    self.active_run_started_at.isoformat() if self.active_run_started_at else None
                ),
            },
        }
        if self.ai is not None:
            d["ai"] = self.ai.to_dict()
        d["runtime"] = self.runtime.to_dict()
        d["middlewares"] = {
            name: stats.to_dict() for name, stats in sorted(self.middlewares.items())
        }
        if self.is_running and self.active_run_id is not None:
            d["live"] = {
                "run_id": self.active_run_id,
                "elapsed_seconds": round(self.active_run_duration_s, 3),
                "throughput_rps": round(self.active_run_throughput_rps, 3),
                "records_consumed": self.live_records_consumed,
                "records_written": self.live_records_written,
                "records_dropped": self.live_records_dropped,
                "records_errored": self.live_records_errored,
                "records_pending": self.live_records_pending,
                "runtime": self.live_runtime.to_dict() if self.live_runtime is not None else {},
                "updated_at": self.last_live_at.isoformat() if self.last_live_at else None,
            }
        if self.last_slowest_middleware is not None:
            d["slowest_middleware"] = {
                "name": self.last_slowest_middleware,
                "avg_time_ms": round(self.last_slowest_middleware_avg_time_ms, 3),
                "total_time_ms": round(self.last_slowest_middleware_total_time_ms, 3),
            }
        return d
