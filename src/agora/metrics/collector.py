"""
agora/metrics/collector.py
===========================
``MetricsCollector`` — zero-dependency, in-process pipeline metrics.

Tracks per-pipeline run statistics that are:
  - Exposed via ``agora worker`` status output
  - Served at ``/health`` endpoint
  - Exported via the metrics exporter registry

Design
------
All counters are monotonically increasing (never reset, like Prometheus).
Gauges (last_run_at, uptime) reflect the most recent observation.

Thread/async safety: ``asyncio.Lock`` is acquired for all mutations in
``record_run()``. Reads (``get()``, ``all()``) are lock-free because
Python dict reads are safe in a single-threaded asyncio event loop.

Usage::

    collector = MetricsCollector()
    await collector.record_run(pipeline_id="places_ingest", summary=summary)
    stats = collector.get("places_ingest")
    stats.success_rate   # → 0.95
    stats.last_run_at    # → datetime
    collector.all()      # → dict[str, PipelineStats]
"""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agora.core.metrics import MiddlewareMetrics, PipelineRunSummary, RuntimeMetrics


@dataclass
class RuntimeRunStats:
    """Observability snapshot derived from pipeline runtime metrics."""

    total_source_prefetch_block_count: int = 0
    total_checkpoint_save_count: int = 0
    total_checkpoint_failure_count: int = 0
    total_dlq_failure_count: int = 0
    total_writer_flush_count: int = 0
    total_adaptive_scale_up_count: int = 0
    total_adaptive_scale_down_count: int = 0
    last_runtime: RuntimeMetrics | None = None

    def absorb(self, runtime: RuntimeMetrics) -> None:
        """Merge one completed run's runtime metrics into cumulative stats."""
        self.total_source_prefetch_block_count += runtime.source_prefetch_block_count
        self.total_checkpoint_save_count += runtime.checkpoint_save_count
        self.total_checkpoint_failure_count += runtime.checkpoint_failure_count
        self.total_dlq_failure_count += runtime.dlq_failure_count
        self.total_writer_flush_count += runtime.writer_flush_count
        self.total_adaptive_scale_up_count += runtime.adaptive_backpressure_scale_up_count
        self.total_adaptive_scale_down_count += runtime.adaptive_backpressure_scale_down_count
        self.last_runtime = runtime.copy()

    def to_dict(self) -> dict:
        payload = {
            "total_source_prefetch_block_count": self.total_source_prefetch_block_count,
            "total_checkpoint_save_count": self.total_checkpoint_save_count,
            "total_checkpoint_failure_count": self.total_checkpoint_failure_count,
            "total_dlq_failure_count": self.total_dlq_failure_count,
            "total_writer_flush_count": self.total_writer_flush_count,
            "total_adaptive_scale_up_count": self.total_adaptive_scale_up_count,
            "total_adaptive_scale_down_count": self.total_adaptive_scale_down_count,
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

    def to_dict(self) -> dict:
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
    """Cumulative AI metrics across all runs of a single pipeline.

    Only populated when AI middlewares ran. Exposed in the ``/health``
    response under ``pipelines.<id>.ai``.
    """

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

    def to_dict(self) -> dict:
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
    """Cumulative statistics for a single pipeline.

    All ``total_*`` fields are monotonically increasing counters.
    """

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

    # AI metrics — None until the first AI-powered run
    ai: AIRunStats | None = None
    runtime: RuntimeRunStats = field(default_factory=RuntimeRunStats)
    middlewares: dict[str, MiddlewareRunStats] = field(default_factory=dict)
    last_slowest_middleware: str | None = None
    last_slowest_middleware_avg_time_ms: float = 0.0
    last_slowest_middleware_total_time_ms: float = 0.0

    @property
    def success_rate(self) -> float:
        """Fraction of runs that completed without error (0.0-1.0)."""
        if self.total_runs == 0:
            return 1.0
        return self.successful_runs / self.total_runs

    @property
    def uptime_seconds(self) -> float:
        return (datetime.now(UTC) - self.started_at).total_seconds()

    @property
    def status(self) -> str:
        """``'ok'``, ``'degraded'``, or ``'failing'``."""
        if self.total_runs == 0:
            return "idle"
        if self.failed_runs == 0:
            return "ok"
        if self.success_rate >= 0.5:
            return "degraded"
        return "failing"

    def _absorb_ai(self, ai_summary: object) -> None:
        """Merge AIMetrics from a run summary into cumulative AIRunStats."""
        if self.ai is None:
            self.ai = AIRunStats()
        self.ai.total_llm_calls += getattr(ai_summary, "total_llm_calls", 0)
        self.ai.total_cache_hits += getattr(ai_summary, "total_cache_hits", 0)
        self.ai.total_cache_misses += getattr(ai_summary, "total_cache_misses", 0)
        self.ai.total_input_tokens += getattr(ai_summary, "total_input_tokens", 0)
        self.ai.total_output_tokens += getattr(ai_summary, "total_output_tokens", 0)
        self.ai.total_errors += getattr(ai_summary, "total_errors", 0)
        self.ai.total_validation_pass += getattr(ai_summary, "total_validation_pass", 0)
        self.ai.total_validation_fail += getattr(ai_summary, "total_validation_fail", 0)

    def to_dict(self) -> dict:
        d: dict = {
            "pipeline_id": self.pipeline_id,
            "status": self.status,
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
        }
        if self.ai is not None:
            d["ai"] = self.ai.to_dict()
        d["runtime"] = self.runtime.to_dict()
        d["middlewares"] = {
            name: stats.to_dict() for name, stats in sorted(self.middlewares.items())
        }
        if self.last_slowest_middleware is not None:
            d["slowest_middleware"] = {
                "name": self.last_slowest_middleware,
                "avg_time_ms": round(self.last_slowest_middleware_avg_time_ms, 3),
                "total_time_ms": round(self.last_slowest_middleware_total_time_ms, 3),
            }
        return d


class MetricsCollector:
    """Aggregate pipeline run statistics.

    One instance is shared across the WorkerPool and all ScheduledPipelines.

    Parameters
    ----------
    process_name:
        Identifies this worker process in the health response.
    """

    def __init__(self, process_name: str = "agora-worker") -> None:
        self._process_name = process_name
        self._pipelines: dict[str, PipelineStats] = {}
        self._started_at = datetime.now(UTC)
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Record                                                               #
    # ------------------------------------------------------------------ #

    async def record_run(
        self,
        pipeline_id: str,
        summary: PipelineRunSummary | None = None,
        error: Exception | None = None,
    ) -> None:
        """Update stats after a pipeline run.

        Parameters
        ----------
        pipeline_id:
            The ``ScheduledPipeline.pipeline_id``.
        summary:
            ``PipelineRunSummary`` from the completed run.
            Pass ``None`` on error.
        error:
            Exception that caused the run to fail, or ``None`` on success.
        """
        async with self._lock:
            if pipeline_id not in self._pipelines:
                self._pipelines[pipeline_id] = PipelineStats(pipeline_id=pipeline_id)

            stats = self._pipelines[pipeline_id]
            stats.total_runs += 1
            stats.last_run_at = datetime.now(UTC)

            if error is not None:
                stats.failed_runs += 1
                stats.last_error = str(error)
            else:
                stats.successful_runs += 1
                stats.last_error = None

            if summary is not None:
                stats.total_records_consumed += summary.records_consumed
                stats.total_records_written += summary.records_written
                stats.total_records_dropped += summary.records_dropped
                stats.total_records_errored += summary.records_errored
                stats.last_run_duration_s = summary.elapsed_seconds
                stats.last_run_throughput_rps = (
                    summary.records_consumed / summary.elapsed_seconds
                    if summary.elapsed_seconds > 0
                    else 0.0
                )
                stats.runtime.absorb(summary.runtime)
                slowest_name: str | None = None
                slowest_avg = -1.0
                slowest_total = 0.0
                for name, middleware_metrics in summary.by_middleware.items():
                    if name not in stats.middlewares:
                        stats.middlewares[name] = MiddlewareRunStats(name=name)
                    stats.middlewares[name].absorb(middleware_metrics)
                    if middleware_metrics.avg_time_ms > slowest_avg:
                        slowest_name = name
                        slowest_avg = middleware_metrics.avg_time_ms
                        slowest_total = middleware_metrics.total_time_ms
                stats.last_slowest_middleware = slowest_name
                stats.last_slowest_middleware_avg_time_ms = max(slowest_avg, 0.0)
                stats.last_slowest_middleware_total_time_ms = slowest_total
                # Absorb AI metrics if present (non-AI pipelines have summary.ai = None)
                if summary.ai is not None:
                    stats._absorb_ai(summary.ai)

    # ------------------------------------------------------------------ #
    # Query                                                                #
    # ------------------------------------------------------------------ #

    def get(self, pipeline_id: str) -> PipelineStats | None:
        return self._pipelines.get(pipeline_id)

    def all(self) -> dict[str, PipelineStats]:
        def _copy_stats(stats: PipelineStats) -> PipelineStats:
            s = copy.copy(stats)
            s.middlewares = {k: copy.copy(v) for k, v in stats.middlewares.items()}
            s.runtime = copy.copy(stats.runtime)
            if stats.ai is not None:
                s.ai = copy.copy(stats.ai)
            return s

        return {pid: _copy_stats(stats) for pid, stats in self._pipelines.items()}

    @property
    def overall_status(self) -> str:
        """Aggregate status across all pipelines."""
        if not self._pipelines:
            return "idle"
        statuses = {s.status for s in self._pipelines.values()}
        if "failing" in statuses:
            return "failing"
        if "degraded" in statuses:
            return "degraded"
        if "idle" in statuses and len(statuses) == 1:
            return "idle"
        return "ok"

    def to_health_dict(self) -> dict:
        """Render full health payload (used by /health endpoint)."""
        return {
            "status": self.overall_status,
            "process": self._process_name,
            "uptime_seconds": round((datetime.now(UTC) - self._started_at).total_seconds(), 1),
            "started_at": self._started_at.isoformat(),
            "pipelines": {pid: stats.to_dict() for pid, stats in self._pipelines.items()},
        }
