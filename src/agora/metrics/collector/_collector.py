"""Facade collector that coordinates cumulative pipeline stats."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from agora.metrics.collector._support import (
    absorb_summary,
    clone_pipeline_stats,
    collector_health_dict,
    ensure_pipeline_stats,
    overall_status,
    summarize_health_error,
    throughput,
)

if TYPE_CHECKING:
    from agora.core.metrics import PipelineRunSummary
    from agora.metrics.collector._stats import PipelineStats


class MetricsCollector:
    """Aggregate pipeline run statistics."""

    def __init__(self, process_name: str = "agora-worker") -> None:
        self._process_name = process_name
        self._pipelines: dict[str, PipelineStats] = {}
        self._started_at = datetime.now(UTC)
        self._lock = asyncio.Lock()

    async def register_pipeline(
        self,
        pipeline_id: str,
        *,
        schedule: str | None = None,
    ) -> None:
        async with self._lock:
            stats = ensure_pipeline_stats(self._pipelines, pipeline_id)
            if schedule is not None:
                stats.schedule = schedule

    async def record_live_run(
        self,
        pipeline_id: str,
        summary: PipelineRunSummary,
        *,
        run_id: str,
        started_at: datetime,
    ) -> None:
        async with self._lock:
            stats = ensure_pipeline_stats(self._pipelines, pipeline_id)
            stats.is_running = True
            stats.active_run_id = run_id
            stats.active_run_started_at = started_at
            stats.active_run_duration_s = summary.elapsed_seconds
            stats.active_run_throughput_rps = throughput(
                summary.records_consumed,
                summary.elapsed_seconds,
            )
            stats.live_records_consumed = summary.records_consumed
            stats.live_records_written = summary.records_written
            stats.live_records_dropped = summary.records_dropped
            stats.live_records_errored = summary.records_errored
            stats.live_runtime = summary.runtime.copy()
            stats.last_live_at = datetime.now(UTC)

    async def record_run(
        self,
        pipeline_id: str,
        summary: PipelineRunSummary | None = None,
        error: BaseException | None = None,
    ) -> None:
        async with self._lock:
            stats = ensure_pipeline_stats(self._pipelines, pipeline_id)
            stats.clear_live_run()
            stats.total_runs += 1
            stats.last_run_at = datetime.now(UTC)

            if error is not None:
                stats.failed_runs += 1
                stats.last_error = summarize_health_error(error)
            else:
                stats.successful_runs += 1
                stats.last_error = None

            if summary is not None:
                absorb_summary(stats, summary)

    def pipeline_stats(self, pipeline_id: str) -> PipelineStats | None:
        """Return a cloned stats snapshot for one pipeline, if present."""
        stats = self._pipelines.get(pipeline_id)
        if stats is None:
            return None
        return clone_pipeline_stats(stats)

    def get(self, pipeline_id: str) -> PipelineStats | None:
        """Backward-compatible alias for ``pipeline_stats()``."""
        return self.pipeline_stats(pipeline_id)

    def pipeline_stats_map(self) -> dict[str, PipelineStats]:
        """Return cloned stats snapshots for every tracked pipeline."""
        return {
            pipeline_id: clone_pipeline_stats(stats)
            for pipeline_id, stats in self._pipelines.items()
        }

    def all(self) -> dict[str, PipelineStats]:
        """Backward-compatible alias for ``pipeline_stats_map()``."""
        return self.pipeline_stats_map()

    @property
    def overall_status(self) -> str:
        return overall_status(self._pipelines)

    def to_health_dict(self) -> dict[str, Any]:
        return collector_health_dict(
            process_name=self._process_name,
            started_at=self._started_at,
            pipelines=self._pipelines,
        )
