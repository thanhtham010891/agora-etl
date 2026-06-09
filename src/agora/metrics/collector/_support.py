"""Support helpers for collector-side aggregation and reporting."""

from __future__ import annotations

import copy
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from agora.metrics.collector._stats import AIRunStats, MiddlewareRunStats, PipelineStats

if TYPE_CHECKING:
    from agora.core.metrics import PipelineRunSummary

_MAX_HEALTH_ERROR_LENGTH = 240
_AUTHORIZATION_BEARER_RE = re.compile(r"(?i)(authorization\s*:\s*bearer\s+)[^\s,;]+")
_BEARER_TOKEN_RE = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+\b")
_URI_PASSWORD_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://[^/\s:@]+:)([^@\s/]+)@")


def ensure_pipeline_stats(
    pipelines: dict[str, PipelineStats],
    pipeline_id: str,
) -> PipelineStats:
    """Return existing pipeline stats or create a new entry."""
    stats = pipelines.get(pipeline_id)
    if stats is None:
        stats = PipelineStats(pipeline_id=pipeline_id)
        pipelines[pipeline_id] = stats
    return stats


def throughput(records_consumed: int, elapsed_seconds: float) -> float:
    """Compute records/sec defensively for summary and live views."""
    if elapsed_seconds <= 0:
        return 0.0
    return records_consumed / elapsed_seconds


def summarize_health_error(error: BaseException | str) -> str:
    """Return a redacted, bounded error string safe for health payloads."""
    text = " ".join(str(error).split())
    if not text:
        return type(error).__name__ if isinstance(error, BaseException) else "error"
    text = _AUTHORIZATION_BEARER_RE.sub(r"\1***", text)
    text = _BEARER_TOKEN_RE.sub("Bearer ***", text)
    text = _URI_PASSWORD_RE.sub(r"\1***@", text)
    if len(text) > _MAX_HEALTH_ERROR_LENGTH:
        return text[: _MAX_HEALTH_ERROR_LENGTH - 3] + "..."
    return text


def absorb_ai_run(stats: PipelineStats, ai_summary: object) -> None:
    """Merge AIMetrics from a run summary into cumulative AIRunStats."""
    if stats.ai is None:
        stats.ai = AIRunStats()
    stats.ai.total_llm_calls += getattr(ai_summary, "total_llm_calls", 0)
    stats.ai.total_cache_hits += getattr(ai_summary, "total_cache_hits", 0)
    stats.ai.total_cache_misses += getattr(ai_summary, "total_cache_misses", 0)
    stats.ai.total_input_tokens += getattr(ai_summary, "total_input_tokens", 0)
    stats.ai.total_output_tokens += getattr(ai_summary, "total_output_tokens", 0)
    stats.ai.total_errors += getattr(ai_summary, "total_errors", 0)
    stats.ai.total_validation_pass += getattr(ai_summary, "total_validation_pass", 0)
    stats.ai.total_validation_fail += getattr(ai_summary, "total_validation_fail", 0)


def absorb_summary(stats: PipelineStats, summary: PipelineRunSummary) -> None:
    """Apply one finished run summary onto cumulative pipeline stats."""
    stats.total_records_consumed += summary.records_consumed
    stats.total_records_written += summary.records_written
    stats.total_records_dropped += summary.records_dropped
    stats.total_records_errored += summary.records_errored
    stats.last_run_duration_s = summary.elapsed_seconds
    stats.last_run_throughput_rps = throughput(
        summary.records_consumed,
        summary.elapsed_seconds,
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
    if summary.ai is not None:
        absorb_ai_run(stats, summary.ai)


def clone_pipeline_stats(stats: PipelineStats) -> PipelineStats:
    """Return a safe shallow/deep hybrid copy for lock-free reads."""
    cloned = copy.copy(stats)
    cloned.middlewares = {k: copy.copy(v) for k, v in stats.middlewares.items()}
    cloned.runtime = copy.copy(stats.runtime)
    if stats.live_runtime is not None:
        cloned.live_runtime = stats.live_runtime.copy()
    if stats.ai is not None:
        cloned.ai = copy.copy(stats.ai)
    return cloned


def overall_status(pipelines: dict[str, PipelineStats]) -> str:
    """Aggregate status across all pipelines."""
    if not pipelines:
        return "idle"
    statuses = {stats.status for stats in pipelines.values()}
    if "failing" in statuses:
        return "failing"
    if "degraded" in statuses:
        return "degraded"
    if "running" in statuses:
        return "running"
    if "idle" in statuses and len(statuses) == 1:
        return "idle"
    return "ok"


def collector_health_dict(
    *,
    process_name: str,
    started_at: datetime,
    pipelines: dict[str, PipelineStats],
) -> dict[str, Any]:
    """Render the full health payload for the collector."""
    return {
        "status": overall_status(pipelines),
        "process": process_name,
        "uptime_seconds": round((datetime.now(UTC) - started_at).total_seconds(), 1),
        "started_at": started_at.isoformat(),
        "pipelines": {pid: stats.to_dict() for pid, stats in pipelines.items()},
    }
