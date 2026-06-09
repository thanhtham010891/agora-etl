"""Aggregation and snapshot helpers for pipeline metrics."""

from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING, Any

from agora.core.metrics._ai import AIMetrics
from agora.core.metrics._summary import PipelineRunSummary

if TYPE_CHECKING:
    from agora.core.metrics._pipeline import PipelineMetrics


def aggregate_ai_metrics(metrics: PipelineMetrics) -> AIMetrics | None:
    """Aggregate AI metrics across middlewares that recorded AI activity."""
    agg = AIMetrics()
    has_ai = False
    for mw in metrics.by_middleware.values():
        ai_metrics = mw.ai
        if ai_metrics.llm_calls > 0 or ai_metrics.cache_hits > 0:
            agg.absorb(ai_metrics)
            has_ai = True
    return agg if has_ai else None


def register_live_metric_overlay(metrics: PipelineMetrics, overlay: Any) -> None:
    metrics._live_metric_overlays.append(overlay)


def unregister_live_metric_overlay(metrics: PipelineMetrics, overlay: Any) -> None:
    with contextlib.suppress(ValueError):
        metrics._live_metric_overlays.remove(overlay)


def snapshot_pipeline_metrics(
    metrics: PipelineMetrics,
    *,
    pipeline_id: str,
    run_id: str,
) -> PipelineRunSummary:
    """Materialize the immutable run summary from live pipeline metrics."""
    elapsed = time.monotonic() - metrics.started_at
    records_consumed = metrics.records_consumed
    records_written = metrics.records_written
    by_source = dict(metrics.by_source)

    for overlay in metrics._live_metric_overlays:
        pending = overlay.snapshot_pending()
        pending_consumed = pending.get("records_consumed", 0)
        pending_written = pending.get("records_written", 0)
        if pending_consumed:
            records_consumed += pending_consumed
            source_name = pending.get("source_name")
            if source_name:
                by_source[source_name] = by_source.get(source_name, 0) + pending_consumed
        if pending_written:
            records_written += pending_written

    return PipelineRunSummary(
        pipeline_id=pipeline_id,
        run_id=run_id,
        elapsed_seconds=elapsed,
        records_consumed=records_consumed,
        records_written=records_written,
        records_dropped=metrics.records_dropped,
        records_errored=metrics.records_errored,
        by_source=by_source,
        by_middleware=dict(metrics.by_middleware.items()),
        ai=aggregate_ai_metrics(metrics),
        runtime=metrics.runtime.copy(),
        last_checkpoint=metrics.last_checkpoint,
    )
