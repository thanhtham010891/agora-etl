"""Mutable live pipeline metrics container."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from agora.core.metrics._middleware import MiddlewareMetrics
from agora.core.metrics._pipeline_support import (
    aggregate_ai_metrics,
    register_live_metric_overlay,
    snapshot_pipeline_metrics,
    unregister_live_metric_overlay,
)
from agora.core.metrics._runtime import RuntimeMetrics

if TYPE_CHECKING:
    from agora.core.checkpoint import Checkpoint
    from agora.core.metrics._ai import AIMetrics
    from agora.core.metrics._summary import PipelineRunSummary


@dataclass
class PipelineMetrics:
    """Mutable metrics container updated in-place during pipeline execution."""

    started_at: float = field(default_factory=time.monotonic)
    records_consumed: int = 0
    records_written: int = 0
    records_dropped: int = 0
    records_errored: int = 0
    by_source: dict[str, int] = field(default_factory=dict)
    by_middleware: dict[str, MiddlewareMetrics] = field(default_factory=dict)
    runtime: RuntimeMetrics = field(default_factory=RuntimeMetrics)
    last_checkpoint: Checkpoint | None = None
    _live_metric_overlays: list[Any] = field(default_factory=list, repr=False)
    _last_middleware_name: str | None = field(default=None, init=False, repr=False)
    _last_middleware_metrics: MiddlewareMetrics | None = field(
        default=None,
        init=False,
        repr=False,
    )

    def middleware(self, name: str) -> MiddlewareMetrics:
        cached_name = self._last_middleware_name
        cached_metrics = self._last_middleware_metrics
        if cached_name == name and cached_metrics is not None:
            return cached_metrics

        metrics = self.by_middleware.get(name)
        if metrics is None:
            metrics = MiddlewareMetrics(name=name)
            self.by_middleware[name] = metrics

        self._last_middleware_name = name
        self._last_middleware_metrics = metrics
        return metrics

    def inc_source(self, source_key: str, count: int = 1) -> None:
        self.by_source[source_key] = self.by_source.get(source_key, 0) + count

    def aggregate_ai(self) -> AIMetrics | None:
        return aggregate_ai_metrics(self)

    def register_live_metric_overlay(self, overlay: Any) -> None:
        register_live_metric_overlay(self, overlay)

    def unregister_live_metric_overlay(self, overlay: Any) -> None:
        unregister_live_metric_overlay(self, overlay)

    def snapshot(self, pipeline_id: str, run_id: str) -> PipelineRunSummary:
        return snapshot_pipeline_metrics(
            self,
            pipeline_id=pipeline_id,
            run_id=run_id,
        )
