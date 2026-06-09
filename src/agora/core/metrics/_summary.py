"""Immutable end-of-run summary models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from agora.core.metrics._runtime import RuntimeMetrics

if TYPE_CHECKING:
    from agora.core.checkpoint import Checkpoint
    from agora.core.metrics._ai import AIMetrics
    from agora.core.metrics._middleware import MiddlewareMetrics


@dataclass(frozen=True)
class PipelineRunSummary:
    """Immutable snapshot of metrics at the end of a pipeline run."""

    pipeline_id: str
    run_id: str
    elapsed_seconds: float
    records_consumed: int
    records_written: int
    records_dropped: int
    records_errored: int
    by_source: dict[str, int]
    by_middleware: dict[str, MiddlewareMetrics]
    ai: AIMetrics | None = None
    runtime: RuntimeMetrics = field(default_factory=RuntimeMetrics)
    last_checkpoint: Checkpoint | None = None

    def __str__(self) -> str:
        from agora.core.metrics._summary_render import render_pipeline_run_summary

        return render_pipeline_run_summary(self)
