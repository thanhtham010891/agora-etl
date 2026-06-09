"""Run-scoped session state for pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agora.core.context import PipelineContext
    from agora.core.metrics import PipelineMetrics, PipelineRunSummary


@dataclass(slots=True)
class PipelineRunState:
    """Mutable run-scoped state used by the pipeline executor."""

    ctx: PipelineContext
    middlewares_started: bool = False
    writer_opened: bool = False
    dlq_opened: bool = False
    interrupted: bool = False
    run_error: BaseException | None = None

    @property
    def metrics(self) -> PipelineMetrics:
        return self.ctx.metrics

    @property
    def suppress_shutdown_exceptions(self) -> bool:
        return self.interrupted or self.run_error is not None

    def complete(self) -> PipelineRunSummary:
        """Finalize and return the immutable summary for this run."""
        summary = self.metrics.snapshot(
            pipeline_id=self.ctx.pipeline_id,
            run_id=self.ctx.run_id,
        )
        self.ctx.log.info(
            "pipeline_complete",
            consumed=summary.records_consumed,
            written=summary.records_written,
            dropped=summary.records_dropped,
            errors=summary.records_errored,
            elapsed=f"{summary.elapsed_seconds:.1f}s",
        )
        return summary
