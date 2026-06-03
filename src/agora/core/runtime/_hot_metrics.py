"""Hot-path metrics accumulator — reduces Python object churn on the record loop."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agora.core.metrics import PipelineMetrics


@dataclass(slots=True)
class HotPathMetrics:
    """Accumulates hot-path counters locally and flushes to PipelineMetrics in batches.

    Lane code increments this instead of touching PipelineMetrics directly on
    every record. Call flush() at natural batch boundaries (end of batch, end
    of run) to commit accumulated values.
    """

    _source_name: str
    _flush_interval: int
    _consumed: int = 0
    _written: int = 0
    _ticks: int = 0

    def snapshot_pending(self) -> dict[str, int | str]:
        return {
            "source_name": self._source_name,
            "records_consumed": self._consumed,
            "records_written": self._written,
        }

    def inc_consumed(self, count: int = 1) -> bool:
        """Increment consumed counter. Returns True when flush interval is reached."""
        self._consumed += count
        self._ticks += count
        return self._ticks >= self._flush_interval

    def inc_written(self, count: int = 1) -> None:
        self._written += count

    def flush(self, metrics: PipelineMetrics) -> None:
        """Commit accumulated counters to PipelineMetrics and reset."""
        if self._consumed:
            metrics.records_consumed += self._consumed
            metrics.by_source[self._source_name] = (
                metrics.by_source.get(self._source_name, 0) + self._consumed
            )
            self._consumed = 0
        if self._written:
            metrics.records_written += self._written
            self._written = 0
        self._ticks = 0

    def flush_final(self, metrics: PipelineMetrics) -> None:
        """Flush any remaining accumulated values at end of run."""
        self.flush(metrics)

    @staticmethod
    def for_source(
        source_name: str,
        *,
        metrics: PipelineMetrics | None = None,
        flush_interval: int = 100,
    ) -> HotPathMetrics:
        hot = HotPathMetrics(_source_name=source_name, _flush_interval=flush_interval)
        if metrics is not None:
            metrics.register_live_metric_overlay(hot)
        return hot
