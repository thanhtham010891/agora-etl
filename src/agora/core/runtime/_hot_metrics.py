"""Hot-path metrics accumulator — reduces Python object churn on the record loop."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from agora.core.acceleration import (
    AccelerationMode,
    acceleration_supports,
    make_metrics_accumulator,
    normalize_acceleration_mode,
)

if TYPE_CHECKING:
    from agora.core.metrics import PipelineMetrics


class RustHotPathMetrics:
    """Adapter around the optional Rust metrics accumulator."""

    def __init__(
        self,
        source_name: str,
        flush_interval: int,
        *,
        acceleration_mode: AccelerationMode | str,
    ) -> None:
        self._source_name = source_name
        self._flush_interval = max(1, flush_interval)
        self._accumulator = make_metrics_accumulator(
            flush_interval=flush_interval,
            mode=acceleration_mode,
        )

    def snapshot_pending(self) -> dict[str, int | str]:
        consumed, written, _since_last_flush = self._accumulator.snapshot()
        return {
            "source_name": self._source_name,
            "records_consumed": int(consumed),
            "records_written": int(written),
        }

    def inc_consumed(self, count: int = 1) -> bool:
        count = max(0, count)
        if count == 0:
            _consumed, _written, since_last_flush = self._accumulator.snapshot()
            return bool(since_last_flush >= self._flush_interval)
        add_consumed = getattr(self._accumulator, "add_consumed", None)
        if callable(add_consumed):
            return bool(add_consumed(self._source_name, count))
        flush_due = False
        for _ in range(count):
            flush_due = bool(self._accumulator.inc_consumed(self._source_name)) or flush_due
        return flush_due

    def inc_written(self, count: int = 1) -> None:
        count = max(0, count)
        add_written = getattr(self._accumulator, "add_written", None)
        if callable(add_written):
            add_written(count)
            return
        for _ in range(count):
            self._accumulator.inc_written()

    def flush(self, metrics: PipelineMetrics) -> None:
        self._accumulator.flush(metrics)

    def flush_final(self, metrics: PipelineMetrics) -> None:
        self._accumulator.flush_final(metrics)


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
        acceleration_mode: AccelerationMode | str = AccelerationMode.AUTO,
    ) -> HotPathMetrics | RustHotPathMetrics:
        mode = normalize_acceleration_mode(acceleration_mode)
        if mode != AccelerationMode.OFF and acceleration_supports(
            "metrics_accumulator",
            mode=mode,
        ):
            hot: HotPathMetrics | RustHotPathMetrics = RustHotPathMetrics(
                source_name=source_name,
                flush_interval=flush_interval,
                acceleration_mode=mode,
            )
            if metrics is not None:
                metrics.runtime.rust_metrics_accumulator_active = True
        else:
            hot = HotPathMetrics(_source_name=source_name, _flush_interval=flush_interval)
        if metrics is not None:
            metrics.register_live_metric_overlay(hot)
        return hot
