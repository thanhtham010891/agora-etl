"""Adaptive backpressure helpers for buffered runtime lanes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class AdaptiveBackpressureController:
    """Tune buffered-stage in-flight limits from writer/checkpoint pressure."""

    current_limit: int
    min_limit: int
    max_limit: int
    scale_up_step: int
    scale_down_step: int
    writer_slow_ms: float
    checkpoint_slow_ms: float
    last_writer_flush_count: int = 0
    last_writer_flush_time_ms: float = 0.0
    last_checkpoint_save_count: int = 0
    last_checkpoint_save_time_ms: float = 0.0

    def observe(self, runtime_metrics: Any, pending_count: int) -> int:
        writer_flush_count = runtime_metrics.writer_flush_count
        checkpoint_save_count = runtime_metrics.checkpoint_save_count
        writer_flush_delta = writer_flush_count - self.last_writer_flush_count
        checkpoint_save_delta = checkpoint_save_count - self.last_checkpoint_save_count
        writer_time_delta = runtime_metrics.writer_flush_time_ms - self.last_writer_flush_time_ms
        checkpoint_time_delta = (
            runtime_metrics.checkpoint_save_time_ms - self.last_checkpoint_save_time_ms
        )

        self.last_writer_flush_count = writer_flush_count
        self.last_writer_flush_time_ms = runtime_metrics.writer_flush_time_ms
        self.last_checkpoint_save_count = checkpoint_save_count
        self.last_checkpoint_save_time_ms = runtime_metrics.checkpoint_save_time_ms

        saw_pressure_signal = writer_flush_delta > 0 or checkpoint_save_delta > 0
        if not saw_pressure_signal:
            return self.current_limit

        writer_flush_avg = writer_time_delta / writer_flush_delta if writer_flush_delta > 0 else 0.0
        checkpoint_save_avg = (
            checkpoint_time_delta / checkpoint_save_delta if checkpoint_save_delta > 0 else 0.0
        )

        writer_is_slow = writer_flush_delta > 0 and writer_flush_avg >= self.writer_slow_ms
        checkpoint_is_slow = (
            checkpoint_save_delta > 0 and checkpoint_save_avg >= self.checkpoint_slow_ms
        )
        if writer_is_slow or checkpoint_is_slow:
            next_limit = max(self.min_limit, self.current_limit - self.scale_down_step)
            if next_limit < self.current_limit:
                self.current_limit = next_limit
                runtime_metrics.adaptive_backpressure_scale_down_count += 1
            return self.current_limit

        writer_fast_threshold = self.writer_slow_ms / 4 if self.writer_slow_ms > 0 else 0.0
        checkpoint_fast_threshold = (
            self.checkpoint_slow_ms / 4 if self.checkpoint_slow_ms > 0 else 0.0
        )
        writer_is_fast = writer_flush_delta == 0 or writer_flush_avg <= writer_fast_threshold
        checkpoint_is_fast = (
            checkpoint_save_delta == 0 or checkpoint_save_avg <= checkpoint_fast_threshold
        )
        backlog_saturated = pending_count >= self.current_limit
        if backlog_saturated and writer_is_fast and checkpoint_is_fast:
            next_limit = min(self.max_limit, self.current_limit + self.scale_up_step)
            if next_limit > self.current_limit:
                self.current_limit = next_limit
                runtime_metrics.adaptive_backpressure_scale_up_count += 1
        return self.current_limit
