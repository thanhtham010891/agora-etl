"""Pipeline delivery-side config models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from agora.core.acceleration import AccelerationMode
from agora.core.types._policies import (
    CheckpointFailurePolicy,
    DLQFailurePolicy,
    SinkFailurePolicy,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from agora.core.checkpoint import CheckpointStore
    from agora.core.dlq import DLQRecord
    from agora.core.sink import BaseSink
    from agora.core.tracing import PipelineTracer


@dataclass(frozen=True, slots=True)
class Backpressure:
    """Adaptive backpressure configuration for pipeline runtime."""

    min_buffer_size: int = 1
    max_buffer_size: int | None = None
    scale_up_step: int = 1
    scale_down_step: int = 1
    writer_slow_ms: float = 25.0
    checkpoint_slow_ms: float = 10.0

    @classmethod
    def adaptive(
        cls,
        *,
        min_buffer_size: int = 1,
        max_buffer_size: int | None = None,
        scale_up_step: int = 1,
        scale_down_step: int = 1,
        writer_slow_ms: float = 25.0,
        checkpoint_slow_ms: float = 10.0,
    ) -> Backpressure:
        return cls(
            min_buffer_size=min_buffer_size,
            max_buffer_size=max_buffer_size,
            scale_up_step=scale_up_step,
            scale_down_step=scale_down_step,
            writer_slow_ms=writer_slow_ms,
            checkpoint_slow_ms=checkpoint_slow_ms,
        )


@dataclass(frozen=True, slots=True)
class DeliveryConfig:
    """Delivery-side pipeline config."""

    acceleration_mode: AccelerationMode | str = AccelerationMode.AUTO
    performance_profile: str = "balanced"
    dlq: BaseSink[DLQRecord] | None = None
    dlq_failure_policy: DLQFailurePolicy = DLQFailurePolicy.LOG_ONLY
    dlq_redactor: Callable[[Any], Any] | None = None
    checkpoint: CheckpointStore | None = None
    checkpoint_key: str | None = None
    checkpoint_every: int = 1
    checkpoint_failure_policy: CheckpointFailurePolicy = CheckpointFailurePolicy.FAIL_CLOSED
    batch_size: int = 1
    batch_flush_interval_ms: int | None = None
    sink_failure_policy: SinkFailurePolicy = SinkFailurePolicy.FAIL_CLOSED
    sink_concurrency: int | None = None
    max_buffer_size: int | None = None
    backpressure: Backpressure | None = None
    tracer: PipelineTracer | None = None


@dataclass(frozen=True, slots=True)
class PerformanceProfileSettings:
    """Resolved runtime knobs represented by a performance profile."""

    profile: str
    writer_batch_size: int
    flush_cadence_ms: int | None
    prefetch_limit: int | None
    max_in_flight_batches: int | None
    backpressure_min_buffer_size: int | None
    backpressure_max_buffer_size: int | None
    backpressure_writer_slow_ms: float | None
    backpressure_checkpoint_slow_ms: float | None

    def to_dict(self) -> dict[str, int | float | str | None]:
        return {
            "profile": self.profile,
            "writer_batch_size": self.writer_batch_size,
            "flush_cadence_ms": self.flush_cadence_ms,
            "prefetch_limit": self.prefetch_limit,
            "max_in_flight_batches": self.max_in_flight_batches,
            "backpressure_min_buffer_size": self.backpressure_min_buffer_size,
            "backpressure_max_buffer_size": self.backpressure_max_buffer_size,
            "backpressure_writer_slow_ms": self.backpressure_writer_slow_ms,
            "backpressure_checkpoint_slow_ms": self.backpressure_checkpoint_slow_ms,
        }
