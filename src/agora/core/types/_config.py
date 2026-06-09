"""Pipeline delivery-side config models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from agora.core.types._policies import (
    CheckpointFailurePolicy,
    DLQFailurePolicy,
    SinkFailurePolicy,
)

if TYPE_CHECKING:
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

    dlq: BaseSink[DLQRecord] | None = None
    dlq_failure_policy: DLQFailurePolicy = DLQFailurePolicy.LOG_ONLY
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
