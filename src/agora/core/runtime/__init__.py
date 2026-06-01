"""Internal runtime coordinators for pipeline execution.

Submodules:
- _delivery        — DeliveryEngine, sink/DLQ/checkpoint types
- _plan            — RuntimePlan and lane selection
- _source_adapter  — SourceRuntimeAdapter
- _writer_transport — WriterTransport
- _buffered        — ExecutionCoordinator and shared source/runtime helpers
"""

import warnings

from agora.core.runtime._buffered import (
    AdaptiveBackpressureController,
    ExecutionCoordinator,
)
from agora.core.runtime._delivery import (
    CheckpointedOutcome,
    CheckpointState,
    CommitOutcome,
    DeliveryEngine,
    Dropped,
    ErroredRouted,
    ErroredUnrouted,
    PendingWrite,
    ProcessedSourceRecord,
    RecordDeliveryError,
    RunState,
    SourceQueueError,
    SourceRecord,
    Written,
    make_checkpoint_state,
)
from agora.core.runtime._hot_metrics import HotPathMetrics
from agora.core.runtime._plan import BufferedStageSpec, RuntimeLane, RuntimePlan, build_runtime_plan
from agora.core.runtime._source_adapter import SOURCE_QUEUE_DONE, SourceRuntimeAdapter

__all__ = [
    "SOURCE_QUEUE_DONE",
    "AdaptiveBackpressureController",
    "BufferedStageSpec",
    "CheckpointState",
    "CheckpointedOutcome",
    "CommitOutcome",
    "DeliveryEngine",
    "Dropped",
    "ErroredRouted",
    "ErroredUnrouted",
    "ExecutionCoordinator",
    "HotPathMetrics",
    "PendingWrite",
    "ProcessedSourceRecord",
    "RecordDeliveryCoordinator",
    "RecordDeliveryError",
    "RunState",
    "RuntimeLane",
    "RuntimePlan",
    "SourceQueueError",
    "SourceRecord",
    "SourceRuntimeAdapter",
    "WriterTransport",
    "Written",
    "build_runtime_plan",
    "make_checkpoint_state",
]


def __getattr__(name: str) -> object:
    if name == "RecordDeliveryCoordinator":
        warnings.warn(
            "RecordDeliveryCoordinator is deprecated; use DeliveryEngine instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return DeliveryEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
