# ruff: noqa: F401

"""Builder-first public facade for ``agora-etl``.

The package root keeps the most common pipeline, middleware, tracing, and state
entrypoints close at hand. Lower-level framework contracts remain available
under ``agora.core.<area>``.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from agora.core import (
    AgoraContainer,
    AgoraError,
    BaseSink,
    BaseSource,
    BoundPipeline,
    Checkpoint,
    CheckpointFailurePolicy,
    CheckpointStore,
    Configurable,
    DataPlane,
    DedupStoreFailurePolicy,
    DLQFailurePolicy,
    DLQRecord,
    DLQSink,
    FilterMiddleware,
    InMemoryCheckpointStore,
    IterableSource,
    Lifecycle,
    MapMiddleware,
    Middleware,
    MiddlewareStageExplain,
    OnError,
    Pipeline,
    PipelineContext,
    PipelineExplain,
    PipelineMetrics,
    PipelineRunSummary,
    Plugin,
    Registry,
    RetryMiddleware,
    RetryPolicy,
    RouteMiddleware,
    SinkDataPlaneSpec,
    SinkFanOut,
    SinkRouter,
    SinkWriteExplain,
    SourceDataPlaneSpec,
    SQLiteCheckpointStore,
    Writer,
    WriteResult,
    discover_plugins,
    retry_async,
    sink_data_plane_spec,
    source_data_plane_spec,
)
from agora.core.batch import (
    ArrowBatchMiddleware,
    ArrowNativeSink,
    BatchableSource,
    BatchFailure,
    BatchMiddleware,
    BatchProcessResult,
    is_arrow_batch_middleware,
    is_arrow_native_sink,
    is_batch_capable_source,
)
from agora.core.middleware import BatchFilterMiddleware, BatchMapMiddleware
from agora.core.source import (
    SourceRecordError,
    SourceRuntimeMetrics,
)
from agora.core.tracing import InMemoryTracer, NoopTracer, OpenTelemetryTracer
from agora.core.types import (
    Backpressure,
    DeliveryConfig,
    SinkFailurePolicy,
    SourceRecordFailurePolicy,
)
from agora.middlewares.arrow import ArrowFilterMiddleware, ArrowMapMiddleware
from agora.middlewares.process import ArrowProcessBatchMiddleware, ProcessBatchMiddleware
from agora.sources.file.csv import ArrowCsvSource
from agora.sources.file.jsonlines import ArrowJsonLinesSource
from agora.state import (
    MembershipKeyStore,
    MemoryBackend,
    SQLiteBackend,
    StateBackend,
    StateValue,
    StoredValue,
    TTLKeyValueStore,
    state_backend_registry,
)

try:
    __version__ = _pkg_version("agora-etl")
except PackageNotFoundError:
    __version__ = "0+unknown"

_ROOT_CORE_EXPORTS = (
    "AgoraContainer",
    "AgoraError",
    "BaseSink",
    "BaseSource",
    "BoundPipeline",
    "Checkpoint",
    "CheckpointFailurePolicy",
    "CheckpointStore",
    "Configurable",
    "DLQFailurePolicy",
    "DLQRecord",
    "DLQSink",
    "DataPlane",
    "DedupStoreFailurePolicy",
    "FilterMiddleware",
    "InMemoryCheckpointStore",
    "IterableSource",
    "Lifecycle",
    "MapMiddleware",
    "Middleware",
    "MiddlewareStageExplain",
    "OnError",
    "Pipeline",
    "PipelineContext",
    "PipelineExplain",
    "PipelineMetrics",
    "PipelineRunSummary",
    "Plugin",
    "Registry",
    "RetryMiddleware",
    "RetryPolicy",
    "RouteMiddleware",
    "SQLiteCheckpointStore",
    "SinkDataPlaneSpec",
    "SinkFanOut",
    "SinkRouter",
    "SinkWriteExplain",
    "SourceDataPlaneSpec",
    "WriteResult",
    "Writer",
    "discover_plugins",
    "retry_async",
    "sink_data_plane_spec",
    "source_data_plane_spec",
)

_BATCH_EXPORTS = (
    "ArrowBatchMiddleware",
    "ArrowCsvSource",
    "ArrowFilterMiddleware",
    "ArrowJsonLinesSource",
    "ArrowMapMiddleware",
    "ArrowNativeSink",
    "ArrowProcessBatchMiddleware",
    "BatchFailure",
    "BatchFilterMiddleware",
    "BatchMapMiddleware",
    "BatchMiddleware",
    "BatchProcessResult",
    "BatchableSource",
    "ProcessBatchMiddleware",
    "is_arrow_batch_middleware",
    "is_arrow_native_sink",
    "is_batch_capable_source",
)

_RUNTIME_EXPORTS = (
    "Backpressure",
    "DeliveryConfig",
    "SinkFailurePolicy",
    "SourceRecordError",
    "SourceRecordFailurePolicy",
    "SourceRuntimeMetrics",
)

_TRACING_EXPORTS = (
    "InMemoryTracer",
    "NoopTracer",
    "OpenTelemetryTracer",
)

_STATE_EXPORTS = (
    "MembershipKeyStore",
    "MemoryBackend",
    "SQLiteBackend",
    "StateBackend",
    "StateValue",
    "StoredValue",
    "TTLKeyValueStore",
    "state_backend_registry",
)

__all__ = [
    *_ROOT_CORE_EXPORTS,
    *_BATCH_EXPORTS,
    *_RUNTIME_EXPORTS,
    *_TRACING_EXPORTS,
    *_STATE_EXPORTS,
    "__version__",
]
