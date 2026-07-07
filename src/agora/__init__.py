# ruff: noqa: F401

"""Builder-first public facade for ``agora-etl``.

The package root keeps the most common pipeline, middleware, tracing, and state
entrypoints close at hand. Lower-level framework contracts remain available
under ``agora.core.<area>``.
"""

import warnings
from importlib import import_module
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from agora.core import (
    AgoraContainer,
    AgoraError,
    BoundPipeline,
    Checkpoint,
    CheckpointFailurePolicy,
    Configurable,
    DataPlane,
    DedupStoreFailurePolicy,
    DLQFailurePolicy,
    DLQRecord,
    DLQSink,
    FilterMiddleware,
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
    RetryMiddleware,
    RetryPolicy,
    RouteMiddleware,
    SinkDataPlaneSpec,
    SinkFanOut,
    SinkRouter,
    SinkWriteExplain,
    SourceDataPlaneSpec,
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

# Internal policy metadata to guide future root-facade slimming without
# breaking the current public import surface in one jump.
_ROOT_KEEP_AT_ROOT_EXPORTS = frozenset(
    {
        "Pipeline",
        "BoundPipeline",
        "IterableSource",
        "DeliveryConfig",
        "Backpressure",
        "FilterMiddleware",
        "MapMiddleware",
        "RetryMiddleware",
        "RouteMiddleware",
        "PipelineExplain",
        "PipelineRunSummary",
    }
)

_ROOT_PREFER_CORE_OVER_ROOT_EXPORTS = frozenset(
    {
        "AgoraContainer",
        "BaseSink",
        "BaseSource",
        "Checkpoint",
        "CheckpointFailurePolicy",
        "CheckpointStore",
        "InMemoryCheckpointStore",
        "Plugin",
        "Registry",
        "SQLiteCheckpointStore",
        "SourceRecordError",
        "SourceRuntimeMetrics",
        "WriteResult",
        "Writer",
        "state_backend_registry",
    }
)

_ROOT_SOFT_DEPRECATED_EXPORTS = frozenset(
    {
        "BaseSink",
        "BaseSource",
        "CheckpointStore",
        "InMemoryCheckpointStore",
        "Registry",
        "SourceRecordError",
        "SourceRuntimeMetrics",
        "SQLiteCheckpointStore",
        "WriteResult",
        "Writer",
        "state_backend_registry",
    }
)

__all__ = [
    *_ROOT_CORE_EXPORTS,
    *_BATCH_EXPORTS,
    *_RUNTIME_EXPORTS,
    *_TRACING_EXPORTS,
    *_STATE_EXPORTS,
    "__version__",
]

_ROOT_PUBLIC_EXPORTS = frozenset(name for name in __all__ if name != "__version__")
_ROOT_REVIEW_BEFORE_KEEPING_EXPORTS = frozenset(
    _ROOT_PUBLIC_EXPORTS - _ROOT_KEEP_AT_ROOT_EXPORTS - _ROOT_PREFER_CORE_OVER_ROOT_EXPORTS
)
_ROOT_DEPRECATION_CANDIDATES = frozenset(
    {
        "BaseSink",
        "BaseSource",
        "CheckpointStore",
        "InMemoryCheckpointStore",
        "Registry",
        "SQLiteCheckpointStore",
        "SourceRecordError",
        "SourceRuntimeMetrics",
        "WriteResult",
        "Writer",
        "state_backend_registry",
    }
)

_DEPRECATED_ROOT_EXPORT_TARGETS: dict[str, tuple[str, str, str]] = {
    "BaseSink": (
        "agora.core.sink",
        "BaseSink",
        "agora.core.sink.BaseSink",
    ),
    "BaseSource": (
        "agora.core.source",
        "BaseSource",
        "agora.core.source.BaseSource",
    ),
    "CheckpointStore": (
        "agora.core.checkpoint",
        "CheckpointStore",
        "agora.core.checkpoint.CheckpointStore",
    ),
    "InMemoryCheckpointStore": (
        "agora.core.checkpoint",
        "InMemoryCheckpointStore",
        "agora.core.checkpoint.InMemoryCheckpointStore",
    ),
    "Registry": (
        "agora.core.registry",
        "Registry",
        "agora.core.registry.Registry",
    ),
    "SourceRecordError": (
        "agora.core.source",
        "SourceRecordError",
        "agora.core.source.SourceRecordError",
    ),
    "SourceRuntimeMetrics": (
        "agora.core.source",
        "SourceRuntimeMetrics",
        "agora.core.source.SourceRuntimeMetrics",
    ),
    "SQLiteCheckpointStore": (
        "agora.core.checkpoint",
        "SQLiteCheckpointStore",
        "agora.core.checkpoint.SQLiteCheckpointStore",
    ),
    "WriteResult": (
        "agora.core.writer",
        "WriteResult",
        "agora.core.writer.WriteResult",
    ),
    "Writer": (
        "agora.core.writer",
        "Writer",
        "agora.core.writer.Writer",
    ),
    "state_backend_registry": (
        "agora.state",
        "state_backend_registry",
        "agora.state.state_backend_registry",
    ),
}


def __getattr__(name: str) -> object:
    try:
        module_name, attr_name, replacement = _DEPRECATED_ROOT_EXPORT_TARGETS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc

    value = getattr(import_module(module_name), attr_name)
    warnings.warn(
        (
            f"`agora.{name}` is soft-deprecated and will move off the root facade "
            f"in a future release; import `{replacement}` instead."
        ),
        DeprecationWarning,
        stacklevel=2,
    )
    globals()[name] = value
    return value
