"""agora.core — framework mechanics."""

from agora.core.checkpoint import (
    Checkpoint,
    CheckpointableSource,
    CheckpointStore,
    CheckpointValue,
    InMemoryCheckpointStore,
    SQLiteCheckpointStore,
)
from agora.core.container import AgoraContainer
from agora.core.context import PipelineContext
from agora.core.data_plane import DataPlane, SinkDataPlaneSpec, SourceDataPlaneSpec
from agora.core.discovery import discover_plugins
from agora.core.dlq import DLQRecord, DLQSink
from agora.core.errors import (
    AgoraError,
    ConfigError,
    PipelineError,
    PluginError,
    PluginNotFoundError,
    PluginValidationError,
    RegistryError,
)
from agora.core.explain import MiddlewareStageExplain, PipelineExplain, SinkWriteExplain
from agora.core.metrics import PipelineMetrics, PipelineRunSummary
from agora.core.middleware import (
    FilterMiddleware,
    MapMiddleware,
    Middleware,
    RetryMiddleware,
    RouteMiddleware,
)
from agora.core.pipeline import BoundPipeline, Pipeline
from agora.core.plugin import Configurable, Lifecycle, Plugin
from agora.core.registry import Registry
from agora.core.retry import RetryPolicy, retry_async
from agora.core.sink import BaseSink, SinkFanOut, SinkRouter
from agora.core.source import BaseSource, IterableSource
from agora.core.types import (
    CheckpointFailurePolicy,
    DedupStoreFailurePolicy,
    DLQFailurePolicy,
    OnError,
)
from agora.core.writer import Writer, WriteResult

__all__ = [
    "AgoraContainer",
    "AgoraError",
    "BaseSink",
    "BaseSource",
    "BoundPipeline",
    "Checkpoint",
    "CheckpointFailurePolicy",
    "CheckpointStore",
    "CheckpointValue",
    "CheckpointableSource",
    "ConfigError",
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
    "PipelineError",
    "PipelineExplain",
    "PipelineMetrics",
    "PipelineRunSummary",
    "Plugin",
    "PluginError",
    "PluginNotFoundError",
    "PluginValidationError",
    "Registry",
    "RegistryError",
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
]
