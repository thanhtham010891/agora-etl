"""Stable core facade for Agora framework contracts.

Advanced domain-specific contracts live under ``agora.core.<area>``.
Underscore-prefixed support modules remain internal implementation detail.
"""

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
from agora.core.registry import AGORA_PLUGIN_MANIFEST_VERSION, Registry
from agora.core.retry import RetryPolicy, retry_async
from agora.core.sink import BaseSink, SinkFanOut, SinkRouter, sink_data_plane_spec
from agora.core.source import BaseSource, IterableSource, source_data_plane_spec
from agora.core.types import (
    CheckpointFailurePolicy,
    DedupStoreFailurePolicy,
    DLQFailurePolicy,
    OnError,
)
from agora.core.writer import Writer, WriteResult

_PIPELINE_EXPORTS = (
    "AgoraContainer",
    "BaseSink",
    "BaseSource",
    "BoundPipeline",
    "DataPlane",
    "FilterMiddleware",
    "IterableSource",
    "MapMiddleware",
    "Middleware",
    "Pipeline",
    "RetryMiddleware",
    "RetryPolicy",
    "RouteMiddleware",
    "SinkDataPlaneSpec",
    "SinkFanOut",
    "SinkRouter",
    "SourceDataPlaneSpec",
    "WriteResult",
    "Writer",
    "discover_plugins",
    "retry_async",
    "sink_data_plane_spec",
    "source_data_plane_spec",
)

_RECOVERY_EXPORTS = (
    "Checkpoint",
    "CheckpointFailurePolicy",
    "CheckpointStore",
    "CheckpointValue",
    "CheckpointableSource",
    "DLQFailurePolicy",
    "DLQRecord",
    "DLQSink",
    "DedupStoreFailurePolicy",
    "InMemoryCheckpointStore",
    "OnError",
    "SQLiteCheckpointStore",
)

_PLUGIN_EXPORTS = (
    "AGORA_PLUGIN_MANIFEST_VERSION",
    "Configurable",
    "Lifecycle",
    "Plugin",
    "Registry",
)

_OBSERVABILITY_EXPORTS = (
    "MiddlewareStageExplain",
    "PipelineContext",
    "PipelineExplain",
    "PipelineMetrics",
    "PipelineRunSummary",
    "SinkWriteExplain",
)

_ERROR_EXPORTS = (
    "AgoraError",
    "ConfigError",
    "PipelineError",
    "PluginError",
    "PluginNotFoundError",
    "PluginValidationError",
    "RegistryError",
)

__all__ = [  # noqa: RUF022
    "AgoraContainer",
    "BaseSink",
    "BaseSource",
    "BoundPipeline",
    "DataPlane",
    "FilterMiddleware",
    "IterableSource",
    "MapMiddleware",
    "Middleware",
    "Pipeline",
    "RetryMiddleware",
    "RetryPolicy",
    "RouteMiddleware",
    "SinkDataPlaneSpec",
    "SinkFanOut",
    "SinkRouter",
    "SourceDataPlaneSpec",
    "WriteResult",
    "Writer",
    "discover_plugins",
    "retry_async",
    "sink_data_plane_spec",
    "source_data_plane_spec",
    "Checkpoint",
    "CheckpointFailurePolicy",
    "CheckpointStore",
    "CheckpointValue",
    "CheckpointableSource",
    "DLQFailurePolicy",
    "DLQRecord",
    "DLQSink",
    "DedupStoreFailurePolicy",
    "InMemoryCheckpointStore",
    "OnError",
    "SQLiteCheckpointStore",
    "AGORA_PLUGIN_MANIFEST_VERSION",
    "Configurable",
    "Lifecycle",
    "Plugin",
    "Registry",
    "MiddlewareStageExplain",
    "PipelineContext",
    "PipelineExplain",
    "PipelineMetrics",
    "PipelineRunSummary",
    "SinkWriteExplain",
    "AgoraError",
    "ConfigError",
    "PipelineError",
    "PluginError",
    "PluginNotFoundError",
    "PluginValidationError",
    "RegistryError",
]
