"""
agora — async ETL framework.

Public API
----------
The most important classes are importable directly from ``agora``::

    from agora import Pipeline, BoundPipeline
    from agora import BaseSource, BaseSink, Middleware
    from agora import Registry, PipelineContext, PipelineRunSummary

    # Plugin system
    from agora import Plugin, Lifecycle, Configurable
    from agora import Writer, WriteResult
    from agora import AgoraContainer, AgoraError
    from agora import discover_plugins

    # Sources
    from agora import IterableSource
    from agora_plugins.kafka import KafkaSource
    from agora.sources.http.http import HTTPSource, StopFetching
    from agora.sources.file import FileSource, JsonLinesSource, ParquetSource

    # Sinks
    from agora_plugins.postgres import PostgresSink
    from agora_plugins.kafka import KafkaSink
    from agora.sinks.io.stdout import StdoutSink

    # Middlewares (built-in)
    from agora.middlewares import ValidateMiddleware, EnrichMiddleware

    # Dedup
    from agora.middlewares.dedup import DedupMiddleware
    from agora.middlewares.dedup.stores.memory import InMemoryStore
    from agora_plugins.redis import RedisStore
    from agora.middlewares.dedup.strategies.fuzzy import FuzzyMatchStrategy

    # Config
    from agora.config import AgoraSettings
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from agora.core.checkpoint import (
    Checkpoint,
    CheckpointStore,
    InMemoryCheckpointStore,
    SQLiteCheckpointStore,
)
from agora.core.container import AgoraContainer
from agora.core.context import PipelineContext
from agora.core.discovery import discover_plugins
from agora.core.dlq import DLQRecord, DLQSink
from agora.core.errors import AgoraError
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
from agora.core.source import BaseSource, IterableSource, SourceRecordError, SourceRuntimeMetrics
from agora.core.tracing import InMemoryTracer, NoopTracer, OpenTelemetryTracer
from agora.core.types import (
    Backpressure,
    CheckpointFailurePolicy,
    DedupStoreFailurePolicy,
    DLQFailurePolicy,
    OnError,
    SinkFailurePolicy,
    SourceRecordFailurePolicy,
)
from agora.core.writer import Writer, WriteResult
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
__all__ = [
    "AgoraContainer",
    "AgoraError",
    "Backpressure",
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
    "DedupStoreFailurePolicy",
    "FilterMiddleware",
    "InMemoryCheckpointStore",
    "InMemoryTracer",
    "IterableSource",
    "Lifecycle",
    "MapMiddleware",
    "MembershipKeyStore",
    "MemoryBackend",
    "Middleware",
    "NoopTracer",
    "OnError",
    "OpenTelemetryTracer",
    "Pipeline",
    "PipelineContext",
    "PipelineMetrics",
    "PipelineRunSummary",
    "Plugin",
    "Registry",
    "RetryMiddleware",
    "RetryPolicy",
    "RouteMiddleware",
    "SQLiteBackend",
    "SQLiteCheckpointStore",
    "SinkFailurePolicy",
    "SinkFanOut",
    "SinkRouter",
    "SourceRecordError",
    "SourceRecordFailurePolicy",
    "SourceRuntimeMetrics",
    "StateBackend",
    "StateValue",
    "StoredValue",
    "TTLKeyValueStore",
    "WriteResult",
    "Writer",
    "__version__",
    "discover_plugins",
    "retry_async",
    "state_backend_registry",
]
