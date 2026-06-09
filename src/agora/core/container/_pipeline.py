"""Pipeline assembly helpers for the container."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.errors import ConfigError
from agora.core.types import DeliveryConfig, DLQFailurePolicy

if TYPE_CHECKING:
    from agora.core.container._container import AgoraContainer
    from agora.core.pipeline import BoundPipeline


def build_pipeline_from_container(container: AgoraContainer) -> BoundPipeline[Any]:
    """Assemble a ``BoundPipeline`` from the registered components."""
    from agora.core.middleware import MiddlewareChain
    from agora.core.pipeline import BoundPipeline
    from agora.core.sink import SinkFanOut

    if not container.has("source"):
        raise ConfigError(
            "Cannot build pipeline: no 'source' registered. "
            "Use from_config() or register_singleton('source', ...)."
        )

    source = container.resolve("source")
    middlewares = container.resolve("_middlewares") if container.has("_middlewares") else []
    sinks = container.resolve("_sinks") if container.has("_sinks") else []
    pipeline_id = container.resolve("_pipeline_id") if container.has("_pipeline_id") else "pipeline"

    if not sinks:
        raise ConfigError(
            "Cannot build pipeline: no sinks are configured. "
            "Declarative pipelines must define at least one sink."
        )

    dlq_sink = container.resolve("_dlq_sink") if container.has("_dlq_sink") else None
    dlq_failure_policy = (
        container.resolve("_dlq_failure_policy")
        if container.has("_dlq_failure_policy")
        else DLQFailurePolicy.LOG_ONLY
    )
    tracer = container.resolve("_tracer") if container.has("_tracer") else None

    return BoundPipeline(
        source=source,
        chain=MiddlewareChain(middlewares),
        writer=SinkFanOut(sinks),
        pipeline_id=pipeline_id,
        config=DeliveryConfig(
            dlq=dlq_sink,
            dlq_failure_policy=dlq_failure_policy,
            tracer=tracer,
        ),
    )
