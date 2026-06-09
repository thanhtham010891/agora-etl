"""Config-driven container assembly helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.component_factory import config_component_factory
from agora.core.errors import ConfigError
from agora.core.tracing import InMemoryTracer, NoopTracer, OpenTelemetryTracer
from agora.core.types import DLQFailurePolicy

if TYPE_CHECKING:
    from agora.core.container._container import AgoraContainer

_DEFAULT_DLQ_PATH = ".agora_dlq.db"


def populate_container_from_config(container: AgoraContainer, config: dict[str, Any]) -> None:
    """Register all pipeline components described by *config*."""
    pipeline_id = config.get("pipeline_id", "pipeline")
    source_cfg = config.get("source")

    build_source(container, source_cfg)
    build_middlewares(container, config)
    build_sinks(container, config)
    build_dlq(container, config)
    build_tracing(container, config, pipeline_id)
    container.register_singleton("_pipeline_id", pipeline_id)

    container._logger.info(
        "container_from_config",
        pipeline_id=pipeline_id,
        source=source_cfg.get("type") if source_cfg else None,
        middlewares=len(config.get("middlewares", [])),
        sinks=len(config.get("sinks", [])),
        dlq=(container.resolve("_dlq_sink_type") if container.has("_dlq_sink_type") else None),
        tracing=(
            container.resolve("_tracing_backend") if container.has("_tracing_backend") else None
        ),
    )


def build_source(container: AgoraContainer, source_cfg: dict[str, Any] | None) -> None:
    if source_cfg is not None:
        source = config_component_factory.build_component(source_cfg, "source")
        container.register_singleton("source", source)


def build_middlewares(container: AgoraContainer, config: dict[str, Any]) -> None:
    middlewares = []
    for i, mw_cfg in enumerate(config.get("middlewares", [])):
        mw = config_component_factory.build_middleware_component(mw_cfg)
        container.register_singleton(f"middleware.{i}.{mw_cfg.get('type', 'unknown')}", mw)
        middlewares.append(mw)
    dedup_cfg = config.get("dedup")
    if dedup_cfg is not None:
        dedup_mw = config_component_factory.build_dedup_component(dedup_cfg)
        container.register_singleton("dedup", dedup_mw)
        middlewares.append(dedup_mw)
    container.register_singleton("_middlewares", middlewares)


def build_sinks(container: AgoraContainer, config: dict[str, Any]) -> None:
    sinks = []
    for i, sink_cfg in enumerate(config.get("sinks", [])):
        sink = config_component_factory.build_component(sink_cfg, "sink")
        container.register_singleton(f"sink.{i}.{sink_cfg.get('type', 'unknown')}", sink)
        sinks.append(sink)
    container.register_singleton("_sinks", sinks)


def build_dlq(container: AgoraContainer, config: dict[str, Any]) -> None:
    dlq_cfg = config.get("dlq")
    if not isinstance(dlq_cfg, dict) or not dlq_cfg.get("enabled", True):
        return
    dlq_sink_cfg = dlq_cfg.get("sink")
    if not isinstance(dlq_sink_cfg, dict):
        dlq_sink_cfg = {
            "type": "sqlite_dlq",
            "path": dlq_cfg.get("path", _DEFAULT_DLQ_PATH),
        }
    dlq_sink = config_component_factory.build_component(dlq_sink_cfg, "sink")
    container.register_singleton("_dlq_sink", dlq_sink)
    container.register_singleton(
        "_dlq_failure_policy",
        DLQFailurePolicy(dlq_cfg.get("failure_policy", "log_only")),
    )
    container.register_singleton("_dlq_sink_type", dlq_sink_cfg.get("type", "sqlite_dlq"))


def build_tracing(container: AgoraContainer, config: dict[str, Any], pipeline_id: str) -> None:
    tracing_cfg = config.get("tracing")
    if not isinstance(tracing_cfg, dict):
        return
    tracer, tracing_backend = build_tracer_from_config(tracing_cfg, pipeline_id=pipeline_id)
    container.register_singleton("_tracer", tracer)
    container.register_singleton("_tracing_backend", tracing_backend)


def build_tracer_from_config(
    tracing_cfg: dict[str, Any],
    *,
    pipeline_id: str,
) -> tuple[Any, str]:
    enabled = tracing_cfg.get("enabled", True)
    backend = str(tracing_cfg.get("backend", "opentelemetry")).strip().lower()
    service_name = tracing_cfg.get("service_name") or pipeline_id

    if not enabled or backend == "noop":
        return NoopTracer(), "noop"
    if backend == "in_memory":
        return InMemoryTracer(), "in_memory"
    if backend == "opentelemetry":
        try:
            return OpenTelemetryTracer(name=service_name), "opentelemetry"
        except ImportError as exc:
            raise ConfigError(
                "Tracing backend 'opentelemetry' requires the optional "
                "'opentelemetry-api' dependency to be installed."
            ) from exc
    raise ConfigError(
        f"Unknown tracing backend '{backend}'. Expected one of: noop, in_memory, opentelemetry."
    )
