"""Config-driven container assembly helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.component_factory import config_component_factory
from agora.core.errors import ConfigError
from agora.core.tracing import InMemoryTracer, NoopTracer
from agora.core.tracing._opentelemetry import build_configured_opentelemetry_tracer
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
    build_performance(container, config)
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
        acceleration_mode=(
            container.resolve("_acceleration_mode") if container.has("_acceleration_mode") else None
        ),
        performance_profile=(
            container.resolve("_performance_profile")
            if container.has("_performance_profile")
            else None
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


def build_performance(container: AgoraContainer, config: dict[str, Any]) -> None:
    performance_cfg = config.get("performance")
    if not isinstance(performance_cfg, dict):
        return
    acceleration_mode = str(performance_cfg.get("acceleration", "auto")).strip().lower()
    profile = str(performance_cfg.get("profile", "balanced")).strip().lower()
    container.register_singleton("_acceleration_mode", acceleration_mode)
    container.register_singleton("_performance_profile", profile)


def build_tracer_from_config(
    tracing_cfg: dict[str, Any],
    *,
    pipeline_id: str,
) -> tuple[Any, str]:
    enabled = tracing_cfg.get("enabled", True)
    backend = str(tracing_cfg.get("backend", "opentelemetry")).strip().lower()
    auto_configure = bool(tracing_cfg.get("auto_configure", True))
    service_name = tracing_cfg.get("service_name") or pipeline_id

    if not enabled or backend == "noop":
        return NoopTracer(), "noop"
    if backend == "in_memory":
        return InMemoryTracer(), "in_memory"
    if backend == "opentelemetry":
        try:
            return (
                build_configured_opentelemetry_tracer(
                    name=service_name,
                    auto_configure=auto_configure,
                ),
                "opentelemetry",
            )
        except ImportError as exc:
            raise ConfigError(
                "Tracing backend 'opentelemetry' requires either a pre-configured "
                "global tracer provider or the optional dependencies "
                "'opentelemetry-api', 'opentelemetry-sdk', and "
                "'opentelemetry-exporter-otlp-proto-grpc'."
            ) from exc
    raise ConfigError(
        f"Unknown tracing backend '{backend}'. Expected one of: noop, in_memory, opentelemetry."
    )
