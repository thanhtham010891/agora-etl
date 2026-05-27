"""
agora/core/container.py
========================
``AgoraContainer`` — lightweight async-aware dependency injection container.

The container manages plugin lifecycles and wiring. It is the primary
entry-point for config-driven pipeline assembly::

    container = AgoraContainer()
    container.register_singleton("db_pool", pool)
    container.register_factory("sink", lambda: sink_registry.create("postgres", pool=container.resolve("db_pool")))

    sink = container.resolve("sink")
    await container.startup_all()   # calls startup() on Lifecycle plugins
    ...
    await container.shutdown_all()  # reverse order

Config-driven pipeline assembly::

    container = AgoraContainer.from_config({
        "pipeline_id": "events_ingest",
        "source": {"type": "kafka", "topics": ["events"]},
        "middlewares": [
            {"type": "validate", "schema": "MyModel"},
        ],
        "sinks": [
            {"type": "postgres", "dsn": "postgresql://...", "table": "events"},
        ],
    })
    pipeline = container.build_pipeline()
    summary = await pipeline.run()

Design decisions
----------------
- **Singleton vs Transient**: singletons are cached after first resolve;
  transients are re-created on every ``resolve()`` call.
- **Lifecycle ordering**: ``startup_all()`` runs in registration order;
  ``shutdown_all()`` runs in reverse (like a stack).
- **No magic**: explicit registration, no classpath scanning.
  Pair with ``Registry.load_entrypoints()`` for auto-discovery.
- **Config-driven**: ``from_config()`` resolves types by name from the active
  plugin registries. Config keys mirror constructor kwargs.
"""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Any

import logstruct

from agora.core.component_factory import config_component_factory
from agora.core.errors import ConfigError
from agora.core.plugin import Lifecycle
from agora.core.tracing import InMemoryTracer, NoopTracer, OpenTelemetryTracer
from agora.core.types import DLQFailurePolicy

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.pipeline import BoundPipeline

logger = logstruct.getLogger(__name__)
_DEFAULT_DLQ_PATH = ".agora_dlq.db"


class _Lifetime(enum.Enum):
    SINGLETON = "singleton"
    TRANSIENT = "transient"


class AgoraContainer:
    """Lightweight async-aware DI container.

    Parameters
    ----------
    name:
        Identifier for log messages (useful when multiple containers exist).
    """

    def __init__(self, name: str = "default") -> None:
        self._name = name
        self._singletons: dict[str, Any] = {}
        self._factories: dict[str, tuple[Callable[..., Any], _Lifetime]] = {}
        self._registration_order: list[str] = []
        self._config: dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    # Registration                                                         #
    # ------------------------------------------------------------------ #

    def register_singleton(self, key: str, instance: Any) -> None:
        """Register a pre-built singleton instance.

        The instance is returned as-is on every ``resolve(key)`` call.
        If it satisfies ``Lifecycle``, it participates in
        ``startup_all()`` / ``shutdown_all()``.
        """
        self._singletons[key] = instance
        if key not in self._registration_order:
            self._registration_order.append(key)
        logger.debug("container_register_singleton", container=self._name, key=key)

    def register_factory(
        self,
        key: str,
        factory: Callable[..., Any],
        *,
        singleton: bool = True,
    ) -> None:
        """Register a factory callable.

        Parameters
        ----------
        key:
            Lookup key.
        factory:
            ``() -> instance`` or ``(**kwargs) -> instance``.
        singleton:
            If True (default), the factory is called once and the result
            cached.  If False, a new instance is created on every
            ``resolve()`` call (transient lifetime).
        """
        lifetime = _Lifetime.SINGLETON if singleton else _Lifetime.TRANSIENT
        self._factories[key] = (factory, lifetime)
        if key not in self._registration_order:
            self._registration_order.append(key)
        logger.debug(
            "container_register_factory",
            container=self._name,
            key=key,
            lifetime=lifetime.value,
        )

    # ------------------------------------------------------------------ #
    # Resolution                                                           #
    # ------------------------------------------------------------------ #

    def resolve(self, key: str, **kwargs: Any) -> Any:
        """Resolve a dependency by key.

        Lookup order:
        1. Cached singletons
        2. Factory → instantiate (cache if singleton lifetime)

        Raises
        ------
        KeyError
            If *key* is not registered.
        """
        # 1. Cached singleton
        if key in self._singletons:
            return self._singletons[key]

        # 2. Factory
        entry = self._factories.get(key)
        if entry is None:
            raise KeyError(
                f"Container '{self._name}': key '{key}' is not registered. "
                f"Available: {list(self.keys())}"
            )

        factory, lifetime = entry
        instance = factory(**kwargs)

        if lifetime == _Lifetime.SINGLETON:
            self._singletons[key] = instance

        return instance

    def has(self, key: str) -> bool:
        """Return True if *key* is registered."""
        return key in self._singletons or key in self._factories

    def keys(self) -> list[str]:
        """All registered keys in registration order."""
        return list(self._registration_order)

    # ------------------------------------------------------------------ #
    # Lifecycle management                                                 #
    # ------------------------------------------------------------------ #

    async def startup_all(self) -> None:
        """Call ``startup()`` on all resolved singletons that satisfy ``Lifecycle``.

        Unresolved singleton factories are eagerly resolved first so that
        plugins registered via ``from_config()`` are always started even
        if ``resolve()`` was never called explicitly.

        Called in registration order.
        """
        # Eagerly resolve singleton factories that haven't been resolved yet (D2 fix)
        for key in self._registration_order:
            entry = self._factories.get(key)
            if entry is not None:
                factory, lifetime = entry
                if lifetime == _Lifetime.SINGLETON and key not in self._singletons:
                    try:
                        self._singletons[key] = factory()
                    except Exception:
                        logger.exception(
                            "container_startup_resolve_error",
                            container=self._name,
                            key=key,
                        )
                        raise

        for key in self._registration_order:
            instance = self._singletons.get(key)
            if instance is not None and isinstance(instance, Lifecycle):
                logger.debug("container_startup", container=self._name, key=key)
                await instance.startup()

    async def shutdown_all(self) -> None:
        """Call ``shutdown()`` on all resolved singletons that satisfy ``Lifecycle``.

        Called in **reverse** registration order (stack semantics).
        Errors are logged but do not abort the shutdown sequence.
        """
        for key in reversed(self._registration_order):
            instance = self._singletons.get(key)
            if instance is not None and isinstance(instance, Lifecycle):
                try:
                    logger.debug("container_shutdown", container=self._name, key=key)
                    await instance.shutdown()
                except Exception:
                    logger.exception(
                        "container_shutdown_error",
                        container=self._name,
                        key=key,
                    )

    # ------------------------------------------------------------------ #
    # Config-driven assembly                                               #
    # ------------------------------------------------------------------ #

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> AgoraContainer:
        """Build a container from a configuration dict.

        The config dict describes a full pipeline declaratively::

            {
                "pipeline_id": "events_ingest",
                "source": {
                    "type": "kafka",
                    "topics": ["events"],
                    "bootstrap_servers": "localhost:9092",
                },
                "middlewares": [
                    {"type": "validate", "schema": "MyModel"},
                    {"type": "enrich", "enricher": some_callable},
                ],
                "sinks": [
                    {"type": "postgres", "dsn": "...", "table": "events"},
                    {"type": "stdout"},
                ],
            }

        Each ``"type"`` key is resolved from the corresponding registry.
        Remaining keys are passed as ``**kwargs`` to the constructor.

        Parameters
        ----------
        config:
            Pipeline configuration dictionary.

        Returns
        -------
        AgoraContainer
            A fully wired container. Call ``build_pipeline()`` to get
            a ``BoundPipeline``.

        Raises
        ------
        ConfigError
            If required keys are missing or a plugin type is unknown.
        """
        pipeline_id = config.get("pipeline_id", "pipeline")
        container = cls(name=pipeline_id)
        container._config = config

        source_cfg = config.get("source")
        _build_source(container, source_cfg)
        _build_middlewares(container, config)
        _build_sinks(container, config)
        _build_dlq(container, config)
        _build_tracing(container, config, pipeline_id)

        container.register_singleton("_pipeline_id", pipeline_id)

        logger.info(
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

        return container

    def build_pipeline(self) -> BoundPipeline[Any]:
        """Assemble a ``BoundPipeline`` from the registered components.

        Requires that the container was built via ``from_config()`` or
        has ``source``, ``_sinks``, and ``_middlewares`` registered.

        Returns
        -------
        BoundPipeline
            Ready to ``await pipeline.run()``.

        Raises
        ------
        ConfigError
            If required components are missing.
        """
        from agora.core.middleware import MiddlewareChain
        from agora.core.pipeline import BoundPipeline
        from agora.core.sink import SinkFanOut

        if not self.has("source"):
            raise ConfigError(
                "Cannot build pipeline: no 'source' registered. "
                "Use from_config() or register_singleton('source', ...)."
            )

        source = self.resolve("source")
        middlewares = self.resolve("_middlewares") if self.has("_middlewares") else []
        sinks = self.resolve("_sinks") if self.has("_sinks") else []
        pipeline_id = self.resolve("_pipeline_id") if self.has("_pipeline_id") else "pipeline"

        if not sinks:
            raise ConfigError(
                "Cannot build pipeline: no sinks are configured. "
                "Declarative pipelines must define at least one sink."
            )

        dlq_sink = self.resolve("_dlq_sink") if self.has("_dlq_sink") else None
        dlq_failure_policy = (
            self.resolve("_dlq_failure_policy")
            if self.has("_dlq_failure_policy")
            else DLQFailurePolicy.LOG_ONLY
        )
        tracer = self.resolve("_tracer") if self.has("_tracer") else None

        return BoundPipeline(
            source=source,
            chain=MiddlewareChain(middlewares),
            writer=SinkFanOut(sinks),
            pipeline_id=pipeline_id,
            dlq=dlq_sink,
            dlq_failure_policy=dlq_failure_policy,
            tracer=tracer,
        )

    # ------------------------------------------------------------------ #
    # Async context manager                                                #
    # ------------------------------------------------------------------ #

    async def __aenter__(self) -> AgoraContainer:
        await self.startup_all()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.shutdown_all()

    # ------------------------------------------------------------------ #
    # Dunder                                                               #
    # ------------------------------------------------------------------ #

    def __contains__(self, key: str) -> bool:
        return self.has(key)

    def __repr__(self) -> str:
        return (
            f"AgoraContainer(name={self._name!r}, "
            f"singletons={len(self._singletons)}, "
            f"factories={len(self._factories)})"
        )


def _build_source(container: AgoraContainer, source_cfg: dict[str, Any] | None) -> None:
    if source_cfg is not None:
        source = config_component_factory.build_component(source_cfg, "source")
        container.register_singleton("source", source)


def _build_middlewares(container: AgoraContainer, config: dict[str, Any]) -> None:
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


def _build_sinks(container: AgoraContainer, config: dict[str, Any]) -> None:
    sinks = []
    for i, sink_cfg in enumerate(config.get("sinks", [])):
        sink = config_component_factory.build_component(sink_cfg, "sink")
        container.register_singleton(f"sink.{i}.{sink_cfg.get('type', 'unknown')}", sink)
        sinks.append(sink)
    container.register_singleton("_sinks", sinks)


def _build_dlq(container: AgoraContainer, config: dict[str, Any]) -> None:
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


def _build_tracing(container: AgoraContainer, config: dict[str, Any], pipeline_id: str) -> None:
    tracing_cfg = config.get("tracing")
    if not isinstance(tracing_cfg, dict):
        return
    tracer, tracing_backend = _build_tracer_from_config(tracing_cfg, pipeline_id=pipeline_id)
    container.register_singleton("_tracer", tracer)
    container.register_singleton("_tracing_backend", tracing_backend)


def _build_tracer_from_config(
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
