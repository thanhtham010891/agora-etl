"""
agora/core/plugin.py
====================
Core plugin protocols — the structural contracts that all agora plugins
may satisfy.

Design decisions
----------------
- **Protocol (not ABC)**: external plugins can satisfy the contract
  via duck-typing without importing agora at all.
- **Granular protocols**: ``Lifecycle`` and ``Configurable`` are opt-in.
  Simple plugins only need to satisfy ``Plugin``.
- **runtime_checkable**: registries can validate at registration time
  via ``isinstance(obj, Plugin)``.

Usage::

    # A minimal plugin — just needs plugin_name
    class MySource:
        plugin_name = "my_source"
        plugin_version = "1.0.0"

    # A full-featured plugin with lifecycle + config
    class MyComplexSink:
        plugin_name = "complex_sink"
        plugin_version = "2.1.0"

        async def startup(self) -> None:
            self._pool = await create_pool(...)

        async def shutdown(self) -> None:
            await self._pool.close()

        @classmethod
        def from_config(cls, config: dict) -> "MyComplexSink":
            return cls(dsn=config["dsn"], batch_size=config.get("batch_size", 100))
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

# ======================================================================
# Plugin — base identity protocol
# ======================================================================


@runtime_checkable
class Plugin(Protocol):
    """Base structural protocol that every agora plugin may satisfy.

    Provides identity metadata used by registries, CLI listing,
    health endpoints, and diagnostic logging.

    Attributes
    ----------
    plugin_name:
        Unique human-readable identifier (e.g. ``"postgres"``, ``"kafka"``).
    plugin_version:
        Semver-like version string for the plugin implementation.
    """

    @property
    def plugin_name(self) -> str: ...

    @property
    def plugin_version(self) -> str: ...


# ======================================================================
# Lifecycle — optional async setup/teardown
# ======================================================================


@runtime_checkable
class Lifecycle(Protocol):
    """Optional lifecycle hooks for plugins that manage resources.

    Plugins satisfying this protocol will have ``startup()`` called
    before first use and ``shutdown()`` called during container/pool
    teardown — in reverse registration order.

    Examples: database pools, HTTP clients, Kafka producers.
    """

    async def startup(self) -> None:
        """Acquire resources (connections, file handles, etc.)."""
        ...

    async def shutdown(self) -> None:
        """Release resources.  Called even if startup() was never called."""
        ...


# ======================================================================
# Configurable — factory from dict/settings
# ======================================================================


@runtime_checkable
class Configurable(Protocol):
    """Plugin that can be instantiated from a configuration dict.

    Enables config-driven pipeline assembly::

        config = {"type": "postgres", "dsn": "postgresql://...", "table": "events"}
        sink_cls = sink_registry.get_or_raise(config["type"])
        sink = sink_cls.from_config(config)

    The ``config`` dict typically comes from YAML, TOML, or
    ``AgoraSettings`` sub-models.
    """

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Configurable:
        """Create a fully configured instance from *config*."""
        ...
