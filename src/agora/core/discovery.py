"""
agora/core/discovery.py
========================
Plugin auto-discovery — loads third-party plugins from setuptools entry-points.

Usage::

    # Discover all plugin types at once
    from agora.core.discovery import discover_plugins
    discover_plugins()

    # Or discover a specific category
    from agora.core.discovery import discover_sources
    discover_sources()

Third-party plugin registration
---------------------------------
In the third-party package's ``pyproject.toml``::

    [project.entry-points."agora.sources"]
    my_source = "my_package.sources:MySource"

    [project.entry-points."agora.sinks"]
    elasticsearch = "my_package.sinks:ElasticSink"

After ``pip install my-package``, calling ``discover_plugins()`` (or
``discover_sources()``) will auto-register ``my_source`` / ``elasticsearch``
into the corresponding registries.
"""

from __future__ import annotations

import logstruct

logger = logstruct.getLogger(__name__)

# Mapping of entry-point group → (module_path, registry_attr)
_ENTRYPOINT_GROUPS: dict[str, tuple[str, str]] = {
    "agora.sources": ("agora.sources", "source_registry"),
    "agora.sinks": ("agora.sinks", "sink_registry"),
    "agora.middlewares": ("agora.middlewares", "middleware_registry"),
    "agora.ai.providers": ("agora.ai", "ai_provider_registry"),
    "agora.ai.caches": ("agora.ai.cache", "ai_cache_registry"),
    "agora.metrics.exporters": ("agora.metrics.exporters", "metrics_exporter_registry"),
    "agora.middlewares.dedup.stores": (
        "agora.middlewares.dedup.stores",
        "dedup_store_registry",
    ),
    "agora.middlewares.dedup.strategies": (
        "agora.middlewares.dedup.strategies",
        "dedup_strategy_registry",
    ),
    "agora.state.backends": ("agora.state.registry", "state_backend_registry"),
    "agora.runner": ("agora.runner", "runner_registry"),
}


def _load_group(group: str) -> int:
    """Load entry-points for a single group. Returns count of plugins loaded."""
    import importlib

    entry = _ENTRYPOINT_GROUPS.get(group)
    if entry is None:
        logger.warning("discover_unknown_group", group=group)
        return 0

    module_path, attr_name = entry
    module = importlib.import_module(module_path)
    registry = getattr(module, attr_name)
    before = len(registry)
    registry.load_entrypoints(group)
    loaded = len(registry) - before
    if loaded > 0:
        logger.info(
            "discover_loaded",
            group=group,
            loaded=loaded,
            total=len(registry),
        )
    return loaded


def discover_plugins() -> dict[str, int]:
    """Discover and register all third-party plugins across all categories.

    Returns a dict of ``{group_name: count_of_new_plugins}``.

    Typically called once at application startup::

        from agora.core.discovery import discover_plugins
        stats = discover_plugins()
        # → {"agora.sources": 2, "agora.sinks": 1, ...}
    """
    results: dict[str, int] = {}
    for group in _ENTRYPOINT_GROUPS:
        results[group] = _load_group(group)
    total = sum(results.values())
    if total > 0:
        logger.info("discover_plugins_complete", total_new=total)
    return results


# ======================================================================
# Category-specific convenience functions
# ======================================================================


def discover_sources() -> int:
    """Discover third-party source plugins. Returns count loaded."""
    return _load_group("agora.sources")


def discover_sinks() -> int:
    """Discover third-party sink plugins. Returns count loaded."""
    return _load_group("agora.sinks")


def discover_middlewares() -> int:
    """Discover third-party middleware plugins. Returns count loaded."""
    return _load_group("agora.middlewares")


def discover_ai_providers() -> int:
    """Discover third-party AI provider plugins. Returns count loaded."""
    return _load_group("agora.ai.providers")


def discover_ai_caches() -> int:
    """Discover third-party AI cache plugins. Returns count loaded."""
    return _load_group("agora.ai.caches")


def discover_metrics_exporters() -> int:
    """Discover third-party metrics exporter plugins. Returns count loaded."""
    return _load_group("agora.metrics.exporters")


def discover_dedup_stores() -> int:
    """Discover third-party dedup store plugins. Returns count loaded."""
    return _load_group("agora.middlewares.dedup.stores")


def discover_dedup_strategies() -> int:
    """Discover third-party dedup strategy plugins. Returns count loaded."""
    return _load_group("agora.middlewares.dedup.strategies")


def discover_state_backends() -> int:
    """Discover third-party state backend plugins. Returns count loaded."""
    return _load_group("agora.state.backends")
