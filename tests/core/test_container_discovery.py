from __future__ import annotations

from agora.core.component_factory import config_component_factory
from agora.core.discovery import _ENTRYPOINT_GROUPS
from agora.middlewares import middleware_registry
from agora.middlewares.dedup.stores import dedup_store_registry
from agora.middlewares.dedup.strategies import dedup_strategy_registry
from agora.sources import source_registry


def test_get_registry_resolves_dedup_store_registry():
    assert config_component_factory.get_registry("dedup_store") is dedup_store_registry


def test_get_registry_resolves_dedup_strategy_registry():
    assert config_component_factory.get_registry("dedup_strategy") is dedup_strategy_registry


def test_discovery_uses_middlewares_dedup_entrypoint_groups():
    assert "agora.middlewares.dedup.stores" in _ENTRYPOINT_GROUPS
    assert "agora.middlewares.dedup.strategies" in _ENTRYPOINT_GROUPS


def test_discovery_includes_state_cache_and_metrics_groups():
    assert "agora.state.backends" in _ENTRYPOINT_GROUPS
    assert "agora.ai.caches" in _ENTRYPOINT_GROUPS
    assert "agora.metrics.exporters" in _ENTRYPOINT_GROUPS


def test_source_registry_exposes_iterable_factory():
    assert source_registry.has("iterable")


def test_middleware_registry_exposes_ai_batch():
    assert middleware_registry.has("ai_batch")
