from __future__ import annotations

import importlib
import sys
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest


class _FakeEntryPoint:
    def __init__(self, name: str, plugin: object) -> None:
        self.name = name
        self._plugin = plugin
        self.dist = SimpleNamespace(name="fake-dedup-plugin", version="0.0.1")

    def load(self) -> object:
        return self._plugin


def _reload_dedup_store_module() -> ModuleType:
    module_name = "agora.middlewares.dedup.stores"
    sys.modules.pop(module_name, None)
    parent = sys.modules.get("agora.middlewares.dedup")
    if parent is not None and hasattr(parent, "stores"):
        delattr(parent, "stores")
    return importlib.import_module(module_name)


def test_dedup_store_registry_import_defers_entrypoint_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    groups: list[str] = []
    monkeypatch.setattr(
        "importlib.metadata.entry_points",
        lambda *, group: groups.append(group) or [],
    )

    module = _reload_dedup_store_module()

    assert "agora.middlewares.dedup.stores" not in groups
    assert "memory" in module.dedup_store_registry._plugins
    assert "sqlite" in module.dedup_store_registry._plugins


def test_dedup_store_registry_lookup_lazy_loads_entrypoints_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _reload_dedup_store_module()

    class FakeRedisStore:
        pass

    FakeRedisStore.__module__ = __name__

    groups: list[str] = []
    monkeypatch.setattr(
        "importlib.metadata.entry_points",
        lambda *, group: groups.append(group) or [_FakeEntryPoint("redis", FakeRedisStore)],
    )

    assert module.dedup_store_registry.get("redis") is FakeRedisStore
    assert module.dedup_store_registry.get("redis") is FakeRedisStore
    assert groups == ["agora.middlewares.dedup.stores"]
