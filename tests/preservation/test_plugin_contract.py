"""
tests/preservation/test_plugin_contract.py
==========================================
Preservation tests for the plugin contract declared in
``packages/agora/docs/plugins/contract.md``.

Each test maps to one declared guarantee. If a test fails, the public
contract is broken — fix the code, not the test.

Coverage:
- Each ``stable`` entry-point group has a registry that exists and exposes
  ``load_entrypoints()``.
- The manifest compatibility matrix (compatible / incompatible / no-manifest).
- Incompatible plugins are excluded from the active registry but recorded
  in diagnostics.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from agora.core.discovery import public_entrypoint_group_contracts
from agora.core.registry import (
    AGORA_PLUGIN_MANIFEST_VERSION,
    Registry,
    RegistryItemInfo,
)

pytestmark = pytest.mark.contract

# ======================================================================
# [CONTRACT-02] Stable registries exist and expose load_entrypoints
# ======================================================================


@pytest.mark.parametrize(
    "module_path,attr",
    [
        ("agora.sources", "source_registry"),
        ("agora.sinks", "sink_registry"),
        ("agora.middlewares", "middleware_registry"),
        ("agora.runner", "runner_registry"),
        ("agora.middlewares.dedup.stores", "dedup_store_registry"),
        ("agora.middlewares.dedup.strategies", "dedup_strategy_registry"),
    ],
)
def test_c02_stable_registry_exists_and_has_load_entrypoints(module_path: str, attr: str) -> None:
    """[CONTRACT-02] Each ``stable`` entry-point group must have a registry
    accessible at the declared module path and attribute name, and that
    registry must expose ``load_entrypoints()``.

    Validates: docs/plugins/contract.md — "Entry-point groups" (stable rows)
    """
    import importlib

    module = importlib.import_module(module_path)
    registry = getattr(module, attr, None)

    assert registry is not None, f"[CONTRACT-02] {module_path}.{attr} must exist"
    assert isinstance(registry, Registry), (
        f"[CONTRACT-02] {module_path}.{attr} must be a Registry instance"
    )
    assert hasattr(registry, "load_entrypoints"), (
        f"[CONTRACT-02] {module_path}.{attr} must expose load_entrypoints()"
    )
    assert callable(registry.load_entrypoints), (
        f"[CONTRACT-02] {module_path}.{attr}.load_entrypoints must be callable"
    )


# ======================================================================
# [CONTRACT-03] Provisional registries exist
# ======================================================================


@pytest.mark.parametrize(
    "module_path,attr",
    [
        ("agora.ai", "ai_provider_registry"),
        ("agora.ai.cache", "ai_cache_registry"),
        ("agora.metrics.exporters", "metrics_exporter_registry"),
        ("agora.state.registry", "state_backend_registry"),
    ],
)
def test_c03_provisional_registry_exists(module_path: str, attr: str) -> None:
    """[CONTRACT-03] Each ``provisional`` entry-point group must have a
    registry at the declared module path and attribute name.

    Validates: docs/plugins/contract.md — "Entry-point groups" (provisional rows)
    """
    import importlib

    module = importlib.import_module(module_path)
    registry = getattr(module, attr, None)

    assert registry is not None, f"[CONTRACT-03] {module_path}.{attr} must exist"
    assert isinstance(registry, Registry), (
        f"[CONTRACT-03] {module_path}.{attr} must be a Registry instance"
    )


def test_c03b_public_entrypoint_group_contracts_match_docs() -> None:
    """[CONTRACT-03B] The public entry-point group helper must reflect the
    same stable/provisional groups declared in docs/plugins/contract.md.

    Validates: docs/plugins/contract.md — "Entry-point groups"
    """
    contracts = {
        (contract.group, contract.registry_attr, contract.stability)
        for contract in public_entrypoint_group_contracts()
    }

    assert contracts == {
        ("agora.sources", "source_registry", "stable"),
        ("agora.sinks", "sink_registry", "stable"),
        ("agora.middlewares", "middleware_registry", "stable"),
        ("agora.runner", "runner_registry", "stable"),
        ("agora.middlewares.dedup.stores", "dedup_store_registry", "stable"),
        ("agora.middlewares.dedup.strategies", "dedup_strategy_registry", "stable"),
        ("agora.ai.providers", "ai_provider_registry", "provisional"),
        ("agora.ai.caches", "ai_cache_registry", "provisional"),
        ("agora.metrics.exporters", "metrics_exporter_registry", "provisional"),
        ("agora.state.backends", "state_backend_registry", "provisional"),
    }


# ======================================================================
# [CONTRACT-04] Manifest matrix — matching version → compatible=True, loaded
# ======================================================================


def test_c04_matching_manifest_version_loads_plugin() -> None:
    """[CONTRACT-04] A plugin with MANIFEST.agora_api_version matching
    AGORA_PLUGIN_MANIFEST_VERSION must be loaded and reported compatible=True.

    Validates: docs/plugins/manifest.md — "Compatibility matrix"
    """
    registry: Registry[Any] = Registry(name="test")

    class _Manifest:
        agora_api_version = AGORA_PLUGIN_MANIFEST_VERSION
        package = "test-plugin"
        version = "1.0.0"

    class _Plugin:
        __module__ = "test_pkg"

    mock_ep = MagicMock()
    mock_ep.name = "test_plugin"
    mock_ep.dist = MagicMock(name="test-plugin", version="1.0.0")
    mock_ep.load.return_value = _Plugin

    import sys

    mock_module = MagicMock()
    mock_module.MANIFEST = _Manifest()
    with (
        patch.dict(sys.modules, {"test_pkg": mock_module}),
        patch("importlib.metadata.entry_points", return_value=[mock_ep]),
    ):
        registry.load_entrypoints("agora.test")

    assert "test_plugin" in registry, (
        "[CONTRACT-04] compatible plugin must be in the active registry"
    )
    items = {i.key: i for i in registry.describe_items()}
    assert items["test_plugin"].compatible is True, (
        "[CONTRACT-04] compatible plugin must have compatible=True in diagnostics"
    )


# ======================================================================
# [CONTRACT-05] Manifest matrix — mismatching version → excluded, compatible=False
# ======================================================================


def test_c05_mismatching_manifest_version_excludes_plugin() -> None:
    """[CONTRACT-05] A plugin with MANIFEST.agora_api_version that does NOT
    match AGORA_PLUGIN_MANIFEST_VERSION must be excluded from the active
    registry but still appear in diagnostics with compatible=False.

    Validates: docs/plugins/manifest.md — "Compatibility matrix"
    Validates: docs/plugins/contract.md — "What the runtime does with incompatible plugins"
    """
    registry: Registry[Any] = Registry(name="test")

    class _Manifest:
        agora_api_version = "0.0-incompatible"
        package = "old-plugin"
        version = "0.1.0"

    class _Plugin:
        __module__ = "old_pkg"

    mock_ep = MagicMock()
    mock_ep.name = "old_plugin"
    mock_ep.dist = MagicMock(name="old-plugin", version="0.1.0")
    mock_ep.load.return_value = _Plugin

    import sys

    mock_module = MagicMock()
    mock_module.MANIFEST = _Manifest()
    with (
        patch.dict(sys.modules, {"old_pkg": mock_module}),
        patch("importlib.metadata.entry_points", return_value=[mock_ep]),
    ):
        registry.load_entrypoints("agora.test")

    assert "old_plugin" not in registry, (
        "[CONTRACT-05] incompatible plugin must NOT be in the active registry"
    )
    items = {i.key: i for i in registry.describe_items()}
    assert "old_plugin" in items, (
        "[CONTRACT-05] incompatible plugin must still appear in diagnostics"
    )
    assert items["old_plugin"].compatible is False, (
        "[CONTRACT-05] incompatible plugin must have compatible=False in diagnostics"
    )
    assert items["old_plugin"].agora_api_version == "0.0-incompatible"


# ======================================================================
# [CONTRACT-06] Manifest matrix — no MANIFEST → loaded, compatible=None
# ======================================================================


def test_c06_no_manifest_loads_plugin_with_compatible_none() -> None:
    """[CONTRACT-06] A plugin with no MANIFEST must be loaded normally and
    reported with compatible=None in diagnostics.

    Validates: docs/plugins/manifest.md — "Compatibility matrix"
    """
    registry: Registry[Any] = Registry(name="test")

    class _Plugin:
        __module__ = "no_manifest_pkg"

    mock_ep = MagicMock()
    mock_ep.name = "no_manifest_plugin"
    mock_ep.dist = MagicMock(name="no-manifest-plugin", version="1.0.0")
    mock_ep.load.return_value = _Plugin

    import sys

    mock_module = MagicMock(spec=[])  # no MANIFEST attribute
    with (
        patch.dict(sys.modules, {"no_manifest_pkg": mock_module}),
        patch("importlib.metadata.entry_points", return_value=[mock_ep]),
    ):
        registry.load_entrypoints("agora.test")

    assert "no_manifest_plugin" in registry, (
        "[CONTRACT-06] plugin without MANIFEST must be in the active registry"
    )
    items = {i.key: i for i in registry.describe_items()}
    assert items["no_manifest_plugin"].compatible is None, (
        "[CONTRACT-06] plugin without MANIFEST must have compatible=None"
    )


# ======================================================================
# [CONTRACT-07] Incompatible plugin does not abort discovery of other plugins
# ======================================================================


def test_c07_incompatible_plugin_does_not_abort_discovery() -> None:
    """[CONTRACT-07] An incompatible plugin must not prevent other plugins in
    the same group from being loaded.

    Validates: docs/plugins/contract.md — "What the runtime does with incompatible plugins"
    """
    registry: Registry[Any] = Registry(name="test")

    class _BadManifest:
        agora_api_version = "0.0-incompatible"

    class _GoodManifest:
        agora_api_version = AGORA_PLUGIN_MANIFEST_VERSION

    class _BadPlugin:
        __module__ = "bad_pkg"

    class _GoodPlugin:
        __module__ = "good_pkg"

    bad_ep = MagicMock()
    bad_ep.name = "bad_plugin"
    bad_ep.dist = None
    bad_ep.load.return_value = _BadPlugin

    good_ep = MagicMock()
    good_ep.name = "good_plugin"
    good_ep.dist = None
    good_ep.load.return_value = _GoodPlugin

    import sys

    bad_module = MagicMock()
    bad_module.MANIFEST = _BadManifest()
    good_module = MagicMock()
    good_module.MANIFEST = _GoodManifest()

    with (
        patch.dict(sys.modules, {"bad_pkg": bad_module, "good_pkg": good_module}),
        patch("importlib.metadata.entry_points", return_value=[bad_ep, good_ep]),
    ):
        registry.load_entrypoints("agora.test")

    assert "bad_plugin" not in registry
    assert "good_plugin" in registry, (
        "[CONTRACT-07] compatible plugin must load even when another plugin in the "
        "same group is incompatible"
    )


# ======================================================================
# [CONTRACT-08] Built-in sources are registered in source_registry
# ======================================================================


def test_c08_builtin_sources_registered() -> None:
    """[CONTRACT-08] The built-in file sources must be registered in
    source_registry under their declared names.

    Validates: docs/plugins/contract.md — stable source group
    """
    from agora.sources import source_registry

    for name in ("jsonl", "csv", "parquet", "http"):
        assert name in source_registry, (
            f"[CONTRACT-08] built-in source '{name}' must be in source_registry"
        )


# ======================================================================
# [CONTRACT-09] Built-in sinks are registered in sink_registry
# ======================================================================


def test_c09_builtin_sinks_registered() -> None:
    """[CONTRACT-09] The built-in sinks must be registered in sink_registry
    under their declared names.

    Validates: docs/plugins/contract.md — stable sink group
    """
    from agora.sinks import sink_registry

    for name in ("stdout", "jsonl", "csv", "parquet", "log", "webhook"):
        assert name in sink_registry, (
            f"[CONTRACT-09] built-in sink '{name}' must be in sink_registry"
        )


# ======================================================================
# [CONTRACT-10] Built-in runners are registered in runner_registry
# ======================================================================


def test_c10_builtin_runners_registered() -> None:
    """[CONTRACT-10] The built-in runner types must be registered in
    runner_registry under their declared names.

    Validates: docs/plugins/contract.md — stable runner group
    """
    from agora.runner import runner_registry

    for name in ("scheduled", "worker_pool"):
        assert name in runner_registry, (
            f"[CONTRACT-10] built-in runner '{name}' must be in runner_registry"
        )


# ======================================================================
# [CONTRACT-11] describe_items returns RegistryItemInfo objects
# ======================================================================


def test_c11_describe_items_returns_registry_item_info() -> None:
    """[CONTRACT-11] Registry.describe_items() must return a list of
    RegistryItemInfo objects — this is the diagnostics contract.

    Validates: docs/plugins/contract.md — "What the runtime does with incompatible plugins"
    """
    from agora.sources import source_registry

    items = source_registry.describe_items()
    assert isinstance(items, list)
    assert len(items) > 0
    for item in items:
        assert isinstance(item, RegistryItemInfo), (
            f"[CONTRACT-11] describe_items() must return RegistryItemInfo, got {type(item)}"
        )
        assert isinstance(item.key, str)
