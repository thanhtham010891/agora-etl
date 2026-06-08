from __future__ import annotations

import sys
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING

from agora.cli.commands.plugins import _registry_rows
from agora.core.registry import AGORA_PLUGIN_MANIFEST_VERSION, Registry

if TYPE_CHECKING:
    import pytest


@dataclass(frozen=True)
class _Manifest:
    name: str
    version: str
    agora_api_version: str
    package: str
    capabilities: tuple[str, ...]


@dataclass(frozen=True)
class _FakeContract:
    kind: str
    group: str
    registry_attr: str
    stability: str


class _FakeEntryPoint:
    def __init__(self, name: str, plugin: object, *, dist_name: str, dist_version: str) -> None:
        self.name = name
        self._plugin = plugin
        self.dist = SimpleNamespace(name=dist_name, version=dist_version)

    def load(self) -> object:
        return self._plugin


def _install_fake_plugin(
    monkeypatch: pytest.MonkeyPatch,
    *,
    package_name: str,
    plugin_module_name: str,
    manifest_api_version: str,
):
    package = ModuleType(package_name)
    package.MANIFEST = _Manifest(
        name=package_name,
        version="1.2.3",
        agora_api_version=manifest_api_version,
        package=f"{package_name}-dist",
        capabilities=("sink:test",),
    )
    plugin_module = ModuleType(plugin_module_name)

    class FakePlugin:
        pass

    FakePlugin.__module__ = plugin_module_name
    plugin_module.FakePlugin = FakePlugin

    monkeypatch.setitem(sys.modules, package_name, package)
    monkeypatch.setitem(sys.modules, plugin_module_name, plugin_module)
    return FakePlugin


def test_registry_load_entrypoints_records_manifest_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin_cls = _install_fake_plugin(
        monkeypatch,
        package_name="fake_plugin_ok",
        plugin_module_name="fake_plugin_ok.sinks",
        manifest_api_version=AGORA_PLUGIN_MANIFEST_VERSION,
    )

    monkeypatch.setattr(
        "importlib.metadata.entry_points",
        lambda *, group: [
            _FakeEntryPoint(
                "fake_sink", plugin_cls, dist_name="fake-plugin-ok", dist_version="1.2.3"
            )
        ],
    )

    registry: Registry[type] = Registry(name="sink")
    registry.load_entrypoints("agora.sinks")

    item = registry.describe_items()[0]
    assert item.key == "fake_sink"
    assert item.origin == "entrypoint"
    assert item.package == "fake_plugin_ok-dist"
    assert item.version == "1.2.3"
    assert item.compatible is True
    assert item.entrypoint_group == "agora.sinks"
    assert item.capabilities == ("sink:test",)


def test_registry_manifest_version_is_current() -> None:
    assert AGORA_PLUGIN_MANIFEST_VERSION == "0.4"


def test_registry_skips_incompatible_manifest_entrypoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin_cls = _install_fake_plugin(
        monkeypatch,
        package_name="fake_plugin_bad",
        plugin_module_name="fake_plugin_bad.sinks",
        manifest_api_version="9.9",
    )

    monkeypatch.setattr(
        "importlib.metadata.entry_points",
        lambda *, group: [
            _FakeEntryPoint(
                "bad_sink", plugin_cls, dist_name="fake-plugin-bad", dist_version="9.9.0"
            )
        ],
    )

    registry: Registry[type] = Registry(name="sink")
    registry.load_entrypoints("agora.sinks")

    assert registry.has("bad_sink") is False

    item = registry.describe_items()[0]
    assert item.key == "bad_sink"
    assert item.type == "unavailable"
    assert item.origin == "entrypoint_incompatible"
    assert item.package == "fake_plugin_bad-dist"
    assert item.version == "1.2.3"
    assert item.agora_api_version == "9.9"
    assert item.compatible is False
    assert item.entrypoint_group == "agora.sinks"
    assert item.capabilities == ("sink:test",)


def test_registry_load_entrypoints_without_manifest_keeps_distribution_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin_module = ModuleType("manifestless_plugin.sinks")

    class ManifestlessPlugin:
        pass

    ManifestlessPlugin.__module__ = "manifestless_plugin.sinks"
    plugin_module.ManifestlessPlugin = ManifestlessPlugin
    monkeypatch.setitem(sys.modules, "manifestless_plugin.sinks", plugin_module)

    monkeypatch.setattr(
        "importlib.metadata.entry_points",
        lambda *, group: [
            _FakeEntryPoint(
                "manifestless_sink",
                ManifestlessPlugin,
                dist_name="manifestless-plugin",
                dist_version="0.5.0",
            )
        ],
    )

    registry: Registry[type] = Registry(name="sink")
    registry.load_entrypoints("agora.sinks")

    item = registry.describe_items()[0]
    assert item.key == "manifestless_sink"
    assert item.package == "manifestless-plugin"
    assert item.version == "0.5.0"
    assert item.compatible is None
    assert item.entrypoint_group == "agora.sinks"
    assert item.capabilities == ()


def test_registry_rows_do_not_label_entrypoint_plugins_as_builtin() -> None:
    registry: Registry[type] = Registry(name="source")

    class RedisStreamSource:
        pass

    registry.register("redis_stream", RedisStreamSource)
    registry._origins["redis_stream"] = "entrypoint"

    rows = _registry_rows(
        registry,
        _FakeContract(
            kind="source",
            group="agora.sources",
            registry_attr="source_registry",
            stability="stable",
        ),
    )

    assert rows[0]["origin"] == "entrypoint"
    assert rows[0]["extra"] == "agora-etl-plugins[redis]"
    assert rows[0]["manifest"] == ""
    assert rows[0]["group"] == "agora.sources"
    assert rows[0]["registry"] == "source_registry"
    assert rows[0]["stability"] == "stable"


def test_registry_rows_mark_incompatible_entrypoints_explicitly() -> None:
    registry: Registry[type] = Registry(name="sink")
    registry._metadata["bad_sink"] = {
        "package": "bad-plugin",
        "version": "0.9.0",
        "agora_api_version": "9.9",
        "compatible": False,
    }
    registry._origins["bad_sink"] = "entrypoint_incompatible"
    registry._registration_types["bad_sink"] = "unavailable"

    rows = _registry_rows(
        registry,
        _FakeContract(
            kind="sink",
            group="agora.sinks",
            registry_attr="sink_registry",
            stability="stable",
        ),
    )

    assert rows[0]["origin"] == "entrypoint_incompatible"
    assert rows[0]["compatibility"] == "incompatible"
    assert rows[0]["manifest"] == "9.9"
    assert rows[0]["extra"] == "agora-etl[all]"
    assert rows[0]["group"] == "agora.sinks"


def test_registry_manifest_lookup_walks_up_to_parent_package(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package = ModuleType("agora_plugins")
    kafka_package = ModuleType("agora_plugins.kafka")
    kafka_package.MANIFEST = _Manifest(
        name="kafka",
        version="0.3.0",
        agora_api_version=AGORA_PLUGIN_MANIFEST_VERSION,
        package="agora-etl-plugins",
        capabilities=("source:kafka", "sink:kafka"),
    )
    kafka_sources_package = ModuleType("agora_plugins.kafka.sources")
    plugin_module = ModuleType("agora_plugins.kafka.sources.kafka")

    class KafkaSource:
        pass

    KafkaSource.__module__ = "agora_plugins.kafka.sources.kafka"
    plugin_module.KafkaSource = KafkaSource

    monkeypatch.setitem(sys.modules, "agora_plugins", package)
    monkeypatch.setitem(sys.modules, "agora_plugins.kafka", kafka_package)
    monkeypatch.setitem(sys.modules, "agora_plugins.kafka.sources", kafka_sources_package)
    monkeypatch.setitem(sys.modules, "agora_plugins.kafka.sources.kafka", plugin_module)

    monkeypatch.setattr(
        "importlib.metadata.entry_points",
        lambda *, group: [
            _FakeEntryPoint(
                "kafka",
                KafkaSource,
                dist_name="agora-etl-plugins",
                dist_version="0.3.0",
            )
        ],
    )

    registry: Registry[type] = Registry(name="source")
    registry.load_entrypoints("agora.sources")

    item = registry.describe_items()[0]
    assert item.package == "agora-etl-plugins"
    assert item.version == "0.3.0"
    assert item.agora_api_version == AGORA_PLUGIN_MANIFEST_VERSION
    assert item.compatible is True
    assert item.entrypoint_group == "agora.sources"
    assert item.capabilities == ("source:kafka", "sink:kafka")


def test_registry_rows_are_sorted_by_key() -> None:
    registry: Registry[type] = Registry(name="sink")

    class ZSink:
        pass

    class ASink:
        pass

    registry.register("zeta", ZSink)
    registry.register("alpha", ASink)

    rows = _registry_rows(
        registry,
        _FakeContract(
            kind="sink",
            group="agora.sinks",
            registry_attr="sink_registry",
            stability="stable",
        ),
    )

    assert [row["key"] for row in rows] == ["alpha", "zeta"]
