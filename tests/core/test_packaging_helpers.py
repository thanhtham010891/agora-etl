from __future__ import annotations

from agora.core.packaging import (
    FIRST_PARTY_PLUGIN_DISTRIBUTION,
    distribution_requirement,
    first_party_plugin_family_from_module,
    first_party_plugin_install_detail,
    first_party_plugin_requirement,
)


def test_distribution_requirement_formats_extras() -> None:
    assert distribution_requirement("pkg") == "pkg"
    assert distribution_requirement("pkg", "redis", "cron") == "pkg[redis,cron]"


def test_first_party_plugin_requirement_formats_distribution() -> None:
    assert first_party_plugin_requirement() == FIRST_PARTY_PLUGIN_DISTRIBUTION
    assert first_party_plugin_requirement("kafka") == "agora-etl-plugins[kafka]"


def test_first_party_plugin_install_detail_formats_command() -> None:
    assert first_party_plugin_install_detail() == "Install with: pip install 'agora-etl-plugins'"
    assert (
        first_party_plugin_install_detail("redis")
        == "Install with: pip install 'agora-etl-plugins[redis]'"
    )


def test_first_party_plugin_family_from_module_reads_namespace() -> None:
    assert first_party_plugin_family_from_module("agora_plugins.redis.sinks.redis") == "redis"
    assert first_party_plugin_family_from_module("third_party.kafka") is None
    assert first_party_plugin_family_from_module(None) is None
