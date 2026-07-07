"""Packaging/install hint helpers shared across CLI and runtime messaging."""

from __future__ import annotations

AGORA_DISTRIBUTION = "agora-etl"
FIRST_PARTY_PLUGIN_DISTRIBUTION = "agora-etl-plugins"
FIRST_PARTY_PLUGIN_NAMESPACE = "agora_plugins"


def distribution_requirement(distribution: str, *extras: str) -> str:
    normalized = tuple(extra for extra in extras if extra)
    if not normalized:
        return distribution
    return f"{distribution}[{','.join(normalized)}]"


def first_party_plugin_requirement(*extras: str) -> str:
    return distribution_requirement(FIRST_PARTY_PLUGIN_DISTRIBUTION, *extras)


def pip_install_command(requirement: str) -> str:
    return f"pip install '{requirement}'"


def first_party_plugin_install_command(*extras: str) -> str:
    return pip_install_command(first_party_plugin_requirement(*extras))


def first_party_plugin_install_detail(*extras: str) -> str:
    return f"Install with: {first_party_plugin_install_command(*extras)}"


def first_party_plugin_family_from_module(module_path: str | None) -> str | None:
    if not module_path:
        return None
    parts = module_path.split(".")
    if len(parts) >= 2 and parts[0] == FIRST_PARTY_PLUGIN_NAMESPACE:
        return parts[1]
    return None
