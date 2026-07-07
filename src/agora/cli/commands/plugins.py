"""agora/cli/commands/plugins.py — ``agora plugins list``

Lists all public built-in and third-party plugin registrations.

Usage::

    agora plugins list
    agora plugins list --kind source
    agora plugins list --kind sink
    agora plugins list --kind middleware
    agora plugins list --kind runner
    agora plugins list --json
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

from agora.cli.commands.base import BaseCommand
from agora.cli.console import console
from agora.core.discovery import EntryPointGroupContract, public_entrypoint_group_contracts
from agora.core.packaging import (
    FIRST_PARTY_PLUGIN_DISTRIBUTION,
    first_party_plugin_family_from_module,
)

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext

_PLUGIN_CONTRACTS = public_entrypoint_group_contracts()
_CONTRACTS_BY_KIND = {contract.kind: contract for contract in _PLUGIN_CONTRACTS}
_ALL_KINDS = tuple(contract.kind for contract in _PLUGIN_CONTRACTS)


class PluginsCommand(BaseCommand):
    """List all public plugin registries and their discovered items."""

    name = "plugins"
    description = "List all public plugin registries and their discovered items."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "subcommand",
            nargs="?",
            choices=["list"],
            default="list",
            help="Subcommand (default: list)",
        )
        parser.add_argument(
            "--kind",
            choices=list(_ALL_KINDS),
            default=None,
            metavar="KIND",
            help="Filter by kind: source | sink | middleware",
        )
        parser.add_argument(
            "--json",
            dest="as_json",
            action="store_true",
            default=False,
            help="Output as JSON (machine-readable).",
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        kinds = [args.kind] if args.kind else list(_ALL_KINDS)
        data = _collect(kinds)

        if args.as_json:
            import json

            console.out(json.dumps(data, indent=2))
            return 0

        console.plugins_table(data)
        return 0


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #


def _collect(kinds: list[str]) -> dict[str, list[dict[str, Any]]]:
    """Return {kind: [{...row metadata...}]} for the requested kinds."""
    result: dict[str, list[dict[str, Any]]] = {}
    for kind in kinds:
        contract = _CONTRACTS_BY_KIND[kind]
        module = importlib.import_module(contract.module_path)
        registry = getattr(module, contract.registry_attr)
        # Force eager import so compatibility/MANIFEST metadata is populated for
        # every plugin — runtime resolution stays lazy, but `plugins list` must
        # report the full picture.
        load_entrypoints = getattr(registry, "load_entrypoints", None)
        if callable(load_entrypoints):
            load_entrypoints(contract.group, eager=True)
        result[kind] = _registry_rows(registry, contract)

    return result


def _registry_rows(registry: Any, contract: EntryPointGroupContract | str) -> list[dict[str, Any]]:
    """Convert a Registry to a list of row dicts."""
    if isinstance(contract, str):
        category = contract
        contract_info = _CONTRACTS_BY_KIND.get(category)
    else:
        contract_info = contract
        category = contract.kind

    describe_items = getattr(registry, "describe_items", None)
    if callable(describe_items):
        rows = []
        for item in describe_items():
            compatibility = "n/a"
            if item.compatible is True:
                compatibility = "ok"
            elif item.compatible is False:
                compatibility = "incompatible"
            elif item.origin == "entrypoint_conflict":
                compatibility = "conflict"
            elif item.origin == "entrypoint_error":
                compatibility = "error"
            rows.append(
                {
                    "key": item.key,
                    "category": category,
                    "group": (
                        item.entrypoint_group or contract_info.group
                        if contract_info is not None
                        else ""
                    ),
                    "registry": contract_info.registry_attr if contract_info is not None else "",
                    "stability": contract_info.stability if contract_info is not None else "",
                    "type": item.type,
                    "origin": item.origin,
                    "package": item.package or "",
                    "version": item.version or "",
                    "manifest": item.agora_api_version or "",
                    "compatibility": compatibility,
                    "capabilities": list(item.capabilities),
                    "error": item.error or "",
                    "extra": _extra_hint(
                        item.key,
                        item.type,
                        category,
                        item.origin,
                        package=item.package,
                        manifest_name=item.manifest_name,
                        module_path=item.module_path,
                    ),
                }
            )
        return sorted(rows, key=lambda row: row["key"])

    rows = []
    for key, kind in registry.all_items():
        rows.append(
            {
                "key": key,
                "category": category,
                "group": contract_info.group if contract_info is not None else "",
                "registry": contract_info.registry_attr if contract_info is not None else "",
                "stability": contract_info.stability if contract_info is not None else "",
                "type": kind,
                "origin": "manual",
                "package": "",
                "version": "",
                "manifest": "",
                "compatibility": "n/a",
                "capabilities": [],
                "error": "",
                "extra": _extra_hint(key, kind, category, "manual"),
            }
        )
    return sorted(rows, key=lambda row: row["key"])


_EXTRA_HINTS: dict[str, dict[str, str]] = {
    "source": {
        "http": "agora-etl",
        "jsonl": "agora-etl",
        "parquet": "agora-etl",
        "csv": "stdlib",
        "file": "stdlib",
        "iterable": "stdlib",
        "websocket": "third-party plugin",
    },
    "sink": {
        "stdout": "stdlib",
        "jsonl": "agora-etl",
        "csv": "stdlib",
        "parquet": "agora-etl",
        "log": "stdlib",
        "webhook": "agora-etl",
        "elasticsearch": "third-party plugin",
        "gcs": "third-party plugin",
    },
    "middleware": {
        "validate": "stdlib",
        "enrich": "stdlib",
        "ai_enrich": "AI provider plugin",
        "ai_classify": "AI provider plugin",
        "ai_batch": "AI provider plugin",
        "ai_extract": "AI provider plugin",
        "ai_validate": "AI provider plugin",
        "ai_translate": "AI provider plugin",
    },
    "runner": {
        "scheduled": "agora-etl",
        "worker_pool": "agora-etl",
    },
    "dedup_store": {
        "memory": "agora-etl",
        "sqlite": "agora-etl",
        "embedding": "agora-etl",
    },
    "dedup_strategy": {
        "exact": "agora-etl",
        "fuzzy": "agora-etl",
    },
    "ai_provider": {
        "gemini": "provider plugin",
        "openai": "provider plugin",
    },
    "ai_cache": {
        "memory": "agora-etl",
        "sqlite": "agora-etl",
        "backend": "agora-etl",
    },
    "metrics_exporter": {
        "prometheus": "agora-etl",
    },
    "state_backend": {
        "memory": "agora-etl",
        "sqlite": "agora-etl",
    },
}


def _extra_hint(
    key: str,
    reg_type: str,
    category: str,
    origin: str,
    *,
    package: str | None = None,
    manifest_name: str | None = None,
    module_path: str | None = None,
) -> str:
    if origin == "manual" and reg_type == "instance":
        return "built-in"
    if package == FIRST_PARTY_PLUGIN_DISTRIBUTION:
        family = manifest_name or first_party_plugin_family_from_module(module_path)
        if family:
            return f"{FIRST_PARTY_PLUGIN_DISTRIBUTION}[{family}]"
    return _EXTRA_HINTS.get(category, {}).get(key, "agora-etl[all]")
