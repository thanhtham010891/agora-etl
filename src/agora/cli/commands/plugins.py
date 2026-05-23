"""agora/cli/commands/plugins.py — ``agora plugins list``

Lists all built-in and third-party plugin registrations for:
  sources, sinks, middlewares.

Usage::

    agora plugins list
    agora plugins list --kind source
    agora plugins list --kind sink
    agora plugins list --kind middleware
    agora plugins list --json
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.cli.commands.base import BaseCommand
from agora.cli.console import console

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext

_ALL_KINDS = ("source", "sink", "middleware")


class PluginsCommand(BaseCommand):
    """List all registered sources, sinks, and middlewares."""

    name = "plugins"
    description = "List all registered sources, sinks, and middlewares."

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


def _collect(kinds: list[str]) -> dict[str, list[dict[str, str]]]:
    """Return {kind: [{key, type, extra}]} for the requested kinds."""
    result: dict[str, list[dict[str, str]]] = {}

    if "source" in kinds:
        from agora.sources import source_registry

        result["source"] = _registry_rows(source_registry, "source")

    if "sink" in kinds:
        from agora.sinks import sink_registry

        result["sink"] = _registry_rows(sink_registry, "sink")

    if "middleware" in kinds:
        from agora.middlewares import middleware_registry

        result["middleware"] = _registry_rows(middleware_registry, "middleware")

    return result


def _registry_rows(registry: Any, category: str) -> list[dict[str, str]]:
    """Convert a Registry to a list of row dicts."""
    describe_items = getattr(registry, "describe_items", None)
    if callable(describe_items):
        rows = []
        for item in describe_items():
            compatibility = "n/a"
            if item.compatible is True:
                compatibility = "ok"
            rows.append(
                {
                    "key": item.key,
                    "type": item.type,
                    "origin": item.origin,
                    "package": item.package or "",
                    "version": item.version or "",
                    "compatibility": compatibility,
                    "extra": _extra_hint(item.key, item.type, category),
                }
            )
        return rows

    rows = []
    for key, kind in registry.all_items():
        rows.append(
            {
                "key": key,
                "type": kind,
                "origin": "manual",
                "package": "",
                "version": "",
                "compatibility": "n/a",
                "extra": _extra_hint(key, kind, category),
            }
        )
    return rows


# Nested by category so identical keys (e.g. "kafka", "postgres") in
# source vs sink can map to different install hints independently.
_EXTRA_HINTS: dict[str, dict[str, str]] = {
    "source": {
        "kafka": "agora-core[kafka]",
        "http": "agora-core",
        "jsonl": "agora-core",
        "parquet": "agora-core",
        "csv": "stdlib",
        "file": "stdlib",
        "iterable": "stdlib",
        "postgres": "agora-core[postgres]",
        "postgres_dlq_source": "agora-core[postgres]",
        "redis_dlq_source": "agora-core[redis]",
        "redis_stream": "agora-core[redis]",
        "websocket": "agora-core[ws]",
    },
    "sink": {
        "postgres": "agora-core[postgres]",
        "postgres_dlq": "agora-core[postgres]",
        "stdout": "stdlib",
        "kafka": "agora-core[kafka]",
        "jsonl": "agora-core",
        "csv": "stdlib",
        "parquet": "agora-core",
        "log": "stdlib",
        "webhook": "agora-core",
        "redis": "agora-core[redis]",
        "redis_dlq": "agora-core[redis]",
        "elasticsearch": "agora-core[elasticsearch]",
        "bigquery": "agora-core[gcp]",
        "s3": "agora-core[aws]",
        "gcs": "agora-core[gcs]",
    },
    "middleware": {
        "validate": "stdlib",
        "enrich": "stdlib",
        "ai_enrich": "agora-core[ai-gemini|ai-openai|ai-anthropic]",
        "ai_classify": "agora-core[ai-gemini|ai-openai|ai-anthropic]",
        "ai_extract": "agora-core[ai-gemini|ai-openai|ai-anthropic]",
        "ai_validate": "agora-core[ai-gemini|ai-openai|ai-anthropic]",
        "ai_translate": "agora-core[ai-gemini|ai-openai|ai-anthropic]",
    },
}


def _extra_hint(key: str, reg_type: str, category: str) -> str:
    if reg_type == "instance":
        return "built-in"
    return _EXTRA_HINTS.get(category, {}).get(key, "agora-core[all]")
