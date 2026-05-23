"""agora/cli/commands/pipelines.py — ``agora pipelines list``"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from agora.cli.commands.base import BaseCommand, CommandError
from agora.cli.console import console

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext


class PipelinesCommand(BaseCommand):
    """List all pipelines discoverable in src/pipelines/."""

    name = "pipelines"
    description = "List all pipelines in src/pipelines/."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "subcommand",
            nargs="?",
            choices=["list"],
            default="list",
            help="Subcommand (default: list)",
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        src = Path(ctx.cwd) / "src" / "pipelines"
        if not src.exists():
            raise CommandError(
                "No src/pipelines/ directory found.\n"
                "  Run  agora new <name>  to scaffold a project first."
            )
        modules = sorted(f.stem for f in src.glob("*.py") if f.stem != "__init__")
        if not modules:
            console.warn("No pipelines found in src/pipelines/")
            return 0
        console.pipelines_list(modules)
        return 0
