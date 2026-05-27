"""agora/cli/commands/config.py — ``agora config show``"""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING

from agora.cli.commands.base import BaseCommand, CommandError
from agora.cli.console import console

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext


class ConfigCommand(BaseCommand):
    """Print resolved project settings as JSON."""

    name = "config"
    description = "Print resolved settings (requires src/settings.py)."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "subcommand",
            nargs="?",
            choices=["show"],
            default="show",
            help="Subcommand (default: show)",
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        import os

        cwd = ctx.cwd or os.getcwd()
        for p in (cwd, os.path.join(cwd, "src")):
            if p not in sys.path:
                sys.path.insert(0, p)

        try:
            from settings import get_settings  # type: ignore[import-not-found, unused-ignore]
        except ImportError as exc:
            raise CommandError(
                "Cannot import 'settings'.\n"
                "  Make sure src/settings.py exists and defines get_settings()."
            ) from exc

        cfg = get_settings()
        try:
            data = json.dumps(cfg.model_dump(), indent=2, default=str)
            console.config_json(data)
        except Exception:
            console.out(repr(cfg))
        return 0
