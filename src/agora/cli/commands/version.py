"""agora/cli/commands/version.py — ``agora version``"""

from __future__ import annotations

from typing import TYPE_CHECKING

from agora.cli.commands.base import BaseCommand
from agora.cli.console import console

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext


class VersionCommand(BaseCommand):
    """Print the agora framework version."""

    name = "version"
    description = "Print agora version."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        pass  # no arguments

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        from agora import __version__

        console.version_panel(__version__)
        return 0
