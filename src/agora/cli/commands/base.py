"""
agora/cli/commands/base.py
==========================
BaseCommand — Abstract base class for the Command Pattern in agora CLI.

Adding a new command
--------------------
1. Create ``agora/cli/commands/my_cmd.py`` with a class extending ``BaseCommand``.
2. Register it in ``agora/cli/app.py`` via ``registry.register(MyCommand())``.
3. Done — parser is built automatically, no if-elif required.

Example::

    class MyCommand(BaseCommand):
        name = "my-cmd"
        description = "Does something useful."

        def setup_parser(self, parser: argparse.ArgumentParser) -> None:
            parser.add_argument("--target", required=True)

        def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
            from agora.cli.console import console
            console.info(f"running with target={args.target}")
            return 0
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext


class CommandError(Exception):
    """Predictable CLI error — caught at top level, rendered as Rich panel.

    Raise instead of sys.exit() or bare Exception for user-visible errors.
    """

    def __init__(self, message: str, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class BaseCommand(ABC):
    """Abstract base for all agora CLI sub-commands.

    Subclasses must define:
        name         — argparse sub-command name, e.g. ``"worker"``
        description  — short help text shown in top-level ``--help``
        setup_parser() — add arguments to the subparser
        execute()    — run the command; return exit code (0 = OK)
    """

    name: str
    description: str

    def register(self, subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
        """Register into the argparse subparser group.

        Default implementation creates a subparser and delegates to
        ``setup_parser()``.  Override only for non-standard parser needs.
        """
        p = subparsers.add_parser(self.name, help=self.description)
        self.setup_parser(p)
        p.set_defaults(command=self.name)

    @abstractmethod
    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        """Add arguments to the subparser for this command."""

    @abstractmethod
    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        """Execute the command.  Return 0 on success, non-zero on failure."""
