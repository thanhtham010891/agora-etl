"""
agora/cli/registry.py
=====================
CommandRegistry — Registry Pattern for agora CLI commands.

Eliminates all if-elif dispatch chains from main().
Each command registers itself; the registry builds the parser and
dispatches execution automatically.

Adding a new command
--------------------
1. Create a class extending ``BaseCommand``.
2. Call ``registry.register(MyCommand())`` in ``app.py``.
3. Done — ``agora my-cmd --help`` works immediately.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

from agora.cli.console import console

if TYPE_CHECKING:
    from agora.cli.commands.base import BaseCommand
    from agora.cli.context import AgoraContext


# ======================================================================
# Rich help renderer
# ======================================================================


def _render_rich_help(parser: argparse.ArgumentParser) -> None:
    """Render an argparse parser's help as a Rich Panel.

    Reads the parser's ``_actions`` directly so we can lay them out
    with ``Table.grid`` instead of the plain argparse formatter.
    """
    from rich.console import Console, Group
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    out = Console(highlight=False)

    # ------------------------------------------------------------------ #
    # Collect actions                                                      #
    # ------------------------------------------------------------------ #
    positionals = [
        a
        for a in parser._actions
        if not a.option_strings
        and a.dest not in ("==SUPPRESS==",)
        and not isinstance(a, argparse._SubParsersAction)
    ]
    optionals = [a for a in parser._actions if a.option_strings and a.help != argparse.SUPPRESS]

    # ------------------------------------------------------------------ #
    # Build renderable groups                                              #
    # ------------------------------------------------------------------ #
    groups: list = []

    # Usage line
    raw_usage = parser.format_usage().replace("usage: ", "").strip()
    usage = Text()
    usage.append("Usage:  ", style="dim")
    usage.append(raw_usage, style="bold white")
    groups.append(usage)

    # Description
    if parser.description:
        groups.append(Text(""))
        groups.append(Text(parser.description, style="dim"))

    # Positional arguments
    if positionals:
        groups.append(Text("\nArguments:", style="bold"))
        grid = Table.grid(padding=(0, 2))
        grid.add_column(style="bold white", min_width=18)
        grid.add_column(style="dim")
        for action in positionals:
            metavar = action.metavar or action.dest.upper()
            groups_help = action.help or ""
            grid.add_row(f"  {metavar}", groups_help)
        groups.append(grid)

    # Options
    if optionals:
        groups.append(Text("\nOptions:", style="bold"))
        opt_grid = Table.grid(padding=(0, 2))
        opt_grid.add_column(style="bold cyan", min_width=22)
        opt_grid.add_column(style="dim")
        for action in optionals:
            flags = ", ".join(action.option_strings)
            if action.metavar:
                flags += f" {action.metavar}"
            opt_grid.add_row(f"  {flags}", action.help or "")
        groups.append(opt_grid)

    # Footer hint
    groups.append(Text(""))
    groups.append(
        Text(
            f"Run  agora {parser.prog.split()[-1] if len(parser.prog.split()) > 1 else '<command>'} --help  "
            "for full usage.",
            style="dim",
        )
    )

    out.print(
        Panel(
            Group(*groups),
            title=f"[bold cyan]{parser.prog}[/bold cyan]",
            border_style="cyan",
            padding=(1, 2),
        )
    )


# ======================================================================
# Custom ArgumentParser — Rich help on error / no args
# ======================================================================


class _RichArgParser(argparse.ArgumentParser):
    """argparse subclass that renders help with Rich instead of plain text."""

    def print_help(self, file=None) -> None:  # type: ignore[override]
        _render_rich_help(self)

    def error(self, message: str) -> None:  # type: ignore[override]
        prog_depth = len(self.prog.split())
        # No args at all → show help and exit 0
        if len(sys.argv) <= prog_depth:
            self.print_help()
            sys.exit(0)
        # Missing required arg / wrong args → show help + error panel
        self.print_help()
        console.error(message)
        self.exit(2)


# ======================================================================
# CommandRegistry
# ======================================================================


class CommandRegistry:
    """Registry of all agora CLI commands.

    Usage::

        registry = CommandRegistry(prog="agora", description="Agora ETL Framework")
        registry.register(NewCommand())
        registry.register(RunCommand())
        registry.register(WorkerCommand())

        parser = registry.build_parser()
        args = parser.parse_args()
        ctx = AgoraContext.build(args)
        exit_code = registry.dispatch(args, ctx)
    """

    def __init__(self, prog: str = "agora", description: str = "Agora ETL Framework CLI") -> None:
        self._prog = prog
        self._description = description
        self._commands: dict[str, BaseCommand] = {}

    def register(self, cmd: BaseCommand) -> CommandRegistry:
        """Register a command.  Returns self for chaining."""
        self._commands[cmd.name] = cmd
        return self

    # ------------------------------------------------------------------ #
    # Parser                                                               #
    # ------------------------------------------------------------------ #

    def build_parser(self) -> argparse.ArgumentParser:
        """Build a complete ArgumentParser from all registered commands."""
        parser = _RichArgParser(
            prog=self._prog,
            description=self._description,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="Enable verbose output",
        )

        sub = parser.add_subparsers(
            dest="command",
            metavar="<command>",
            parser_class=_RichArgParser,
        )

        for cmd in self._commands.values():
            cmd.register(sub)

        return parser

    # ------------------------------------------------------------------ #
    # Dispatch                                                             #
    # ------------------------------------------------------------------ #

    def dispatch(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        """Resolve the command by name and call execute().

        Returns
        -------
        int
            Exit code (0 = success).
        """
        if args.command is None:
            return 0  # no subcommand — help was printed by parser

        cmd = self._commands.get(args.command)
        if cmd is None:
            available = ", ".join(self._commands.keys())
            console.error(f"Unknown command: {args.command!r}.  Available: {available}")
            return 2

        return cmd.execute(args, ctx)

    @property
    def command_names(self) -> list[str]:
        """Names of all registered commands."""
        return list(self._commands.keys())

    @property
    def commands(self) -> dict[str, BaseCommand]:
        """All registered commands keyed by name."""
        return dict(self._commands)
