"""
agora/cli/main.py
=================
``agora`` CLI — entry point.

Commands are registered in ``_build_registry()`` — no if-elif chains.
Adding a new command: create a BaseCommand subclass and register it here.

    agora new <project>          scaffold a new project
    agora run <pipeline>         run a pipeline by module path
    agora worker                 start the WorkerPool (long-running)
    agora pipelines list         list discoverable pipelines
    agora plugins list           list all sources / sinks / middlewares
    agora plugins list --kind sink
    agora plugins list --json
    agora config show            print resolved settings
    agora dlq replay            replay dead-letter records
    agora version                print agora version
"""

from __future__ import annotations

import sys

from agora.cli.commands.base import CommandError
from agora.cli.commands.checkpoint import CheckpointCommand
from agora.cli.commands.config import ConfigCommand
from agora.cli.commands.diagnose import DiagnoseCommand
from agora.cli.commands.dlq import DLQCommand
from agora.cli.commands.doctor import DoctorCommand
from agora.cli.commands.new import NewCommand
from agora.cli.commands.pipelines import PipelinesCommand
from agora.cli.commands.plugins import PluginsCommand
from agora.cli.commands.run import RunCommand
from agora.cli.commands.version import VersionCommand
from agora.cli.commands.worker import WorkerCommand
from agora.cli.console import console
from agora.cli.context import AgoraContext
from agora.cli.registry import CommandRegistry


def _build_registry() -> CommandRegistry:
    """Register all agora CLI commands.

    To add a new command:
      1. Create ``agora/cli/commands/my_cmd.py`` with MyCommand(BaseCommand)
      2. Import it here and call registry.register(MyCommand())
      3. Done — parser and dispatch are automatic.
    """
    registry = CommandRegistry(
        prog="agora",
        description="Agora ETL Framework — async pipeline orchestration",
    )
    registry.register(NewCommand())
    registry.register(RunCommand())
    registry.register(WorkerCommand())
    registry.register(PipelinesCommand())
    registry.register(PluginsCommand())
    registry.register(ConfigCommand())
    registry.register(DLQCommand())
    registry.register(CheckpointCommand())
    registry.register(DiagnoseCommand())
    registry.register(DoctorCommand())
    registry.register(VersionCommand())
    return registry


def main() -> None:
    registry = _build_registry()
    parser = registry.build_parser()
    args = parser.parse_args()

    if args.command is None:
        from agora import __version__

        console.agora_help(
            commands=[(name, cmd.description) for name, cmd in registry.commands.items()],
            version=__version__,
        )
        sys.exit(0)

    ctx = AgoraContext.build(args)

    try:
        exit_code = registry.dispatch(args, ctx)
        sys.exit(exit_code)
    except CommandError as exc:
        console.error(str(exc))
        sys.exit(exc.exit_code)
    except KeyboardInterrupt:
        console.warn("Interrupted.")
        sys.exit(130)
    except Exception as exc:
        console.exception("Unexpected error", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
