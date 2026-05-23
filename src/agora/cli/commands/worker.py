"""
agora/cli/commands/worker.py
==============================
``agora worker`` — start the WorkerPool from a project's worker module.

Convention: the project exposes a ``worker.py`` that defines
``get_worker() -> WorkerPool``.

Usage::

    agora worker                            # loads worker.py in cwd
    agora worker --module pipelines.worker  # custom module path
    agora worker --list                     # list registered pipelines without starting
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
from typing import TYPE_CHECKING

import logstruct

from agora.cli._path import ensure_project_on_path
from agora.cli.commands.base import BaseCommand, CommandError
from agora.cli.console import console

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext
    from agora.runner.coordinator import WorkerCoordinator, WorkerInfo
    from agora.runner.worker import WorkerPool

logger = logstruct.getLogger(__name__)


class WorkerCommand(BaseCommand):
    """Start the WorkerPool — run scheduled pipelines as a long-running process."""

    name = "worker"
    description = "Start the WorkerPool (long-running, Ctrl+C to stop)."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--module",
            default="worker",
            metavar="MODULE",
            help="Dotted module path to worker definition (default: 'worker')",
        )
        parser.add_argument(
            "--list",
            action="store_true",
            help="List registered pipelines without starting",
        )
        parser.add_argument(
            "--health-auth-token",
            default=os.getenv("AGORA_HEALTH_AUTH_TOKEN"),
            help=(
                "Optional Bearer token for /health, /metrics, and /ready. "
                "Defaults to AGORA_HEALTH_AUTH_TOKEN when set."
            ),
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        ensure_project_on_path(ctx)

        if args.list:
            return _list_pipelines(args.module)

        pool = _load_worker(args.module)
        if args.health_auth_token is not None:
            pool.set_health_auth_token(args.health_auth_token)

        pipelines = pool.registered_pipelines()
        console.worker_header(
            module=args.module,
            pipelines=[(p.pipeline_id, str(p.schedule)) for p in pipelines],
            health_port=pool._health_port,
            health_host=pool._health_host,
            health_auth_enabled=pool._health_auth_token is not None,
        )

        asyncio.run(pool.run())

        console.info("Worker stopped.")
        return 0


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #


def _load_worker(module_path: str) -> WorkerPool:
    """Import module and call get_worker() → WorkerPool.

    Raises
    ------
    CommandError
        If the module cannot be imported, the factory is missing,
        or the factory returns the wrong type.
    """
    from agora.runner import WorkerPool

    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        raise CommandError(
            f"Cannot import '{module_path}': {exc}.\n"
            f"  Make sure the module is importable from your project root."
        ) from exc

    factory_fn = getattr(mod, "get_worker", None)
    if factory_fn is None:
        raise CommandError(f"Module '{module_path}' must define  get_worker() -> WorkerPool")

    result = factory_fn()
    if inspect.iscoroutine(result):
        result = asyncio.run(result)

    if not isinstance(result, WorkerPool):
        raise CommandError(f"get_worker() must return WorkerPool, got {type(result).__name__}")

    return result


def _list_pipelines(module_path: str) -> int:
    pool = _load_worker(module_path)
    coordinator = getattr(pool, "_coordinator", None)
    if coordinator is not None and hasattr(coordinator, "list_workers"):
        workers = asyncio.run(_list_fleet(coordinator))
        if workers:
            total_pipelines = sum(len(w.assigned_pipelines) for w in workers)
            console.section(f"Worker fleet ({len(workers)} workers, {total_pipelines} pipelines):")
            for w in workers:
                pipelines_str = (
                    ", ".join(w.assigned_pipelines) if w.assigned_pipelines else "(none)"
                )
                console.item(f"{w.worker_id:<40s}  {w.status:<10s}  {pipelines_str}")
        else:
            console.section("No live workers found in fleet.")
        return 0

    console.section(f"Pipelines in '{module_path}':")
    for p in pool.registered_pipelines():
        console.item(f"{p.pipeline_id:<35s}  {p.schedule}")
    return 0


async def _list_fleet(coordinator: WorkerCoordinator) -> list[WorkerInfo]:
    await coordinator.connect()
    try:
        return await coordinator.list_workers()
    finally:
        await coordinator.close()
