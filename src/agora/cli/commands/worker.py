"""
agora/cli/commands/worker.py
==============================
``agora worker`` — start the WorkerPool from a project's worker module.

Convention: the project exposes a ``worker.py`` that defines
``get_worker() -> WorkerPool``.

Usage::

    agora worker                               # loads worker.py in cwd
    agora worker --module pipelines.worker     # custom module path
    agora worker --config pipelines.toml       # build WorkerPool from config alone
    agora worker --list                        # list registered pipelines without starting
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any

import logstruct

from agora.cli._path import ensure_project_on_path
from agora.cli.commands.base import BaseCommand, CommandError
from agora.cli.console import console
from agora.config import (
    ResolvedWorkerConfig,
    collect_import_references,
    resolve_worker_config_document,
    validate_config_document,
)

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext
    from agora.core.pipeline import BoundPipeline
    from agora.runner.coordinator import WorkerCoordinator, WorkerInfo
    from agora.runner.scheduled import Schedule
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
            "--config",
            default=None,
            metavar="FILE",
            help="Build the WorkerPool from an agora/v1 TOML config file.",
        )
        parser.add_argument(
            "--profile",
            default=None,
            help="Select a config profile overlay from [profiles.<name>].",
        )
        parser.add_argument(
            "--environment",
            default=None,
            help="Select a config environment overlay from [environments.<name>].",
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
            if args.config:
                return _list_pipelines_from_config(
                    args.config,
                    profile_name=args.profile,
                    environment_name=args.environment,
                )
            return _list_pipelines(args.module)

        pool = (
            _load_worker_from_config(
                args.config,
                profile_name=args.profile,
                environment_name=args.environment,
            )
            if args.config
            else _load_worker(args.module)
        )
        if args.health_auth_token is not None:
            pool.set_health_auth_token(args.health_auth_token)

        pipelines = pool.registered_pipelines()
        console.worker_header(
            module=args.config or args.module,
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


def _load_worker_from_config(
    config_path: str | None,
    *,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> WorkerPool:
    if config_path is None:
        raise CommandError("Config path is required when using --config.")

    from agora.core.container import AgoraContainer
    from agora.runner import ScheduledPipeline, WorkerPool

    resolved = _load_resolved_worker_config(
        config_path,
        profile_name=profile_name,
        environment_name=environment_name,
    )
    _warn_if_worker_config_uses_import_refs(config_path, resolved)

    worker_cfg = resolved.worker_config
    pool = WorkerPool(
        graceful_shutdown_timeout=worker_cfg.get("graceful_shutdown_timeout", 30.0),
        health_port=worker_cfg.get("health_port"),
        health_host=worker_cfg.get("health_host", "127.0.0.1"),
        health_auth_token=worker_cfg.get("health_auth_token"),
    )

    for resolved_pipeline in resolved.pipelines:
        pipeline_cfg = resolved_pipeline.pipeline_config
        schedule = _schedule_from_config(pipeline_cfg["schedule"])
        pipeline_id = pipeline_cfg["pipeline_id"]
        max_records = pipeline_cfg.get("max_records")

        async def _factory(config: dict[str, Any] = pipeline_cfg) -> BoundPipeline[Any]:
            container = AgoraContainer.from_config(config)
            return container.build_pipeline()

        pool.register(
            ScheduledPipeline(
                factory=_factory,
                schedule=schedule,
                pipeline_id=pipeline_id,
                max_records=max_records,
            )
        )

    return pool


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


def _list_pipelines_from_config(
    config_path: str,
    *,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> int:
    resolved = _load_resolved_worker_config(
        config_path,
        profile_name=profile_name,
        environment_name=environment_name,
    )

    console.section(f"Pipelines in '{config_path}':")
    for item in resolved.pipelines:
        schedule = _schedule_from_config(item.pipeline_config["schedule"])
        console.item(f"{item.pipeline_config['pipeline_id']:<35s}  {schedule}")
    return 0


async def _list_fleet(coordinator: WorkerCoordinator) -> list[WorkerInfo]:
    await coordinator.connect()
    try:
        return await coordinator.list_workers()
    finally:
        await coordinator.close()


def _load_resolved_worker_config(
    config_path: str,
    *,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> ResolvedWorkerConfig:
    path = Path(config_path)
    if not path.exists():
        raise CommandError(f"Config file not found: {config_path}")

    try:
        with path.open("rb") as file_obj:
            raw = tomllib.load(file_obj)
    except tomllib.TOMLDecodeError as exc:
        raise CommandError(f"Invalid TOML in '{config_path}': {exc}") from exc

    try:
        validate_config_document(raw)
        return resolve_worker_config_document(
            raw,
            profile_name=profile_name,
            environment_name=environment_name or os.getenv("AGORA_ENV"),
        )
    except Exception as exc:
        raise CommandError(f"Invalid worker config in '{config_path}': {exc}") from exc


def _warn_if_worker_config_uses_import_refs(
    config_path: str, resolved: ResolvedWorkerConfig
) -> None:
    refs: list[str] = []
    for item in resolved.pipelines:
        refs.extend(collect_import_references(item.pipeline_config))
    if not refs:
        return
    console.warn(
        f"Config '{config_path}' resolves {len(refs)} trusted Python import reference(s). "
        "Review declarative worker configs like code: Agora imports these objects after prepending your project root and src/ to sys.path."
    )


def _schedule_from_config(config: dict[str, Any]) -> Schedule:
    from agora.runner import Schedule

    mode = str(config["mode"])
    if mode == "every":
        return Schedule.every(
            seconds=float(config.get("seconds", 0.0)),
            minutes=float(config.get("minutes", 0.0)),
            hours=float(config.get("hours", 0.0)),
            days=float(config.get("days", 0.0)),
        )
    if mode == "cron":
        return Schedule.cron(str(config["expression"]))
    if mode == "continuous":
        return Schedule.continuous()
    if mode == "once":
        return Schedule.once()
    raise CommandError(f"Unknown schedule mode '{mode}'.")
