"""
agora/cli/commands/run.py
=========================
``agora run <pipeline>`` — import and execute a pipeline factory.

Convention
----------
The pipeline module at ``src/pipelines/<name>.py`` must expose either:

1. An async factory function named ``build_pipeline`` that returns a
   ``BoundPipeline``::

       async def build_pipeline() -> BoundPipeline:
           return Pipeline(MySource()).pipe(...).build(MySink())

2. A top-level ``BoundPipeline`` instance named ``pipeline``.

Then run with::

    agora run pipelines.places
    agora run pipelines.places --max-records 500 --dry-run
    agora run my_pipeline --config pipelines.toml
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
    collect_import_references,
    describe_pipeline_config,
    resolve_config_document,
    validate_config_document,
)

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext

logger = logstruct.getLogger(__name__)


class RunCommand(BaseCommand):
    """Run a pipeline by dotted module path."""

    name = "run"
    description = "Run a pipeline by module path (e.g. pipelines.example)."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "pipeline",
            nargs="?",
            help=(
                "Dotted module path, e.g. 'pipelines.places'. "
                "When used with --config, selects a named pipeline from the config file."
            ),
        )
        parser.add_argument(
            "--config",
            default=None,
            metavar="FILE",
            help="Run a declarative pipeline from an agora/v1 TOML config file.",
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
            "--max-records",
            type=int,
            default=None,
            metavar="N",
            help="Stop after N records",
        )
        parser.add_argument(
            "--run-id",
            default=None,
            help="Stable run ID for idempotent replays",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Use StdoutSink — do not write to real storage",
        )
        parser.add_argument(
            "--plan",
            action="store_true",
            help="Validate declarative config and print the resolved pipeline plan without running.",
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        ensure_project_on_path(ctx)
        asyncio.run(_build_and_run(args))
        return 0


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #


async def _run_pipeline(pipeline: Any, args: argparse.Namespace) -> None:
    """Execute the pipeline with CLI-provided arguments.

    Parameters
    ----------
    pipeline:
        A ``BoundPipeline`` instance returned by the project factory.
    args:
        Parsed CLI arguments.
    """
    if getattr(args, "dry_run", False):
        from agora.sinks.io.stdout import StdoutSink

        console.warn("Dry-run mode — using StdoutSink")
        pipeline = pipeline.with_sink(StdoutSink())

    max_records = getattr(args, "max_records", None)
    run_id = getattr(args, "run_id", None)

    console.header(f"Starting pipeline: {pipeline.pipeline_id}")
    summary = await pipeline.run(max_records=max_records, run_id=run_id)
    console.run_summary(summary)


async def _build_and_run(args: argparse.Namespace) -> None:
    """Import pipeline module, resolve factory, and run.

    Raises
    ------
    CommandError
        If the module cannot be imported or has no recognised entry point.
    """
    if args.config:
        resolved = _load_resolved_pipeline_config(
            args.config,
            pipeline_name=args.pipeline,
            profile_name=getattr(args, "profile", None),
            environment_name=getattr(args, "environment", None),
        )
        _warn_if_config_uses_import_refs(args.config, resolved.pipeline_config)
        if getattr(args, "plan", False):
            _print_pipeline_plan(args.config, resolved)
            return

        container = _build_container_from_pipeline_config(args.config, resolved.pipeline_config)
        async with container:
            pipeline = container.build_pipeline()
            await _run_pipeline(pipeline, args)
        return

    module_path = args.pipeline
    if module_path is None:
        raise CommandError("Provide a pipeline module path or use --config FILE.")
    if getattr(args, "plan", False):
        raise CommandError("--plan currently requires --config FILE.")
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        raise CommandError(
            f"Cannot import '{module_path}': {exc}.\n"
            f"  Ensure the module is importable from your project root."
        ) from exc

    if hasattr(module, "build_pipeline"):
        factory = module.build_pipeline
        pipeline = await factory() if inspect.iscoroutinefunction(factory) else factory()
    elif hasattr(module, "pipeline"):
        pipeline = module.pipeline
    else:
        raise CommandError(
            f"'{module_path}' must define  build_pipeline()  or a top-level  pipeline  instance."
        )

    await _run_pipeline(pipeline, args)


def _load_container_from_config(config_path: str, pipeline_name: str | None = None) -> Any:
    resolved = _load_resolved_pipeline_config(config_path, pipeline_name=pipeline_name)
    return _build_container_from_pipeline_config(config_path, resolved.pipeline_config)


def _build_container_from_pipeline_config(config_path: str, pipeline_cfg: dict[str, Any]) -> Any:
    from agora.core.container import AgoraContainer

    try:
        return AgoraContainer.from_config(pipeline_cfg)
    except Exception as exc:
        raise CommandError(f"Cannot build pipeline from '{config_path}': {exc}") from exc


def _load_resolved_pipeline_config(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> Any:
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
        return resolve_config_document(
            raw,
            pipeline_name=pipeline_name,
            profile_name=profile_name,
            environment_name=environment_name or os.getenv("AGORA_ENV"),
        )
    except Exception as exc:
        raise CommandError(f"Invalid pipeline config in '{config_path}': {exc}") from exc


def _print_pipeline_plan(config_path: str, resolved: Any) -> None:
    plan = describe_pipeline_config(resolved.pipeline_config)

    console.section("Pipeline Plan")
    console.item("config", config_path)
    console.item("pipeline", resolved.pipeline_name)
    console.item("pipeline_id", plan["pipeline_id"])
    console.item("profile", resolved.profile_name or "none")
    console.item("environment", resolved.environment_name or "none")
    console.item("source", plan["source"])
    console.item("middlewares", ", ".join(plan["middlewares"]) or "none")
    import_refs = plan.get("import_refs", [])
    if import_refs:
        console.item("imports", ", ".join(import_refs))
    else:
        console.item("imports", "none")

    dedup = plan["dedup"]
    if dedup is None:
        console.item("dedup", "disabled")
    else:
        details = [f"key={dedup['key']}"]
        if dedup["store"] is not None:
            details.append(f"store={dedup['store']}")
        if dedup["strategy"] is not None:
            details.append(f"strategy={dedup['strategy']}")
        console.item("dedup", ", ".join(details))

    dlq = plan["dlq"]
    if dlq is None or not dlq["enabled"]:
        console.item("dlq", "disabled")
    else:
        details = [f"sink={dlq['sink'] or 'unknown'}"]
        details.append(f"failure_policy={dlq['failure_policy']}")
        console.item("dlq", ", ".join(details))

    tracing = plan.get("tracing")
    if tracing is None or not tracing["enabled"]:
        console.item("tracing", "disabled")
    else:
        details = [f"backend={tracing['backend']}"]
        if tracing["service_name"] is not None:
            details.append(f"service_name={tracing['service_name']}")
        console.item("tracing", ", ".join(details))

    console.item("sinks", ", ".join(plan["sinks"]))
    console.blank()


def _warn_if_config_uses_import_refs(config_path: str, pipeline_config: dict[str, Any]) -> None:
    import_refs = collect_import_references(pipeline_config)
    if not import_refs:
        return
    console.warn(
        f"Config '{config_path}' resolves {len(import_refs)} trusted Python import reference(s). "
        "Review declarative configs like code: Agora imports these objects after prepending your project root and src/ to sys.path."
    )
