"""agora/cli/commands/dlq.py — ``agora dlq replay``"""

from __future__ import annotations

import asyncio
import copy
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any

from agora.cli._path import ensure_project_on_path
from agora.cli.commands.base import BaseCommand, CommandError
from agora.cli.commands.run import (
    _build_container_from_pipeline_config,
    _load_resolved_pipeline_config,
    _warn_if_config_uses_import_refs,
)
from agora.cli.console import console
from agora.core.component_factory import config_component_factory
from agora.core.middleware import MiddlewareChain
from agora.core.pipeline import BoundPipeline
from agora.core.source import IterableSource

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext
    from agora.core.dlq import DLQRecord

_DEFAULT_DLQ_PATH = ".agora_dlq.db"
_DLQ_SOURCE_BY_SINK_TYPE = {
    "sqlite_dlq": "sqlite_dlq_source",
    "postgres_dlq": "postgres_dlq_source",
    "redis_dlq": "redis_dlq_source",
}
_DLQ_SOURCE_FIELDS_BY_SINK_TYPE = {
    "sqlite_dlq": ("path",),
    "postgres_dlq": ("dsn", "table"),
    "redis_dlq": ("url", "key_prefix"),
}


class DLQCommand(BaseCommand):
    """Replay dead-letter records back through a configured pipeline."""

    name = "dlq"
    description = "Replay dead-letter records from the configured DLQ backend."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "subcommand",
            nargs="?",
            choices=["replay"],
            default="replay",
            help="Subcommand (default: replay)",
        )
        parser.add_argument(
            "pipeline",
            nargs="?",
            help="Select a named pipeline from the config file.",
        )
        parser.add_argument(
            "--config",
            required=True,
            metavar="FILE",
            help="Replay DLQ records using an agora/v1 TOML config file.",
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
            "--stage",
            default=None,
            help="Replay only records captured at the given stage.",
        )
        parser.add_argument(
            "--mode",
            choices=["pipeline", "sink"],
            default="pipeline",
            help="Replay through the full pipeline or re-drive only the sink writer path.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            metavar="N",
            help="Replay at most N DLQ records.",
        )
        parser.add_argument(
            "--run-id",
            default=None,
            help="Override the replay pipeline run ID.",
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        ensure_project_on_path(ctx)
        return asyncio.run(_run_dlq_command(args))


async def _run_dlq_command(args: argparse.Namespace) -> int:
    if args.subcommand != "replay":
        raise CommandError(f"Unsupported dlq subcommand: {args.subcommand}")
    if args.limit is not None and args.limit < 1:
        raise CommandError("--limit must be >= 1.")
    if getattr(args, "mode", "pipeline") == "sink" and getattr(args, "stage", None) not in (
        None,
        "sink_write",
    ):
        raise CommandError("--mode sink only supports DLQ records from stage 'sink_write'.")

    resolved = _load_resolved_pipeline_config(
        args.config,
        pipeline_name=args.pipeline,
        profile_name=getattr(args, "profile", None),
        environment_name=getattr(args, "environment", None),
    )
    pipeline_cfg = resolved.pipeline_config
    _warn_if_config_uses_import_refs(args.config, pipeline_cfg)
    dlq_sink_cfg = _resolve_dlq_sink_config(pipeline_cfg)
    dlq_source_cfg = _build_dlq_source_config(
        pipeline_cfg,
        stage=getattr(args, "stage", None),
        limit=getattr(args, "limit", None),
    )
    replay_cfg = copy.deepcopy(pipeline_cfg)
    replay_cfg.pop("dlq", None)

    replay_container = _build_container_from_pipeline_config(args.config, replay_cfg)
    dlq_sink = config_component_factory.build_component(dlq_sink_cfg, "sink")
    dlq_source = config_component_factory.build_component(dlq_source_cfg, "source")

    console.section("DLQ Replay")
    console.item("config", args.config)
    console.item("pipeline", resolved.pipeline_name)
    console.item("backend", dlq_sink_cfg["type"])
    console.item("mode", getattr(args, "mode", "pipeline"))
    console.item("stage", getattr(args, "stage", None) or "all")
    console.item("limit", str(getattr(args, "limit", None) or "unbounded"))

    attempted = 0
    succeeded = 0
    failed = 0

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(replay_container)
        await stack.enter_async_context(dlq_source)
        await dlq_sink.open()
        stack.push_async_callback(dlq_sink.close)

        replay_template = replay_container.build_pipeline()
        async for record in dlq_source.stream():
            attempted += 1
            replay_mode = getattr(args, "mode", "pipeline")
            if replay_mode == "sink" and record.stage != "sink_write":
                failed += 1
                console.warn(
                    f"Replay skipped for {record.pipeline_id}/{record.stage}: "
                    "sink mode only supports sink_write records"
                )
                continue
            replay_payload = record.replay_payload(mode=replay_mode)
            if replay_payload is None:
                failed += 1
                console.warn(
                    f"Replay skipped for {record.pipeline_id}/{record.stage}: "
                    f"no {replay_mode} replay payload available"
                )
                continue
            try:
                updated = await dlq_sink.replay(record)
            except Exception as exc:
                failed += 1
                console.warn(
                    f"Replay failed for {record.pipeline_id}/{record.stage}: "
                    f"could not mark replay attempt: {type(exc).__name__}: {exc}"
                )
                continue
            replay_pipeline = _build_replay_pipeline(
                replay_template,
                replay_payload,
                mode=replay_mode,
            )
            replay_run_id = getattr(args, "run_id", None) or _default_replay_run_id(record, updated)
            try:
                summary = await replay_pipeline.run(max_records=1, run_id=replay_run_id)
            except Exception as exc:
                failed += 1
                console.warn(
                    f"Replay failed for {record.pipeline_id}/{record.stage}: "
                    f"{type(exc).__name__}: {exc}"
                )
                continue

            if summary.records_errored:
                failed += 1
                console.warn(
                    f"Replay failed for {record.pipeline_id}/{record.stage}: "
                    f"{summary.records_errored} record errors"
                )
                continue

            if summary.records_written != 1:
                failed += 1
                console.warn(
                    f"Replay failed for {record.pipeline_id}/{record.stage}: "
                    "record was not written during replay"
                )
                continue

            try:
                await dlq_sink.acknowledge(updated)
            except Exception as exc:
                failed += 1
                console.warn(
                    f"Replay failed for {record.pipeline_id}/{record.stage}: "
                    f"could not acknowledge replayed record: {type(exc).__name__}: {exc}"
                )
                continue
            succeeded += 1

    if attempted == 0:
        console.info("No DLQ records eligible for replay.")
    console.blank()
    console.item("attempted", str(attempted))
    console.item("succeeded", str(succeeded))
    console.item("failed", str(failed))
    return 1 if failed else 0


def _resolve_dlq_sink_config(pipeline_cfg: dict[str, Any]) -> dict[str, Any]:
    dlq_cfg = pipeline_cfg.get("dlq")
    if not isinstance(dlq_cfg, dict) or not dlq_cfg.get("enabled", True):
        raise CommandError("DLQ is not enabled for this pipeline config.")

    sink_cfg = dlq_cfg.get("sink")
    if isinstance(sink_cfg, dict):
        return dict(sink_cfg)

    return {
        "type": "sqlite_dlq",
        "path": dlq_cfg.get("path", _DEFAULT_DLQ_PATH),
    }


def _build_dlq_source_config(
    pipeline_cfg: dict[str, Any],
    *,
    stage: str | None,
    limit: int | None,
) -> dict[str, Any]:
    sink_cfg = _resolve_dlq_sink_config(pipeline_cfg)
    sink_type = sink_cfg["type"]
    source_type = _DLQ_SOURCE_BY_SINK_TYPE.get(sink_type)
    if source_type is None:
        raise CommandError(
            f"DLQ replay is not supported for sink type '{sink_type}'. "
            "Install a backend with a matching DLQ source or add a source mapping."
        )

    source_cfg: dict[str, Any] = {
        "type": source_type,
        "pipeline_id": pipeline_cfg.get("pipeline_id", "pipeline"),
    }
    for field in _DLQ_SOURCE_FIELDS_BY_SINK_TYPE.get(sink_type, ()):
        if field in sink_cfg:
            source_cfg[field] = sink_cfg[field]
    if stage is not None:
        source_cfg["stage"] = stage
    if limit is not None:
        source_cfg["limit"] = limit
    return source_cfg


def _build_replay_pipeline(
    template: BoundPipeline[Any],
    record: Any,
    *,
    mode: str = "pipeline",
) -> BoundPipeline[Any]:
    chain = template._chain if mode == "pipeline" else MiddlewareChain([])
    return BoundPipeline(
        source=IterableSource([record]),
        chain=chain,
        writer=template._writer,
        pipeline_id=template.pipeline_id,
        dlq=template._dlq_sink,
        dlq_failure_policy=template._dlq_failure_policy,
        checkpoint=template._checkpoint_store,
        checkpoint_key=template._checkpoint_key,
        checkpoint_every=template._checkpoint_every,
        checkpoint_failure_policy=template._checkpoint_failure_policy,
        batch_size=template._writer_batch_size,
        sink_failure_policy=template._sink_failure_policy,
        max_buffer_size=template._max_buffer_size,
        backpressure=template._backpressure,
        tracer=template._tracer,
    )


def _default_replay_run_id(original: DLQRecord, updated: DLQRecord) -> str:
    return f"{original.run_id}:replay:{updated.attempt}"


__all__ = ["DLQCommand", "_run_dlq_command"]
