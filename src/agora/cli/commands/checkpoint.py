"""
agora/cli/commands/checkpoint.py
=================================
``agora checkpoint`` — inspect and manage pipeline resume state.

Subcommands
-----------
    agora checkpoint show <pipeline_id>     print last checkpoint value
    agora checkpoint inspect <pipeline_id>  detailed checkpoint breakdown
    agora checkpoint reset <pipeline_id> --yes  delete checkpoint (destructive)

All subcommands require --store to identify the checkpoint backend:
    --store sqlite:<path>   (default: .agora_checkpoint.db)
    --store memory          (in-memory, mainly for testing)
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

from agora.cli.commands.base import BaseCommand, CommandError
from agora.cli.console import console
from agora.cli.recovery import recovery_insight_for_source, recovery_insight_to_dict

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext

_DEFAULT_STORE = "sqlite:.agora_checkpoint.db"


def _build_store(store_spec: str) -> Any:
    """Build a CheckpointStore from a spec string like 'sqlite:path' or 'memory'."""
    from agora.core.checkpoint import InMemoryCheckpointStore, SQLiteCheckpointStore

    if store_spec == "memory":
        return InMemoryCheckpointStore()
    if store_spec.startswith("sqlite:"):
        path = store_spec[len("sqlite:") :]
        if not path:
            raise CommandError("sqlite store requires a path: --store sqlite:<path>")
        return SQLiteCheckpointStore(path=path)
    raise CommandError(f"Unknown store spec {store_spec!r}. Use 'sqlite:<path>' or 'memory'.")


def _render_value(value: Any) -> str:
    """Render a checkpoint value for display."""
    if value is None:
        return "[dim]none[/dim]"
    if isinstance(value, dict):
        return json.dumps(value, default=str)
    return str(value)


class CheckpointCommand(BaseCommand):
    """Inspect and manage pipeline checkpoint (resume) state."""

    name = "checkpoint"
    description = "Inspect and manage pipeline checkpoint resume state."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "subcommand",
            choices=["show", "inspect", "reset"],
            help="show: print value  |  inspect: detailed breakdown  |  reset: delete (destructive)",
        )
        parser.add_argument(
            "pipeline_id",
            help="Pipeline ID whose checkpoint to operate on.",
        )
        parser.add_argument(
            "--store",
            default=_DEFAULT_STORE,
            metavar="SPEC",
            help=(
                "Checkpoint backend spec. "
                "sqlite:<path> (default: sqlite:.agora_checkpoint.db) or memory."
            ),
        )
        parser.add_argument(
            "--yes",
            action="store_true",
            default=False,
            help="Required for 'reset' to confirm the destructive operation.",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            default=False,
            help="Emit machine-readable JSON output.",
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        return asyncio.run(_run_checkpoint_command(args))


async def _run_checkpoint_command(args: argparse.Namespace) -> int:
    subcommand: str = args.subcommand
    pipeline_id: str = args.pipeline_id
    store_spec: str = args.store

    store = _build_store(store_spec)
    try:
        if subcommand == "show":
            return await _cmd_show(store, pipeline_id, as_json=args.json)
        if subcommand == "inspect":
            return await _cmd_inspect(store, pipeline_id, as_json=args.json)
        if subcommand == "reset":
            return await _cmd_reset(store, pipeline_id, confirmed=args.yes, as_json=args.json)
        raise CommandError(f"Unknown subcommand: {subcommand!r}")
    finally:
        await store.close()


# ======================================================================
# show
# ======================================================================


async def _cmd_show(store: Any, pipeline_id: str, *, as_json: bool) -> int:
    """Print the last checkpoint value for *pipeline_id*."""
    checkpoint = await store.load(pipeline_id)

    if checkpoint is None:
        if as_json:
            console.out(
                json.dumps(
                    {
                        "pipeline_id": pipeline_id,
                        "found": False,
                        "message": "No checkpoint found.",
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        console.warn(f"No checkpoint found for pipeline {pipeline_id!r}.")
        return 1

    if as_json:
        console.out(
            json.dumps(
                {
                    "pipeline_id": checkpoint.pipeline_id,
                    "found": True,
                    "checkpoint": _checkpoint_payload(checkpoint),
                },
                ensure_ascii=False,
                default=str,
            )
        )
        return 0

    console.section(f"Checkpoint — {pipeline_id}")
    console.item("pipeline", checkpoint.pipeline_id)
    console.item("source", checkpoint.source)
    console.item("value", _render_value(checkpoint.value))
    console.item("recorded_at", checkpoint.recorded_at.isoformat())
    return 0


# ======================================================================
# inspect
# ======================================================================


async def _cmd_inspect(store: Any, pipeline_id: str, *, as_json: bool) -> int:
    """Print a detailed breakdown of the checkpoint for *pipeline_id*."""
    checkpoint = await store.load(pipeline_id)

    if checkpoint is None:
        if as_json:
            console.out(
                json.dumps(
                    {
                        "pipeline_id": pipeline_id,
                        "found": False,
                        "message": "Pipeline has never run or checkpoint was already reset.",
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        console.warn(f"No checkpoint found for pipeline {pipeline_id!r}.")
        console.info("Pipeline has never run or checkpoint was already reset.")
        return 1

    value = checkpoint.value
    insight = recovery_insight_for_source(checkpoint.source, checkpoint_value=value)
    if as_json:
        console.out(
            json.dumps(
                {
                    "pipeline_id": checkpoint.pipeline_id,
                    "found": True,
                    "checkpoint": _checkpoint_payload(checkpoint),
                    "recovery": recovery_insight_to_dict(insight),
                    "next_actions": {
                        "resume": "Start the pipeline normally to resume from this checkpoint.",
                        "reset_command": f"agora checkpoint reset {pipeline_id} --yes",
                    },
                },
                ensure_ascii=False,
                default=str,
            )
        )
        return 0

    console.section(f"Checkpoint Inspect — {pipeline_id}")
    console.item("pipeline_id", checkpoint.pipeline_id)
    console.item("run_id", checkpoint.run_id)
    console.item("source", checkpoint.source)
    console.item("recorded_at", checkpoint.recorded_at.isoformat())
    console.blank()

    if value is None:
        console.item("value", "[dim]none — source will restart from beginning[/dim]")
    elif isinstance(value, dict):
        console.item("value type", "structured cursor")
        for k, v in value.items():
            console.item(f"  {k}", str(v))
    else:
        console.item("value type", type(value).__name__)
        console.item("value", str(value))

    if insight is not None:
        console.blank()
        console.item("recovery support", insight.support)
        console.item("resume key", insight.resume_key)
        console.item("granularity", insight.granularity)
        console.item("resume cost", insight.resume_cost_model)
        console.item("resume behavior", insight.resume_behavior)
        if insight.warning is not None:
            console.blank()
            console.warn(insight.warning.message)
        if insight.runbook_hooks:
            console.blank()
            for hook in insight.runbook_hooks:
                console.item("operator hook", hook)

    console.blank()
    console.info(
        "To resume a pipeline from this checkpoint, start the pipeline normally — "
        "it will pick up from the value shown above."
    )
    console.info(f"To clear this checkpoint, run: agora checkpoint reset {pipeline_id} --yes")
    return 0


# ======================================================================
# reset
# ======================================================================


async def _cmd_reset(store: Any, pipeline_id: str, *, confirmed: bool, as_json: bool) -> int:
    """Delete the checkpoint for *pipeline_id* (destructive — requires --yes)."""
    if not confirmed:
        if as_json:
            console.out(
                json.dumps(
                    {
                        "pipeline_id": pipeline_id,
                        "confirmed": False,
                        "reset": False,
                        "status": "confirmation_required",
                        "message": "Reset is destructive and requires --yes.",
                        "reset_command": f"agora checkpoint reset {pipeline_id} --yes",
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        console.error(
            f"agora checkpoint reset is destructive — the pipeline will restart from "
            f"the beginning on next run.\n\n"
            f"To confirm, re-run with --yes:\n"
            f"  agora checkpoint reset {pipeline_id} --yes"
        )
        return 1

    checkpoint = await store.load(pipeline_id)
    if checkpoint is None:
        if as_json:
            console.out(
                json.dumps(
                    {
                        "pipeline_id": pipeline_id,
                        "found": False,
                        "reset": False,
                        "status": "missing",
                        "message": "No checkpoint found — nothing to reset.",
                    },
                    ensure_ascii=False,
                )
            )
            return 0
        console.warn(f"No checkpoint found for pipeline {pipeline_id!r} — nothing to reset.")
        return 0

    # Prefer a real store delete when supported. Fallback to a sentinel checkpoint
    # that makes the next run restart from the beginning.
    deleted = False
    delete_mode = "cleared"
    delete_fn = getattr(store, "delete", None)
    if callable(delete_fn):
        deleted = bool(await delete_fn(pipeline_id))
        delete_mode = "deleted"

    if not deleted:
        # Fallback: save a fresh checkpoint with value=None so the source
        # restarts from the beginning. Not a true delete but functionally
        # equivalent for all built-in sources.
        from datetime import UTC, datetime

        from agora.core.checkpoint import Checkpoint

        reset_checkpoint = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            run_id=checkpoint.run_id,
            source=checkpoint.source,
            value=None,
            recorded_at=datetime.now(UTC),
        )
        await store.save(pipeline_id, reset_checkpoint)
        delete_mode = "cleared"

    if as_json:
        console.out(
            json.dumps(
                {
                    "pipeline_id": pipeline_id,
                    "found": True,
                    "reset": True,
                    "status": delete_mode,
                    "checkpoint": _checkpoint_payload(checkpoint),
                    "message": "Pipeline will restart from the beginning on next run.",
                },
                ensure_ascii=False,
                default=str,
            )
        )
        return 0

    console.section(f"Checkpoint Reset — {pipeline_id}")
    console.item("pipeline", pipeline_id)
    console.item("source", checkpoint.source)
    console.item("previous value", _render_value(checkpoint.value))
    console.item("status", "[bold green]reset[/bold green]")
    console.item("mode", delete_mode)
    console.blank()
    console.info(f"Pipeline {pipeline_id!r} will restart from the beginning on next run.")
    return 0


def _checkpoint_payload(checkpoint: Any) -> dict[str, Any]:
    value = checkpoint.value
    if value is None:
        value_kind = "none"
    elif isinstance(value, dict):
        value_kind = "structured_cursor"
    else:
        value_kind = type(value).__name__
    return {
        "pipeline_id": checkpoint.pipeline_id,
        "run_id": checkpoint.run_id,
        "source": checkpoint.source,
        "recorded_at": checkpoint.recorded_at.isoformat(),
        "value": value,
        "value_kind": value_kind,
    }


__all__ = ["CheckpointCommand"]
