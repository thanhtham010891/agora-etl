"""
agora/cli/commands/diagnose.py
================================
``agora diagnose <pipeline_id>`` — summarize the last visible failure for a
pipeline from checkpoint and DLQ data.

Example output::

    Pipeline:   places_ingest
    Last run:   2026-06-04 10:42 UTC  (FAILED)
    Stage:      batch_middleware
    Error:      ValueError: deliberate worker failure
    Checkpoint: {"batch_index": 3}
    DLQ:        2 replayable records

Options
-------
    --checkpoint-store  sqlite:<path>  (default: sqlite:.agora_checkpoint.db)
    --dlq-path          path to SQLite DLQ file  (default: .agora_dlq.db)
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from agora.cli.commands.base import BaseCommand, CommandError
from agora.cli.console import console

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext

_DEFAULT_CHECKPOINT_STORE = "sqlite:.agora_checkpoint.db"
_DEFAULT_DLQ_PATH = ".agora_dlq.db"


def _build_checkpoint_store(spec: str) -> Any:
    from agora.core.checkpoint import InMemoryCheckpointStore, SQLiteCheckpointStore

    if spec == "memory":
        return InMemoryCheckpointStore()
    if spec.startswith("sqlite:"):
        path = spec[len("sqlite:") :]
        if not path:
            raise CommandError("sqlite store requires a path: --checkpoint-store sqlite:<path>")
        return SQLiteCheckpointStore(path=path)
    raise CommandError(f"Unknown checkpoint store spec {spec!r}. Use 'sqlite:<path>' or 'memory'.")


class DiagnoseCommand(BaseCommand):
    """Summarize the last visible failure for a pipeline from checkpoint and DLQ data."""

    name = "diagnose"
    description = "Summarize the last pipeline failure from checkpoint and DLQ data."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "pipeline_id",
            help="Pipeline ID to diagnose.",
        )
        parser.add_argument(
            "--checkpoint-store",
            default=_DEFAULT_CHECKPOINT_STORE,
            metavar="SPEC",
            help=(
                "Checkpoint backend spec. "
                "sqlite:<path> (default: sqlite:.agora_checkpoint.db) or memory."
            ),
        )
        parser.add_argument(
            "--dlq-path",
            default=_DEFAULT_DLQ_PATH,
            metavar="PATH",
            help="Path to SQLite DLQ file (default: .agora_dlq.db).",
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        return asyncio.run(_run_diagnose(args))


async def _run_diagnose(args: argparse.Namespace) -> int:
    pipeline_id: str = args.pipeline_id
    checkpoint_spec: str = args.checkpoint_store
    dlq_path: str = args.dlq_path

    checkpoint_store = _build_checkpoint_store(checkpoint_spec)
    checkpoint = None
    checkpoint_error: str | None = None
    try:
        checkpoint = await checkpoint_store.load(pipeline_id)
    except Exception as exc:
        checkpoint_error = f"{type(exc).__name__}: {exc}"
    finally:
        await checkpoint_store.close()

    dlq_records: list[Any] = []
    dlq_error: str | None = None
    try:
        dlq_records = await _load_dlq_records(pipeline_id, dlq_path)
    except Exception as exc:
        dlq_error = f"{type(exc).__name__}: {exc}"

    return _render_diagnosis(
        pipeline_id=pipeline_id,
        checkpoint=checkpoint,
        checkpoint_error=checkpoint_error,
        dlq_records=dlq_records,
        dlq_error=dlq_error,
    )


async def _load_dlq_records(pipeline_id: str, dlq_path: str) -> list[Any]:
    """Load the most recent DLQ records for *pipeline_id* from SQLite."""
    import os

    from agora.core.dlq import SQLiteDLQSource

    if not os.path.exists(dlq_path):
        return []

    source = SQLiteDLQSource(dlq_path, pipeline_id=pipeline_id, limit=100)
    await source.open()
    records: list[Any] = []
    try:
        async for record in source.stream():
            records.append(record)
    finally:
        await source.close()
    return records


def _render_diagnosis(
    *,
    pipeline_id: str,
    checkpoint: Any,
    checkpoint_error: str | None,
    dlq_records: list[Any],
    dlq_error: str | None,
) -> int:
    console.section(f"Diagnosis — {pipeline_id}")

    # ---- Pipeline identity ----
    console.item("Pipeline", pipeline_id)

    # ---- Checkpoint section ----
    if checkpoint_error:
        console.item("Checkpoint", f"[red]error reading: {checkpoint_error}[/red]")
    elif checkpoint is None:
        console.item(
            "Checkpoint", "[dim]none — pipeline has never run or checkpoint was reset[/dim]"
        )
    else:
        import json

        value_str = (
            json.dumps(checkpoint.value, default=str)
            if isinstance(checkpoint.value, dict)
            else str(checkpoint.value)
            if checkpoint.value is not None
            else "none"
        )
        console.item("Last run", checkpoint.recorded_at.strftime("%Y-%m-%d %H:%M UTC"))
        console.item("Run ID", checkpoint.run_id)
        console.item("Source", checkpoint.source)
        console.item("Checkpoint", value_str)

    # ---- DLQ section ----
    console.blank()
    if dlq_error:
        console.item("DLQ", f"[red]error reading: {dlq_error}[/red]")
    elif not dlq_records:
        console.item("DLQ", "[dim]no replayable records[/dim]")
    else:
        replayable = [
            r for r in dlq_records if r.max_attempts is None or r.attempt < r.max_attempts
        ]
        console.item("DLQ records", str(len(dlq_records)))
        console.item("Replayable", str(len(replayable)))

        # Surface the most recent failure detail
        latest = max(dlq_records, key=lambda r: r.created_at)
        console.blank()
        console.item("Last failure stage", latest.stage)
        console.item("Error type", latest.error_type)
        console.item("Error message", latest.error_message)
        if latest.middleware:
            console.item("Middleware", latest.middleware)
        if latest.checkpoint is not None:
            import json

            cp_str = (
                json.dumps(latest.checkpoint, default=str)
                if isinstance(latest.checkpoint, dict)
                else str(latest.checkpoint)
            )
            console.item("Record checkpoint", cp_str)
        console.item("Recorded at", latest.created_at.strftime("%Y-%m-%d %H:%M UTC"))

        # Recovery hint
        console.blank()
        if replayable:
            console.info(f"To replay: agora dlq replay {pipeline_id} --config <config.toml>")

    # ---- Summary status ----
    console.blank()
    has_issues = bool(dlq_records) or checkpoint is None
    if has_issues:
        console.warn("Pipeline shows signs of failure. Review the details above.")
        return 1

    console.info("No failure indicators found for this pipeline.")
    return 0


__all__ = ["DiagnoseCommand"]
