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
import json
from typing import TYPE_CHECKING, Any

from agora.cli.commands.base import BaseCommand, CommandError
from agora.cli.console import console
from agora.cli.recovery import recovery_insight_for_source, recovery_insight_to_dict

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
        parser.add_argument(
            "--json",
            action="store_true",
            default=False,
            help="Emit machine-readable JSON output.",
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
        as_json=args.json,
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
    as_json: bool,
) -> int:
    latest_dlq_record = max(dlq_records, key=lambda r: r.created_at) if dlq_records else None
    source_name = checkpoint.source if checkpoint is not None else None
    if source_name is None and latest_dlq_record is not None:
        source_name = latest_dlq_record.source
    checkpoint_value = checkpoint.value if checkpoint is not None else None
    insight = recovery_insight_for_source(source_name, checkpoint_value=checkpoint_value)
    replayable = [r for r in dlq_records if r.max_attempts is None or r.attempt < r.max_attempts]
    has_issues = (
        bool(dlq_records)
        or checkpoint is None
        or checkpoint_error is not None
        or dlq_error is not None
    )

    if as_json:
        console.out(
            json.dumps(
                {
                    "pipeline_id": pipeline_id,
                    "status": "attention_needed" if has_issues else "ok",
                    "checkpoint": _checkpoint_payload(checkpoint, checkpoint_error),
                    "recovery": recovery_insight_to_dict(insight),
                    "dlq": _dlq_payload(
                        pipeline_id,
                        dlq_records,
                        replayable,
                        latest_dlq_record,
                        dlq_error,
                    ),
                    "summary": {
                        "has_failure_indicators": has_issues,
                        "replay_command": (
                            f"agora dlq replay {pipeline_id} --config <config.toml>"
                            if replayable
                            else None
                        ),
                    },
                },
                ensure_ascii=False,
                default=str,
            )
        )
        return 1 if has_issues else 0

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

    if insight is not None:
        console.blank()
        console.item("Recovery support", insight.support)
        console.item("Resume key", insight.resume_key)
        console.item("Granularity", insight.granularity)
        console.item("Resume cost", insight.resume_cost_model)
        console.item("Resume behavior", insight.resume_behavior)
        if insight.warning is not None:
            console.blank()
            console.warn(insight.warning.message)

    # ---- DLQ section ----
    console.blank()
    if dlq_error:
        console.item("DLQ", f"[red]error reading: {dlq_error}[/red]")
    elif not dlq_records:
        console.item("DLQ", "[dim]no replayable records[/dim]")
    else:
        console.item("DLQ records", str(len(dlq_records)))
        console.item("Replayable", str(len(replayable)))

        # Surface the most recent failure detail
        latest = latest_dlq_record
        assert latest is not None
        console.blank()
        console.item("Last failure stage", latest.stage)
        console.item("Error type", latest.error_type)
        console.item("Error message", latest.error_message)
        if latest.middleware:
            console.item("Middleware", latest.middleware)
        if latest.checkpoint is not None:
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
    if has_issues:
        console.warn("Pipeline shows signs of failure. Review the details above.")
        return 1

    console.info("No failure indicators found for this pipeline.")
    return 0


def _checkpoint_payload(checkpoint: Any, checkpoint_error: str | None) -> dict[str, Any]:
    if checkpoint_error is not None:
        return {
            "status": "error",
            "error": checkpoint_error,
        }
    if checkpoint is None:
        return {
            "status": "missing",
            "message": "Pipeline has never run or checkpoint was reset.",
        }

    value = checkpoint.value
    if value is None:
        value_kind = "none"
    elif isinstance(value, dict):
        value_kind = "structured_cursor"
    else:
        value_kind = type(value).__name__
    return {
        "status": "present",
        "pipeline_id": checkpoint.pipeline_id,
        "run_id": checkpoint.run_id,
        "source": checkpoint.source,
        "recorded_at": checkpoint.recorded_at.isoformat(),
        "value": value,
        "value_kind": value_kind,
    }


def _dlq_payload(
    pipeline_id: str,
    records: list[Any],
    replayable: list[Any],
    latest_record: Any,
    dlq_error: str | None,
) -> dict[str, Any]:
    if dlq_error is not None:
        return {
            "status": "error",
            "error": dlq_error,
        }
    if not records:
        return {
            "status": "empty",
            "record_count": 0,
            "replayable_count": 0,
        }

    assert latest_record is not None
    return {
        "status": "records_present",
        "record_count": len(records),
        "replayable_count": len(replayable),
        "latest_failure": {
            "pipeline_id": pipeline_id,
            "run_id": latest_record.run_id,
            "stage": latest_record.stage,
            "source": latest_record.source,
            "error_type": latest_record.error_type,
            "error_message": latest_record.error_message,
            "middleware": latest_record.middleware,
            "checkpoint": latest_record.checkpoint,
            "recorded_at": latest_record.created_at.isoformat(),
        },
        "replay_command": (
            f"agora dlq replay {pipeline_id} --config <config.toml>" if replayable else None
        ),
    }


__all__ = ["DiagnoseCommand"]
