"""
tests/core/test_recovery_cli.py
================================
CLI contract tests for recovery UX surfaced by:

- ``docs/guides/checkpointing.md``
- ``docs/guides/recovery-matrix.md``
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from agora.cli.commands.checkpoint import _run_checkpoint_command
from agora.cli.commands.diagnose import _run_diagnose
from agora.core.checkpoint import Checkpoint, InMemoryCheckpointStore
from agora.core.dlq import DLQRecord


class _FakeConsole:
    def __init__(self) -> None:
        self.sections: list[str] = []
        self.items: list[tuple[str, ...]] = []
        self.warns: list[str] = []
        self.infos: list[str] = []
        self.errors: list[str] = []
        self.outs: list[str] = []
        self.blanks = 0

    def section(self, title: str) -> None:
        self.sections.append(title)

    def item(self, *columns: str) -> None:
        self.items.append(columns)

    def warn(self, message: str) -> None:
        self.warns.append(message)

    def info(self, message: str) -> None:
        self.infos.append(message)

    def error(self, message: str) -> None:
        self.errors.append(message)

    def out(self, message: str) -> None:
        self.outs.append(message)

    def blank(self) -> None:
        self.blanks += 1


def _checkpoint_args(
    subcommand: str,
    pipeline_id: str,
    *,
    store: str = "memory",
    yes: bool = False,
    json_output: bool = False,
) -> argparse.Namespace:
    return argparse.Namespace(
        subcommand=subcommand,
        pipeline_id=pipeline_id,
        store=store,
        yes=yes,
        json=json_output,
    )


def _diagnose_args(
    pipeline_id: str,
    *,
    checkpoint_store: str = "memory",
    dlq_path: str = "/nonexistent/path/.agora_dlq.db",
    json_output: bool = False,
) -> argparse.Namespace:
    return argparse.Namespace(
        pipeline_id=pipeline_id,
        checkpoint_store=checkpoint_store,
        dlq_path=dlq_path,
        json=json_output,
    )


def _checkpoint(
    pipeline_id: str,
    *,
    source: str,
    value: object,
) -> Checkpoint:
    return Checkpoint(
        pipeline_id=pipeline_id,
        run_id="run-abc",
        source=source,
        value=value,
        recorded_at=datetime(2026, 6, 4, 10, 0, 0, tzinfo=UTC),
    )


def _dlq_record(
    pipeline_id: str,
    *,
    source: str,
) -> DLQRecord:
    return DLQRecord(
        pipeline_id=pipeline_id,
        run_id="run-abc",
        stage="source_stream",
        error_type="RuntimeError",
        error_message="boom",
        record={"id": 1},
        source=source,
        created_at=datetime(2026, 6, 4, 10, 42, 0, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_checkpoint_inspect_surfaces_recovery_contract_for_parquet_source() -> None:
    """Validates: checkpoint inspect shows resume contract for ParquetSource."""
    store = InMemoryCheckpointStore()
    await store.save(
        "parquet_pipe",
        _checkpoint(
            "parquet_pipe",
            source="parquet",
            value={"row_number": 42},
        ),
    )
    fake_console = _FakeConsole()

    with (
        patch("agora.cli.commands.checkpoint._build_store", return_value=store),
        patch("agora.cli.commands.checkpoint.console", fake_console),
    ):
        exit_code = await _run_checkpoint_command(_checkpoint_args("inspect", "parquet_pipe"))

    assert exit_code == 0
    assert ("recovery support", "yes") in fake_console.items
    assert ("resume key", "row_number") in fake_console.items
    assert ("granularity", "row number across the full file") in fake_console.items
    assert ("resume cost", "linear re-read from file start") in fake_console.items
    assert any(
        "no row-group seek" in row[1] for row in fake_console.items if row[0] == "resume behavior"
    )


@pytest.mark.asyncio
async def test_checkpoint_inspect_warns_for_large_file_resume_offset() -> None:
    """Validates: checkpoint inspect warns about expensive high-offset file resume."""
    store = InMemoryCheckpointStore()
    await store.save(
        "csv_pipe",
        _checkpoint(
            "csv_pipe",
            source="csv",
            value={"row_number": 250_000},
        ),
    )
    fake_console = _FakeConsole()

    with (
        patch("agora.cli.commands.checkpoint._build_store", return_value=store),
        patch("agora.cli.commands.checkpoint.console", fake_console),
    ):
        exit_code = await _run_checkpoint_command(_checkpoint_args("inspect", "csv_pipe"))

    assert exit_code == 0
    assert any("High resume offset detected" in message for message in fake_console.warns)
    assert any("scan from file start" in message for message in fake_console.warns)


@pytest.mark.asyncio
async def test_diagnose_surfaces_recovery_contract_from_checkpoint_source() -> None:
    """Validates: diagnose shows resume contract from the stored checkpoint source."""
    store = InMemoryCheckpointStore()
    await store.save(
        "jsonl_pipe",
        _checkpoint(
            "jsonl_pipe",
            source="jsonl",
            value={"line_number": 7},
        ),
    )
    fake_console = _FakeConsole()

    with (
        patch("agora.cli.commands.diagnose._build_checkpoint_store", return_value=store),
        patch("agora.cli.commands.diagnose._load_dlq_records", return_value=[]),
        patch("agora.cli.commands.diagnose.console", fake_console),
    ):
        exit_code = await _run_diagnose(_diagnose_args("jsonl_pipe"))

    assert exit_code == 0
    assert ("Recovery support", "yes") in fake_console.items
    assert ("Resume key", "line_number") in fake_console.items
    assert ("Resume cost", "linear re-read from file start") in fake_console.items
    assert any(
        "skips lines up to the saved line_number" in row[1]
        for row in fake_console.items
        if row[0] == "Resume behavior"
    )


@pytest.mark.asyncio
async def test_diagnose_uses_dlq_source_when_checkpoint_is_missing() -> None:
    """Validates: diagnose falls back to the latest DLQ source for recovery hints."""
    store = InMemoryCheckpointStore()
    fake_console = _FakeConsole()

    with (
        patch("agora.cli.commands.diagnose._build_checkpoint_store", return_value=store),
        patch(
            "agora.cli.commands.diagnose._load_dlq_records",
            return_value=[_dlq_record("iterable_pipe", source="iterable")],
        ),
        patch("agora.cli.commands.diagnose.console", fake_console),
    ):
        exit_code = await _run_diagnose(_diagnose_args("iterable_pipe"))

    assert exit_code == 1
    assert ("Recovery support", "no") in fake_console.items
    assert ("Resume key", "not supported") in fake_console.items
    assert any(
        "always restarts from the beginning" in row[1]
        for row in fake_console.items
        if row[0] == "Resume behavior"
    )


@pytest.mark.asyncio
async def test_checkpoint_inspect_json_includes_recovery_warning_details() -> None:
    store = InMemoryCheckpointStore()
    await store.save(
        "csv_pipe",
        _checkpoint(
            "csv_pipe",
            source="csv",
            value={"row_number": 250_000},
        ),
    )
    fake_console = _FakeConsole()

    with (
        patch("agora.cli.commands.checkpoint._build_store", return_value=store),
        patch("agora.cli.commands.checkpoint.console", fake_console),
    ):
        exit_code = await _run_checkpoint_command(
            _checkpoint_args("inspect", "csv_pipe", json_output=True)
        )

    assert exit_code == 0
    assert fake_console.outs
    payload = json.loads(fake_console.outs[-1])
    assert payload["found"] is True
    assert payload["checkpoint"]["value"] == {"row_number": 250000}
    assert payload["recovery"]["resume_cost_model"] == "linear re-read from file start"
    assert payload["recovery"]["warning"]["code"] == "high_resume_offset"
    assert payload["recovery"]["warning"]["estimated_replay_units"] == 250000


@pytest.mark.asyncio
async def test_checkpoint_reset_json_reports_deleted_status() -> None:
    store = InMemoryCheckpointStore()
    await store.save(
        "reset_pipe",
        _checkpoint(
            "reset_pipe",
            source="jsonl",
            value={"line_number": 12},
        ),
    )
    fake_console = _FakeConsole()

    with (
        patch("agora.cli.commands.checkpoint._build_store", return_value=store),
        patch("agora.cli.commands.checkpoint.console", fake_console),
    ):
        exit_code = await _run_checkpoint_command(
            _checkpoint_args("reset", "reset_pipe", yes=True, json_output=True)
        )

    assert exit_code == 0
    payload = json.loads(fake_console.outs[-1])
    assert payload["reset"] is True
    assert payload["status"] == "deleted"
    assert await store.load("reset_pipe") is None


@pytest.mark.asyncio
async def test_diagnose_json_reports_recovery_and_dlq_summary() -> None:
    store = InMemoryCheckpointStore()
    await store.save(
        "json_pipe",
        _checkpoint(
            "json_pipe",
            source="jsonl",
            value={"line_number": 7},
        ),
    )
    fake_console = _FakeConsole()

    with (
        patch("agora.cli.commands.diagnose._build_checkpoint_store", return_value=store),
        patch(
            "agora.cli.commands.diagnose._load_dlq_records",
            return_value=[_dlq_record("json_pipe", source="jsonl")],
        ),
        patch("agora.cli.commands.diagnose.console", fake_console),
    ):
        exit_code = await _run_diagnose(_diagnose_args("json_pipe", json_output=True))

    assert exit_code == 1
    payload = json.loads(fake_console.outs[-1])
    assert payload["status"] == "attention_needed"
    assert payload["checkpoint"]["status"] == "present"
    assert payload["recovery"]["resume_key"] == "line_number"
    assert payload["dlq"]["record_count"] == 1
    assert payload["summary"]["has_failure_indicators"] is True
