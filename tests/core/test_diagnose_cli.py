"""
tests/core/test_diagnose_cli.py
================================
Tests for ``agora diagnose <pipeline_id>`` CLI command.

Coverage:
- No checkpoint + no DLQ → exit 0, info message
- Checkpoint present, no DLQ → exit 0, shows checkpoint details
- DLQ records present → exit 1, shows failure detail
- Checkpoint read error degrades gracefully
- DLQ read error degrades gracefully
- DLQ absent (file not found) returns empty list gracefully
- DiagnoseCommand name/description and parser
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

import pytest

from agora.cli.commands.diagnose import DiagnoseCommand, _dlq_payload, _run_diagnose
from agora.core.checkpoint import Checkpoint, InMemoryCheckpointStore
from agora.core.dlq import DLQRecord

# ======================================================================
# Helpers
# ======================================================================


def _make_args(
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


def _make_checkpoint(pipeline_id: str, value: Any = None) -> Checkpoint:
    if value is None:
        value = {"offset": 42}
    return Checkpoint(
        pipeline_id=pipeline_id,
        run_id="run-abc",
        source="test_source",
        value=value,
        recorded_at=datetime(2026, 6, 4, 10, 0, 0, tzinfo=UTC),
    )


def _make_dlq_record(
    pipeline_id: str,
    *,
    stage: str = "batch_middleware",
    error_type: str = "ValueError",
    error_message: str = "worker exploded",
    middleware: str | None = "doubler",
) -> DLQRecord:
    return DLQRecord(
        pipeline_id=pipeline_id,
        run_id="run-abc",
        stage=stage,
        error_type=error_type,
        error_message=error_message,
        record={"id": "a", "value": 1},
        source="test_source",
        middleware=middleware,
        created_at=datetime(2026, 6, 4, 10, 42, 0, tzinfo=UTC),
    )


# ======================================================================
# Tests — no data
# ======================================================================


@pytest.mark.asyncio
async def test_diagnose_no_checkpoint_no_dlq_returns_1() -> None:
    store = InMemoryCheckpointStore()

    with (
        patch("agora.cli.commands.diagnose._build_checkpoint_store", return_value=store),
        patch("agora.cli.commands.diagnose._load_dlq_records", return_value=[]),
    ):
        args = _make_args("empty_pipeline")
        exit_code = await _run_diagnose(args)

    # No checkpoint = pipeline never ran = worth flagging
    assert exit_code == 1


@pytest.mark.asyncio
async def test_diagnose_checkpoint_only_no_dlq_returns_0() -> None:
    store = InMemoryCheckpointStore()
    cp = _make_checkpoint("healthy_pipeline")
    await store.save("healthy_pipeline", cp)

    with (
        patch("agora.cli.commands.diagnose._build_checkpoint_store", return_value=store),
        patch("agora.cli.commands.diagnose._load_dlq_records", return_value=[]),
    ):
        args = _make_args("healthy_pipeline")
        exit_code = await _run_diagnose(args)

    assert exit_code == 0


# ======================================================================
# Tests — DLQ records present
# ======================================================================


@pytest.mark.asyncio
async def test_diagnose_dlq_records_returns_1() -> None:
    store = InMemoryCheckpointStore()
    cp = _make_checkpoint("failing_pipeline")
    await store.save("failing_pipeline", cp)
    records = [_make_dlq_record("failing_pipeline")]

    with (
        patch("agora.cli.commands.diagnose._build_checkpoint_store", return_value=store),
        patch("agora.cli.commands.diagnose._load_dlq_records", return_value=records),
    ):
        args = _make_args("failing_pipeline")
        exit_code = await _run_diagnose(args)

    assert exit_code == 1


@pytest.mark.asyncio
async def test_diagnose_shows_latest_failure_details() -> None:
    """Smoke test — just verifies it runs without error when records present."""
    store = InMemoryCheckpointStore()
    cp = _make_checkpoint("p1")
    await store.save("p1", cp)
    records = [
        _make_dlq_record("p1", error_message="first error"),
        _make_dlq_record("p1", error_message="second error"),
    ]

    with (
        patch("agora.cli.commands.diagnose._build_checkpoint_store", return_value=store),
        patch("agora.cli.commands.diagnose._load_dlq_records", return_value=records),
    ):
        args = _make_args("p1")
        exit_code = await _run_diagnose(args)

    assert exit_code == 1


def test_diagnose_json_payload_exposes_failure_decision() -> None:
    record = DLQRecord(
        pipeline_id="p1",
        run_id="run-abc",
        stage="sink_write",
        error_type="PostgresError",
        error_message="duplicate key",
        record={"id": 1},
        details={
            "failure": {
                "classification": "constraint_violation",
                "retryable": False,
                "dlq_eligible": True,
                "alert_severity": "error",
                "reason": "UniqueViolation",
                "details": {},
            }
        },
    )

    payload = _dlq_payload("p1", [record], [record], record, None)

    assert payload["latest_failure"]["failure"] == record.details["failure"]


# ======================================================================
# Tests — degraded paths
# ======================================================================


@pytest.mark.asyncio
async def test_diagnose_checkpoint_read_error_degrades_gracefully() -> None:
    """Checkpoint store that raises must not crash diagnose."""
    from unittest.mock import AsyncMock, MagicMock

    bad_store = MagicMock()
    bad_store.load = AsyncMock(side_effect=RuntimeError("db locked"))
    bad_store.close = AsyncMock()

    with (
        patch("agora.cli.commands.diagnose._build_checkpoint_store", return_value=bad_store),
        patch("agora.cli.commands.diagnose._load_dlq_records", return_value=[]),
    ):
        args = _make_args("broken_pipe")
        exit_code = await _run_diagnose(args)

    # Should not raise — exit code 1 because of failure indicator
    assert exit_code == 1


@pytest.mark.asyncio
async def test_diagnose_dlq_read_error_degrades_gracefully() -> None:
    """DLQ that raises must not crash diagnose."""
    store = InMemoryCheckpointStore()
    cp = _make_checkpoint("p2")
    await store.save("p2", cp)

    with (
        patch("agora.cli.commands.diagnose._build_checkpoint_store", return_value=store),
        patch(
            "agora.cli.commands.diagnose._load_dlq_records",
            side_effect=RuntimeError("dlq unavailable"),
        ),
    ):
        args = _make_args("p2")
        exit_code = await _run_diagnose(args)

    # Should not raise — degraded output is acceptable
    assert exit_code in (0, 1)


@pytest.mark.asyncio
async def test_diagnose_missing_dlq_file_returns_empty() -> None:
    """_load_dlq_records should return [] when the DLQ file does not exist."""
    from agora.cli.commands.diagnose import _load_dlq_records

    records = await _load_dlq_records("any_pipeline", "/definitely/does/not/exist.db")
    assert records == []


# ======================================================================
# Tests — structured checkpoint value
# ======================================================================


@pytest.mark.asyncio
async def test_diagnose_structured_checkpoint_value() -> None:
    store = InMemoryCheckpointStore()
    cp = _make_checkpoint("structured_pipe", value={"batch_index": 7, "offset": 1400})
    await store.save("structured_pipe", cp)

    with (
        patch("agora.cli.commands.diagnose._build_checkpoint_store", return_value=store),
        patch("agora.cli.commands.diagnose._load_dlq_records", return_value=[]),
    ):
        args = _make_args("structured_pipe")
        exit_code = await _run_diagnose(args)

    assert exit_code == 0


@pytest.mark.asyncio
async def test_diagnose_none_checkpoint_value() -> None:
    store = InMemoryCheckpointStore()
    cp = _make_checkpoint("none_val_pipe", value=None)
    await store.save("none_val_pipe", cp)

    with (
        patch("agora.cli.commands.diagnose._build_checkpoint_store", return_value=store),
        patch("agora.cli.commands.diagnose._load_dlq_records", return_value=[]),
    ):
        args = _make_args("none_val_pipe")
        exit_code = await _run_diagnose(args)

    assert exit_code == 0


# ======================================================================
# Command class
# ======================================================================


def test_command_name_and_description() -> None:
    cmd = DiagnoseCommand()
    assert cmd.name == "diagnose"
    assert cmd.description


def test_command_setup_parser() -> None:
    cmd = DiagnoseCommand()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    sub = subparsers.add_parser("diagnose")
    cmd.setup_parser(sub)

    args = sub.parse_args(["my_pipeline"])
    assert args.pipeline_id == "my_pipeline"
    assert args.json is False


def test_command_setup_parser_with_options() -> None:
    cmd = DiagnoseCommand()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    sub = subparsers.add_parser("diagnose")
    cmd.setup_parser(sub)

    args = sub.parse_args(
        [
            "my_pipeline",
            "--checkpoint-store",
            "sqlite:/tmp/test.db",
            "--dlq-path",
            "/tmp/test_dlq.db",
        ]
    )
    assert args.pipeline_id == "my_pipeline"
    assert args.checkpoint_store == "sqlite:/tmp/test.db"
    assert args.dlq_path == "/tmp/test_dlq.db"


def test_command_setup_parser_accepts_json() -> None:
    cmd = DiagnoseCommand()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    sub = subparsers.add_parser("diagnose")
    cmd.setup_parser(sub)

    args = sub.parse_args(["my_pipeline", "--json"])
    assert args.json is True
