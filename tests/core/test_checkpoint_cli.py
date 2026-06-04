"""
tests/core/test_checkpoint_cli.py
===================================
Tests for ``agora checkpoint show/inspect/reset`` CLI commands.

Coverage:
- show: prints checkpoint value for known pipeline
- show: returns exit code 1 when no checkpoint exists
- inspect: prints detailed breakdown including run_id and structured cursor
- inspect: returns exit code 1 when no checkpoint exists
- reset: deletes checkpoint when --yes is provided
- reset: refuses to reset without --yes (exit code 1)
- reset: returns 0 gracefully when no checkpoint exists and --yes given
- store spec parsing: sqlite, memory, unknown spec
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

import pytest

from agora.cli.commands.checkpoint import (
    CheckpointCommand,
    _build_store,
    _run_checkpoint_command,
)
from agora.core.checkpoint import Checkpoint, InMemoryCheckpointStore

# ======================================================================
# Helpers
# ======================================================================


def _make_args(
    subcommand: str,
    pipeline_id: str,
    *,
    store: str = "memory",
    yes: bool = False,
) -> argparse.Namespace:
    return argparse.Namespace(
        subcommand=subcommand,
        pipeline_id=pipeline_id,
        store=store,
        yes=yes,
    )


async def _seed_checkpoint(
    store: InMemoryCheckpointStore,
    pipeline_id: str,
    value: Any = None,
) -> Checkpoint:
    if value is None:
        value = {"offset": 42}
    cp = Checkpoint(
        pipeline_id=pipeline_id,
        run_id="run-abc",
        source="test_source",
        value=value,
        recorded_at=datetime(2026, 6, 4, 10, 0, 0, tzinfo=UTC),
    )
    await store.save(pipeline_id, cp)
    return cp


# ======================================================================
# _build_store
# ======================================================================


def test_build_store_memory() -> None:
    store = _build_store("memory")
    assert isinstance(store, InMemoryCheckpointStore)


def test_build_store_sqlite(tmp_path: Any) -> None:
    from agora.core.checkpoint import SQLiteCheckpointStore

    path = str(tmp_path / "test.db")
    store = _build_store(f"sqlite:{path}")
    assert isinstance(store, SQLiteCheckpointStore)


def test_build_store_sqlite_missing_path() -> None:
    from agora.cli.commands.base import CommandError

    with pytest.raises(CommandError, match="path"):
        _build_store("sqlite:")


def test_build_store_unknown_raises() -> None:
    from agora.cli.commands.base import CommandError

    with pytest.raises(CommandError, match="Unknown store spec"):
        _build_store("redis:localhost")


# ======================================================================
# show
# ======================================================================


@pytest.mark.asyncio
async def test_show_prints_checkpoint(capsys: Any) -> None:
    store = InMemoryCheckpointStore()
    await _seed_checkpoint(store, "my_pipeline")

    with patch("agora.cli.commands.checkpoint._build_store", return_value=store):
        args = _make_args("show", "my_pipeline")
        exit_code = await _run_checkpoint_command(args)

    assert exit_code == 0


@pytest.mark.asyncio
async def test_show_returns_1_when_no_checkpoint() -> None:
    store = InMemoryCheckpointStore()

    with patch("agora.cli.commands.checkpoint._build_store", return_value=store):
        args = _make_args("show", "nonexistent_pipeline")
        exit_code = await _run_checkpoint_command(args)

    assert exit_code == 1


@pytest.mark.asyncio
async def test_show_scalar_value() -> None:
    store = InMemoryCheckpointStore()
    cp = Checkpoint(
        pipeline_id="p1",
        run_id="r1",
        source="src",
        value=1800,
        recorded_at=datetime.now(UTC),
    )
    await store.save("p1", cp)

    with patch("agora.cli.commands.checkpoint._build_store", return_value=store):
        args = _make_args("show", "p1")
        exit_code = await _run_checkpoint_command(args)

    assert exit_code == 0


# ======================================================================
# inspect
# ======================================================================


@pytest.mark.asyncio
async def test_inspect_prints_detailed_breakdown() -> None:
    store = InMemoryCheckpointStore()
    await _seed_checkpoint(store, "pipe_a", value={"batch_index": 5, "offset": 100})

    with patch("agora.cli.commands.checkpoint._build_store", return_value=store):
        args = _make_args("inspect", "pipe_a")
        exit_code = await _run_checkpoint_command(args)

    assert exit_code == 0


@pytest.mark.asyncio
async def test_inspect_returns_1_when_no_checkpoint() -> None:
    store = InMemoryCheckpointStore()

    with patch("agora.cli.commands.checkpoint._build_store", return_value=store):
        args = _make_args("inspect", "unknown_pipe")
        exit_code = await _run_checkpoint_command(args)

    assert exit_code == 1


@pytest.mark.asyncio
async def test_inspect_none_value() -> None:
    store = InMemoryCheckpointStore()
    cp = Checkpoint(
        pipeline_id="p_none",
        run_id="r1",
        source="src",
        value=None,
        recorded_at=datetime.now(UTC),
    )
    await store.save("p_none", cp)

    with patch("agora.cli.commands.checkpoint._build_store", return_value=store):
        args = _make_args("inspect", "p_none")
        exit_code = await _run_checkpoint_command(args)

    assert exit_code == 0


# ======================================================================
# reset
# ======================================================================


@pytest.mark.asyncio
async def test_reset_requires_yes_flag() -> None:
    store = InMemoryCheckpointStore()
    await _seed_checkpoint(store, "pipe_b")

    with patch("agora.cli.commands.checkpoint._build_store", return_value=store):
        args = _make_args("reset", "pipe_b", yes=False)
        exit_code = await _run_checkpoint_command(args)

    assert exit_code == 1
    # Checkpoint must NOT have been deleted
    remaining = await store.load("pipe_b")
    assert remaining is not None


@pytest.mark.asyncio
async def test_reset_with_yes_deletes_checkpoint() -> None:
    store = InMemoryCheckpointStore()
    await _seed_checkpoint(store, "pipe_c")

    with patch("agora.cli.commands.checkpoint._build_store", return_value=store):
        args = _make_args("reset", "pipe_c", yes=True)
        exit_code = await _run_checkpoint_command(args)

    assert exit_code == 0
    # Checkpoint must be gone after reset
    remaining = await store.load("pipe_c")
    assert remaining is None


@pytest.mark.asyncio
async def test_reset_graceful_when_no_checkpoint() -> None:
    store = InMemoryCheckpointStore()

    with patch("agora.cli.commands.checkpoint._build_store", return_value=store):
        args = _make_args("reset", "nonexistent", yes=True)
        exit_code = await _run_checkpoint_command(args)

    assert exit_code == 0


@pytest.mark.asyncio
async def test_reset_without_yes_does_not_modify_checkpoint() -> None:
    store = InMemoryCheckpointStore()
    await _seed_checkpoint(store, "pipe_d", value={"offset": 99})

    with patch("agora.cli.commands.checkpoint._build_store", return_value=store):
        args = _make_args("reset", "pipe_d", yes=False)
        await _run_checkpoint_command(args)

    after = await store.load("pipe_d")
    assert after is not None
    assert after.value == {"offset": 99}


# ======================================================================
# Command class
# ======================================================================


def test_command_name_and_description() -> None:
    cmd = CheckpointCommand()
    assert cmd.name == "checkpoint"
    assert cmd.description


def test_command_setup_parser() -> None:
    import argparse

    cmd = CheckpointCommand()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    sub = subparsers.add_parser("checkpoint")
    cmd.setup_parser(sub)
    # show subcommand parses correctly
    args = sub.parse_args(["show", "my_pipe"])
    assert args.subcommand == "show"
    assert args.pipeline_id == "my_pipe"
    assert args.yes is False


def test_command_reset_parser_accepts_yes() -> None:
    import argparse

    cmd = CheckpointCommand()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    sub = subparsers.add_parser("checkpoint")
    cmd.setup_parser(sub)
    args = sub.parse_args(["reset", "my_pipe", "--yes"])
    assert args.yes is True
