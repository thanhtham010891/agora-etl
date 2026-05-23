from __future__ import annotations

import json
import sqlite3
import sys
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from agora.cli.commands.base import CommandError
from agora.cli.commands.dlq import _run_dlq_command
from agora.cli.commands.run import _load_container_from_config
from agora.sinks import sink_registry

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.asyncio
async def test_dlq_replay_replays_and_acknowledges_sqlite_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writes: list[dict[str, object]] = []
    state = {"fail": True}

    module = ModuleType("fake_dlq_replay_module")

    async def append_history(record, ctx):
        del ctx
        return {
            **record,
            "history": [*record.get("history", []), "normalized"],
        }

    module.append_history = append_history
    monkeypatch.setitem(sys.modules, "fake_dlq_replay_module", module)

    class _ToggleSink:
        sink_name = "toggle_replay_sink"

        async def open(self) -> None:
            return None

        async def write(self, record) -> None:
            if state["fail"]:
                raise RuntimeError("sink exploded")
            writes.append(record)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink_registry.register_factory("toggle_replay_sink", lambda **kwargs: _ToggleSink())

    dlq_path = tmp_path / "replay_dlq.db"
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        f"""
format = "agora/v1"

[defaults]
pipeline = "orders"

[pipelines.orders]
pipeline_id = "orders-etl"

[pipelines.orders.source]
type = "iterable"
records = [{{ id = 1 }}]

[[pipelines.orders.sinks]]
type = "toggle_replay_sink"

[[pipelines.orders.middlewares]]
type = "enrich"
enricher = {{ import = "fake_dlq_replay_module:append_history" }}

[pipelines.orders.dlq]
enabled = true

[pipelines.orders.dlq.sink]
type = "sqlite_dlq"
path = "{dlq_path}"
""".strip(),
        encoding="utf-8",
    )

    container = _load_container_from_config(str(config_path))
    async with container:
        summary = await container.build_pipeline().run(run_id="run-1")

    assert summary.records_errored == 1

    conn = sqlite3.connect(dlq_path)
    try:
        row = conn.execute("SELECT original_record, processed_record FROM dlq_records").fetchone()
    finally:
        conn.close()
    assert row is not None
    assert json.loads(row[0]) == {"id": 1}
    assert json.loads(row[1]) == {"id": 1, "history": ["normalized"]}

    state["fail"] = False
    exit_code = await _run_dlq_command(
        SimpleNamespace(
            subcommand="replay",
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            stage=None,
            limit=None,
            run_id=None,
        )
    )

    assert exit_code == 0
    assert writes == [{"id": 1, "history": ["normalized"]}]

    conn = sqlite3.connect(dlq_path)
    try:
        remaining = conn.execute("SELECT COUNT(*) FROM dlq_records").fetchone()[0]
    finally:
        conn.close()
    assert remaining == 0


@pytest.mark.asyncio
async def test_dlq_replay_sink_mode_replays_processed_payload_without_rerunning_middlewares(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writes: list[dict[str, object]] = []
    state = {"fail": True}

    module = ModuleType("fake_dlq_sink_replay_module")

    async def append_history(record, ctx):
        del ctx
        return {
            **record,
            "history": [*record.get("history", []), "normalized"],
        }

    module.append_history = append_history
    monkeypatch.setitem(sys.modules, "fake_dlq_sink_replay_module", module)

    class _ToggleSink:
        sink_name = "toggle_sink_replay_sink"

        async def open(self) -> None:
            return None

        async def write(self, record) -> None:
            if state["fail"]:
                raise RuntimeError("sink exploded")
            writes.append(record)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink_registry.register_factory("toggle_sink_replay_sink", lambda **kwargs: _ToggleSink())

    dlq_path = tmp_path / "sink_replay_dlq.db"
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        f"""
format = "agora/v1"

[defaults]
pipeline = "orders"

[pipelines.orders]
pipeline_id = "orders-etl"

[pipelines.orders.source]
type = "iterable"
records = [{{ id = 1 }}]

[[pipelines.orders.sinks]]
type = "toggle_sink_replay_sink"

[[pipelines.orders.middlewares]]
type = "enrich"
enricher = {{ import = "fake_dlq_sink_replay_module:append_history" }}

[pipelines.orders.dlq]
enabled = true

[pipelines.orders.dlq.sink]
type = "sqlite_dlq"
path = "{dlq_path}"
""".strip(),
        encoding="utf-8",
    )

    container = _load_container_from_config(str(config_path))
    async with container:
        summary = await container.build_pipeline().run(run_id="run-1")

    assert summary.records_errored == 1

    state["fail"] = False
    exit_code = await _run_dlq_command(
        SimpleNamespace(
            subcommand="replay",
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            stage="sink_write",
            mode="sink",
            limit=None,
            run_id=None,
        )
    )

    assert exit_code == 0
    assert writes == [{"id": 1, "history": ["normalized"]}]


@pytest.mark.asyncio
async def test_dlq_replay_sink_mode_rejects_non_sink_stage(tmp_path: Path) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[defaults]
pipeline = "orders"

[pipelines.orders.source]
type = "iterable"
records = [1]

[[pipelines.orders.sinks]]
type = "stdout"

[pipelines.orders.dlq]
enabled = true
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        CommandError, match="mode sink only supports DLQ records from stage 'sink_write'"
    ):
        await _run_dlq_command(
            SimpleNamespace(
                subcommand="replay",
                pipeline=None,
                config=str(config_path),
                profile=None,
                environment=None,
                stage="middleware",
                mode="sink",
                limit=None,
                run_id=None,
            )
        )
