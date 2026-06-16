from __future__ import annotations

import json
import sqlite3
import sys
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from agora.cli.commands.base import CommandError
from agora.cli.commands.dlq import _build_dlq_source_config, _run_dlq_command
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


def test_build_dlq_source_config_maps_kafka_dlq_fields() -> None:
    source_cfg = _build_dlq_source_config(
        {
            "pipeline_id": "orders-etl",
            "dlq": {
                "sink": {
                    "type": "kafka_dlq",
                    "bootstrap_servers": "127.0.0.1:19092",
                    "topic": "orders.dlq",
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "PLAIN",
                    "sasl_username": "svc",
                    "sasl_password": "secret",
                    "ssl_cafile": "/tmp/ca.pem",
                    "ssl_check_hostname": False,
                }
            },
        },
        stage="middleware",
        limit=25,
    )

    assert source_cfg == {
        "type": "kafka_dlq_source",
        "pipeline_id": "orders-etl",
        "bootstrap_servers": "127.0.0.1:19092",
        "topic": "orders.dlq",
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_username": "svc",
        "sasl_password": "secret",
        "ssl_cafile": "/tmp/ca.pem",
        "ssl_check_hostname": False,
        "stage": "middleware",
        "limit": 25,
    }


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


@pytest.mark.asyncio
async def test_dlq_replay_failure_keeps_record_and_increments_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("fake_dlq_retry_module")

    async def passthrough(record, ctx):
        del ctx
        return record

    module.passthrough = passthrough
    monkeypatch.setitem(sys.modules, "fake_dlq_retry_module", module)

    class _AlwaysFailSink:
        sink_name = "always_fail_replay_sink"

        async def open(self) -> None:
            return None

        async def write(self, record) -> None:
            del record
            raise RuntimeError("sink exploded")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink_registry.register_factory("always_fail_replay_sink", lambda **kwargs: _AlwaysFailSink())

    dlq_path = tmp_path / "replay_retry_dlq.db"
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
type = "always_fail_replay_sink"

[[pipelines.orders.middlewares]]
type = "enrich"
enricher = {{ import = "fake_dlq_retry_module:passthrough" }}

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

    exit_code = await _run_dlq_command(
        SimpleNamespace(
            subcommand="replay",
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            stage=None,
            mode="pipeline",
            limit=None,
            run_id=None,
        )
    )

    assert exit_code == 1

    conn = sqlite3.connect(dlq_path)
    try:
        row = conn.execute("SELECT attempt, COUNT(*) FROM dlq_records").fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == 1
    assert row[1] == 1


@pytest.mark.asyncio
async def test_dlq_replay_sink_mode_skip_does_not_increment_attempt_for_non_sink_stage(
    tmp_path: Path,
) -> None:
    dlq_path = tmp_path / "skip_non_sink_stage_dlq.db"
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        f"""
format = "agora/v1"

[defaults]
pipeline = "orders"

[pipelines.orders.source]
type = "iterable"
records = [1]

[[pipelines.orders.middlewares]]
type = "validate"
validator = {{ import = "builtins:len" }}
on_error = "raise"

[[pipelines.orders.sinks]]
type = "stdout"

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

    exit_code = await _run_dlq_command(
        SimpleNamespace(
            subcommand="replay",
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            stage=None,
            mode="sink",
            limit=None,
            run_id=None,
        )
    )

    assert exit_code == 1

    conn = sqlite3.connect(dlq_path)
    try:
        row = conn.execute("SELECT stage, attempt, COUNT(*) FROM dlq_records").fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "middleware"
    assert row[1] == 0
    assert row[2] == 1


@pytest.mark.asyncio
async def test_dlq_replay_dropped_record_is_not_acknowledged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"drop": False}
    writes: list[dict[str, object]] = []
    module = ModuleType("fake_dlq_drop_replay_module")

    def normalize_or_drop(record):
        if state["drop"]:
            return None
        return {
            **record,
            "history": [*record.get("history", []), "normalized"],
        }

    module.normalize_or_drop = normalize_or_drop
    monkeypatch.setitem(sys.modules, "fake_dlq_drop_replay_module", module)

    class _ToggleSink:
        sink_name = "drop_replay_sink"

        async def open(self) -> None:
            return None

        async def write(self, record) -> None:
            if not state["drop"]:
                raise RuntimeError("sink exploded")
            writes.append(record)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink_registry.register_factory("drop_replay_sink", lambda **kwargs: _ToggleSink())

    dlq_path = tmp_path / "drop_replay_dlq.db"
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
type = "drop_replay_sink"

[[pipelines.orders.middlewares]]
type = "validate"
validator = {{ import = "fake_dlq_drop_replay_module:normalize_or_drop" }}

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

    state["drop"] = True
    exit_code = await _run_dlq_command(
        SimpleNamespace(
            subcommand="replay",
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            stage=None,
            mode="pipeline",
            limit=None,
            run_id=None,
        )
    )

    assert exit_code == 1
    assert writes == []

    conn = sqlite3.connect(dlq_path)
    try:
        row = conn.execute("SELECT attempt, COUNT(*) FROM dlq_records").fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == 1
    assert row[1] == 1


@pytest.mark.asyncio
async def test_dlq_replay_metadata_update_failure_returns_nonzero_and_keeps_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("fake_dlq_replay_failure_module")

    async def passthrough(record, ctx):
        del ctx
        return record

    module.passthrough = passthrough
    monkeypatch.setitem(sys.modules, "fake_dlq_replay_failure_module", module)

    class _ToggleSink:
        sink_name = "replay_failure_sink"

        async def open(self) -> None:
            return None

        async def write(self, record) -> None:
            del record
            raise RuntimeError("sink exploded")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink_registry.register_factory("replay_failure_sink", lambda **kwargs: _ToggleSink())

    dlq_path = tmp_path / "replay_failure_dlq.db"
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
type = "replay_failure_sink"

[[pipelines.orders.middlewares]]
type = "enrich"
enricher = {{ import = "fake_dlq_replay_failure_module:passthrough" }}

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

    async def _broken_replay(self, record):
        del self, record
        raise RuntimeError("replay metadata broke")

    monkeypatch.setattr("agora.core.dlq.SQLiteDLQSink.replay", _broken_replay)

    exit_code = await _run_dlq_command(
        SimpleNamespace(
            subcommand="replay",
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            stage=None,
            mode="pipeline",
            limit=None,
            run_id=None,
        )
    )

    assert exit_code == 1

    conn = sqlite3.connect(dlq_path)
    try:
        row = conn.execute("SELECT attempt, COUNT(*) FROM dlq_records").fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == 0
    assert row[1] == 1


@pytest.mark.asyncio
async def test_dlq_replay_acknowledge_failure_returns_nonzero_and_keeps_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writes: list[dict[str, object]] = []
    state = {"fail": True}

    module = ModuleType("fake_dlq_ack_failure_module")

    async def append_history(record, ctx):
        del ctx
        return {
            **record,
            "history": [*record.get("history", []), "normalized"],
        }

    module.append_history = append_history
    monkeypatch.setitem(sys.modules, "fake_dlq_ack_failure_module", module)

    class _ToggleSink:
        sink_name = "ack_failure_sink"

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

    sink_registry.register_factory("ack_failure_sink", lambda **kwargs: _ToggleSink())

    dlq_path = tmp_path / "ack_failure_dlq.db"
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
type = "ack_failure_sink"

[[pipelines.orders.middlewares]]
type = "enrich"
enricher = {{ import = "fake_dlq_ack_failure_module:append_history" }}

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

    async def _broken_ack(self, record):
        del self, record
        raise RuntimeError("ack broke")

    monkeypatch.setattr("agora.core.dlq.SQLiteDLQSink.acknowledge", _broken_ack)

    exit_code = await _run_dlq_command(
        SimpleNamespace(
            subcommand="replay",
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            stage=None,
            mode="pipeline",
            limit=None,
            run_id=None,
        )
    )

    assert exit_code == 1
    assert writes == [{"id": 1, "history": ["normalized"]}]

    conn = sqlite3.connect(dlq_path)
    try:
        row = conn.execute("SELECT attempt, COUNT(*) FROM dlq_records").fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == 1
    assert row[1] == 1
