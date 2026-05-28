from __future__ import annotations

import json
import sqlite3
import sys
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from agora import InMemoryTracer, NoopTracer
from agora.cli.commands import run as run_command
from agora.cli.commands.base import CommandError
from agora.cli.commands.run import _build_and_run, _load_container_from_config
from agora.sinks import sink_registry

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.asyncio
async def test_load_container_from_toml_config_runs_iterable_pipeline(tmp_path: Path) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[pipelines.config.source]
type = "iterable"
records = [1, 2, 3]

[[pipelines.config.sinks]]
type = "stdout"
""".strip(),
        encoding="utf-8",
    )

    container = _load_container_from_config(str(config_path), pipeline_name="config")

    async with container:
        summary = await container.build_pipeline().run()

    assert summary.records_consumed == 3
    assert summary.records_written == 3


def test_load_container_from_config_applies_dlq_environment_overlay(tmp_path: Path) -> None:
    class _ProdDLQSink:
        sink_name = "prod_dlq_test"

        async def open(self) -> None:
            return None

        async def write(self, record) -> None:
            del record
            return

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink_registry.register_factory("prod_dlq_test", lambda **kwargs: _ProdDLQSink())

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

[environments.local.pipelines.orders.dlq]
enabled = true
failure_policy = "log_only"

[environments.local.pipelines.orders.dlq.sink]
type = "sqlite_dlq"
path = ".local_dlq.db"

[environments.prod.pipelines.orders.dlq]
enabled = true
failure_policy = "raise"

[environments.prod.pipelines.orders.dlq.sink]
type = "prod_dlq_test"
""".strip(),
        encoding="utf-8",
    )

    local_container = _load_container_from_config(str(config_path), pipeline_name="orders")
    local_resolved = run_command._load_resolved_pipeline_config(
        str(config_path),
        pipeline_name="orders",
        environment_name="local",
    )
    local_container = run_command._build_container_from_pipeline_config(
        str(config_path),
        local_resolved.pipeline_config,
    )
    prod_resolved = run_command._load_resolved_pipeline_config(
        str(config_path),
        pipeline_name="orders",
        environment_name="prod",
    )
    prod_container = run_command._build_container_from_pipeline_config(
        str(config_path),
        prod_resolved.pipeline_config,
    )

    local_pipeline = local_container.build_pipeline()
    prod_pipeline = prod_container.build_pipeline()

    assert local_pipeline._config.dlq.sink_name == "sqlite_dlq"
    assert local_pipeline._config.dlq_failure_policy == "log_only"
    assert prod_pipeline._config.dlq.sink_name == "prod_dlq_test"
    assert prod_pipeline._config.dlq_failure_policy == "raise"


def test_load_container_from_config_applies_tracing_environment_overlay(tmp_path: Path) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[defaults]
pipeline = "orders"

[pipelines.orders]
pipeline_id = "orders-pipeline"

[pipelines.orders.source]
type = "iterable"
records = [1]

[[pipelines.orders.sinks]]
type = "stdout"

[environments.local.pipelines.orders.tracing]
enabled = true
backend = "in_memory"

[environments.prod.pipelines.orders.tracing]
enabled = false
""".strip(),
        encoding="utf-8",
    )

    local_resolved = run_command._load_resolved_pipeline_config(
        str(config_path),
        pipeline_name="orders",
        environment_name="local",
    )
    local_container = run_command._build_container_from_pipeline_config(
        str(config_path),
        local_resolved.pipeline_config,
    )
    prod_resolved = run_command._load_resolved_pipeline_config(
        str(config_path),
        pipeline_name="orders",
        environment_name="prod",
    )
    prod_container = run_command._build_container_from_pipeline_config(
        str(config_path),
        prod_resolved.pipeline_config,
    )

    local_pipeline = local_container.build_pipeline()
    prod_pipeline = prod_container.build_pipeline()

    assert isinstance(local_pipeline._config.tracer, InMemoryTracer)
    assert isinstance(prod_pipeline._config.tracer, NoopTracer)


def test_load_container_from_config_defaults_to_builtin_sqlite_dlq(tmp_path: Path) -> None:
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
failure_policy = "log_only"
path = ".fallback_dlq.db"
""".strip(),
        encoding="utf-8",
    )

    container = _load_container_from_config(str(config_path))
    pipeline = container.build_pipeline()

    assert pipeline._config.dlq.sink_name == "sqlite_dlq"
    assert pipeline._config.dlq_failure_policy == "log_only"


@pytest.mark.asyncio
async def test_load_container_from_config_routes_failures_to_sqlite_dlq(
    tmp_path: Path,
) -> None:
    class _BoomSink:
        sink_name = "boom_sink_test"

        async def open(self) -> None:
            return None

        async def write(self, record) -> None:
            del record
            raise RuntimeError("sink exploded")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink_registry.register_factory("boom_sink_test", lambda **kwargs: _BoomSink())

    dlq_path = tmp_path / "events_dlq.db"
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        f"""
format = "agora/v1"

[defaults]
pipeline = "events"

[pipelines.events.source]
type = "iterable"
records = [{{ id = 1 }}]

[[pipelines.events.sinks]]
type = "boom_sink_test"

[pipelines.events.dlq]
enabled = true
failure_policy = "log_only"

[pipelines.events.dlq.sink]
type = "sqlite_dlq"
path = "{dlq_path}"
""".strip(),
        encoding="utf-8",
    )

    container = _load_container_from_config(str(config_path))
    async with container:
        summary = await container.build_pipeline().run()

    assert summary.records_errored == 1
    assert summary.records_written == 0

    conn = sqlite3.connect(dlq_path)
    try:
        row = conn.execute(
            "SELECT stage, error_type, error_message, record FROM dlq_records"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "sink_write"
    assert row[1] == "RuntimeError"
    assert row[2] == "sink exploded"
    assert json.loads(row[3]) == {"id": 1}


@pytest.mark.asyncio
async def test_load_container_from_config_routes_failures_to_implicit_sqlite_dlq(
    tmp_path: Path,
) -> None:
    class _BoomSink:
        sink_name = "boom_sink_test_implicit"

        async def open(self) -> None:
            return None

        async def write(self, record) -> None:
            del record
            raise RuntimeError("implicit sink exploded")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink_registry.register_factory("boom_sink_test_implicit", lambda **kwargs: _BoomSink())

    dlq_path = tmp_path / "implicit_events_dlq.db"
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        f"""
format = "agora/v1"

[defaults]
pipeline = "events"

[pipelines.events.source]
type = "iterable"
records = [{{ id = 1 }}]

[[pipelines.events.sinks]]
type = "boom_sink_test_implicit"

[pipelines.events.dlq]
enabled = true
path = "{dlq_path}"
""".strip(),
        encoding="utf-8",
    )

    container = _load_container_from_config(str(config_path))
    async with container:
        summary = await container.build_pipeline().run()

    assert summary.records_errored == 1
    conn = sqlite3.connect(dlq_path)
    try:
        row = conn.execute(
            "SELECT stage, error_type, error_message, record FROM dlq_records"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "sink_write"
    assert row[1] == "RuntimeError"
    assert row[2] == "implicit sink exploded"
    assert json.loads(row[3]) == {"id": 1}


@pytest.mark.asyncio
async def test_load_container_selects_default_pipeline_and_applies_overlays(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "pipelines.toml"
    config_path.write_text(
        """
format = "agora/v1"

[defaults]
pipeline = "beta"
profile = "batch"
environment = "prod"

[pipelines.alpha.source]
type = "iterable"
records = [1]

[[pipelines.alpha.sinks]]
type = "stdout"

[pipelines.beta.source]
type = "iterable"
records = [1]
batch_size = 1

[[pipelines.beta.sinks]]
type = "stdout"

[profiles.batch.pipelines.beta.source]
batch_size = 50

[environments.prod.pipelines.beta.source]
records = [1, 2, 3, 4]
""".strip(),
        encoding="utf-8",
    )

    container = _load_container_from_config(str(config_path))

    async with container:
        pipeline = container.build_pipeline()
        summary = await pipeline.run()

    assert pipeline.pipeline_id == "beta"
    assert summary.records_consumed == 4
    assert summary.records_written == 4


def test_load_container_requires_pipeline_name_when_no_default_and_many_pipelines(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "pipelines.toml"
    config_path.write_text(
        """
format = "agora/v1"

[pipelines.alpha.source]
type = "iterable"
records = [1]

[[pipelines.alpha.sinks]]
type = "stdout"

[pipelines.beta.source]
type = "iterable"
records = [2]

[[pipelines.beta.sinks]]
type = "stdout"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        CommandError,
        match=r"Config defines multiple pipelines\. Select one: alpha, beta",
    ):
        _load_container_from_config(str(config_path))


def test_load_container_from_config_requires_at_least_one_sink(tmp_path: Path) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[defaults]
pipeline = "missing-sink"

[pipelines.missing-sink.source]
type = "iterable"
records = [1]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        CommandError,
        match=r"Invalid pipeline config in '.*': Invalid pipeline:\n  - sinks: At least one sink must be defined\.",
    ):
        _load_container_from_config(str(config_path))


@pytest.mark.asyncio
async def test_load_container_from_config_resolves_import_refs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("fake_run_config_module")
    module.RECORDS = [{"name": "alice"}, {"name": "bob"}]

    async def uppercase(record, ctx):
        del ctx
        return {"name": record["name"].upper()}

    module.uppercase = uppercase
    monkeypatch.setitem(sys.modules, "fake_run_config_module", module)

    output_path = tmp_path / "output.jsonl"
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        f"""
format = "agora/v1"

[defaults]
pipeline = "imported"

[pipelines.imported.source]
type = "iterable"
records = {{ import = "fake_run_config_module:RECORDS" }}

[[pipelines.imported.middlewares]]
type = "enrich"
enricher = {{ import = "fake_run_config_module:uppercase" }}

[[pipelines.imported.sinks]]
type = "jsonl"
path = "{output_path}"
""".strip(),
        encoding="utf-8",
    )

    container = _load_container_from_config(str(config_path))

    async with container:
        summary = await container.build_pipeline().run()

    assert summary.records_consumed == 2
    assert summary.records_written == 2
    assert [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()] == [
        {"name": "ALICE"},
        {"name": "BOB"},
    ]


@pytest.mark.asyncio
async def test_build_and_run_supports_config_without_pipeline_argument(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[defaults]
pipeline = "config-only"

[pipelines.config-only]
pipeline_id = "config-only"

[pipelines.config-only.source]
type = "iterable"
records = [1, 2, 3]

[[pipelines.config-only.sinks]]
type = "stdout"
""".strip(),
        encoding="utf-8",
    )

    seen: dict[str, str] = {}

    async def fake_run_pipeline(pipeline, args):
        del args
        seen["pipeline_id"] = pipeline.pipeline_id

    monkeypatch.setattr(run_command, "_run_pipeline", fake_run_pipeline)

    await _build_and_run(
        SimpleNamespace(
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            max_records=None,
            run_id=None,
            dry_run=False,
            plan=False,
        )
    )

    assert seen == {"pipeline_id": "config-only"}


@pytest.mark.asyncio
async def test_build_and_run_plan_prints_resolved_pipeline_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[defaults]
pipeline = "planned"
profile = "batch"
environment = "prod"

[profiles.batch.pipelines.planned.source]
records = [1]

[environments.prod.pipelines.planned.source]
records = [1]

[pipelines.planned]
pipeline_id = "planned-id"

[pipelines.planned.source]
type = "iterable"
records = [1]

[[pipelines.planned.middlewares]]
type = "enrich"
field = "city"
value = "HCM"

[pipelines.planned.dedup]
key = "id"

[[pipelines.planned.sinks]]
type = "stdout"
""".strip(),
        encoding="utf-8",
    )

    printed: list[tuple[str, tuple[str, ...]]] = []

    monkeypatch.setattr(
        run_command.console,
        "section",
        lambda title: printed.append(("section", (title,))),
    )
    monkeypatch.setattr(
        run_command.console,
        "item",
        lambda *columns: printed.append(("item", columns)),
    )
    monkeypatch.setattr(run_command.console, "blank", lambda: printed.append(("blank", ())))

    async def fail_run_pipeline(pipeline, args):
        del pipeline, args
        raise AssertionError("plan mode should not execute the pipeline")

    monkeypatch.setattr(run_command, "_run_pipeline", fail_run_pipeline)

    await _build_and_run(
        SimpleNamespace(
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            max_records=None,
            run_id=None,
            dry_run=False,
            plan=True,
        )
    )

    assert ("section", ("Pipeline Plan",)) in printed
    assert ("item", ("pipeline", "planned")) in printed
    assert ("item", ("pipeline_id", "planned-id")) in printed
    assert ("item", ("profile", "batch")) in printed
    assert ("item", ("environment", "prod")) in printed
    assert ("item", ("source", "iterable")) in printed
    assert ("item", ("middlewares", "enrich")) in printed
    assert ("item", ("dedup", "key=id")) in printed
    assert ("item", ("dlq", "disabled")) in printed
    assert ("item", ("tracing", "disabled")) in printed
    assert ("item", ("sinks", "stdout")) in printed


@pytest.mark.asyncio
async def test_build_and_run_plan_prints_dlq_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[defaults]
pipeline = "planned"
environment = "local"

[pipelines.planned.source]
type = "iterable"
records = [1]

[[pipelines.planned.sinks]]
type = "stdout"

[environments.local.pipelines.planned.dlq]
enabled = true
failure_policy = "raise"

[environments.local.pipelines.planned.dlq.sink]
type = "sqlite_dlq"
path = ".agora_dlq.db"
""".strip(),
        encoding="utf-8",
    )

    printed: list[tuple[str, tuple[str, ...]]] = []

    monkeypatch.setattr(
        run_command.console,
        "section",
        lambda title: printed.append(("section", (title,))),
    )
    monkeypatch.setattr(
        run_command.console,
        "item",
        lambda *columns: printed.append(("item", columns)),
    )
    monkeypatch.setattr(run_command.console, "blank", lambda: printed.append(("blank", ())))

    await _build_and_run(
        SimpleNamespace(
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            max_records=None,
            run_id=None,
            dry_run=False,
            plan=True,
        )
    )

    assert ("item", ("dlq", "sink=sqlite_dlq, failure_policy=raise")) in printed
    assert ("item", ("tracing", "disabled")) in printed


@pytest.mark.asyncio
async def test_build_and_run_plan_prints_implicit_builtin_dlq_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[defaults]
pipeline = "planned"

[pipelines.planned.source]
type = "iterable"
records = [1]

[[pipelines.planned.sinks]]
type = "stdout"

[pipelines.planned.dlq]
enabled = true
""".strip(),
        encoding="utf-8",
    )

    printed: list[tuple[str, tuple[str, ...]]] = []

    monkeypatch.setattr(
        run_command.console,
        "section",
        lambda title: printed.append(("section", (title,))),
    )
    monkeypatch.setattr(
        run_command.console,
        "item",
        lambda *columns: printed.append(("item", columns)),
    )
    monkeypatch.setattr(run_command.console, "blank", lambda: printed.append(("blank", ())))

    await _build_and_run(
        SimpleNamespace(
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            max_records=None,
            run_id=None,
            dry_run=False,
            plan=True,
        )
    )

    assert ("item", ("dlq", "sink=sqlite_dlq (implicit), failure_policy=log_only")) in printed
    assert ("item", ("tracing", "disabled")) in printed


@pytest.mark.asyncio
async def test_build_and_run_plan_prints_tracing_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[defaults]
pipeline = "planned"

[pipelines.planned]
pipeline_id = "planned-traced"

[pipelines.planned.source]
type = "iterable"
records = [1]

[pipelines.planned.tracing]
enabled = true
backend = "in_memory"
service_name = "etl-tracing"

[[pipelines.planned.sinks]]
type = "stdout"
""".strip(),
        encoding="utf-8",
    )

    printed: list[tuple[str, tuple[str, ...]]] = []

    monkeypatch.setattr(
        run_command.console,
        "section",
        lambda title: printed.append(("section", (title,))),
    )
    monkeypatch.setattr(
        run_command.console,
        "item",
        lambda *columns: printed.append(("item", columns)),
    )
    monkeypatch.setattr(run_command.console, "blank", lambda: printed.append(("blank", ())))

    await _build_and_run(
        SimpleNamespace(
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            max_records=None,
            run_id=None,
            dry_run=False,
            plan=True,
        )
    )

    assert ("item", ("tracing", "backend=in_memory, service_name=etl-tracing")) in printed


@pytest.mark.asyncio
async def test_build_and_run_plan_warns_when_config_uses_import_refs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[defaults]
pipeline = "planned"

[pipelines.planned.source]
type = "iterable"
records = { import = "fake.module:RECORDS" }

[[pipelines.planned.middlewares]]
type = "enrich"
enricher = { import = "fake.module:uppercase" }

[[pipelines.planned.sinks]]
type = "stdout"
""".strip(),
        encoding="utf-8",
    )

    printed: list[tuple[str, tuple[str, ...]]] = []
    warnings: list[str] = []

    monkeypatch.setattr(
        run_command.console,
        "section",
        lambda title: printed.append(("section", (title,))),
    )
    monkeypatch.setattr(
        run_command.console,
        "item",
        lambda *columns: printed.append(("item", columns)),
    )
    monkeypatch.setattr(run_command.console, "blank", lambda: printed.append(("blank", ())))
    monkeypatch.setattr(run_command.console, "warn", warnings.append)

    await _build_and_run(
        SimpleNamespace(
            pipeline=None,
            config=str(config_path),
            profile=None,
            environment=None,
            max_records=None,
            run_id=None,
            dry_run=False,
            plan=True,
        )
    )

    assert warnings == [
        (
            f"Config '{config_path}' resolves 2 trusted Python import reference(s). "
            "Review declarative configs like code: Agora imports these objects after "
            "prepending your project root and src/ to sys.path."
        )
    ]
    assert (
        "item",
        (
            "imports",
            ("source.records=fake.module:RECORDS, middlewares.0.enricher=fake.module:uppercase"),
        ),
    ) in printed
