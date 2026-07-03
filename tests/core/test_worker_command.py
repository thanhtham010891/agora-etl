from __future__ import annotations

import pytest

from agora import InMemoryTracer
from agora.cli.commands.worker import _load_worker_from_config


def test_load_worker_from_config_builds_registered_pipelines(tmp_path) -> None:
    config_path = tmp_path / "worker.toml"
    config_path.write_text(
        """
format = "agora/v1"

[worker]
health_port = 18080

[pipelines.orders]
pipeline_id = "orders-sync"

[pipelines.orders.source]
type = "iterable"
records = [1, 2, 3]

[pipelines.orders.schedule]
mode = "every"
minutes = 15

[pipelines.orders.tracing]
enabled = true
backend = "in_memory"

[[pipelines.orders.sinks]]
type = "stdout"

[pipelines.audit.source]
type = "iterable"
records = [4]

[pipelines.audit.schedule]
mode = "once"

[[pipelines.audit.sinks]]
type = "stdout"
""".strip(),
        encoding="utf-8",
    )

    pool = _load_worker_from_config(str(config_path))

    assert pool.health_port == 18080
    pipelines = pool.registered_pipelines()
    assert [pipeline.pipeline_id for pipeline in pipelines] == ["orders-sync", "audit"]
    assert str(pipelines[0].schedule) in {"every 900s", "every 900.0s"}
    assert str(pipelines[1].schedule) == "once"


@pytest.mark.asyncio
async def test_load_worker_from_config_uses_existing_tracing_path(tmp_path) -> None:
    config_path = tmp_path / "worker.toml"
    config_path.write_text(
        """
format = "agora/v1"

[pipelines.orders.source]
type = "iterable"
records = [1]

[pipelines.orders.schedule]
mode = "once"

[pipelines.orders.tracing]
enabled = true
backend = "in_memory"

[[pipelines.orders.sinks]]
type = "stdout"
""".strip(),
        encoding="utf-8",
    )

    pool = _load_worker_from_config(str(config_path))
    scheduled = pool.registered_pipelines()[0]
    pipeline = await scheduled.build()

    assert isinstance(pipeline.config.tracer, InMemoryTracer)
