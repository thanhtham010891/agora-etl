from __future__ import annotations

from worker import get_worker

from agora.runner import Schedule


def test_worker_registers_only_long_lived_consumers() -> None:
    worker = get_worker()
    pipelines = {pipeline.pipeline_id: pipeline for pipeline in worker.registered_pipelines()}

    assert set(pipelines) == {"orders_normalize", "orders_projection"}
    assert pipelines["orders_normalize"].max_records is None
    assert pipelines["orders_normalize"].schedule == Schedule.continuous()
    assert pipelines["orders_projection"].max_records is None
    assert pipelines["orders_projection"].schedule == Schedule.continuous()
