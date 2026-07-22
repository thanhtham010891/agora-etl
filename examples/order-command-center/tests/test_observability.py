import json
from types import SimpleNamespace

import pytest
from order_command_center import runtime

from agora.metrics import MetricsCollector


@pytest.mark.asyncio
async def test_projection_runtime_uses_one_native_worker_pool_and_collector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[object] = []

    class FakeWorkerPool:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.pipeline: object | None = None
            created.append(self)

        def register(self, pipeline: object) -> None:
            self.pipeline = pipeline

        async def run(self) -> None:
            assert self.pipeline is not None
            for observer in self.pipeline.observers:
                await observer(
                    SimpleNamespace(
                        run_number=1,
                        error=None,
                        summary=SimpleNamespace(
                            records_consumed=3,
                            records_written=3,
                            records_errored=0,
                            elapsed_seconds=0.1,
                        ),
                    )
                )

    monkeypatch.setattr(runtime, "WorkerPool", FakeWorkerPool)

    async def build_pipeline() -> object:
        return object()

    delivered = await runtime.ProjectionRuntime(
        runtime.ProjectionSpec(
            pipeline_id="test-projection",
            process_name="test-worker",
            consumer_group="test-group",
            metrics_host="127.0.0.1",
            metrics_port=8080,
            metrics_auth_token="token",
            idle_log_interval_seconds=60,
            error_backoff_seconds=2,
            max_consecutive_errors=4,
        )
    ).run(
        build_pipeline=build_pipeline,
        max_records=3,
        forever=False,
        emit_report=False,
    )
    worker = created[0]
    assert isinstance(worker, FakeWorkerPool)
    assert delivered == 3
    assert isinstance(worker.kwargs["metrics"], MetricsCollector)
    assert worker.kwargs["health_port"] == 8080
    assert worker.kwargs["health_auth_token"] == "token"
    assert worker.pipeline.max_records == 3
    assert str(worker.pipeline.schedule) == "once"


@pytest.mark.asyncio
async def test_continuous_runtime_coalesces_empty_runs_into_idle_heartbeat(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    empty_summary = SimpleNamespace(
        records_consumed=0,
        records_written=0,
        records_errored=0,
        elapsed_seconds=3.8,
    )
    active_summary = SimpleNamespace(
        records_consumed=1,
        records_written=1,
        records_errored=0,
        elapsed_seconds=0.1,
    )

    class FakeWorkerPool:
        def __init__(self, **_: object) -> None:
            self.pipeline: object | None = None

        def register(self, pipeline: object) -> None:
            self.pipeline = pipeline

        async def run(self) -> None:
            assert self.pipeline is not None
            for observer in self.pipeline.observers:
                for run_number, summary in enumerate(
                    (empty_summary, empty_summary, empty_summary, active_summary), start=1
                ):
                    await observer(
                        SimpleNamespace(run_number=run_number, error=None, summary=summary)
                    )

    timestamps = iter((0.0, 30.0, 61.0))
    monkeypatch.setattr(runtime, "WorkerPool", FakeWorkerPool)
    monkeypatch.setattr(runtime, "monotonic", lambda: next(timestamps))

    async def build_pipeline() -> object:
        return object()

    await runtime.ProjectionRuntime(
        runtime.ProjectionSpec(
            pipeline_id="test-projection",
            process_name="test-worker",
            consumer_group="test-group",
            metrics_host="127.0.0.1",
            metrics_port=None,
            metrics_auth_token=None,
            idle_log_interval_seconds=60,
            error_backoff_seconds=2,
            max_consecutive_errors=4,
        )
    ).run(
        build_pipeline=build_pipeline,
        max_records=None,
        forever=True,
        emit_report=False,
    )

    events = [json.loads(line)["event"] for line in capsys.readouterr().out.splitlines()]
    assert events == ["projection_worker_starting", "projection_idle", "projection_run_completed"]
