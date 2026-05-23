from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from agora import IterableSource, Pipeline
from agora.runner import Schedule, ScheduledPipeline, WorkerPool


class _CollectSink:
    sink_name = "collect"

    async def open(self) -> None:
        return None

    async def write(self, record: int) -> None:
        del record

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


@dataclass
class _CapturedHealthServer:
    port: int
    host: str
    collector: object
    auth_token: str | None

    def __post_init__(self) -> None:
        self._stop_event = asyncio.Event()

    async def serve(self) -> None:
        await self._stop_event.wait()

    def stop(self) -> None:
        self._stop_event.set()


@pytest.mark.asyncio
async def test_worker_pool_passes_health_auth_token(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[_CapturedHealthServer] = []

    def _fake_health_server(*, port: int, host: str, collector, auth_token: str | None):
        server = _CapturedHealthServer(
            port=port,
            host=host,
            collector=collector,
            auth_token=auth_token,
        )
        captured.append(server)
        return server

    monkeypatch.setattr("agora.health.HealthServer", _fake_health_server)

    async def _build_pipeline():
        return Pipeline(IterableSource([1])).build(_CollectSink())  # type: ignore[arg-type]

    pool = WorkerPool(
        health_port=18080,
        health_host="127.0.0.1",
        health_auth_token="secret-token",
    )
    pool.register(
        ScheduledPipeline(
            factory=_build_pipeline,
            schedule=Schedule.once(),
            pipeline_id="auth-pipeline",
        )
    )

    await pool.run()

    assert len(captured) == 1
    assert captured[0].auth_token == "secret-token"
