from __future__ import annotations

import asyncio

import pytest

from agora.health.server import HealthServer


class _FakeSocket:
    def __init__(self, address: tuple[str, int]) -> None:
        self._address = address

    def getsockname(self) -> tuple[str, int]:
        return self._address


class _FakeAsyncServer:
    def __init__(self, port: int) -> None:
        self.sockets = [_FakeSocket(("127.0.0.1", port))]
        self.close_calls = 0
        self.wait_closed_calls = 0

    def close(self) -> None:
        self.close_calls += 1

    async def wait_closed(self) -> None:
        self.wait_closed_calls += 1

    async def __aenter__(self) -> _FakeAsyncServer:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        self.close()
        await self.wait_closed()


@pytest.mark.asyncio
async def test_health_server_can_be_reused_after_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[_FakeAsyncServer] = []

    async def _fake_start_server(handler, *, host: str, port: int):
        del handler, host
        server = _FakeAsyncServer(port or 18080 + len(created))
        created.append(server)
        return server

    monkeypatch.setattr("agora.health.server.asyncio.start_server", _fake_start_server)

    server = HealthServer(port=0)

    first = asyncio.create_task(server.serve())
    await asyncio.sleep(0)
    server.stop()
    await asyncio.wait_for(first, timeout=1.0)

    assert server._server is None
    assert server._stop_event is None

    second = asyncio.create_task(server.serve())
    await asyncio.sleep(0)
    server.stop()
    await asyncio.wait_for(second, timeout=1.0)

    assert len(created) == 2
    assert all(fake.close_calls >= 1 for fake in created)
    assert all(fake.wait_closed_calls >= 1 for fake in created)
    assert server._server is None
    assert server._stop_event is None


@pytest.mark.asyncio
async def test_health_server_rejects_concurrent_serve_on_same_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_start_server(handler, *, host: str, port: int):
        del handler, host
        return _FakeAsyncServer(port or 18080)

    monkeypatch.setattr("agora.health.server.asyncio.start_server", _fake_start_server)

    server = HealthServer(port=0)
    task = asyncio.create_task(server.serve())
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="already serving"):
        await server.serve()

    server.stop()
    await asyncio.wait_for(task, timeout=1.0)
