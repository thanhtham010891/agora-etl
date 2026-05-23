from __future__ import annotations

import pytest

from agora.sinks.file.csv import CsvSink
from agora.sinks.file.jsonlines import JsonLinesSink
from agora.sinks.http.webhook import WebhookSink


@pytest.mark.asyncio
async def test_jsonl_flush_keeps_buffer_on_write_errors(tmp_path) -> None:
    sink = JsonLinesSink(path=tmp_path / "events.jsonl")
    sink._buffer = [{"id": 1}, {"id": 2}]  # type: ignore[attr-defined]

    def _boom(rows, mode: str) -> None:
        del rows, mode
        raise RuntimeError("disk write failed")

    sink._write_rows = _boom  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="disk write failed"):
        await sink.flush()

    assert sink._buffer == [{"id": 1}, {"id": 2}]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_csv_flush_keeps_buffer_on_write_errors(tmp_path) -> None:
    sink = CsvSink(
        path=tmp_path / "events.csv",
        row_mapper=lambda record: {"id": record["id"]},
    )
    sink._buffer = [{"id": 1}, {"id": 2}]  # type: ignore[attr-defined]

    def _boom(rows, write_header: bool, mode: str) -> None:
        del rows, write_header, mode
        raise RuntimeError("disk write failed")

    sink._write_rows = _boom  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="disk write failed"):
        await sink.flush()

    assert sink._buffer == [{"id": 1}, {"id": 2}]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_webhook_flush_keeps_buffer_on_post_errors() -> None:
    sink = WebhookSink(url="https://example.invalid/webhook", batch_mode=True, flush_every=2)
    sink._buffer = [{"id": 1}, {"id": 2}]  # type: ignore[attr-defined]

    async def _boom(payload) -> None:
        del payload
        raise RuntimeError("post failed")

    sink._post_with_retry = _boom  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="post failed"):
        await sink.flush()

    assert sink._buffer == [{"id": 1}, {"id": 2}]  # type: ignore[attr-defined]
