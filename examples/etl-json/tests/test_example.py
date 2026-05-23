from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture
def events_jsonl(tmp_path: Path) -> Path:
    events = [
        {
            "id": "1",
            "type": "click",
            "user_id": "u1",
            "timestamp": "2026-01-01T00:00:00Z",
            "payload": {"btn": "buy"},
        },
        {
            "id": "2",
            "type": "ping",
            "user_id": "u2",
            "timestamp": "2026-01-01T00:00:01Z",
            "payload": {},
        },
        {
            "id": "3",
            "type": "view",
            "user_id": "u1",
            "timestamp": "2026-01-01T00:00:02Z",
            "payload": {"page": "/home"},
        },
        {
            "id": "4",
            "type": "ping",
            "user_id": "u3",
            "timestamp": "2026-01-01T00:00:03Z",
            "payload": {},
        },
        {
            "id": "5",
            "type": "purchase",
            "user_id": "u1",
            "timestamp": "2026-01-01T00:00:04Z",
            "payload": {"amount": 42},
        },
    ]
    path = tmp_path / "events.jsonl"
    path.write_text("\n".join(json.dumps(e) for e in events))
    return path


@pytest.mark.asyncio
async def test_json_pipeline_drops_ping_events(events_jsonl: Path, tmp_path: Path) -> None:
    from normalizers import row_to_event
    from pipelines.example import _event_to_dict

    from agora.core.pipeline import Pipeline
    from agora.sinks.file.jsonlines import JsonLinesSink
    from agora.sources.file.jsonlines import JsonLinesSource

    output = tmp_path / "out.jsonl"
    source = JsonLinesSource(path=events_jsonl, row_mapper=row_to_event)
    sink = JsonLinesSink(path=output, serializer=_event_to_dict)
    pipeline = (
        Pipeline(source, id="test-json")
        .filter(lambda e: e.type != "ping", name="drop_ping")
        .build(sink)
    )
    summary = await pipeline.run()

    assert summary.records_consumed == 5
    assert summary.records_written == 3  # click, view, purchase
    assert summary.records_dropped == 2  # 2 pings

    lines = [json.loads(line) for line in output.read_text().splitlines()]
    assert all(e["type"] != "ping" for e in lines)
