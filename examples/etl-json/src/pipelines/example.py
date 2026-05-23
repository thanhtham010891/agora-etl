from __future__ import annotations

from pathlib import Path

from models import Event  # noqa: TC002
from normalizers import row_to_event

from agora.core.pipeline import BoundPipeline, Pipeline
from agora.sinks.file.jsonlines import JsonLinesSink
from agora.sources.file.jsonlines import JsonLinesSource

INPUT_PATH = Path("data/events.jsonl")
OUTPUT_PATH = Path("output/events_filtered.jsonl")


def _event_to_dict(e: Event) -> dict:
    return {
        "id": e.id,
        "type": e.type,
        "user_id": e.user_id,
        "timestamp": e.timestamp,
        "payload": e.payload,
    }


async def build_pipeline() -> BoundPipeline:
    source = JsonLinesSource(
        path=INPUT_PATH,
        row_mapper=row_to_event,
    )
    sink = JsonLinesSink(
        path=OUTPUT_PATH,
        serializer=_event_to_dict,
    )
    return (
        Pipeline(source, id="etl-json")
        .filter(lambda e: e.type != "ping", name="drop_ping")
        .build(sink, batch_size=100)
    )
