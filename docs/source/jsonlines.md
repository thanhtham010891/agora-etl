# JsonLinesSource

_Use this when: the upstream file is newline-delimited JSON and the pipeline still wants ordinary Python records._

`JsonLinesSource` reads a `.jsonl` file and maps each JSON object into a
pipeline record.

## Good fits

- event exports already stored as JSONL
- append-only logs converted to newline-delimited JSON
- developer-friendly file ingestion with easy local inspection

## Characteristics

- prefers `orjson` when the file extra is installed
- supports checkpointing by line number
- can emit ordinary records or Python `list` batches
- easy fit for row-oriented middleware chains

## Example

```python
from agora import Pipeline
from agora.sources.file.jsonlines import JsonLinesSource
from agora.sinks.io.stdout import StdoutSink

source = JsonLinesSource(
    path="data/events.jsonl",
    row_mapper=lambda row: {
        "event_id": row["event_id"],
        "kind": row["kind"],
        "payload": row["payload"],
    },
)

summary = await Pipeline(source).build(StdoutSink()).run()
```

## Batch lane

Set `emit_batches=True` to emit `list[record]` batches:

```python
source = JsonLinesSource(
    path="data/events.jsonl",
    row_mapper=lambda row: row,
    emit_batches=True,
    emit_batch_size=2_000,
)
```

## Resume behavior

`JsonLinesSource` stores the last handled line number and skips forward to that
position on restart.

