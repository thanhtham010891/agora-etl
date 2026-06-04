# SQLiteDLQSource

_Use this when: failed records were stored in the built-in SQLite DLQ and now need local replay or inspection._

`SQLiteDLQSource` reads `DLQRecord` entries back out of the built-in SQLite DLQ
database.

## Good fits

- local development replay
- single-process production recovery
- inspecting only failed middleware or sink stages

## Characteristics

- reads `DLQRecord` objects, not domain records
- filterable by `pipeline_id` and `stage`
- intended for replay workflows, not primary ingestion
- pairs with `SQLiteDLQSink`

## Example

```python
from agora import MapMiddleware, Pipeline
from agora.core.dlq import SQLiteDLQSource
from agora.sinks.io.stdout import StdoutSink

source = SQLiteDLQSource(
    ".agora_dlq.db",
    pipeline_id="orders-sync",
    stage="sink",
)

def unwrap(dlq_record):
    return dlq_record.replay_payload("pipeline")

summary = await (
    Pipeline(source)
    .pipe(MapMiddleware(unwrap, name="unwrap"))
    .build(StdoutSink())
    .run()
)
```

## Boundaries

This source is a local operational tool. For shared replay or multi-worker
recovery, use a plugin-backed DLQ such as Redis or PostgreSQL instead.

