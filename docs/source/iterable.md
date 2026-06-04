# IterableSource

_Use this when: the input already exists in memory and the goal is to exercise pipeline logic without any I/O setup._

`IterableSource` wraps a Python iterable and emits each item as a pipeline
record.

## Good fits

- unit tests
- quick experiments
- examples and docs
- replaying a small hand-built record list

## Characteristics

- zero external dependencies
- no checkpoint support
- no source-side batching
- best for small or medium in-memory collections

## Example

```python
from agora import IterableSource, Pipeline
from agora.sinks.io.stdout import StdoutSink

records = [
    {"id": 1, "name": "alice"},
    {"id": 2, "name": "bob"},
]

summary = await (
    Pipeline(IterableSource(records))
    .build(StdoutSink())
    .run()
)
```

## When not to use it

Do not use `IterableSource` for large files, paginated APIs, or anything that
needs restart recovery. It is a convenience source, not a production ingestion
boundary.

