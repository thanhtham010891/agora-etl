# LogSink

_Use this when: pipeline output should become structured log events instead of rows in a file or table._

`LogSink` emits each record through the logger.

## Good fits

- audit-style event logs
- debugging in services that already aggregate logs
- development runs where stdout is too noisy

## Characteristics

- structured logging friendly
- configurable log level
- not a durable storage system by itself

## Example

```python
from agora import IterableSource, Pipeline
from agora.sinks.io.log import LogSink

summary = await (
    Pipeline(IterableSource([{"event": "start"}, {"event": "finish"}]))
    .build(LogSink(level="info"))
    .run()
)
```

