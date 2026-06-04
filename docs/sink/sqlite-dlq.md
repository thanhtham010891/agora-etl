# SQLiteDLQSink

_Use this when: failed records need a built-in local DLQ without adding Redis or PostgreSQL._

`SQLiteDLQSink` stores `DLQRecord` entries in a local SQLite database.

## Good fits

- local development
- single-process deployments
- recovery workflows where a file-backed DLQ is enough

## Characteristics

- created automatically on first use
- stores replay metadata and serialized records
- not designed for multi-process concurrent writers
- pairs with `SQLiteDLQSource`

## Example

```python
from agora import DeliveryConfig, Pipeline
from agora.core.dlq import SQLiteDLQSink

dlq = SQLiteDLQSink(".agora_dlq.db")

summary = await (
    Pipeline(source)
    .build(real_sink, config=DeliveryConfig(dlq=dlq))
    .run()
)
```

## Boundaries

For shared operational replay across multiple workers, use a plugin-backed DLQ
instead of SQLite.

