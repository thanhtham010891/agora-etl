# Redis Plugins

_When to read this: you need shared fast state, stream ingestion, replay storage, or dedup behavior backed by Redis._

Use the Redis family when your pipeline needs a fast shared backend for stream
ingestion, state, replay, or deduplication.

## Install

```bash
pip install "agora-etl-plugins[redis]"
```

## When Redis is a good fit

- ingest records from Redis Streams
- publish lightweight outputs back into Redis
- keep DLQ records in a shared store for replay
- share state or dedup keys across workers
- cache AI responses close to the runtime

## What the Redis family includes

### Redis Stream source

`RedisStreamSource` is the event-ingestion piece.

It is built for Redis Streams consumer-group workflows:

- reads with consumer groups
- keeps checkpoints based on Redis stream message IDs
- can acknowledge on success
- can batch success acknowledgements
- can reclaim stale pending messages with idle-time thresholds
- can fail closed or log-and-continue on deserialize errors

Use it when records already land in Redis Streams and you want Agora to process
them as a resumable pipeline.

If your deserializer prefers raw bytes instead of decoded strings, set
`decode_responses=False` and deserialize directly from the Redis payload shape
you expect.

### Redis sink

`RedisSink` is the write side.

It supports several output patterns:

- `set` for key-value writes
- `lpush` and `rpush` for list-backed queues
- `xadd` for writing back into Redis Streams

Practical rule:

- use `set` when each record has a stable key
- use `xadd` when you want Redis to remain an event bus
- use list modes for simple queue-like fan-out

For TTL-free `set` batches, Redis can use the multi-key path instead of issuing
one write command per record.

### Redis DLQ

`RedisDLQSink` and `RedisDLQSource` let you keep dead-letter records in Redis
instead of local disk.

This is useful when:

- workers are ephemeral
- more than one operator may need to inspect or replay failures
- replay should happen from a shared backend instead of a local SQLite file

The replay side is meant for operational filtering and bounded reads, not only
for dumping the entire DLQ into memory at once.

### Redis state backend

`RedisBackend` gives Agora a shared key-value state store with TTL support.

Use it when state must survive beyond one process, or when more than one worker
needs to observe the same pipeline state.

TTL-backed keys are handled with short-lived runtime state in mind, so expiry
behavior stays close to the requested timestamp instead of drifting to coarse
whole-second rounding.

### Redis dedup stores

The Redis family ships two different dedup backends:

- `redis`: exact-match dedup using membership keys
- `redis_embedding`: semantic dedup using embeddings and cosine similarity

The semantic store is intentionally small-scale. It performs an O(N) scan over
stored embeddings and is meant for modest datasets, not as a replacement for a
real vector database.

It now scans incrementally rather than assuming the full embedding index should
be pulled into memory at once, but the architectural boundary is still the
same: this is a pragmatic small-scale dedup helper, not a vector search system.

### Redis AI cache

`RedisLLMCache` lets AI-heavy workflows reuse completion results through Redis.

Use it when the same prompt patterns repeat across runs and you want cache hits
to survive process restarts.

## Quickstart

This example consumes JSON-like events from a Redis Stream and writes a compact
status record back into Redis as a key-value entry.

```python
from agora import DeliveryConfig, Pipeline
from agora_plugins.redis import RedisSink, RedisStreamSource


def deserialize(fields: dict[str, str]) -> dict[str, str]:
    return {
        "event_id": fields["event_id"],
        "customer_id": fields["customer_id"],
        "status": fields.get("status", "new"),
    }


source = RedisStreamSource(
    url="redis://localhost:6379",
    stream="orders:raw",
    group="orders-pipeline",
    consumer="worker-1",
    deserializer=deserialize,
    ack_on_success=True,
    ack_batch_size=200,
    reclaim_idle_ms=60_000,
)

sink = RedisSink(
    url="redis://localhost:6379",
    mode="set",
    key_fn=lambda record: f"orders:status:{record['event_id']}",
    serializer=lambda record: record["status"],
    ttl_seconds=3600,
)

summary = await (
    Pipeline(source)
    .build(sink, config=DeliveryConfig(batch_size=100))
    .run(max_records=1_000)
)
```

What this shows:

- `RedisStreamSource` is the event-ingestion edge
- checkpoints are based on stream message IDs
- acknowledgements can be flushed in batches instead of one message at a time
- `RedisSink` in `set` mode is good for lightweight derived state
- TTL is only applied on `set`, not on list or stream modes

## Quick recipes

### Inspect only failed sink writes from a shared DLQ

```python
from agora_plugins.redis import RedisDLQSource


source = RedisDLQSource(
    url="redis://localhost:6379",
    key_prefix="agora:dlq",
    pipeline_id="orders-sync",
    stage="sink",
    limit=100,
)

async with source:
    async for record in source.stream():
        print(record.error_type, record.error_message)
```

Use this when operators need a bounded, shared replay view instead of a
node-local SQLite file.

### Keep short-lived shared pipeline state in Redis

```python
import time

from agora_plugins.redis import RedisBackend


backend = RedisBackend(url="redis://localhost:6379", prefix="agora:state:")
backend.set(
    "orders:last-success",
    {"cursor": 42},
    expires_at=time.time() + 3600,
)

stored = backend.get("orders:last-success")
print(stored.value if stored else None)
backend.close()
```

Use this when more than one worker or process needs to observe the same runtime
state.

## Common patterns

### Stream in, process, write elsewhere

- source from Redis Streams
- transform with Agora middleware
- write to PostgreSQL, Kafka, or file sinks

### Shared dedup for multiple workers

- use Redis dedup keys
- let every worker consult the same store
- avoid duplicate work across a fleet

### Shared replay and operator workflows

- send failures to Redis DLQ
- inspect and replay from a central backend
- avoid node-local recovery state

## Boundaries

Redis is a strong fit when you want one fast shared system that can hold event
streams, short-lived state, and replay metadata.

It is a weak fit when:

- records are large and relational
- you need analytical queries
- semantic search needs to scale well past small-to-medium working sets
