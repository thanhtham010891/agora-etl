# Redis Plugins

_When to read this: the pipeline needs Redis Streams, Redis writes, shared
state, DLQ/replay, deduplication, AI cache, or Redis-backed Kafka bridge
helpers._

The Redis family in `agora-etl-plugins 0.4.x` is a production-ready flagship
backend. It includes stream ingestion, sink writes, shared state, exact and
embedding dedup stores, Redis-backed DLQ, distributed LLM cache, observability
reports, Redis Sentinel/Cluster wiring, and Kafka-to-Redis composed flows.

## Maturity card

| Field | Value |
|---|---|
| Support label | Production-ready flagship |
| In scope | Redis Streams source, Redis sink, Redis DLQ/replay, Redis state backend, exact dedup, AI cache, Sentinel/Cluster coverage, and observability hooks. |
| Out of scope | General Redis administration, vector-database breadth, and treating Kafka-to-Redis runtime helpers as default onboarding primitives. |
| Required validation gate | `make test-release-gate-redis` |
| Operator hooks | `metrics_snapshot()`, `health_snapshot()`, `acceptance_report()`, `render_prometheus_metrics()` on supported source/sink/DLQ surfaces. |

## Install

```bash
pip install "agora-etl-plugins[redis]"
```

## Public surface

| Component | Kind | Use it for |
|---|---|---|
| `RedisStreamSource` | Source | Consuming Redis Streams through consumer groups. |
| `RedisSink` | Sink | Writing `set`, `lpush`, `rpush`, or `xadd` mutations. |
| `RedisDLQSink` | DLQ sink | Persisting dead-letter records in Redis hashes plus indexes. |
| `RedisDLQSource` | DLQ source | Filtering and replaying Redis-backed DLQ records. |
| `RedisBackend` | State backend | Shared synchronous state with TTL, prefix scans, `set_if_absent`, and CAS. |
| `RedisStore` | Dedup store | Exact dedup built on `RedisBackend` and offloaded blocking calls. |
| `RedisEmbeddingStore` | Dedup store | Small-to-medium semantic dedup using embeddings and cosine similarity. |
| `RedisLLMCache` | AI cache | Distributed LLM response cache backed by Redis state. |
| `RedisPrometheusExporter` | Observability | Prometheus rendering for Redis source/sink/DLQ metrics. |
| `RedisEnterpriseAcceptanceGate` | Observability | Acceptance reports over source/sink/DLQ snapshots. |
| `KafkaRedisRuntime` and builders | Composed flow helper | Advanced Kafka-to-Redis wedge runtime, metrics, and acceptance reports. Start with the Redis primitives above unless the pipeline is explicitly a Kafka-to-Redis composed flow. |

`KafkaRedis*` surfaces are exported for advanced composed pipelines, but they
are not the default Redis onboarding story.

Entry-points installed by the package:

| Group | Key | Target |
|---|---|---|
| `agora.sources` | `redis_stream` | `RedisStreamSource` |
| `agora.sources` | `redis_dlq_source` | `RedisDLQSource` |
| `agora.sinks` | `redis` | `RedisSink` |
| `agora.sinks` | `redis_dlq` | `RedisDLQSink` |
| `agora.ai.caches` | `redis` | `RedisLLMCache` |
| `agora.middlewares.dedup.stores` | `redis` | `RedisStore` |
| `agora.middlewares.dedup.stores` | `redis_embedding` | `RedisEmbeddingStore` |
| `agora.state.backends` | `redis` | `RedisBackend` |

## RedisStreamSource

`RedisStreamSource` consumes with `XREADGROUP` and supports checkpoint resume.

Important constructor options:

| Option | Meaning |
|---|---|
| `url`, `stream`, `group`, `consumer` | Required stream consumer-group identity. |
| `deserializer` | Callable receiving the Redis field map. Defaults to identity. |
| `block_ms`, `batch_size` | Read polling and batch size. |
| `ack_on_success=True` | Acknowledges only after successful downstream delivery. |
| `ack_batch_size` | Batches `XACK` calls. Defaults to `batch_size`. |
| `decode_responses=True` | Use `False` for raw byte payloads. |
| `reclaim_idle_ms` | Enables stale pending message reclaim through `XAUTOCLAIM`. |
| `reclaim_batch_size` | Size of each reclaim batch. |
| `max_consecutive_reclaim_batches` | Fairness guard so reclaim loops yield back to fresh reads. |
| `on_deserialize_error` | Fail closed or log/drop/continue via core source failure policy. |
| `redis_cluster`, `sentinel_service_name`, `sentinel_urls` | Cluster and Sentinel connection modes. |
| `reconnect_retry_policy` | Retry policy for read/reclaim/ack reconnection. |

Resume uses `XGROUP SETID`, so it is guarded for single-consumer group resume.
Multi-consumer groups should use a dedicated replay group or an
operator-managed reset.

The source exposes:

- `current_checkpoint()` with stream, group, consumer, and message ID
- `metrics_snapshot()`
- `health_snapshot()`
- `acceptance_report()`
- `render_prometheus_metrics()`

## RedisSink

`RedisSink` supports four modes:

| Mode | Redis operation | Notes |
|---|---|---|
| `set` | `SET` / `MSET` | TTL supported. TTL-free non-cluster batches use `MSET`. |
| `lpush` | Lua-wrapped `LPUSH` | Uses idempotency keys for retry-safe list writes. |
| `rpush` | Lua-wrapped `RPUSH` | Uses idempotency keys for retry-safe list writes. |
| `xadd` | `XADD` | Serializer must return a dict. Supports approximate `maxlen`. |

Important constructor options:

| Option | Meaning |
|---|---|
| `key_fn` | Required key resolver. |
| `serializer` | Converts records to Redis values. Defaults to JSON-ish string serialization. |
| `ttl_seconds` | Only valid for `mode="set"`. |
| `maxlen` | List/stream trimming. For streams it is passed to `XADD` as approximate maxlen. |
| `redis_cluster`, `redis_cluster_address_remap` | Cluster mode and address rewrite support. |
| `sentinel_service_name`, `sentinel_urls` | Redis Sentinel mode. |
| `retry_policy` | Overrides default Redis retry behavior. |

The sink exposes `metrics_snapshot()`, `acceptance_report()`, and
`render_prometheus_metrics()`.

## Quickstart

```python
from agora import DeliveryConfig, Pipeline
from agora_plugins.redis import RedisSink, RedisStreamSource


source = RedisStreamSource(
    url="redis://localhost:6379",
    stream="orders:raw",
    group="orders-pipeline",
    consumer="worker-1",
    deserializer=lambda fields: {
        "event_id": fields["event_id"],
        "customer_id": fields["customer_id"],
        "status": fields.get("status", "new"),
    },
    ack_on_success=True,
    ack_batch_size=200,
    reclaim_idle_ms=60_000,
    max_consecutive_reclaim_batches=5,
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

## Redis Streams delivery profiles

`RedisStreamSource` is at-least-once: with `ack_on_success=True`, an entry is
acknowledged only after its downstream delivery callback succeeds. A crash
after a target write and before `XACK` can therefore replay the entry. The
target must make that replay safe.

| Target | Replay-safe recipe | Boundary |
|---|---|---|
| PostgreSQL | Prefer `build_redis_postgres_runtime()`; it persists a `stream:message_id` delivery key with `upsert=True` and a unique conflict key. | A replay updates the same logical row; there is no distributed Redis/PostgreSQL transaction. |
| Redis `set` | Use a deterministic `key_fn` derived from the stable event ID and explicitly set `replay_safe_key_contract=True`. | `SET` overwrites the same key on replay; that contract is accepted only for `set`. |
| Redis `lpush` / `rpush` / `xadd` | Do not claim replay safety without a separate application deduplication mechanism. | Retry markers do not deduplicate a process-crash replay. |

For generic `RedisSink(mode="set")`, Agora cannot prove that an arbitrary
`key_fn` is deterministic. It therefore reports `replay_safe=False` until the
application explicitly attests to a stable delivery key:

```python
RedisSink(
    url="redis://localhost:6379",
    key_fn=lambda record: f"orders:{record['event_id']}",
    mode="set",
    replay_safe_key_contract=True,
)
```

Set that flag only when the key is derived from an immutable event identity.

Require this contract before a run when the pipeline must not use an unsafe
target:

```python
from agora import DeliveryConfig
from agora.core.delivery import DeliveryPolicy

config = DeliveryConfig(
    delivery_policy=DeliveryPolicy(
        require_replay_safe=True,
        require_idempotent_sinks=True,
    )
)
```

The source acceptance report now also rejects `ack_on_success=False` by
default. Override that threshold only when acknowledgement is explicitly
coordinated outside Agora and the resulting loss/replay policy is documented.
For poison entries, inspect the pending list and DLQ before retrying; do not
acknowledge a failed entry merely to clear consumer-group lag.

### Certified Redis Streams → PostgreSQL profile

Use the composed runtime when Redis message identity, rather than a producer
event ID, is the intended idempotency key. It injects
`redis_delivery_key="<stream>:<message_id>"`, flushes PostgreSQL, then flushes
the queued Redis acknowledgement as `XACK`. Per-record flush is mandatory:
buffered writes cannot be acknowledged early because a process crash would
otherwise lose an unpersisted message.

Install both backend extras for this profile:

```bash
pip install "agora-etl-plugins[redis,postgres]"
```

```python
from agora_plugins.postgres import build_redis_postgres_runtime
from agora_plugins.redis.sources import RedisStreamSource

source = RedisStreamSource(
    url="redis://localhost:6379",
    stream="orders",
    group="orders-writers",
    consumer="worker-1",
    ack_on_success=True,
)
runtime = build_redis_postgres_runtime(
    source=source,
    dsn="postgresql://app:secret@localhost:5432/app",
    table="order_projection",
    transform=lambda fields: {"order_id": fields["order_id"]},
)

await runtime.open()
try:
    assert (await runtime.ensure_ready()).passed
    await runtime.drain()
finally:
    await runtime.close()
```

The profile remains at-least-once: a crash after PostgreSQL commits and before
`XACK` replays the message, while the delivery-key upsert updates the same row.
The certified builder rejects a missing delivery key or `upsert=False`. Do not
disable `ack_on_success` or strict write safety without an independently
operated duplicate-handling policy. Metadata is opt-in; if a target persists
it, the row mapper must serialize it for the target column type.

## DLQ and replay

`RedisDLQSink` stores each DLQ record as a Redis hash and maintains ordered and
secondary indexes for filtering. `RedisDLQSource` reads bounded replay windows.

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

Use a DLQ payload policy when payloads must be redacted or encrypted before
being stored in Redis.

## Shared state and dedup

`RedisBackend` implements Agora's synchronous `StateBackend` contract. That is
intentional: core state operations are sync. Async callers should offload it
through the core helpers or call it from a thread when needed.

```python
import time

from agora_plugins.redis import RedisBackend


backend = RedisBackend(url="redis://localhost:6379", prefix="agora:state:")
stored = backend.get("orders:last-success")

updated = backend.compare_and_set(
    "orders:last-success",
    expected=stored,
    value={"cursor": 42},
    expires_at=time.time() + 3600,
)

backend.close()
```

`RedisStore` builds exact dedup on top of this backend and offloads blocking
state calls. `RedisEmbeddingStore` is a pragmatic semantic dedup helper for
small-to-medium working sets; it is not a vector database replacement.

## AI cache

`RedisLLMCache` is an async cache backed by `RedisBackend`.

```python
from agora_plugins.redis import RedisLLMCache


cache = RedisLLMCache(
    url="redis://localhost:6379",
    key_prefix="agora:llm:",
    default_ttl_s=3600,
)
```

Use it when prompt/response reuse should survive process restarts and be shared
across workers.

## Redis Cluster and Sentinel

The Redis source, sink, state backend, and dedup store expose Cluster/Sentinel
wiring where the underlying operation supports it:

- `redis_cluster=True`
- `redis_cluster_address_remap=...`
- `sentinel_service_name="mymaster"`
- `sentinel_urls=["redis://sentinel-a:26379", ...]`

For Cluster list writes, use keys with hash tags when related idempotency keys
must live in the same slot.

## Observability

Redis components expose:

- health snapshots for streams
- metrics snapshots for stream source, sink, and DLQ
- Prometheus text rendering
- acceptance reports with threshold objects
- poison-loop risk snapshots for stream reclaim/deserialization loops

These APIs are useful in release gates and health endpoints; they are not
required for simple pipelines.

## Production checklist

- Use consumer groups for resumable stream processing.
- Keep `ack_on_success=True` for normal pipelines.
- Set `reclaim_idle_ms` only after choosing an idle window that matches the
  slowest healthy downstream writes.
- Use `max_consecutive_reclaim_batches` to avoid starving fresh messages during
  large pending-list recovery.
- Use Redis DLQ for shared operational replay, not long-term analytics.
- Use CAS (`compare_and_set`) for conflict-detecting shared state writes.
- Treat Redis embedding dedup as bounded helper infrastructure, not a search
  database.

## Boundaries

Redis is a strong fit for fast shared runtime state, streams, replay metadata,
lightweight queues, and cache-like workflows.

It is a weak fit for analytical querying, large relational datasets, or
large-scale semantic search.
