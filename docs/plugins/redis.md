# Redis Plugins

_When to read this: the pipeline needs Redis Streams, Redis writes, shared
state, DLQ/replay, deduplication, AI cache, or Redis-backed Kafka bridge
helpers._

The Redis family in `agora-etl-plugins 0.4.x` is a production-ready flagship
backend. It includes stream ingestion, sink writes, shared state, exact and
embedding dedup stores, Redis-backed DLQ, distributed LLM cache, observability
reports, Redis Sentinel/Cluster wiring, and Kafka-to-Redis runtime helpers.

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
| `KafkaRedisRuntime` and builders | Runtime helper | Kafka-to-Redis wedge runtime, metrics, and acceptance reports. |

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
