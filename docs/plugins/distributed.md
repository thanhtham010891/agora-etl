# Distributed Coordination

_When to read this: more than one worker instance may contend for the same
scheduled pipeline and the deployment needs shared lease ownership._

The distributed family in `agora-etl-plugins 0.4.x` provides Redis-backed
worker coordination for Agora workers. It coordinates _who may run a scheduled
pipeline now_; it does not move records and it does not replace Kafka,
PostgreSQL, or Redis Streams.

## Install

```bash
pip install "agora-etl-plugins[distributed]"
```

The extra installs `redis`.

## Public surface

| Component | Kind | Use it for |
|---|---|---|
| `RedisWorkerCoordinator` | Worker coordinator | Worker registration, heartbeats, per-pipeline leases, lease renewal, fencing tokens, and fleet discovery. |
| `DistributedConfig` | Settings | Environment-backed Redis URL, TTL, heartbeat, key prefix, fallback, and Redlock settings. |

`DistributedConfig` reads environment variables prefixed with
`AGORA_DISTRIBUTED_`, for example:

```bash
AGORA_DISTRIBUTED_REDIS_URL=redis://redis:6379
AGORA_DISTRIBUTED_LEASE_TTL_SECONDS=300
AGORA_DISTRIBUTED_HEARTBEAT_INTERVAL=30
AGORA_DISTRIBUTED_REDLOCK_REDIS_URLS='["redis://r1:6379","redis://r2:6379","redis://r3:6379"]'
```

## RedisWorkerCoordinator

Important constructor options:

| Option | Meaning |
|---|---|
| `redis_url` | Primary Redis URL. Worker registry and fencing counters use this connection. |
| `lease_ttl_seconds=300` | Lease TTL between renewals. |
| `heartbeat_interval=30` | Worker heartbeat interval. Must be less than lease TTL. |
| `key_prefix="agora:distributed:"` | Namespace for worker, lease, and fence keys. |
| `fallback_to_local=False` | Fail-safe default. If `True`, Redis unavailability behaves as locally acquired leases and can duplicate runs. |
| `fencing_key_ttl_seconds` | TTL for fencing-token counters. |
| `lease_renewal_deadline_seconds` | Optional renewal deadline before a lease is considered lost. |
| `redlock_redis_urls` | Optional list of at least three independent Redis master URLs for majority-quorum leases. |
| `redlock_clock_drift_factor=0.01` | Redlock lease validity safety margin. |

Lifecycle and operator APIs:

- `start(worker_id, pipeline_ids)`
- `stop()`
- `try_acquire_lease(pipeline_id, run_number)`
- `release_lease(pipeline_id)`
- `renew_lease(pipeline_id)`
- `validate_lease(pipeline_id, fencing_token)`
- `current_lease(pipeline_id)`
- `set_lease_lost_callback(callback)`
- `list_workers()`
- `connect()` / `close()` for read-only fleet inspection

## Lease model

The default mode uses one Redis primary:

1. workers register a heartbeat record
2. each scheduled run attempts a per-pipeline lease
3. lease acquisition increments a fencing token
4. the coordinator renews held leases while the worker is alive
5. release deletes the lease only when worker ID and fencing token still match
6. lost leases are removed locally and can call a lease-lost callback

Fencing tokens matter because they let downstream systems reject stale writers
even if two workers overlap during failure recovery.

## Redlock mode

Set `redlock_redis_urls` when lease acquisition and renewal should require a
majority quorum across independent Redis masters.

```python
from agora_plugins.distributed import RedisWorkerCoordinator


coordinator = RedisWorkerCoordinator(
    redis_url="redis://redis-primary:6379",
    redlock_redis_urls=[
        "redis://redis-a:6379",
        "redis://redis-b:6379",
        "redis://redis-c:6379",
    ],
)
```

Important boundary: Redlock nodes must be independent Redis masters. Replicas,
Sentinel members for one primary, or unrelated Cluster shards are not
interchangeable quorum members.

Worker registry and the authoritative fencing counter both use `redis_url`.
The Redlock quorum nodes still decide lease ownership, but they now persist the
same reserved fencing token instead of minting per-node counters.

## Direct sample

```python
from agora_plugins.distributed import RedisWorkerCoordinator


coordinator = RedisWorkerCoordinator(
    redis_url="redis://localhost:6379",
    lease_ttl_seconds=300,
    heartbeat_interval=30,
)

await coordinator.start(
    worker_id="worker-a",
    pipeline_ids=["daily-customers", "daily-orders"],
)

acquired = await coordinator.try_acquire_lease("daily-orders", run_number=42)
if acquired:
    lease = coordinator.current_lease("daily-orders")
    try:
        if lease is not None:
            print(f"fencing token: {lease.fencing_token}")
        print("run the pipeline here")
    finally:
        await coordinator.release_lease("daily-orders")

await coordinator.stop()
```

## WorkerPool example

This is the common deployment shape: each replica starts the same
`WorkerPool`, but only one replica owns each scheduled run.

```python
from agora.runner import Schedule, ScheduledPipeline, WorkerPool
from agora_plugins.distributed import RedisWorkerCoordinator


async def build_orders_pipeline():
    return make_orders_pipeline()


def get_worker() -> WorkerPool:
    pool = WorkerPool(
        coordinator=RedisWorkerCoordinator(
            redis_url="redis://localhost:6379",
            lease_ttl_seconds=300,
            heartbeat_interval=30,
        ),
        health_port=8080,
    )
    pool.register(
        ScheduledPipeline(
            factory=build_orders_pipeline,
            schedule=Schedule.cron("0 * * * *"),
            pipeline_id="orders-hourly",
        )
    )
    return pool
```

## Fail-safe versus fallback

Default behavior is fail-safe:

- if Redis cannot be reached, no shared lease is acquired
- duplicate scheduled runs are avoided
- availability is traded for correctness

`fallback_to_local=True` flips that tradeoff:

- if Redis cannot be reached, the worker behaves as if it acquired the lease
- pipelines keep running
- duplicate runs are possible across replicas

Use fallback only when duplicate work is acceptable or downstream fencing makes
stale writes harmless.

## Fleet inspection

`list_workers()` scans worker heartbeat keys and returns `WorkerInfo` records.
Use `connect()` / `close()` when an operator process only needs read-only fleet
inspection and is not itself a running worker.

## Production checklist

- Keep `heartbeat_interval < lease_ttl_seconds`.
- Choose a lease TTL longer than expected transient pauses but shorter than the
  maximum acceptable stale-ownership window.
- Keep `fallback_to_local=False` unless availability is more important than
  exclusivity.
- Persist or enforce fencing tokens in downstream critical writes.
- Register a lease-lost callback for workloads that must stop immediately after
  renewal failure.
- Use Redlock only with at least three independent Redis masters.
- Use a deployment-specific `key_prefix` when several environments share Redis.

## Boundaries

Distributed coordination answers one question: which worker owns this scheduled
run right now?

It does not replace:

- Kafka for event transport
- PostgreSQL for operational storage
- Redis Streams for record ingestion
- a full scheduler platform for complex calendar/business workflows
