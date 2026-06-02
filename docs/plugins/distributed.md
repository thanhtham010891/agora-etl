# Distributed Coordination

_When to read this: more than one worker instance may contend for the same scheduled pipeline and you need shared lease ownership._

Use distributed coordination when more than one worker instance may try to run
the same scheduled pipeline and you need shared lease ownership.

## Install

```bash
pip install "agora-etl-plugins[distributed]"
```

## What it does

The distributed family provides a Redis-backed worker coordinator.

Its job is not to move records. Its job is to coordinate ownership:

- register workers with heartbeats
- acquire per-pipeline leases
- release leases safely
- list visible workers

The lease model is TTL-based, so workers that disappear do not hold ownership
forever.

## When it is a good fit

- the same pipelines are deployed on multiple worker instances
- duplicate scheduled runs would be harmful
- you already operate Redis and want lightweight coordination instead of a
  heavier scheduler platform

## Operational behavior

The coordinator is designed around a few deliberate choices:

- heartbeats refresh worker presence
- leases are separate from worker registration
- release is ownership-checked before delete
- Redis outages can either fail safe or fall back to local behavior, depending
  on configuration

That last point matters:

- fail-safe is safer for correctness
- fallback-to-local is more permissive but can allow duplicate runs

For community-facing deployments, treat `fallback_to_local` as an explicit
tradeoff, not a harmless convenience switch.

## Sample

This example shows two important moments: starting the coordinator for a worker
and attempting lease ownership before a scheduled run.

```python
from agora_plugins.distributed.coordinator import RedisWorkerCoordinator


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
    try:
        print("run the pipeline here")
    finally:
        await coordinator.release_lease("daily-orders")

await coordinator.stop()
```

What this shows:

- worker registration and pipeline lease ownership are separate concerns
- only the worker holding the lease should execute the scheduled run
- graceful stop matters because it releases leases and deregisters the worker

## WorkerPool example

This is the shape most teams actually use: a normal `WorkerPool` with scheduled
pipelines, plus a coordinator that prevents duplicate runs across replicas.

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

If you run two or five copies of the same worker deployment, each replica can
start normally, but only one should acquire the lease for a given pipeline run.

## Common pattern

- several worker processes share the same schedule definitions
- each tries to acquire the lease for a pipeline run
- exactly one proceeds
- the rest skip that run cleanly

## Boundary

Distributed coordination solves “who gets to run this pipeline right now?”

It does not replace:

- Kafka for event transport
- PostgreSQL for operational storage
- Redis Streams for record ingestion
