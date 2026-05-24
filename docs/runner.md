# Runner

The runner layer manages long-running pipeline processes: scheduling, error backoff, health monitoring, and graceful shutdown.

## ScheduledPipeline

Run a pipeline on a repeating schedule. Takes a factory coroutine — not a pipeline instance — because pipelines are stateful and must be rebuilt each run.

```python
from agora.runner import ScheduledPipeline, Schedule

async def build_pipeline() -> BoundPipeline:
    return (
        Pipeline(MySource())
        .pipe(NormalizeMiddleware())
        .build(MySink())
    )

scheduled = ScheduledPipeline(
    factory=build_pipeline,
    schedule=Schedule.every(hours=6),
    pipeline_id="my_pipeline",
    max_records=10_000,
    max_consecutive_errors=3,
    error_backoff_seconds=60.0,
)

await scheduled.start()
```

### Schedule types

| Schedule | Description |
|---|---|
| `Schedule.every(seconds=N)` | Fixed interval after each run completes |
| `Schedule.every(minutes=N)` | |
| `Schedule.every(hours=N)` | |
| `Schedule.cron("0 */6 * * *")` | Cron expression (requires `agora-etl-plugins[cron]`) |
| `Schedule.continuous()` | Restart immediately after each run |
| `Schedule.once()` | Run exactly once then stop |

### Error handling

On failure, the scheduler logs the error, waits `error_backoff_seconds` (with exponential backoff), and retries. After `max_consecutive_errors` consecutive failures, the scheduler stops.

A successful run resets the consecutive error counter.

## WorkerPool

Run multiple `ScheduledPipeline` instances concurrently as a single long-running process.

```python
from agora.runner import WorkerPool, ScheduledPipeline, Schedule

pool = WorkerPool(
    health_port=8080,
    graceful_shutdown_timeout=30.0,
)

pool.register(ScheduledPipeline(
    factory=build_ingest_pipeline,
    schedule=Schedule.every(hours=6),
    pipeline_id="ingest",
))

pool.register(ScheduledPipeline(
    factory=build_cleanup_pipeline,
    schedule=Schedule.every(hours=24),
    pipeline_id="cleanup",
))

await pool.run()
```

`WorkerPool.run()` blocks until all pipelines complete or a shutdown signal (SIGINT / SIGTERM) is received.

### Graceful shutdown

On SIGINT or SIGTERM:

1. All `ScheduledPipeline.stop()` are called — sleeping pipelines wake up immediately.
2. The health server is stopped.
3. The pool waits up to `graceful_shutdown_timeout` seconds for running pipelines to finish.
4. Any remaining tasks are force-cancelled.

### Health server

When `health_port` is set, the worker pool starts an HTTP health server:

| Endpoint | Description |
|---|---|
| `GET /health` | JSON status of all registered pipelines |
| `GET /metrics` | Prometheus text format |
| `GET /ready` | 200 OK when all pipelines are running, 503 otherwise |

Secure the health endpoints with a bearer token:

```python
WorkerPool(
    health_port=8080,
    health_auth_token="my-secret-token",
)
```

Or via environment variable:

```bash
AGORA_HEALTH_AUTH_TOKEN=my-secret-token agora worker
```

The built-in health server is intentionally minimal. It is a good fit for
private cluster networks, local development, and simple process supervision.
For internet-facing deployments, put it behind a reverse proxy or service mesh
that already handles TLS, rate limiting, and request filtering.

## agora worker CLI

The `agora worker` command loads a `WorkerPool` from a `worker.py` module in the project root:

```python
# worker.py
from agora.runner import WorkerPool, ScheduledPipeline, Schedule
from pipelines.ingest import build_pipeline

def get_worker() -> WorkerPool:
    pool = WorkerPool(health_port=8080)
    pool.register(ScheduledPipeline(
        factory=build_pipeline,
        schedule=Schedule.every(hours=6),
        pipeline_id="ingest",
    ))
    return pool
```

```bash
agora worker                        # loads worker.py
agora worker --module pipelines.worker  # custom module path
agora worker --list                 # list registered pipelines without starting
```

## DLQ replay

Replay failed records through a pipeline:

```bash
agora dlq replay --config pipelines.toml
agora dlq replay --config pipelines.toml --stage sink_write --mode sink
```
