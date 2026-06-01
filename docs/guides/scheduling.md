# Scheduling and Production Workers

_When to read this: you want to run one or more pipelines on a schedule — hourly, on a cron, or continuously — and keep them running as a long-lived process._

## The factory pattern

`ScheduledPipeline` takes a factory coroutine, not a pipeline instance. This is intentional. Pipelines are stateful — they hold open file handles, database connections, and in-flight buffers. Reusing the same instance across runs leaks that state. The factory is called fresh before every run, so each run gets a clean pipeline.

```python
from agora.runner import Schedule, ScheduledPipeline

async def build_pipeline():
    db = await connect_db(settings.DATABASE_URL)
    return (
        Pipeline(CsvSource("events.csv"), id="events_ingest")
        .build(DatabaseSink(db))
    )

scheduled = ScheduledPipeline(
    factory=build_pipeline,
    schedule=Schedule.every(hours=1),
    pipeline_id="events_ingest",
)
await scheduled.start()  # blocks until stopped
```

If your factory raises, the run counts as a failure. If it raises on every run, `max_consecutive_errors` will eventually stop the scheduler.

## Schedule types

`Schedule.every()` waits the given duration after each run completes before starting the next. The timer starts when the run finishes, not when it started. A pipeline that takes 45 minutes with `Schedule.every(hours=1)` runs every 1h45m in practice.

```python
Schedule.every(seconds=30)
Schedule.every(minutes=15)
Schedule.every(hours=6)
Schedule.every(days=1)
```

`Schedule.cron()` runs at wall-clock times defined by a cron expression. Use this when you need runs at fixed times regardless of how long each run takes. Requires `agora-etl-plugins[cron]`.

```python
Schedule.cron("0 */6 * * *")   # every 6 hours on the hour
Schedule.cron("0 2 * * *")     # daily at 02:00
```

`Schedule.continuous()` restarts immediately after each run completes with no delay. Use this for consumer pipelines that should always be running — message queue consumers, CDC streams, anything where idle time means falling behind.

`Schedule.once()` runs exactly once and stops. Useful for one-shot jobs launched by a scheduler like Airflow or a cron job, where you want the pipeline's error handling and metrics but not the loop.

```python
# One-shot: run and exit
scheduled = ScheduledPipeline(
    factory=build_pipeline,
    schedule=Schedule.once(),
    pipeline_id="backfill_2024",
)
await scheduled.start()
```

## Error handling

When a run fails, `ScheduledPipeline` logs the error, waits `error_backoff_seconds`, and retries. The default backoff is exponential starting at 60 seconds, capped at 10 minutes.

```python
scheduled = ScheduledPipeline(
    factory=build_pipeline,
    schedule=Schedule.every(hours=1),
    pipeline_id="events_ingest",
    error_backoff_seconds=30.0,    # base delay after first failure
    max_consecutive_errors=5,      # stop after 5 failures in a row
)
```

After `max_consecutive_errors` consecutive failures the scheduler stops and the task exits with the last error. `WorkerPool` treats this as a terminal pipeline error and initiates graceful shutdown of the whole pool.

A successful run resets the consecutive error counter to zero. If your pipeline fails 4 times then succeeds, the counter goes back to 0.

## WorkerPool: running multiple pipelines

`WorkerPool` runs multiple `ScheduledPipeline`s as concurrent asyncio tasks. Register pipelines before calling `run()`.

```python
from agora.runner import WorkerPool

pool = WorkerPool(
    health_port=8080,
    graceful_shutdown_timeout=30.0,
)

pool.register(ScheduledPipeline(
    factory=build_ingest_pipeline,
    schedule=Schedule.every(hours=6),
    pipeline_id="events_ingest",
))

pool.register(ScheduledPipeline(
    factory=build_consumer_pipeline,
    schedule=Schedule.continuous(),
    pipeline_id="events_consumer",
))

await pool.run()  # blocks until shutdown
```

`register()` returns `self`, so you can chain calls:

```python
pool.register(sp1).register(sp2).register(sp3)
```

`health_port` starts a `HealthServer` on that port. If you don't need the health endpoints, omit it. The pool works fine without them.

`graceful_shutdown_timeout` is how long the pool waits for running pipelines to finish after a shutdown signal before force-cancelling them. 30 seconds is usually enough for pipelines that flush their writers on close. If your sink does large batch writes, increase this.

## The worker.py convention

The `agora worker` CLI looks for a `worker.py` module in your project. The
convention is to define a `get_worker()` function that returns the configured
pool:

```python
# myproject/worker.py
from agora.runner import Schedule, ScheduledPipeline, WorkerPool
from myproject.pipelines import build_consumer, build_ingest


def get_worker() -> WorkerPool:
    pool = WorkerPool(health_port=8080)
    pool.register(
        ScheduledPipeline(
            factory=build_ingest,
            schedule=Schedule.every(hours=1),
            pipeline_id="ingest",
        )
    )
    pool.register(
        ScheduledPipeline(
            factory=build_consumer,
            schedule=Schedule.continuous(),
            pipeline_id="consumer",
        )
    )
    return pool
```

Run it directly from Python if you want, or via the CLI with:

```bash
agora worker --module myproject.worker
```

## SIGINT and SIGTERM behavior

`WorkerPool.run()` installs handlers for both `SIGINT` (Ctrl+C) and `SIGTERM` (systemd stop, `kill`). When either signal arrives:

1. The shutdown event is set.
2. Every registered `ScheduledPipeline.stop()` is called — pipelines sleeping between runs wake up immediately and exit their loop.
3. Pipelines that are mid-run finish their current run normally (flushing writers, saving checkpoints).
4. The pool waits up to `graceful_shutdown_timeout` for all tasks to finish.
5. Any tasks still running after the timeout are force-cancelled.

A second Ctrl+C (double SIGINT) skips the graceful wait and cancels everything immediately.

This means a single Ctrl+C is always safe — it will not corrupt a checkpoint or leave a writer half-flushed. The pipeline finishes its current batch before exiting.
