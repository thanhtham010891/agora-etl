# Failure Handling

_When to read this: something in your pipeline is failing and you need to decide whether to crash, skip, or queue for later._

For the full list of runtime promises this page builds on (sink fail-closed semantics, checkpoint advancement under failure, DLQ replay acknowledgement), see [Runtime Guarantees](runtime-guarantees.md).

## What happens when a middleware raises

When a middleware's `process()` raises an exception, the chain stops for that record. No subsequent middlewares run, and the record does not reach the sink. The runtime calls the middleware's `on_error()` hook (which logs by default), then checks whether a DLQ is configured.

If a DLQ sink is configured, the runtime attempts to write the failed record
there as a `DLQRecord` with `stage="middleware"` and the middleware name in
`DLQRecord.middleware`.

- if that DLQ write succeeds, the pipeline continues with the next record
- if that DLQ write fails and no checkpoint is in play for the record, the
  failure is logged under `DLQFailurePolicy.LOG_ONLY` and the pipeline can
  still continue
- if that DLQ write fails and checkpointing is enabled for the record, the run
  stops before later records can advance the checkpoint past an unhandled
  failure

If no DLQ is configured, the error is counted in `PipelineRunSummary.records_errored` and the record is silently discarded.

The pipeline itself does not stop on a middleware error. If you want it to stop, raise inside `on_error()` or use `SinkFailurePolicy.FAIL_CLOSED` at the build step (covered below).

## What happens when a sink raises

Sink failures are handled separately from middleware failures. When `write()` raises, the runtime consults `SinkFailurePolicy`.

### SinkFailurePolicy.FAIL_CLOSED (default)

The pipeline raises immediately and stops. Use this when a write failure means the data is lost and you cannot afford to continue — for example, writing to a primary database where every record must land.

```python
from agora import DeliveryConfig, SinkFailurePolicy

bound = (
    Pipeline(source)
    .pipe(NormalizeMiddleware())
    .build(
        PostgresSink(dsn=dsn),
        config=DeliveryConfig(sink_failure_policy=SinkFailurePolicy.FAIL_CLOSED),
    )
)
```

`FAIL_CLOSED` is the default, so you only need to set it explicitly when overriding a previous configuration.

### SinkFailurePolicy.LOG_AND_CONTINUE

The error is logged, the record is counted as errored, and the pipeline moves on. Use this when partial delivery is acceptable — for example, writing to a secondary analytics store where a missed record is tolerable.

```python
from agora import DeliveryConfig
from agora.core.dlq import SQLiteDLQSink
from agora import SinkFailurePolicy

bound = (
    Pipeline(source)
    .pipe(NormalizeMiddleware())
    .build(
        AnalyticsSink(),
        config=DeliveryConfig(
            sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
            dlq=SQLiteDLQSink(".agora_dlq.db"),
        ),
    )
)
```

Pair `LOG_AND_CONTINUE` with a DLQ so failed records are not silently lost. Without a DLQ, `LOG_AND_CONTINUE` means the record is gone.

## The DLQ

A DLQ (dead-letter queue) is just another sink — one that receives `DLQRecord` objects instead of your domain records. You attach it at `.build()` time:

```python
from agora import DeliveryConfig
from agora.core.dlq import SQLiteDLQSink

dlq = SQLiteDLQSink(".agora_dlq.db")

bound = (
    Pipeline(source)
    .pipe(NormalizeMiddleware())
    .build(
        PostgresSink(dsn=dsn),
        config=DeliveryConfig(dlq=dlq),
    )
)
```

Every failed record — whether from a middleware or a sink — is written to the DLQ with enough context to replay it: the pipeline ID, run ID, stage name, error type, error message, and the original record payload.

### DLQFailurePolicy

If the DLQ sink itself fails to write, `DLQFailurePolicy` controls what happens next.

`DLQFailurePolicy.LOG_ONLY` (default) — log the DLQ write failure and continue
when doing so does not violate checkpoint correctness. The original error is
already counted; losing the DLQ entry is treated as a secondary failure only
when the runtime can still keep resume semantics honest.

`DLQFailurePolicy.RAISE` — treat a DLQ write failure as fatal and stop the pipeline. Use this when you need a hard guarantee that no failed record is silently discarded.

```python
from agora import DeliveryConfig, DLQFailurePolicy

bound = (
    Pipeline(source)
    .pipe(NormalizeMiddleware())
    .build(
        PostgresSink(dsn=dsn),
        config=DeliveryConfig(
            dlq=SQLiteDLQSink(".agora_dlq.db"),
            dlq_failure_policy=DLQFailurePolicy.RAISE,
        ),
    )
)
```

## When to use DLQ vs retry vs passthrough

**Use a DLQ** when the failure is likely transient or data-dependent and you want to inspect and replay later. A DLQ is the right default for production pipelines where you cannot afford to lose records but also cannot block indefinitely on a single bad one.

**Use `RetryMiddleware`** when the failure is transient and you want to recover in-process without human intervention — for example, a flaky HTTP enrichment call:

```python
from agora import DeliveryConfig
from agora import RetryMiddleware

pipeline = (
    Pipeline(source)
    .pipe(RetryMiddleware(EnrichMiddleware(), max_retries=3, backoff_base=2.0))
    .build(sink, config=DeliveryConfig(dlq=SQLiteDLQSink(".agora_dlq.db")))
)
```

`RetryMiddleware` wraps any middleware and retries on exception with exponential backoff. If all retries are exhausted, the exception propagates and the record lands in the DLQ (if configured). Combine retry with a DLQ: retry handles transient failures, the DLQ catches the ones that don't recover.

**Use `OnError.PASSTHROUGH`** (on individual middlewares that support it) when a failure in that stage should not block the record from reaching the sink. This is appropriate for optional enrichment where a missing value is acceptable — the record passes through with whatever the middleware managed to produce before failing.

Do not use `LOG_AND_CONTINUE` without a DLQ as a substitute for passthrough. `LOG_AND_CONTINUE` discards the record at the sink level; passthrough keeps it moving through the pipeline.

## SQLiteDLQSink

`SQLiteDLQSink` writes failed records to a local SQLite file. It's the right choice for development, single-process deployments, and any situation where you don't have a message broker available.

```python
from agora import DeliveryConfig
from agora.core.dlq import SQLiteDLQSink
from agora import SinkFailurePolicy

dlq = SQLiteDLQSink(".agora_dlq.db")  # path is relative to cwd

bound = (
    Pipeline(source)
    .pipe(NormalizeMiddleware())
    .build(
        PostgresSink(dsn=dsn),
        config=DeliveryConfig(
            dlq=dlq,
            sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
        ),
    )
)

summary = await bound.run()
print(f"Failed records in DLQ: {summary.records_errored}")
```

The database is created automatically on first use. The schema is a single `dlq_records` table with columns for pipeline ID, run ID, stage, error type, error message, the serialized record, and retry attempt counters.

`SQLiteDLQSink` is not safe for concurrent writes from multiple processes. For multi-process deployments, use a broker-backed DLQ sink instead.

## Replaying the DLQ

Once you've fixed the underlying issue, replay failed records using `SQLiteDLQSource` as the pipeline source:

```python
import asyncio
from agora import MapMiddleware, Pipeline
from agora.core.dlq import SQLiteDLQSink, SQLiteDLQSource

async def replay() -> None:
    dlq_sink = SQLiteDLQSink(".agora_dlq.db")
    source = SQLiteDLQSource(
        ".agora_dlq.db",
        pipeline_id="my_pipeline",  # replay only this pipeline's failures
        stage="sink_write",         # optional: filter by failure stage
    )

    # The DLQ source yields DLQRecord objects.
    # Use replay_payload() to extract the original record for re-processing.
    async def unwrap_and_ack(dlq_record):
        payload = dlq_record.replay_payload("pipeline")
        # acknowledge removes the record from the DLQ after successful replay
        await dlq_sink.acknowledge(dlq_record)
        return payload

    summary = await (
        Pipeline(source)
        .pipe(MapMiddleware(unwrap_and_ack, name="unwrap"))
        .pipe(NormalizeMiddleware())
        .build(PostgresSink(dsn=dsn))
        .run()
    )
    print(summary)

asyncio.run(replay())
```

`replay_payload("pipeline")` returns the original pre-middleware record so the full pipeline runs again. Use `replay_payload("sink")` if the failure was at the sink stage and the record was already transformed — you want to skip re-processing and write directly.

`SQLiteDLQSource` only yields records where `attempt < max_attempts` (or all records when `max_attempts` is `None`). Records that have hit their retry ceiling are skipped automatically.

### agora dlq replay (CLI)

If you're running pipelines via the agora runner, the CLI provides a shortcut:

```bash
agora dlq replay --config pipelines.toml my_pipeline
agora dlq replay --config pipelines.toml my_pipeline --stage sink_write
agora dlq replay --config pipelines.toml my_pipeline --mode sink
```

This reads the DLQ backend from your pipeline config, builds the matching DLQ
source automatically, and replays only records for the selected pipeline. See
`agora dlq --help` for filtering options.

## Putting it together

A production pipeline that handles failures gracefully looks like this:

```python
import asyncio
from agora import Backpressure, DLQFailurePolicy, DeliveryConfig, RetryMiddleware, SinkFailurePolicy
from agora.core.dlq import SQLiteDLQSink
from agora import Pipeline

async def main() -> None:
    dlq = SQLiteDLQSink(".agora_dlq.db")

    summary = await (
        Pipeline(KafkaSource(topics=["raw_events"], config=kafka_cfg), id="event_pipeline")
        .pipe(NormalizeMiddleware())
        .pipe(RetryMiddleware(EnrichMiddleware(), max_retries=3, backoff_base=2.0))
        .filter(lambda r: r.confidence > 0.5)
        .build(
            PostgresSink(dsn=dsn),
            config=DeliveryConfig(
                dlq=dlq,
                dlq_failure_policy=DLQFailurePolicy.LOG_ONLY,
                sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
                batch_size=100,
                backpressure=Backpressure.adaptive(max_buffer_size=500),
            ),
        )
        .run()
    )

    print(summary)
    if summary.records_errored > 0:
        print(f"  {summary.records_errored} records in DLQ — run `agora dlq replay` to retry")

asyncio.run(main())
```

The retry middleware handles transient enrichment failures in-process. Anything that exhausts retries or fails at the sink lands in the DLQ. The pipeline keeps running. After the run, you inspect the summary and replay if needed.
