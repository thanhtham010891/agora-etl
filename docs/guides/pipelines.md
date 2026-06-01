# Composing Pipelines

_When to read this: you understand the basics and want to know how to wire up fan-out, routing, batching, and backpressure correctly._

## The builder is immutable

Every call to `.pipe()` or `.filter()` returns a new `Pipeline` object — the original is unchanged. This means you can share a base pipeline and branch from it:

```python
base = (
    Pipeline(KafkaSource(topics=["events"], config=kafka_cfg))
    .pipe(NormalizeMiddleware())
    .filter(lambda r: r.confidence > 0.5)
)

# Two independent pipelines from the same base — useful in tests or multi-env setups
prod_pipeline = base.build(PostgresSink(dsn=prod_dsn))
dev_pipeline  = base.build(PostgresSink(dsn=dev_dsn))
```

The chain is: `source → middlewares → writer (sink/fan-out/router)`. Nothing runs until you call `.run()`.

## .pipe() and .filter()

`.pipe(middleware)` appends any `Middleware` subclass to the chain. `.filter(predicate)` is shorthand for `.pipe(FilterMiddleware(predicate))` — use it when you only need a boolean gate and don't want to write a class.

```python
from agora import MapMiddleware

pipeline = (
    Pipeline(source)
    .pipe(MapMiddleware(lambda r: r.strip(), name="strip_whitespace"))
    .filter(lambda r: len(r) > 0, name="drop_empty")
    .pipe(EnrichMiddleware())
)
```

Name your middlewares. The name shows up in `PipelineRunSummary.by_middleware` and in logs, so `"strip_whitespace"` is more useful than the default `"map"`.

## .build() — one sink

`.build(sink)` is the right choice when every record goes to one destination.
Delivery options (batching, DLQ, checkpointing, backpressure, tracing) are
passed as a single `DeliveryConfig` via the `config=` keyword:

```python
from agora import DeliveryConfig

bound = (
    Pipeline(source)
    .pipe(NormalizeMiddleware())
    .build(
        PostgresSink(dsn=dsn),
        config=DeliveryConfig(batch_size=200, sink_concurrency=4),
    )
)
summary = await bound.run()
```

`DeliveryConfig` is a frozen dataclass — construct it once and reuse it across
`build()`, `fan_out()`, and `route()`. Its fields:

| Field | Default | What it controls |
|---|---|---|
| `dlq` | `None` | Dead-letter sink for failed records |
| `dlq_failure_policy` | `LOG_ONLY` | What happens when the DLQ write itself fails |
| `checkpoint` | `None` | Checkpoint store for resumable runs |
| `checkpoint_key` | `None` (→ pipeline id) | Key the checkpoint is stored under |
| `checkpoint_every` | `1` | Save checkpoint every N records/batches |
| `checkpoint_failure_policy` | `FAIL_CLOSED` | Behavior when a checkpoint save fails |
| `batch_size` | `1` | Records buffered before the writer flushes |
| `sink_failure_policy` | `FAIL_CLOSED` | Behavior after a sink write error |
| `sink_concurrency` | `None` | Concurrent sink writes (build/fan_out only) |
| `max_buffer_size` | `None` | Hard ceiling on in-flight records |
| `backpressure` | `None` | Adaptive backpressure config (see below) |
| `tracer` | `None` (→ NoopTracer) | Pipeline tracer |

### batch_size

`batch_size` controls how many records are buffered before the writer flushes to the sink. The default is `1` (flush every record).

If your sink implements `write_batch()` — for example a Postgres sink doing a bulk `INSERT` — raise `batch_size` to match your sink's sweet spot. A value of 100–500 is typical for database sinks. If your sink only implements `write()`, the runtime calls it in a loop; batching still reduces flush overhead but won't give you bulk-insert gains.

If your sink does not implement `write_batch()`, keep `batch_size=1` unless you've measured that flush overhead is a bottleneck.

### sink_concurrency

`sink_concurrency` fans out writes to the sink concurrently up to that limit. It only has an effect when the sink advertises `parallel_writes_safe = True`. For most database sinks that use a connection pool, setting this to the pool size is a reasonable starting point.

Leave it unset (the default) for sinks that require ordered writes or manage their own connection internally.

### backpressure

Without backpressure, the source can produce records faster than the sink can consume them, growing memory unboundedly. Use `Backpressure.adaptive()` when your source is faster than your sink and you want the runtime to self-regulate — pass it through `DeliveryConfig(backpressure=...)`:

```python
from agora import DeliveryConfig
from agora import Backpressure

bound = (
    Pipeline(source)
    .pipe(NormalizeMiddleware())
    .build(
        SlowSink(),
        config=DeliveryConfig(
            backpressure=Backpressure.adaptive(
                max_buffer_size=500,
                writer_slow_ms=50.0,    # slow threshold for sink writes
                checkpoint_slow_ms=20.0,
            ),
        ),
    )
)
```

The runtime measures write latency. When writes exceed `writer_slow_ms`, it shrinks the in-flight buffer. When writes are fast, it grows it back up to `max_buffer_size`. You get bounded memory without a fixed sleep.

The full set of tuning parameters:

| Parameter | Default | What it controls |
|---|---|---|
| `max_buffer_size` | `None` (unbounded) | Hard ceiling on in-flight records |
| `min_buffer_size` | `1` | Floor — never shrinks below this |
| `scale_up_step` | `1` | How many slots to add when writes are fast |
| `scale_down_step` | `1` | How many slots to remove when writes are slow |
| `writer_slow_ms` | `25.0` | Latency threshold for sink writes |
| `checkpoint_slow_ms` | `10.0` | Latency threshold for checkpoint saves |

For most pipelines, only `max_buffer_size` and `writer_slow_ms` need tuning. Start with `max_buffer_size` set to 2–5× your expected records-per-second and adjust from there.

If your source is bounded (a file, a list) and you're not worried about memory, skip backpressure entirely.

## fan_out — write to multiple sinks

Use `.fan_out()` when every record must reach every sink. A common case is writing to both a database and an audit log:

```python
from agora import DeliveryConfig

summary = await (
    Pipeline(source)
    .pipe(NormalizeMiddleware())
    .fan_out(
        [PostgresSink(dsn=dsn), AuditLogSink(path="audit.jsonl")],
        config=DeliveryConfig(batch_size=100, sink_concurrency=2),
    )
    .run()
)
```

A failure in one sink does not prevent writes to the others — each sink's result is tracked independently. The `SinkFailurePolicy` (see [failure handling](failure-handling.md)) controls what happens after a sink error.

`sink_concurrency` here limits how many sinks write concurrently. With two sinks and `sink_concurrency=2`, both write in parallel. With `sink_concurrency=1`, they write sequentially.

## route — write to one matching sink per record

Use `.route()` when different records belong in different destinations. The router evaluates predicates in order and sends each record to the first match:

```python
from agora import SinkRouter

router = (
    SinkRouter()
    .route(lambda r: r.region == "asia",   asia_sink)
    .route(lambda r: r.region == "europe", europe_sink)
    .default(fallback_sink)
)

summary = await (
    Pipeline(source)
    .pipe(NormalizeMiddleware())
    .route(router)
    .run()
)
```

Each record goes to exactly one sink — unlike `fan_out`, which sends every record to all sinks. If no predicate matches and there's no `.default()`, the record is silently dropped (no error, no DLQ entry). Add a `.default()` if unmatched records are unexpected.

`route` ignores `DeliveryConfig.sink_concurrency` because routing is inherently sequential — the predicate must be evaluated before the target is known. The other `DeliveryConfig` fields (batching, DLQ, checkpointing) apply as usual.

## When to use fan_out vs route

Use `fan_out` when the same record needs to land in multiple places — replication, dual-write, audit trails.

Use `route` when records are mutually exclusive by type, region, or tenant and each belongs in exactly one place. Routing also keeps per-sink batches coherent: records routed to the same sink are flushed together, which matters for bulk-insert sinks.

## BoundPipeline.run()

`.run()` is async and blocks until the source is exhausted (or `max_records` is reached):

```python
summary = await bound.run(max_records=10_000)
```

`max_records` is useful for smoke tests and backfill jobs where you want to process a fixed slice. Leave it unset for continuous or full-dataset runs.

`.run()` returns a `PipelineRunSummary`:

```python
print(summary.records_consumed)   # emitted by source
print(summary.records_written)    # accepted by at least one sink
print(summary.records_dropped)    # returned None by any middleware
print(summary.records_errored)    # unrecoverable errors
print(summary.elapsed_seconds)

# Per-middleware breakdown
for name, mw in summary.by_middleware.items():
    print(f"{name}: in={mw.records_in} out={mw.records_out} dropped={mw.records_dropped}")
```

`records_written` counts records accepted by at least one sink. In a `fan_out` with three sinks, a record written to two out of three still counts as written once. If you need per-sink counts, inspect the sink's own state or metrics.

## Dry-run mode

`BoundPipeline.with_sink()` replaces the sink without rebuilding the pipeline. Use it to swap in a no-op sink for dry runs:

```python
from agora.sinks.io.stdout import StdoutSink

dry_run = bound.with_sink(StdoutSink())
summary = await dry_run.run(max_records=10)
```

The source, middlewares, checkpointing, and DLQ configuration are all preserved — only the destination changes.
