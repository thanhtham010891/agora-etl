# Architecture

_When to read this: you want the mental model behind Agora's runtime pieces, lane selection, and how records move through the system._

## The five components

Every pipeline is composed of five parts. Understanding what each one owns makes the rest of the runtime predictable.

**Source** emits records via an async generator. It owns the cursor into the data — file position, page number, Kafka offset, whatever makes sense for that source. The runtime never pulls faster than the source yields.

**MiddlewareChain** is the ordered list of middlewares you registered with `.pipe()`. Records flow through it left to right. If any middleware returns `None`, the record is dropped and does not continue. If any middleware raises, the record is routed to the DLQ (if configured) and the chain stops for that record.

**Writer** delivers processed records to one or more sinks. It handles fan-out, batching, and sink concurrency. You don't interact with it directly — `.build()`, `.fan_out()`, and `.route()` construct it for you.

**DLQSink** captures failed records. A DLQ record preserves the original payload, the processed payload (if the failure happened at the sink), the pipeline and run IDs, the error type and message, and the source checkpoint at the time of failure. Failed records can be replayed with `agora dlq replay`.

**CheckpointStore** persists the source's position so a pipeline can resume after a restart. The runtime calls `checkpoint_store.save()` every `checkpoint_every` records. On the next run, `source.prepare_resume(checkpoint)` is called before streaming begins. Not all sources support checkpointing — see [Sources](sources.md) for which ones do.

For the exact hook order of startup, streaming, and shutdown, see
[Lifecycle](guides/lifecycle.md).

## Execution lanes

The runtime selects one of three execution lanes based on the pipeline's source and middleware chain.

**Linear lane** is the default. Records move through the chain one at a time:

```
source.stream() → chain.process(record) → writer.write(result)
```

This is the right lane for most pipelines. Simple, predictable, easy to reason about under failure.

**Buffered lane** activates when a middleware in the chain exposes a `submit` method **and** declares `min_concurrency > 1` — in practice, `AIBatchMiddleware`. The runtime splits the chain at that middleware and runs the buffered stage concurrently up to the configured limit, then drains results in source order before passing them to the suffix of the chain and the writer.

```
source.stream()
  → chain.process_range(0, split_index, record)   # sync prefix
  → buffered_stage.submit(record)                 # concurrent
  → chain.process_range(split_index+1, end, result)  # sync suffix
  → writer.write(result)
```

A submit-capable middleware with `min_concurrency == 1` runs on the linear lane — there is no concurrency benefit to pay the per-record task overhead for.

The key point: buffered mode helps when one middleware stage is slow and
concurrent, but it does not change the ordering or failure semantics. Output
order is still source order. A sink failure or cancellation still aborts
pending buffered work rather than committing later records out of order.

**Batch lane** activates when the source sets `supports_batch_emit = True` (e.g. `CsvSource(emit_batches=True)`, `ArrowCsvSource`, `ParquetSource(use_arrow_batches=True)`). The runtime calls `source.stream_batches()` instead of `stream()` and processes whole batches at once:

```
source.stream_batches() → chain.process_batch(batch) → writer.write_batch(results)
```

Checkpointing, DLQ routing, and ordering guarantees are all preserved — the checkpoint advances once per batch, after the batch is durably written.

**Arrow fast path** is a sub-case of the batch lane. When the source emits `pa.RecordBatch` objects (`emits_arrow_batches = True`) **and** every middleware in the chain is an `ArrowBatchMiddleware` subclass **and** the sink is Arrow-native (`write_arrow_batch` present), the runtime keeps data columnar end-to-end:

```
source.stream_batches()          # yields pa.RecordBatch
  → chain.process_arrow_batch()  # each stage: RecordBatch → RecordBatch (no to_pylist)
  → sink.write_arrow_batch()     # zero Python object allocation per row
```

If any stage is a regular `Middleware` or `BatchMiddleware`, the runtime falls
back to `to_pylist()` before that stage. In practice, that means an Arrow
source alone is not enough to keep the whole pipeline columnar.

## How To Predict The Selected Lane

Use this as the quick mental model:

| Source shape | Middleware chain | Sink shape | Selected lane | Fast path notes |
| --- | --- | --- | --- | --- |
| `stream()` only | regular middleware or no middleware | any sink | `linear` | default path |
| `stream()` only | any stage with `submit()` and `min_concurrency > 1` | any sink | `buffered` | preserves source order while running the buffered stage concurrently |
| `stream_batches()` via `supports_batch_emit=True` | regular `BatchMiddleware` or no middleware | batch-writable sink | `batch` | avoids per-record runtime orchestration |
| Arrow batch source (`emits_arrow_batches=True`) | all stages Arrow-native | Arrow-native sink | `batch` + Arrow fast path | `arrow_fast_path_active=true`, `arrow_chain_active=true` |
| Arrow batch source (`emits_arrow_batches=True`) | mixed Arrow + regular middleware | any sink | `batch` | falls back to `to_pylist()` before the regular stage |
| Arrow batch source (`emits_arrow_batches=True`) | all stages Arrow-native | non-Arrow sink | `batch` | Arrow source still helps read-side throughput, but write path materialises rows |

To confirm the decision at runtime, inspect:

- `summary.runtime.execution_lane`
- `summary.runtime.direct_flush_active`
- `summary.runtime.arrow_fast_path_active`
- `summary.runtime.arrow_chain_active`

If tracing is enabled, the same decision appears in span attributes:

- `pipeline.run`: `planned_lane`, `direct_flush_eligible`,
  `arrow_fast_path_eligible`, `arrow_chain_eligible`
- `source.stream`: `lane`, `batch_source`, `buffered_stage_count`

## Runtime guarantees

The full contract — what is guaranteed, what is intentionally not — lives in [Runtime Guarantees](guides/runtime-guarantees.md). The high points:

- Records are committed to sinks in source order in both linear and buffered modes.
- The source checkpoint advances only through records that were durably handled under the active failure policy.
- DLQ replay acknowledges a record only after replay produces one successful write.
- At-least-once delivery is the model. There is no exactly-once guarantee and no transactional coupling between sink writes and the checkpoint store.

For the per-source resume contract (which sources support checkpointing, what their resume position means), see the [Recovery Support Matrix](guides/recovery-matrix.md).

## Backpressure

When `backpressure=Backpressure.adaptive(...)` is set, the runtime monitors writer flush latency and checkpoint save latency to dynamically scale the in-flight record limit up or down. This prevents a fast source from overwhelming a slow sink.

To set a fixed buffer size without adaptive scaling, pass `max_buffer_size` directly:

```python
Backpressure.adaptive(max_buffer_size=200)
```

Adaptive backpressure is a throughput tuning mechanism. It does not relax the ordering or fail-closed guarantees above.

## State backends

Checkpoints, DLQ records, and the HTTP response cache all use the same `StateBackend` abstraction:

| Backend | When to use |
|---|---|
| `MemoryBackend` | Tests and single-run pipelines where persistence is not needed |
| `SQLiteBackend` | Default for local and single-process deployments |

Third-party backends (Redis, Postgres) are available as separate packages under `agora-etl-plugins`.

For direct use of `StateBackend`, `TTLKeyValueStore`, and `MembershipKeyStore`,
see [State](state.md).

## Tracing

Three tracers are available:

| Tracer | When to use |
|---|---|
| `NoopTracer` | Default — zero overhead |
| `InMemoryTracer` | Tests — inspect spans after a run |
| `OpenTelemetryTracer` | Production — exports to any OTLP-compatible backend |

## Plugin system

Agora discovers plugins via Python entry-points when the relevant registries are loaded. Third-party packages register themselves under the `agora.*` entry-point groups. See [Plugins](plugins/index.md) for details.
