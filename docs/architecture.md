# Architecture

## The five components

Every pipeline is composed of five parts. Understanding what each one owns makes the rest of the runtime predictable.

**Source** emits records via an async generator. It owns the cursor into the data — file position, page number, Kafka offset, whatever makes sense for that source. The runtime never pulls faster than the source yields.

**MiddlewareChain** is the ordered list of middlewares you registered with `.pipe()`. Records flow through it left to right. If any middleware returns `None`, the record is dropped and does not continue. If any middleware raises, the record is routed to the DLQ (if configured) and the chain stops for that record.

**Writer** delivers processed records to one or more sinks. It handles fan-out, batching, and sink concurrency. You don't interact with it directly — `.build()`, `.fan_out()`, and `.route()` construct it for you.

**DLQSink** captures failed records. A DLQ record preserves the original payload, the processed payload (if the failure happened at the sink), the pipeline and run IDs, the error type and message, and the source checkpoint at the time of failure. Failed records can be replayed with `agora dlq replay`.

**CheckpointStore** persists the source's position so a pipeline can resume after a restart. The runtime calls `checkpoint_store.save()` every `checkpoint_every` records. On the next run, `source.prepare_resume(checkpoint)` is called before streaming begins. Not all sources support checkpointing — see [Sources](sources.md) for which ones do.

## Linear vs buffered execution

**Linear mode** is the default. Records move through the chain one at a time:

```
source.stream() → chain.process(record) → writer.write(result)
```

This is the right mode for most pipelines. It is simple, predictable, and easy to reason about under failure.

**Buffered mode** activates automatically when a middleware in the chain exposes a `submit` method — in practice, `AIBatchMiddleware`. The runtime splits the chain at that middleware and runs the buffered stage concurrently up to the configured limit, then drains results in source order before passing them to the suffix of the chain and the writer.

```
source.stream()
  → chain.process_range(0, split_index, record)   # sync prefix
  → buffered_stage.submit(record)                 # concurrent
  → chain.process_range(split_index+1, end, result)  # sync suffix
  → writer.write(result)
```

The key point: buffered mode increases throughput for slow async stages (LLM calls, external APIs) without changing the ordering or failure semantics. Output order is still source order. A sink failure or cancellation still aborts pending buffered work rather than committing later records out of order.

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

## Tracing

Three tracers are available:

| Tracer | When to use |
|---|---|
| `NoopTracer` | Default — zero overhead |
| `InMemoryTracer` | Tests — inspect spans after a run |
| `OpenTelemetryTracer` | Production — exports to any OTLP-compatible backend |

## Plugin system

Agora discovers plugins via Python entry-points when the relevant registries are loaded. Third-party packages register themselves under the `agora.*` entry-point groups. See [Plugins](plugins/index.md) for details.
