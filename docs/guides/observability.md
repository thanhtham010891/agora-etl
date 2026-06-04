# Observability

_When to read this: your pipelines are running in production and you need to know whether they're healthy, how fast they're processing, and where time is being spent._

## PipelineRunSummary: what each run tells you

Every pipeline run returns a `PipelineRunSummary`. When running under `ScheduledPipeline`, you get this via the `on_run_complete` callback. When running a pipeline directly, it's the return value of `pipeline.run()`.

The fields that matter most:

`records_consumed` — how many records the source emitted this run. If this is lower than expected, your source is returning less data than it should.

`records_written` — how many records reached the sink. If this is lower than `records_consumed`, the difference was dropped or errored somewhere in the middleware chain.

`records_dropped` — records that a middleware returned `None` for (intentional filtering). This is expected if you have filter middleware. If it's unexpectedly high, a middleware may be silently dropping records it shouldn't.

`records_errored` — records that caused an unhandled exception in a middleware or sink. These went to the DLQ if one is configured, or were lost if not. Non-zero here warrants investigation.

`elapsed_seconds` — wall-clock time for the run. Track this over time. A sudden increase usually means a slow sink, a slow middleware, or a source returning more data than usual.

`by_middleware` — per-middleware breakdown. Each entry has `records_in`, `records_out`, `records_dropped`, `records_errored`, and `total_time_ms`. Use this to find which stage is dropping records or taking the most time.

`runtime` — runtime-side pressure and execution signals. For Rust-prefetch
pipelines, this now tells you whether the run actually used the Rust path
(`rust_prefetch_active`) and how often that path had to wait, batch-drain, and
batch-push records. It also tells you which execution lane actually ran
(`execution_lane`) and whether direct flush or Arrow fast paths were active.
For CSV sinks on Arrow runs, it also tells you whether the sink stayed on the
native Arrow writer or had to downgrade to Python-row CSV writing.

```python
async def on_run_complete(record: RunRecord) -> None:
    if record.summary is None:
        return
    s = record.summary
    print(f"consumed={s.records_consumed} written={s.records_written} "
          f"dropped={s.records_dropped} errors={s.records_errored} "
          f"elapsed={s.elapsed_seconds:.1f}s")
    for name, mw in s.by_middleware.items():
        if mw.records_errored > 0:
            print(f"  {name}: {mw.records_errored} errors")

scheduled = ScheduledPipeline(
    factory=build_pipeline,
    schedule=Schedule.every(hours=1),
    pipeline_id="events_ingest",
    on_run_complete=on_run_complete,
)
```

`print(summary)` is also more informative now. When runtime hints are present,
the string form includes markers such as:

```text
PipelineRunSummary(
    consumed=63826,
    written=63826,
    dropped=0,
    errors=0,
    elapsed=1.0s,
    lane=batch,
    arrow_chain=on,
    arrow_fast_path=on,
    csv_arrow_downgraded=1b/63826r,
)
```

## MetricsCollector: cumulative stats across runs

`MetricsCollector` accumulates stats across all runs of all pipelines in a `WorkerPool`. It's created automatically by the pool, or you can inject your own:

```python
from agora.metrics.collector import MetricsCollector

metrics = MetricsCollector(process_name="events-worker")
pool = WorkerPool(health_port=8080, metrics=metrics)
```

After runs complete, query it directly:

```python
stats = metrics.get("events_ingest")
if stats is not None:
    print(f"success_rate={stats.success_rate:.2%}")
    print(f"total_runs={stats.total_runs}")
    print(f"last_run_throughput={stats.last_run_throughput_rps:.1f} rps")
    print(f"status={stats.status}")  # "ok", "degraded", "failing", or "idle"
```

`stats.status` is derived automatically:
- `"idle"` — no runs yet
- `"ok"` — no failures ever
- `"degraded"` — some failures but success rate >= 50%
- `"failing"` — success rate below 50%

`metrics.overall_status` gives the worst status across all pipelines. This is what the `/health` endpoint reports at the top level.

You rarely need to call `MetricsCollector` directly in production — the health endpoints expose everything. It's most useful in tests and in `on_run_complete` callbacks for custom alerting logic.

## HealthServer: the three endpoints

When you pass `health_port` to `WorkerPool`, it starts a `HealthServer` on that port. You can also run one standalone:

```python
from agora.health import HealthServer
from agora.metrics.collector import MetricsCollector

collector = MetricsCollector()
server = HealthServer(port=8080, collector=collector)
await server.serve()
```

The three endpoints:

`GET /health` — full JSON payload. Includes overall status, uptime, per-pipeline stats (runs, success rate, throughput, last error, per-middleware breakdown). Use this for dashboards and detailed debugging.

```json
{
  "status": "ok",
  "process": "events-worker",
  "uptime_seconds": 3612.4,
  "started_at": "2026-05-28T00:00:00+00:00",
  "pipelines": {
    "events_ingest": {
      "status": "ok",
      "total_runs": 4,
      "successful_runs": 4,
      "failed_runs": 0,
      "success_rate": 1.0,
      "total_records_consumed": 48291,
      "last_run_throughput_rps": 1243.7,
      "last_run_duration_s": 9.71
    }
  }
}
```

When a pipeline uses Rust-backed prefetch, the `runtime.last_run` block will
include:

- `rust_prefetch_active` — `true` when the run used the Rust prefetch path
- `rust_prefetch_wait_count` — how many times the consumer had to block waiting
  for the producer
- `rust_prefetch_batch_drain_count` — how many non-empty batch drains the
  consumer performed
- `rust_prefetch_push_batch_count` — how many batches the producer pushed into
  the Rust buffer

If `source_prefetch_enabled` is `true` but `rust_prefetch_active` is `false`,
the pipeline used the Python fallback prefetch path instead of the Rust one.

The same `runtime.last_run` block also exposes execution-shape signals:

- `execution_lane` — `"linear"`, `"buffered"`, or `"batch"` for the lane that
  actually ran
- `source_data_plane` — `"python_rows"`, `"python_batches"`, or
  `"arrow_batches"` for the plane emitted by the source
- `writer_input_data_plane` — the plane that actually crossed into the writer
  after any materialization at the writer boundary
- `direct_flush_active` — `true` when the linear lane used the direct batched
  flush path
- `arrow_fast_path_active` — `true` when a batch source wrote Arrow batches
  straight into an Arrow-native sink
- `arrow_chain_active` — `true` when both the source/sink fast path and the
  middleware chain stayed Arrow-native end to end
- `writer_downgraded_sink_count` — how many sinks had to receive a lower-plane
  representation than the writer input plane
- `csv_arrow_native_batch_count` — how many CSV Arrow batches were written
  natively via `pyarrow.csv.write_csv()`
- `csv_arrow_native_row_count` — how many rows stayed on the CSV Arrow-native
  writer
- `csv_arrow_downgrade_batch_count` — how many CSV Arrow batches had to
  downgrade to Python-row writing
- `csv_arrow_downgrade_row_count` — how many rows were written through that
  downgrade path

This makes it easier to tell the difference between a planner capability and a
runtime path that was truly exercised.

One subtle but important case: `arrow_chain_active=true` and
`arrow_fast_path_active=true` do not guarantee that `CsvSink` stayed native for
every batch. A CSV sink can still downgrade internally when `pyarrow.csv`
rejects the schema or payload, for example:

- nested Arrow types such as `struct` or `list`
- invalid UTF-8 payloads

That is why the CSV native/downgrade counters matter: they show what really
happened inside the sink after the planner selected the Arrow lane.

If you also enable tracing, the `pipeline.run` span records the planned shape
before execution starts:

- `planned_lane`
- `source_data_plane`
- `writer_input_data_plane`
- `downgraded_sink_count`
- `direct_flush_eligible`
- `arrow_fast_path_eligible`
- `arrow_chain_eligible`

After the run completes, that same span is updated with the runtime outcome:

- `execution_lane`
- `source_data_plane`
- `writer_input_data_plane`
- `direct_flush_active`
- `arrow_fast_path_active`
- `arrow_chain_active`
- `writer_downgraded_sink_count`
- `rust_prefetch_active`

That combination is useful when a pipeline was *eligible* for a fast path but
did not actually take it during the run.

`GET /metrics` — Prometheus text format. Wire this into your scrape config:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: agora
    static_configs:
      - targets: ["localhost:8080"]
    metrics_path: /metrics
```

`GET /ready` — lightweight readiness check. Returns `200 {"ready": true}` when at least one pipeline has completed a successful run, `503 {"ready": false}` otherwise. Use this as a Kubernetes readiness probe — it tells you the worker has actually processed data, not just that the process started.

```yaml
# Kubernetes deployment
readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 15
```

`GET /` redirects to `/health` with a 301.

## Securing health endpoints

By default the health server has no authentication. In production, set an auth token:

```python
import os

pool = WorkerPool(
    health_port=8080,
    health_host="0.0.0.0",   # bind to all interfaces if behind a proxy
    health_auth_token=os.environ["HEALTH_TOKEN"],
)
```

With a token set, all requests to `/health`, `/metrics`, and `/ready` must include `Authorization: Bearer <token>`. Requests without it get `401 Unauthorized`. The comparison uses `hmac.compare_digest` to prevent timing attacks.

If you're running behind an internal load balancer or service mesh that handles auth, leave `health_auth_token=None` (the default). If the endpoints are reachable from outside your network, always set a token.

## Tracing

Tracing gives you span-level visibility into what happens inside a pipeline run — checkpoint loads, writer flushes, individual middleware stages.

`NoopTracer` is the default. It allocates no-op spans with zero overhead. You don't need to configure anything to use it.

`InMemoryTracer` records all spans in memory. Use it in tests and local debugging to inspect what the runtime did:

```python
from agora import DeliveryConfig, InMemoryTracer

tracer = InMemoryTracer()
pipeline = (
    Pipeline(source, id="debug_run")
    .build(sink, config=DeliveryConfig(tracer=tracer))
)
await pipeline.run()

for span in tracer.spans:
    print(f"{span.name} attrs={span.attributes} exceptions={span.exceptions}")
```

`InMemoryTracer` keeps every span for the lifetime of the object. Don't use it in production — it will grow unbounded.

`OpenTelemetryTracer` bridges into your existing OTel setup. It requires `opentelemetry-api` to be installed:

```python
from agora import DeliveryConfig, OpenTelemetryTracer
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(provider)

tracer = OpenTelemetryTracer(name="agora")
pipeline = (
    Pipeline(source, id="events_ingest")
    .build(sink, config=DeliveryConfig(tracer=tracer))
)
```

If you already have OTel configured in your application, `OpenTelemetryTracer()` with no arguments picks up the global tracer provider automatically.

## What to alert on

These are the signals worth wiring into your alerting system.

`status == "failing"` on any pipeline — success rate has dropped below 50%. Something is systematically broken. Page on this.

`status == "degraded"` sustained for more than two consecutive runs — occasional failures are normal; sustained degradation is not. Alert with lower urgency.

`last_error` is non-null after a run — the last run failed. Log this to your error tracker (Sentry, Datadog, etc.) with the pipeline ID and error string.

`records_errored > 0` — records went to the DLQ or were lost. If you have a DLQ, check it. If you don't, these records are gone.

`last_run_duration_s` increasing over time — your pipeline is slowing down. Check `slowest_middleware` in the health payload to find the bottleneck. Common causes: a sink that's getting slower under load, a middleware doing expensive I/O per record, or a source returning more data per run.

`/ready` returning 503 for more than a few minutes after startup — the worker started but hasn't completed a successful run. Check logs for factory errors or source connectivity issues.

For Prometheus users, the key metrics to alert on are:

```
# Success rate below 80% for 10 minutes
agora_pipeline_success_rate{pipeline="events_ingest"} < 0.8

# No successful run in the last 2 hours (for an hourly pipeline)
time() - agora_pipeline_last_run_timestamp_seconds{pipeline="events_ingest"} > 7200

# Records errored in last run
agora_pipeline_records_errored_total{pipeline="events_ingest"} > 0
```
