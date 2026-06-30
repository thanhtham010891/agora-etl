# Kafka Plugins

_When to read this: your pipeline boundary is Kafka topics, partitions,
consumer groups, schema registry contracts, or Kafka-backed replay._

The Kafka family in `agora-etl-plugins 0.4.x` is a production-ready flagship
backend. It is no longer just a source/sink pair: it includes topic ingestion,
topic publishing, poison-record handling, Kafka-backed DLQ/replay,
schema-registry serializers, optional OpenTelemetry propagation, runtime
metrics, and transactional delivery hooks.

## Install

```bash
pip install "agora-etl-plugins[kafka]"
```

The extra installs `aiokafka`, `fastavro`, `jsonschema`, and `protobuf`.

## Public surface

| Component | Kind | Use it for |
|---|---|---|
| `KafkaSource` | Source | Consuming topics, topic patterns, or manual partition assignments. |
| `KafkaSink` | Sink | Publishing records with bounded pending acks, idempotent defaults, optional transactions, and per-record routing. |
| `KafkaSinkMessage` | Envelope | Overriding topic, key, partition, headers, timestamp, or value per record. |
| `KafkaDLQSink` | DLQ sink | Writing failed records to a compactable Kafka error topic. |
| `KafkaDLQSource` | DLQ source | Reconstructing replayable DLQ state from Kafka put/delete envelopes. |
| `KafkaSecurityConfig`, `KafkaTLSConfig`, `KafkaSASLConfig` | Config | First-class PLAINTEXT, SSL, SASL_PLAINTEXT, and SASL_SSL wiring. |
| `ConfluentSchemaRegistryClient` | Schema registry | Standard HTTP registry client. |
| `PooledConfluentSchemaRegistryClient` | Schema registry | Reused transport for heavier registry traffic. |
| `AvroSchemaRegistrySerializer` / `Deserializer` | Serializer | Confluent wire-format Avro. |
| `JsonSchemaRegistrySerializer` / `Deserializer` | Serializer | Confluent wire-format JSON Schema with optional payload validation. |
| `ProtobufSchemaRegistrySerializer` / `Deserializer` | Serializer | Confluent wire-format Protobuf with message-index binding checks. |
| `KafkaOpenTelemetryTracing` | Observability | Fail-open W3C trace propagation through Kafka headers. |
| `KafkaSourceRuntime`, `KafkaTransformSinkRuntime` | Runtime helpers | Advanced source/sink runtime coordination and transactional offset handoff. |
| `KafkaSourcePrometheusExporter`, `KafkaDLQPrometheusExporter` | Observability | Rendering source and DLQ metrics snapshots as Prometheus text. |

Entry-points installed by the package:

| Group | Key | Target |
|---|---|---|
| `agora.sources` | `kafka` | `KafkaSource` |
| `agora.sources` | `kafka_dlq_source` | `KafkaDLQSource` |
| `agora.sinks` | `kafka` | `KafkaSink` |
| `agora.sinks` | `kafka_dlq` | `KafkaDLQSink` |

## KafkaSource

`KafkaSource` is an async `aiokafka` consumer with checkpoint support.

It can subscribe by:

- `topics=["orders.raw"]`
- `topic_pattern="orders\\..*"`
- `assignments=[("orders.raw", 0), ("orders.raw", 1)]`

Important constructor options:

| Option | Meaning |
|---|---|
| `group_id` | Consumer-group identity. Keep stable for production resume. |
| `deserializer` | Sync or async callable. May accept Kafka metadata when its signature supports it. |
| `batch_deserializer` | Optional callable for decoding one Kafka message into multiple records. |
| `enable_auto_commit=False` | Default manual offset discipline. |
| `commit_every=100` | Manual commit cadence. |
| `start_offsets` | Exact topic-partition offsets to seek at startup. |
| `rebalance_listener` | Optional listener wrapped by the source. |
| `on_deserialize_error` | Core source failure policy. |
| `poison_record_policy` | Kafka-specific poison handling. |
| `poison_record_sink` | Required when a DLQ poison policy is selected. |
| `tracing` | `False`, `True`, or a `KafkaOpenTelemetryTracing` instance. |

Runtime methods/operators can also call:

- `commit_now()` to flush tracked offsets immediately
- `seek_to_offsets(...)` for operator-driven repositioning
- `runtime_metrics()` and `operational_metrics()`
- health/partition snapshots through exported source health models

## KafkaSink

`KafkaSink` is an async `aiokafka` producer sink.

Production defaults are intentionally conservative:

- `linger_ms=5`
- `compression_type="gzip"`
- `enable_idempotence=True`
- `acks="all"` when idempotence is enabled
- retry on Kafka send/flush failures
- bounded in-flight delivery futures through `max_pending_acks`

Important constructor options:

| Option | Meaning |
|---|---|
| `serializer` | Sync/async callable returning bytes. Serializer objects may expose `open()` and `close()`. |
| `message_fn` | Full per-record `KafkaSinkMessage` override. |
| `topic_fn`, `key_fn`, `partition_fn`, `headers_fn`, `timestamp_ms_fn` | Narrow per-record routing hooks. |
| `transactional_id` | Enables Kafka transactions. |
| `transaction_per_batch=True` | Wraps each batch in a transaction; requires `transactional_id`. |
| `security` | First-class Kafka security config. |
| `tracing` | Injects trace headers and emits producer/client spans when enabled. |

Use `message_fn` when record-level routing needs more than a key or header:

```python
from agora_plugins.kafka import KafkaSinkMessage


def route(record: dict) -> KafkaSinkMessage:
    return KafkaSinkMessage(
        topic=f"orders.{record['region']}",
        key=str(record["order_id"]).encode("utf-8"),
        headers=[("event-type", b"order-updated")],
    )
```

## Quickstart

```python
import json

from agora import DeliveryConfig, Pipeline
from agora_plugins.kafka import KafkaSink, KafkaSource


source = KafkaSource(
    topics=["orders.raw"],
    bootstrap_servers="localhost:9092",
    group_id="orders-enricher",
    deserializer=lambda value: json.loads(value.decode("utf-8")),
    commit_every=200,
)

sink = KafkaSink(
    topic="orders.cleaned",
    bootstrap_servers="localhost:9092",
    serializer=lambda record: json.dumps(record).encode("utf-8"),
    key_fn=lambda record: str(record["order_id"]).encode("utf-8"),
)

summary = await (
    Pipeline(source)
    .build(sink, config=DeliveryConfig(batch_size=100))
    .run(max_records=1_000)
)
```

## Secure client configuration

Prefer `KafkaSecurityConfig` for new code. It validates protocol/SASL/TLS
combinations before `aiokafka` sees them.

```python
from pydantic import SecretStr

from agora_plugins.kafka import KafkaSASLConfig, KafkaSecurityConfig, KafkaTLSConfig


security = KafkaSecurityConfig(
    security_protocol="SASL_SSL",
    tls=KafkaTLSConfig(cafile="/etc/ssl/certs/ca.pem"),
    sasl=KafkaSASLConfig(
        mechanism="SCRAM-SHA-512",
        username="etl",
        password=SecretStr("secret"),
    ),
)
```

Supported security protocols are `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and
`SASL_SSL`. Supported SASL mechanisms include `PLAIN`, `SCRAM-SHA-256`,
`SCRAM-SHA-512`, `OAUTHBEARER`, and `GSSAPI`.

## Schema registry

Schema registry serializers are lifecycle-aware: the runtime opens them before
use, resolves or registers the schema, then calls them as serializers.

```python
from agora_plugins.kafka import (
    AvroSchemaRegistryDeserializer,
    AvroSchemaRegistrySerializer,
    ConfluentSchemaRegistryClient,
    KafkaSink,
    KafkaSource,
)


registry = ConfluentSchemaRegistryClient("http://localhost:8081")

schema = {
    "type": "record",
    "name": "OrderEvent",
    "fields": [
        {"name": "order_id", "type": "long"},
        {"name": "status", "type": "string"},
    ],
}

source = KafkaSource(
    topics=["orders.raw"],
    bootstrap_servers="localhost:9092",
    group_id="orders-avro",
    deserializer=AvroSchemaRegistryDeserializer(registry_client=registry),
)

sink = KafkaSink(
    topic="orders.validated",
    bootstrap_servers="localhost:9092",
    serializer=AvroSchemaRegistrySerializer(
        registry_client=registry,
        subject="orders.validated-value",
        schema=schema,
        auto_register="missing_subject",
    ),
)
```

`auto_register` accepts the schema auto-register modes exported by the package.
For governed production topics, prefer an explicit mode such as
`"missing_subject"` or disabled registration rather than silently mutating
schemas in every environment.

## DLQ and poison records

Use `KafkaDLQSink` when poison records should stay inside Kafka:

```python
from agora.core.types import SourceRecordFailurePolicy
from agora_plugins.kafka import KafkaDLQSink, KafkaPoisonRecordPolicy, KafkaSource


dlq = KafkaDLQSink(
    topic="orders.dlq",
    bootstrap_servers="localhost:9092",
)

source = KafkaSource(
    topics=["orders.raw"],
    bootstrap_servers="localhost:9092",
    group_id="orders-worker",
    deserializer=decode_order,
    on_deserialize_error=SourceRecordFailurePolicy.FAIL_CLOSED,
    poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
    poison_record_sink=dlq,
)
```

`KafkaDLQSink` writes upsert/delete envelopes keyed by a stable storage key.
`KafkaDLQSource` reconstructs the current replayable state by scanning the DLQ
topic, applying deletes, and optionally filtering by `pipeline_id`, `stage`,
and `limit`.

For large DLQ topics, `KafkaDLQSource(compaction_spill_threshold=...)` can spill
compaction state instead of keeping everything in memory. Set
`payload_policy` when redacted or encrypted DLQ payloads are used.

## Observability

Kafka components expose metrics snapshots and Prometheus renderers:

- `KafkaSourceRuntime.metrics_snapshot()`
- `KafkaSourceRuntime.render_prometheus_metrics()`
- `KafkaDLQSink.metrics_snapshot()`
- `KafkaDLQSource.metrics_snapshot()`
- `KafkaDLQPrometheusExporter`

Tracing is opt-in and fail-open:

```python
from agora_plugins.kafka import KafkaOpenTelemetryTracing, KafkaSink


sink = KafkaSink(
    topic="orders.cleaned",
    bootstrap_servers="localhost:9092",
    serializer=encode,
    tracing=KafkaOpenTelemetryTracing(enabled=True),
)
```

If OpenTelemetry is not installed or propagation fails, Kafka processing
continues and logs a warning.

## Production checklist

- Keep `group_id` stable for resumable consumers.
- Leave `enable_auto_commit=False` unless the application explicitly owns the
  weaker offset discipline.
- Keep producer idempotence enabled and use `acks=all`.
- Use `transactional_id` only when the broker/topic setup supports Kafka
  transactions and operators understand transaction timeout behavior.
- Route poison records to `KafkaDLQSink` or another DLQ before unattended
  production runs.
- Use schema-registry serializers for governed topics.
- Treat tracing as deployment policy: it propagates headers and emits spans, so
  the telemetry backend must be trusted for that data.

## Boundaries

Kafka is the right fit when event transport, partitioned throughput,
consumer-group coordination, and replayable topic history are core to the
system design.

It is the wrong fit for one-off imports, simple relational extraction, or teams
that do not want to operate broker/topic semantics.
