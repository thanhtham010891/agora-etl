# Kafka Plugins

_When to read this: your pipeline boundary is Kafka topics and partitions rather than files or direct database reads._

Use the Kafka family when your pipeline lives on topics, partitions, and
consumer groups rather than on files or direct database reads.

## Install

```bash
pip install "agora-etl-plugins[kafka]"
```

## When Kafka is a good fit

- ingest records from one or more topics
- fan out processed records to downstream topics
- keep producer and consumer behavior async and backpressure-aware
- use schema-registry-backed Avro payloads

## What the Kafka family includes

### Kafka source

`KafkaSource` is an async consumer backed by `aiokafka`.

It supports:

- multiple topics
- explicit consumer groups
- checkpoint-aware resume through topic-partition offsets
- configurable commit cadence
- fail-closed or log-and-continue deserialize error handling

Startup and shutdown paths are cleanup-aware, so failed startup or final commit
problems do not silently skip consumer teardown.

Use it when Agora should sit directly on top of a Kafka consumer flow instead
of polling an API or reading files.

### Kafka sink

`KafkaSink` is an async producer sink.

It is built around a few strong defaults:

- bounded pending acknowledgements
- producer flush on shutdown
- retries around send and flush
- idempotence enabled by default
- `acks=all` enforced when idempotence is on

That makes it a good fit for pipelines where delivery discipline matters more
than chasing the absolute loosest producer settings.

If producer startup fails, serializer lifecycle is rolled back instead of being
left half-open, and the producer object is cleaned up best-effort as well.

Batch writes also stay on a leaner success path, so synchronous serializers do
not pay extra coroutine/inspection churn on every record in a healthy producer
run.

Async callable serializers are also supported, including serializer objects
that expose `open()` and `close()` lifecycle hooks.

### Schema registry helpers

The Kafka family also ships Confluent-compatible schema registry helpers.

This includes:

- a minimal schema registry client
- Avro serializer using Confluent wire format
- Avro deserializer that resolves writer schemas by schema ID

Use these when:

- topics are schema-managed
- different services need the same payload contract
- you want registry-backed Avro without adding another integration layer

Registry subjects are treated as path segments rather than raw URL fragments, so
names that include characters like `/` stay safe and unambiguous.

## Sample

This example consumes JSON payloads from one topic, enriches them, and publishes
the result to another topic.

```python
import json

from agora import Pipeline
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
    linger_ms=5,
    compression_type="gzip",
)

pipeline = Pipeline(source).build(sink)
```

What this shows:

- `KafkaSource` owns the consumer-group edge
- commits are explicit and cadence-controlled
- `KafkaSink` is async, bounded, and idempotence-friendly by default
- record keys are optional but useful when downstream partitioning matters

## Common patterns

### Topic to topic enrichment

- consume raw events from Kafka
- normalize or enrich in middleware
- publish the cleaned record to a downstream topic

### Kafka as the shared event backbone

- ingest from Kafka
- branch to PostgreSQL, files, or APIs through sinks
- keep Kafka as the durable event log

### Schema-governed pipelines

- deserialize Avro using the registry
- transform as Python records
- serialize back with the matching subject contract

## Boundaries

Kafka is the right choice when event transport, partitioned throughput, and
consumer-group coordination are already part of your system design.

It is usually the wrong choice when:

- the pipeline is really a one-off file import
- a single relational query is your source of truth
- the team does not want to operate topic and broker concerns
