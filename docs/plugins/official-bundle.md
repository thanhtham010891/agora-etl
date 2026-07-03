# Official Bundle

_When to read this: you want the supported first-party integration story and need to know which extras to install._

The official first-party plugin package is `agora-etl-plugins`.

It exists to keep the core runtime focused while still giving the ecosystem a
clear, supported integration layer.

For the `0.4.x` production line, this package is the official first-party
backend and helper layer for Redis, Kafka, PostgreSQL, Anthropic, cron
scheduling, and distributed coordination. The core package remains
intentionally smaller: `agora-etl` supplies runtime contracts and registries;
the plugin package supplies backend implementations.

Anthropic completion support is part of this official first-party package
through the `anthropic` extra.

## Compatibility

| Package | Supported line |
|---|---|
| `agora-etl` | `>=0.4.1,<1` |
| `agora-etl-plugins` | `0.4.x` |
| Python | `3.11`, `3.12`, `3.13` |

## Install

Install only the extras you need:

```bash
pip install "agora-etl-plugins[redis]"
pip install "agora-etl-plugins[cron]"
pip install "agora-etl-plugins[distributed]"
pip install "agora-etl-plugins[kafka]"
pip install "agora-etl-plugins[postgres]"
pip install "agora-etl-plugins[anthropic]"
```

Or install everything:

```bash
pip install "agora-etl-plugins[all]"
```

## Quick decision table

| If you need... | Install | First object to try |
|---|---|---|
| Redis Streams, shared state, Redis-backed DLQ, exact dedup, AI cache | `agora-etl-plugins[redis]` | `RedisStreamSource`, `RedisSink`, `RedisBackend` |
| Topic-based ingestion/delivery, schema registry, Kafka DLQ | `agora-etl-plugins[kafka]` | `KafkaSource`, `KafkaSink`, `KafkaDLQSink` |
| SQL extraction, upsert, `COPY`, relational DLQ, schema adapter | `agora-etl-plugins[postgres]` | `PostgresSource`, `PostgresSink`, `PostgresSchemaAdapter` |
| Claude completions and structured JSON output | `agora-etl-plugins[anthropic]` | `AnthropicProvider` |
| Cron expressions in `Schedule.cron(...)` | `agora-etl-plugins[cron]` | `Schedule.cron(...)` |
| Multi-worker lease ownership | `agora-etl-plugins[distributed]` | `RedisWorkerCoordinator` |

## Production install rule

Install the smallest extra set that matches the deployment. `all` is useful for
local evaluation, but production images are easier to audit when they only
include the backend clients they actually need.

## Example install profiles

### Event-driven stack

```bash
pip install "agora-etl-plugins[redis,kafka]"
```

Use this when Redis is the ingest/control plane and Kafka is the durable event
backbone.

### Kafka to operational store stack

```bash
pip install "agora-etl-plugins[kafka,postgres]"
```

Use this when Kafka owns ingestion and PostgreSQL owns queryable operational
state or SQL-backed replay.

### Relational sync stack

```bash
pip install "agora-etl-plugins[postgres,cron]"
```

Use this when jobs are scheduled and PostgreSQL is both the source of truth and
the operational sink.

### Multi-replica scheduled stack

```bash
pip install "agora-etl-plugins[postgres,cron,distributed]"
```

Use this when the same scheduled pipelines may run on more than one worker
replica and exactly one worker should own each run.

## Pick the right family

### Redis

Choose Redis when one fast shared backend needs to cover stream ingestion,
shared state, DLQ replay, dedup, or AI caching.

See: [Redis](redis.md)

### Kafka

Choose Kafka when your pipeline belongs on topics, partitions, and
consumer-group semantics.

See: [Kafka](kafka.md)

### PostgreSQL

Choose PostgreSQL when extract/load behavior is relational, SQL is an operator
tooling strength, or DLQ records should live in a database table.

See: [PostgreSQL](postgresql.md)

### Anthropic

Choose Anthropic when the AI workflow is completion-driven and Claude models
fit the job, especially for enrichment, extraction, validation, translation, or
structured JSON output. In the current plugin line, the default Claude API
model is `claude-haiku-4-5-20251001`, and teams that want a newer unlisted id
must opt in explicitly.

See: [Anthropic](anthropic.md)

### Scheduling

Choose the scheduling plugin when jobs should follow calendar rules rather than
just fixed intervals.

See: [Scheduling](scheduling.md)

### Distributed coordination

Choose distributed coordination when more than one worker instance may contend
for the same scheduled pipeline run.

See: [Distributed Coordination](distributed.md)
