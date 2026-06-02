# Official Bundle

_When to read this: you want the supported first-party integration story and need to know which extras to install._

The official first-party plugin package is `agora-etl-plugins`.

It exists to keep the core runtime focused while still giving the ecosystem a
clear, supported set of integrations.

## Install

Install only the extras you need:

```bash
pip install "agora-etl-plugins[redis]"
pip install "agora-etl-plugins[cron]"
pip install "agora-etl-plugins[distributed]"
pip install "agora-etl-plugins[kafka]"
pip install "agora-etl-plugins[postgres]"
```

Or install everything:

```bash
pip install "agora-etl-plugins[all]"
```

## Quick decision table

| If you need... | Install | First object to try |
|---|---|---|
| Redis Streams, shared state, Redis-backed DLQ | `agora-etl-plugins[redis]` | `RedisStreamSource`, `RedisSink`, `RedisBackend` |
| Topic-based ingestion and delivery | `agora-etl-plugins[kafka]` | `KafkaSource`, `KafkaSink` |
| SQL extraction, upsert, `COPY`, relational DLQ | `agora-etl-plugins[postgres]` | `PostgresSource`, `PostgresSink` |
| Cron expressions in `Schedule.cron(...)` | `agora-etl-plugins[cron]` | `Schedule.cron(...)` |
| Multi-worker lease ownership | `agora-etl-plugins[distributed]` | `RedisWorkerCoordinator` |

## Example install profiles

### Event-driven stack

```bash
pip install "agora-etl-plugins[redis,kafka]"
```

Use this when Redis is the ingest/control plane and Kafka is the durable event
backbone.

### Relational sync stack

```bash
pip install "agora-etl-plugins[postgres,cron]"
```

Use this when jobs are scheduled and PostgreSQL is both the source of truth and
the operational sink.

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

### Scheduling

Choose the scheduling plugin when jobs should follow calendar rules rather than
just fixed intervals.

See: [Scheduling](scheduling.md)

### Distributed coordination

Choose distributed coordination when more than one worker instance may contend
for the same scheduled pipeline run.

See: [Distributed Coordination](distributed.md)
