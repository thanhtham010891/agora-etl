# Official Bundle

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
