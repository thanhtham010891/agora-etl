# Official Bundle

_When to read this: you want the supported first-party integration story and need to know which extras to install._

The official first-party plugin package is `agora-etl-plugins`.

It keeps the core runtime focused while still giving the ecosystem a clear,
supported integration layer.

For the `0.4.x` line, this package covers Redis, Kafka, PostgreSQL, BigQuery,
S3, Anthropic, cron scheduling, and distributed coordination. `agora-etl`
supplies runtime contracts and registries; `agora-etl-plugins` supplies the
backend implementations and official helpers.

## Compatibility

| Package | Supported line |
|---|---|
| `agora-etl` | `>=0.4.6,<1` |
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
pip install "agora-etl-plugins[bigquery]"
pip install "agora-etl-plugins[s3]"
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
| Warehouse table/query extraction and batch table loads | `agora-etl-plugins[bigquery]` | `BigQuerySource`, `BigQuerySink` |
| Dataset prefixes and partitioned object files | `agora-etl-plugins[s3]` | `S3Source`, `S3Sink` |
| Claude completions and structured JSON output | `agora-etl-plugins[anthropic]` | `AnthropicProvider` |
| Cron expressions in `Schedule.cron(...)` | `agora-etl-plugins[cron]` | `Schedule.cron(...)` |
| Multi-worker lease ownership | `agora-etl-plugins[distributed]` | `RedisWorkerCoordinator` |

Use the family pages for the detailed support boundary, operator hooks, and
local validation guidance:

- [Redis](redis.md)
- [Kafka](kafka.md)
- [PostgreSQL](postgresql.md)
- [BigQuery](bigquery.md)
- [S3](s3.md)
- [Anthropic](anthropic.md)
- [Scheduling](scheduling.md)
- [Distributed Coordination](distributed.md)

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
