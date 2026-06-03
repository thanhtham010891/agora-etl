# Order Reliability Demo

This example is a practical skeleton for demonstrating:

- long-running, plugin-driven pipelines
- shared dedup across runs and workers
- DLQ routing for poison records
- resumable consumption with Redis-backed checkpoints
- Kafka as the event backbone
- PostgreSQL as the operational projection target
- worker health and run summaries

## Story

The demo models a simple order-event flow:

1. A producer pipeline runs every minute and generates a few thousand sample
   order events into Kafka on `demo.orders.raw`.
2. A second pipeline consumes the raw topic, normalizes records, and drops
   duplicates with a shared Redis dedup store.
3. Clean records are published into Kafka on `demo.orders.cleaned`.
4. A third pipeline consumes the clean topic and upserts the latest order
   state into PostgreSQL.
5. Redis is used only for shared dedup keys, checkpoints, and DLQ storage.

This is intentionally not an "exactly once" demo. It is an at-least-once,
operator-friendly, resumable pipeline demo that matches Agora's public runtime
contracts.

## Suggested local flow

From this example directory, start the backing services:

```bash
docker compose -f ../../docker-compose.redis.yaml -f ../../docker-compose.kafka.yml -f ../../docker-compose.postgres.yaml up -d
```

Then continue in the same directory:

```bash
cp agora.env.example agora.env
set -a
source agora.env
set +a
PYTHONPATH=src python -m bootstrap_topics
docker compose -f ../../docker-compose.postgres.yaml exec -T postgres \
  psql -U agora -d agora_test < sql/init.sql
agora worker
```

Run the sample producer separately whenever you want to inject another batch:

```bash
agora run pipelines.sample_producer
```

Kafka UI:

```text
http://127.0.0.1:18080
```

Health endpoints:

- `GET /health`
- `GET /metrics`
- `GET /ready`

Default address:

```text
http://127.0.0.1:8080
```

## Failure drills

Each producer run already injects three useful behaviors:

- a large batch of normal records
- periodic duplicates controlled by `SAMPLE_DUPLICATE_EVERY`
- periodic poison records controlled by `SAMPLE_POISON_EVERY`

Expected behavior:

- raw events first land in Kafka
- the duplicate is dropped by `DedupMiddleware`
- the poison record goes to the Redis DLQ
- the valid records move from raw topic to clean topic, then into PostgreSQL

Default producer batch shape:

- `SAMPLE_RECORDS_PER_RUN=5000`
- `SAMPLE_DUPLICATE_EVERY=250`
- `SAMPLE_POISON_EVERY=1000`

Trigger `pipelines.sample_producer` manually whenever you want another batch.

## Runtime shape

The worker runs only the two long-lived Kafka consumers:

- `orders_normalize`
- `orders_projection`

The consumer pipelines do not stop after a fixed number of records and do not
use an idle-exit window. Each worker process joins its Kafka consumer group
once and keeps the session open, which avoids the repeated leave/join cycle
that can trigger avoidable rebalance churn.

To avoid partial batches sitting in memory forever during low traffic, the
consumer pipelines also use `WORKER_BATCH_FLUSH_INTERVAL_MS` so a single owner
task flushes incomplete sink batches after a short timeout even when the batch
size has not been reached yet.

Because the consumers are long-lived, their visibility comes primarily from
health checks, metrics, Redis-backed checkpoints, Kafka lag, and Kafka UI.

## Next variants

Good follow-up variants once this skeleton is in place:

- replace `sql/init.sql` with `PostgresSchemaAdapter`
- add a third projection pipeline writing a Redis status cache
- add a replay script that reads from `RedisDLQSource`
