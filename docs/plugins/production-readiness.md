# Plugin Production Readiness

_When to read this: you are deciding whether `agora-etl-plugins` is ready for a production deployment, or you need the supported compatibility and operational boundaries for the official bundle._

`agora-etl-plugins` is the official first-party integration bundle for Agora.
The production-ready surface is intentionally centered on a small set of
flagship backend families rather than a large connector catalog.

## Compatibility Matrix

| Package | Supported line | Notes |
|---|---|---|
| `agora-etl-plugins` | `0.4.x` | Official plugin bundle release line. |
| `agora-etl` | `>=0.4.0,<1` | Required runtime contract for the plugin bundle. |
| Python | `3.11`, `3.12` | Matches package classifiers and CI coverage. |
| Redis client | `redis>=7.0,<8` | Used by Redis, distributed coordination, Redis DLQ, state, dedup, and cache surfaces. |
| Kafka client | `aiokafka>=0.11,<1` | Used by Kafka source, sink, DLQ, and Kafka-backed runtime helpers. |
| PostgreSQL client | `psycopg[binary]>=3.1,<4`, `psycopg_pool>=3.2,<4` | Used by PostgreSQL source, sink, schema adapter, DLQ surfaces, and pooled sink writes. |

## Maturity Labels

| Surface | Maturity | Support boundary |
|---|---|---|
| PostgreSQL source, sink, schema adapter, and DLQ | Includes SQL, `COPY`, `COPY + MERGE`, checkpoint-aware source reads, schema-drift controls, HA routing tests, and SQL-backed DLQ/replay. |
| Kafka source, sink, schema registry helpers, and DLQ | Includes consumer-group semantics, idempotent producer defaults, secure schema-registry paths, transactional delivery hooks, replay, and DLQ policy controls. |
| Redis core: stream source, sink, DLQ, and state backend | Includes stream resume/reclaim, TLS/ACL, Sentinel, Cluster, Redis Stack matrix coverage, shared DLQ, `set_if_absent`, and `compare_and_set` state atomicity checks. |
| Distributed coordination | Redis-backed lease/fencing semantics are covered for the default single-Redis lease model and opt-in Redlock-style quorum across independent Redis masters. |
| Cron scheduling helpers | Official | Unit/contract covered. No external service is required. |
| Anthropic provider | Official | Completion and structured-output provider surface. Live API behavior should be validated in application environments that own API cost and model policy. |
| Redis embedding dedup and Redis AI cache | Official helper surfaces | Useful production helpers, but they should not inherit the same maturity claim as Redis Streams, Redis DLQ, Redis state, or Redis sink. |

## Release Gates

A release candidate should pass these gates before publishing:

```bash
make ci
make test-release-gate-base
make test-release-gate-postgres
make test-release-gate-kafka-secure
make test-release-gate-kafka-cluster
make test-release-gate-redis
make test-installed-package-smoke
```

These gates cover:

- non-integration lint, formatting, type checking, unit tests, and coverage
- Redis/Kafka base wedge behavior
- PostgreSQL HA reconnect, replica routing, staleness guard, and Kafka to PostgreSQL failover
- Kafka secure schema-registry paths, auth failure, mTLS failure, Avro, JSON Schema, and Protobuf
- Kafka multi-broker failover, rolling restart, coordinator failover, replay windows, and live tail
- Redis TLS/ACL, Sentinel, Cluster, Redis Stack, and Redlock coordinator topology behavior
- wheel build, installed package metadata, public extras, public imports, and entry-points

## Deployment Notes

### Kafka

- Use stable consumer-group IDs for resumable production pipelines.
- Keep idempotence enabled unless a deployment has a clear reason to loosen producer guarantees.
- Use `acks=all` with idempotent production.
- For governed topics, prefer schema-registry serializers with `auto_register="disabled"` or `auto_register="missing_subject"` instead of ad hoc mutation.
- Use `KafkaOpenTelemetryTracing` with `KafkaSource(..., tracing=...)` and `KafkaSink(..., tracing=...)` when deployments need W3C trace-context propagation through Kafka headers.
- Route poison records to a Kafka or PostgreSQL DLQ when operators need replay.
- Use `DLQPayloadPolicy.redacted(...)` or `DLQPayloadPolicy.encrypted(...)` for sensitive DLQ payloads.

### PostgreSQL

- Use `copy_merge` for large upsert-heavy loads and `copy` only for append-only writes.
- Keep `conflict_key` backed by a real primary key or unique constraint.
- Use `PostgresSchemaAdapter` only when automatic table creation or additive schema drift is intended.
- `PostgresTLSConfig`, `PostgresConfig`, and `PostgresPluginConfig` default to `sslmode="verify-full"`; use an explicit DSN `sslmode` only when a deployment intentionally owns that override.
- Monitor `*_sink_latency_seconds` histogram series for PostgreSQL sink connect, pool acquire, and flush latency.
- Use PostgreSQL DLQ when operators need SQL-level inspection, audit, backup, or controlled replay.
- Use `DLQPayloadPolicy.redacted(...)` or `DLQPayloadPolicy.encrypted(...)` for sensitive DLQ payloads.

### Redis

- Use Redis Streams with consumer groups when records need resumable ingestion.
- Enable `ack_on_success` for normal stream processing.
- Configure reclaim windows deliberately; too-short idle windows can cause unnecessary reclaim churn.
- Use Redis Sentinel or Cluster deployment patterns already validated by the integration matrix.
- Use `RedisBackend.compare_and_set(...)` when multiple workers need conflict-detecting state updates.
- Treat Redis embedding dedup as a small-to-medium working-set helper, not a vector database replacement.
- Use `DLQPayloadPolicy.redacted(...)` or `DLQPayloadPolicy.encrypted(...)` for sensitive Redis DLQ payloads.

### Distributed Coordination

- Use distributed coordination when the same scheduled pipelines run on more than one worker replica.
- Prefer fail-safe behavior when duplicate runs are more dangerous than skipped runs.
- Treat `fallback_to_local` as an explicit availability-over-exclusivity tradeoff.
- The default lease model uses one Redis primary with TTL leases and fencing tokens.
- Use `redlock_redis_urls` with at least three independent Redis masters when a deployment needs majority-quorum lease acquisition and renewal.
- Keep downstream fencing checks enabled for critical writes; Redlock reduces split-brain risk, while fencing tokens remain the last line of write safety.

## DLQ And Replay Runbook

1. Identify the failed stage and backend.
2. Inspect the DLQ store with the matching source:
   - `KafkaDLQSource` for Kafka DLQ topics
   - `PostgresDLQSource` for SQL-backed DLQ tables
   - `RedisDLQSource` for Redis-backed shared DLQ records
3. Confirm the downstream issue is fixed before replay.
4. If payload encryption is enabled, configure the matching decryptor policy before replay.
5. Replay a bounded batch first.
6. Acknowledge DLQ records only after the replayed records have been durably processed.
7. Keep the original DLQ data until the recovery result is verified by downstream checks.

## Known Boundaries

- Kafka OpenTelemetry tracing is opt-in and currently covers source header extraction, sink header injection, deserialize, commit, and produce spans.
- Kafka Protobuf schema binding uses the built-in tokenizer/block parser and should still be validated against the schemas used in each production deployment.
- PostgreSQL sink pooled writes prefer `psycopg_pool.AsyncConnectionPool` when the `postgres` extra is installed, with a legacy fallback for older environments.
- PostgreSQL sink latency histograms cover connect, pool acquire, and flush; statement-level execute, COPY, and upsert sub-operation histograms are deeper operational follow-up.
- `RedisBackend.compare_and_set(...)` is Redis-specific until the core `StateBackend` contract grows a backend-neutral CAS method.
- Redlock quorum requires independent Redis masters. Redis replicas, Sentinel members for a single primary, or Cluster shards for unrelated hash slots are not interchangeable quorum nodes.
