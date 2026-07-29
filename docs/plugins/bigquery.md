# BigQuery Plugins

_When to read this: the pipeline needs BigQuery table/query extraction or
batch-oriented table loads for analytical ETL workflows._

The BigQuery family in `agora-etl-plugins 0.4.x` is a production-ready
first-party dataset backend. It is intentionally narrower than the flagship
Redis/Kafka/PostgreSQL surfaces: the current support claim is for table/query reads, batch
table loads, and the bounded append-only Storage Write path documented below,
not warehouse administration or merge-heavy mutation workflows.

## Maturity card

| Field | Value |
|---|---|
| Support label | Production-ready dataset backend |
| In scope | Table/query reads, batch load-job sink writes, machine-readable readiness hooks, and the bounded append-only `_default`-stream Storage Write path. |
| Out of scope | Warehouse administration, merge-heavy mutation workflows, schema auto-evolution, and treating advanced runtime/session helpers as first-stop onboarding primitives. |
| Required validation gate | `make test-release-gate-bigquery-ga` |
| Operator hooks | `health_snapshot()` and `acceptance_report()` on supported BigQuery source/sink surfaces, plus the local live verification suite. |

## Install

```bash
pip install "agora-etl-plugins[bigquery]"
```

## Public surface

| Component | Kind | Use it for |
|---|---|---|
| `BigQuerySource` | Source | Reading a table or explicit SQL query into row-oriented pipelines. |
| `BigQuerySink` | Sink | Buffering rows and submitting batch load jobs into a target table. |
| `BigQueryStorageWriteSink` | Sink | Production-ready append-only Storage Write API path for low-latency writes into typed table schemas within its bounded contract. |
| `BigQueryConnectionConfig` | Config | Project, location, and credential wiring. |

Advanced BigQuery runtime/session/operator-adjacent helpers are still exported
from the family root for composed integrations, but they are treated as
advanced composed-flow helpers rather than as first-stop BigQuery primitives. The onboarding table
above intentionally stays focused on `BigQuerySource`, `BigQuerySink`, and
`BigQueryStorageWriteSink`.

Entry-points installed by the package:

| Group | Key | Target |
|---|---|---|
| `agora.sources` | `bigquery` | `BigQuerySource` |
| `agora.sinks` | `bigquery` | `BigQuerySink` |
| `agora.sinks` | `bigquery_storage_write` | `BigQueryStorageWriteSink` |

## Recovery boundary

- `BigQuerySource(table=...)` can expose checkpoint reruns only with an explicit
  safe cursor strategy: set `checkpoint_column_is_unique=True` for a genuinely
  unique cursor, or provide `checkpoint_tiebreaker_column` and order by both
  fields. A merely monotonic timestamp is not sufficient.
- A table checkpoint also records a secret-free identity of the resolved table,
  query shape, cursor/order fields, project, and location. Resuming with a
  different identity fails closed by default rather than applying the old
  cursor to another dataset. Set `source_identity_mismatch_policy="reset"` to
  start that input from the beginning, or `"allow"` only when preserving its
  cursor has been independently shown safe.
- `BigQuerySource(query=...)` is always full-rerun.
- `BigQuerySink` writes through batch load jobs; v1 does not promise merge,
  upsert, or schema auto-evolution.
- `BigQueryStorageWriteSink` is part of the production-ready BigQuery family,
  but with a narrower contract: append-only, default-stream only, no
  auto-create/truncate, and only the current protobuf-mappable schema subset is
  supported out of the box. It does not provide exactly-once delivery: an
  ambiguous append timeout followed by replay can duplicate rows. Use a stable
  business key and downstream deduplication when duplicates matter.

### File/Parquet → BigQuery delivery profile

The source side can validate file identity and resume safely at its cursor, but
`BigQuerySink` load jobs and `BigQueryStorageWriteSink` append operations are
not replay-safe upserts. Both therefore declare `replay_safe=False`; a
pipeline with `DeliveryPolicy(require_replay_safe=True)` fails before opening
the sink. Use this profile only when the target is duplicate-tolerant or when
the table has a separately operated business-key dedup/merge step. Never treat
load-job success, a default-stream append, or a stable input cursor as a
distributed exactly-once transaction.

## Quickstart

```python
from agora import DeliveryConfig, Pipeline
from agora_plugins.bigquery import BigQuerySink, BigQuerySource


source = BigQuerySource(
    table="analytics.events",
    checkpoint_column="event_id",
    checkpoint_column_is_unique=True,
    order_by=["event_id"],
    row_mapper=lambda row: {
        "event_id": row["event_id"],
        "status": row["status"],
    },
)

sink = BigQuerySink(
    table="analytics.event_projection",
    row_mapper=lambda record: record,
    batch_size=500,
    write_disposition="append",
)

summary = await (
    Pipeline(source)
    .build(sink, config=DeliveryConfig(batch_size=100))
    .run(max_records=10_000)
)
```

## When to choose BigQuery

Choose BigQuery when:

- the upstream or downstream contract is an analytical table
- teams want SQL/query-job based extraction rather than stream semantics
- append-style batch loads are a better fit than OLTP upserts

## Verification notes

The public package ships unit and contract coverage by default. Public support
claims for BigQuery still expect a required local live GCP run with a real
dataset plus service-account credentials. That live suite verifies:

- `write_disposition="truncate"` followed by append-style batch loads
- checkpoint-aware table resume via an explicitly unique cursor or a composite cursor
- explicit `query=` mode rerunning the full query after resume preparation
- multi-page query reads under small source batch sizes
- denied-dataset isolation for both sink writes and query-mode reads
- record-level error/drop accounting under `LOG_AND_CONTINUE`
- Storage Write typed append round-trips, request-size chunking, and denied-dataset
  isolation within the append-only `_default` stream contract

For local validation, run:

```bash
INTEGRATION_BIGQUERY_PROJECT=...
INTEGRATION_BIGQUERY_DATASET=...
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
make test-release-gate-bigquery-ga
```

`BigQueryStorageWriteSink` is part of that verification gate. The current evidence
also includes unit coverage, a mocked pipeline integration slice, and local
live Storage Write verification for:

- default-stream append success
- request-size guard failures for single oversized rows
- logical flush chunking into multiple append requests under the request-size guard
- unsupported-schema rejection for non-phase-2 field types
- live round-trips for typed rows and denied-dataset isolation
- live round-trips for `JSON`, `GEOGRAPHY`, and repeated scalar arrays inside
  the bounded append-only contract

## Storage Write sink

Use `BigQueryStorageWriteSink` when:

- low-latency append behavior matters more than load-job batching
- the target table already exists
- the schema fits the current supported subset: `STRING`, `BYTES`, `BOOL`,
  `INT64`, `FLOAT64`, `DATE`, `DATETIME`, `TIME`, `TIMESTAMP`, `NUMERIC`,
  `BIGNUMERIC`, `JSON`, `GEOGRAPHY`, and `REPEATED` arrays of those scalar
  types

Current bounded support boundaries:

- writes only to the table's `_default` committed stream
- appends only; no truncate/create/merge contract
- large logical flushes are chunked under the configured request-size guard,
  but a single oversized row still fails closed
- `RECORD` / nested struct columns are intentionally outside the current
  boundary and fail fast during schema validation
- part of the current BigQuery support claim only within this bounded contract

## Operational hooks

- `BigQuerySource.health_snapshot()` and `BigQuerySink.health_snapshot()` expose
  machine-readable readiness state for local operators and release checks.
- `BigQuerySource.acceptance_report(...)` and
  `BigQuerySink.acceptance_report(...)` evaluate the current metrics snapshot
  against explicit thresholds.
- Query-backed reads now advance in bounded batches instead of materializing
  the full result set before the first record is emitted.
- The current support claim stays scoped to dataset ETL behavior; it does not
  expand the v1 boundary into merge/upsert orchestration, schema evolution, or
  table-admin workflows.
