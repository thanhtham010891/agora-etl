# Order Command Center

A runnable Agora reference deployment for operating an order-event pipeline.
Kafka is the immutable event log; PostgreSQL is the durable ledger and
current-state projection; Redis is a disposable serving cache. Prometheus and
Grafana are the observability surface.

```text
producer → Kafka → postgres-worker → PostgreSQL ledger/current state
                 └→ redis-worker    → Redis current-order cache
workers → native Agora /metrics, /health, /ready → Prometheus → Grafana
```

Each branch above is a separate deployable projection runtime, not two
hand-written polling loops.  A runtime owns exactly one native Agora
`WorkerPool`, one `ScheduledPipeline`, and one shared `MetricsCollector` /
`HealthServer` pair.  The shared runtime contract gives both workers the same
graceful shutdown, exponential retry/backoff, bounded consecutive-error policy
and structured run report without coupling PostgreSQL durability to Redis.

This is deliberately not a bespoke dashboard or a generic control plane.
Grafana owns charts, alerting and SQL drill-down; the example keeps only its
pipeline, operational CLI and backend data.

## Code layout

```text
src/order_command_center/
├── pipelines/
│   ├── base.py       # typed invocation, runtime-spec and CLI contract
│   ├── postgres.py   # Kafka → PostgreSQL ledger/current-state mapping
│   └── redis.py      # Kafka → Redis serving-cache mapping
├── runtime.py        # native WorkerPool lifecycle, retry and run reporting
├── settings.py       # validated environment configuration and table names
├── contracts.py      # versioned order-event contract and lineage mapping
└── producer.py, verify.py, reconcile.py, dlq.py, migrate.py
```

To add a projection, create a focused module under `pipelines/`, build its
source/sink there, and invoke `execute_projection`. It receives the same typed
runtime options, health/metrics wiring and retry policy without sharing a
process with another sink. Pipeline identity, consumer group, observable
mapper name, table names, topic topology and operational limits are all
environment-backed settings; database identifiers are validated before they
can be interpolated into SQL.

## Delivery model

- Kafka delivery is **at least once**. A worker acknowledges only after its
  sink flush completes.
- PostgreSQL uses the Kafka delivery coordinate as an idempotency key. A
  replay after a crash produces one durable ledger row per Kafka record.
- Every ledger row also stores `kafka_topic`, `kafka_partition` and numeric
  `kafka_offset`. Latest state is ordered by business time, then the numeric
  coordinate; string sorting never decides Kafka offset order.
- `order_id` is the Kafka key: order is preserved per order/partition, never
  globally.
- Redis is a seven-day cache by default, not the source of truth. PostgreSQL
  is used for durable current state and cache recovery.
- PostgreSQL and Redis are independent consumer groups. There is no
  distributed transaction and convergence must be observed separately.

## Contract compatibility

The source accepts order-event versions `1` and `2`, timezone-aware event
timestamps, and the valid lifecycle pairs `created/paid/packed`.

- V1 is preserved for existing producers. It has no `fulfillment_channel` on
  the wire; the source normalizes it to the explicit canonical value
  `standard` before either projection sees it.
- V2 requires `fulfillment_channel` to be `delivery` or `pickup`. The value is
  retained in the PostgreSQL ledger/current state and the Redis cache.
- V1 and V2 may share a topic and consumer group during a rolling upgrade.
  Unknown versions, a V2 event without the required field, and invalid
  lifecycle payloads are poison records routed to `agora_dlq`.

Publish a version deliberately when demonstrating or staging an upgrade:

```bash
make producer ORDER_EVENT_VERSION=1
make producer ORDER_EVENT_VERSION=2
```

The compatibility policy is additive-only within a major event line: a
consumer must accept every supported older version and normalize it before the
projection boundary. Removing/renaming a field or introducing V3 requires a
new compatibility decision and migration; unsupported versions never receive
best-effort coercion.

## Run locally

```bash
cd packages/agora/examples/order-command-center
make setup
make up
make producer ORDER_COUNT=25
```

Open Grafana at <http://localhost:13000> (`admin` / `admin`). The provisioned
**Order Command Center** dashboard uses Prometheus for runtime metrics and the
PostgreSQL datasource for producer-run and DLQ tables.

Run `make help` for the current command reference. It is generated directly
from the targets, so it cannot drift from the supported local workflows.

`make producer` only publishes to Kafka; the independent workers continue
in the background. Use `make verify` to inspect the latest completed batch, or
`make smoke ORDER_COUNT=25` when the walkthrough must wait for PostgreSQL and
Redis convergence. `make up` is silent when the expected stack is already
running and its Compose/source/migration inputs are unchanged. It rebuilds and
reconciles the workers only on first start, a missing service, or changed
inputs, so the running projection contract cannot lag source or migration
changes.

`make up` starts Kafka, PostgreSQL, Redis, migration, two independent Agora
projection workers, Prometheus, Grafana, and standard Kafka/PostgreSQL/Redis
exporters. Prometheus reaches each worker through the Docker network:

```text
postgres-worker:8080/metrics
redis-worker:8080/metrics
```

Those endpoints are Agora's native `HealthServer`, backed by the same
`MetricsCollector` used by the projection process. Each exposes `/metrics`,
`/health`, and `/ready`; there is no hand-written metrics endpoint. The local
Compose stack protects them with a demo bearer token and keeps their ports
inside the Docker network. The checked-in value in
`observability/secrets/metrics-token` is strictly local-demo material; replace
that file and the worker `METRICS_AUTH_TOKEN` together with a secret manager in
any real deployment.

`/health` is used as the container liveness check and reports the collector's
pipeline state. `/ready` is Agora's stricter aggregate readiness endpoint: it
returns 200 only after a clean completed run and while the collector is not
degraded or failing. Prometheus independently confirms that each native
metrics endpoint is scrapeable.

## Projection runtime and failure policy

The two service roles use the same `ProjectionRuntime` but never share a
`WorkerPool`. For each role, Agora creates a fresh pipeline for every bounded
Kafka poll run, records its outcome, and retries a failed run with exponential
backoff. `PROJECTION_ERROR_BACKOFF_SECONDS` sets the first retry delay;
`PROJECTION_MAX_CONSECUTIVE_ERRORS` bounds retries before the worker exits and
Compose restarts only that role.

Runs that consume records and every failure emit an immediate JSON log event
with projection name, consumer group, run number, record outcomes and error
type. Empty continuous poll cycles are intentionally coalesced into one
`projection_idle` heartbeat every `PROJECTION_IDLE_LOG_INTERVAL_SECONDS`
(60 seconds by default). This keeps logs actionable without hiding an idle
worker; native metrics retain cumulative run, record, writer-flush, checkpoint
and DLQ-failure counters for the same pipeline identity.

## What to observe

Grafana provides these signals without scanning Redis keys:

- Agora pipeline active-session, throughput, record outcome, run duration and
  error metrics.
- Kafka exporter consumer-group lag and topic/broker metrics.
- PostgreSQL and Redis exporter health metrics.
- Latest producer manifests and `agora_dlq` records through PostgreSQL table
  panels.

Prometheus provisions worker-unavailable and errored-record alert rules. Wire
an Alertmanager receiver in a real deployment; the local example leaves alert
delivery unconfigured intentionally.

For durable lineage, use the Grafana PostgreSQL datasource to follow:

```text
order_id / event_id / Kafka coordinate
  → order_event_ledger
  → order_current_state
  → agora_dlq (when source validation failed)
```

For one exact cache key or a bounded verification run, use the operational
commands rather than a Redis scan:

```bash
make verify
make dlq
```

`make verify` deliberately does not publish data. It validates the newest
completed producer run for the configured topic; on a fresh stack, run
`make producer` first.

`order-demo-convergence` reads Kafka committed offsets and partition end offsets
directly. It is the deterministic companion to Grafana's lag chart when an
operator needs a precise decision rather than waiting for the next scrape:

```bash
order-demo-convergence
order-demo-convergence --require-zero
```

## DLQ triage and corrected replay

Poison records stay in PostgreSQL as evidence; this example never deletes a
DLQ row after a repair. Inspect one record, including its previous replay
attempts and append-only audit history:

```bash
order-demo-dlq list
order-demo-dlq show <dlq-record-id>
```

An operator supplies a corrected version of the event in a JSON file. A
ticket/change reference and a written reason are mandatory. The ticket is the
evidence that an external approval workflow authorized the action; Agora does
not embed approval policy or user management in a worker.

```bash
# No side effects: schema validation, source-record existence, and payload hash.
order-demo-dlq replay <dlq-record-id> \
  --payload-file corrected-event.json \
  --ticket CHG-1234 \
  --reason "customer_id was repaired from the source system"

# Creates a durable request, writes immutable audit entries, then publishes.
order-demo-dlq replay <dlq-record-id> \
  --payload-file corrected-event.json \
  --ticket CHG-1234 \
  --reason "customer_id was repaired from the source system" \
  --execute
```

The replay gets its own `producer_run_id` (`dlq_replay_...`), which joins the
new PostgreSQL ledger rows back to the immutable replay request and its source
DLQ record. PostgreSQL and Kafka cannot share one transaction, so replay is
honestly **at least once** across those systems. If a process dies after Kafka
acknowledgement but before request completion, the request remains visibly
`publishing`; it is never silently called successful. Grafana exposes records
awaiting triage and the append-only replay audit.

After PostgreSQL has consumed the corrected Kafka event, reconcile a stranded
request without publishing again:

```bash
order-demo-dlq reconcile replay_...
# or: make dlq-reconcile REPLAY_ID=replay_...
```

Reconciliation accepts only a `publishing` request with exactly one ledger row
for its replay `producer_run_id` and corrected `event_id`. It then appends a
`reconciled` audit event and marks the request `published`. Missing or
duplicate ledger proof leaves the request unchanged for investigation. Grafana
lists every request awaiting that action and its current age.

## Producer manifest recovery

Before Kafka publishing, the producer reserves an `order_producer_runs`
manifest in `publishing` state. Successful Kafka acknowledgement marks it
`published`, which makes it eligible for verification.

If Kafka succeeds but PostgreSQL is unavailable while updating that state, the
producer prints the stable `producer_run_id`. Once the PostgreSQL projection
has caught up, recover without republishing:

```bash
make reconcile PRODUCER_RUN_ID=run_...
make verify
```

Reconciliation checks the expected ledger event/order counts before finalizing
the manifest. A failed or partial Kafka publishing attempt remains failed; it
is never silently treated as a completed run.

## Failure drills

```bash
make drill-crash
```

The dedicated PostgreSQL worker is hard-terminated after a successful database
flush and before Kafka acknowledgement. Re-running the same consumer group replays the
delivery; the numeric Kafka coordinate and PostgreSQL primary key ensure one
ledger row. `make verify` confirms the durable/Redis projection convergence.

For the operationally complete version of that drill, run:

```bash
make drill-worker-restart
```

It creates an isolated topic and two consumer groups, hard-crashes the
PostgreSQL projection after a durable flush, restarts that exact group, then
builds the Redis projection. The drill passes only after both groups' committed
offsets reach their Kafka partition end offsets, the PostgreSQL delivery ledger
has no duplicate coordinate, and Redis matches durable current state. Grafana
alerts when the persistent Compose projection groups retain lag for five
minutes.

## Recovery release gate

The executable recovery contracts run against the real Compose backends, not
mocks. Each test creates an isolated Kafka topic and consumer group, so it
does not disturb the persistent demo workers:

```bash
make test-integration
make test-release-gate
```

The gate proves the two recovery boundaries that define this example:

- PostgreSQL survives a sink-flush-before-Kafka-acknowledgement crash, then
  replay produces one row for every Kafka delivery coordinate.
- Redis can be deleted and rebuilt by a fresh consumer group from the same
  immutable event log, while PostgreSQL durable state remains authoritative.

Useful operations:

```bash
make status
make logs
make test
make stop  # keep local data
make down  # remove local volumes and all demo data
```

`make down` is destructive: it deletes the Docker volumes holding this local
Kafka/PostgreSQL/Redis demo data.
