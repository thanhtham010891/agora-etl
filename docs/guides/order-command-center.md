# Flagship example: Order Command Center

The repository includes a complete Kafka, PostgreSQL, and Redis project at
[`examples/order-command-center`](https://github.com/thanhtham010891/agora-etl/tree/main/examples/order-command-center).

It demonstrates the boundary that matters for an event-driven ETL system:
Kafka provides at-least-once delivery; PostgreSQL and Redis make replay safe
through explicit idempotency. PostgreSQL owns the durable event ledger while
Redis owns the replaceable current-order serving view. The two projections use
separate consumer groups and are deliberately not presented as a distributed
transaction.

Follow its [README](https://github.com/thanhtham010891/agora-etl/tree/main/examples/order-command-center#readme)
to run the Docker stack, inspect the two persistent projection workers, and
execute the crash-after-flush recovery drill.

Use `make sample-data ORDER_COUNT=25`, then `make observe`, for a local dashboard that refreshes
the PostgreSQL delivery timeline, Redis current-order view, and DLQ count. The
dashboard is read-only: it does not join a Kafka consumer group or commit any
offsets. It pages from PostgreSQL `order_current_state` and Redis `MGET`s only
that bounded page; Redis is a TTL-bound serving cache rather than the event
history store.
