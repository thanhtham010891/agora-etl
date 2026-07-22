CREATE TABLE IF NOT EXISTS order_event_ledger (
    kafka_delivery_key TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    customer_id TEXT NOT NULL,
    total_cents INTEGER NOT NULL,
    status TEXT NOT NULL,
    kafka_metadata TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS order_event_ledger_order_id_event_time_idx
    ON order_event_ledger (order_id, event_time);
