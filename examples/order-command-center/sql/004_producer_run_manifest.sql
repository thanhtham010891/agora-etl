CREATE TABLE IF NOT EXISTS order_producer_runs (
    producer_run_id TEXT PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    expected_event_count INTEGER NOT NULL CHECK (expected_event_count > 0),
    expected_order_count INTEGER NOT NULL CHECK (expected_order_count > 0),
    published_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS order_producer_runs_topic_published_idx
    ON order_producer_runs (kafka_topic, published_at DESC, producer_run_id DESC);
