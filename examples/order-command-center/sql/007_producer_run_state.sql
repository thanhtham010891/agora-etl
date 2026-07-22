ALTER TABLE order_producer_runs
    ADD COLUMN IF NOT EXISTS publish_state TEXT NOT NULL DEFAULT 'published';

ALTER TABLE order_producer_runs
    ADD COLUMN IF NOT EXISTS failure_detail TEXT;

ALTER TABLE order_producer_runs
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS order_producer_runs_topic_state_published_idx
    ON order_producer_runs (kafka_topic, publish_state, published_at DESC, producer_run_id DESC);
