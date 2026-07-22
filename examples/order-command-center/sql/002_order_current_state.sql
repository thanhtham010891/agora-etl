CREATE TABLE IF NOT EXISTS order_current_state (
    kafka_topic TEXT NOT NULL,
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    total_cents INTEGER NOT NULL,
    status TEXT NOT NULL,
    kafka_delivery_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_topic, order_id)
);

-- Keep this baseline migration replay-safe for databases initialized by
-- docker-entrypoint scripts before the migration registry existed.
ALTER TABLE order_event_ledger
    ADD COLUMN IF NOT EXISTS producer_run_id TEXT;

UPDATE order_event_ledger
SET producer_run_id = 'legacy'
WHERE producer_run_id IS NULL;

ALTER TABLE order_event_ledger
    ALTER COLUMN producer_run_id SET NOT NULL;

ALTER TABLE order_current_state
    ADD COLUMN IF NOT EXISTS producer_run_id TEXT;

UPDATE order_current_state
SET producer_run_id = 'legacy'
WHERE producer_run_id IS NULL;

ALTER TABLE order_current_state
    ALTER COLUMN producer_run_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS order_current_state_topic_event_time_idx
    ON order_current_state (kafka_topic, event_time DESC, kafka_delivery_key DESC);

CREATE OR REPLACE FUNCTION project_order_current_state()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    projected_topic TEXT;
BEGIN
    projected_topic := NEW.kafka_metadata::jsonb ->> 'topic';
    IF projected_topic IS NULL THEN
        RAISE EXCEPTION 'Kafka topic is required in kafka_metadata';
    END IF;

    INSERT INTO order_current_state (
        kafka_topic, order_id, customer_id, event_id, event_type, event_version,
        event_time, total_cents, status, kafka_delivery_key, producer_run_id
    ) VALUES (
        projected_topic, NEW.order_id, NEW.customer_id, NEW.event_id, NEW.event_type,
        NEW.event_version, NEW.event_time, NEW.total_cents, NEW.status,
        NEW.kafka_delivery_key, NEW.producer_run_id
    )
    ON CONFLICT (kafka_topic, order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        event_id = EXCLUDED.event_id,
        event_type = EXCLUDED.event_type,
        event_version = EXCLUDED.event_version,
        event_time = EXCLUDED.event_time,
        total_cents = EXCLUDED.total_cents,
        status = EXCLUDED.status,
        kafka_delivery_key = EXCLUDED.kafka_delivery_key,
        producer_run_id = EXCLUDED.producer_run_id
    WHERE (EXCLUDED.event_time, EXCLUDED.kafka_delivery_key) >
          (order_current_state.event_time, order_current_state.kafka_delivery_key);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS order_event_ledger_current_state ON order_event_ledger;
CREATE TRIGGER order_event_ledger_current_state
AFTER INSERT ON order_event_ledger
FOR EACH ROW EXECUTE FUNCTION project_order_current_state();

INSERT INTO order_current_state (
    kafka_topic, order_id, customer_id, event_id, event_type, event_version,
    event_time, total_cents, status, kafka_delivery_key, producer_run_id
)
SELECT DISTINCT ON (kafka_metadata::jsonb ->> 'topic', order_id)
    kafka_metadata::jsonb ->> 'topic', order_id, customer_id, event_id,
    event_type, event_version, event_time, total_cents, status, kafka_delivery_key,
    producer_run_id
FROM order_event_ledger
WHERE kafka_metadata::jsonb ->> 'topic' IS NOT NULL
ORDER BY kafka_metadata::jsonb ->> 'topic', order_id, event_time DESC, kafka_delivery_key DESC
ON CONFLICT (kafka_topic, order_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    event_id = EXCLUDED.event_id,
    event_type = EXCLUDED.event_type,
    event_version = EXCLUDED.event_version,
    event_time = EXCLUDED.event_time,
    total_cents = EXCLUDED.total_cents,
    status = EXCLUDED.status,
    kafka_delivery_key = EXCLUDED.kafka_delivery_key,
    producer_run_id = EXCLUDED.producer_run_id
WHERE (EXCLUDED.event_time, EXCLUDED.kafka_delivery_key) >
      (order_current_state.event_time, order_current_state.kafka_delivery_key);
