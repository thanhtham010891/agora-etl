ALTER TABLE order_event_ledger
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE order_current_state
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE order_current_state
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS order_event_ledger_topic_created_at_idx
    ON order_event_ledger ((kafka_metadata::jsonb ->> 'topic'), created_at DESC);

CREATE INDEX IF NOT EXISTS order_current_state_topic_updated_at_idx
    ON order_current_state (kafka_topic, updated_at DESC, kafka_delivery_key DESC);

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
        producer_run_id = EXCLUDED.producer_run_id,
        updated_at = now()
    WHERE (EXCLUDED.event_time, EXCLUDED.kafka_delivery_key) >
          (order_current_state.event_time, order_current_state.kafka_delivery_key);
    RETURN NEW;
END;
$$;
