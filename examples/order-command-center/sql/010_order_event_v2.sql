ALTER TABLE order_event_ledger
    ADD COLUMN IF NOT EXISTS fulfillment_channel TEXT;

UPDATE order_event_ledger
SET fulfillment_channel = 'standard'
WHERE fulfillment_channel IS NULL;

ALTER TABLE order_event_ledger
    ALTER COLUMN fulfillment_channel SET NOT NULL;

ALTER TABLE order_current_state
    ADD COLUMN IF NOT EXISTS fulfillment_channel TEXT;

UPDATE order_current_state AS state
SET fulfillment_channel = ledger.fulfillment_channel
FROM order_event_ledger AS ledger
WHERE ledger.kafka_delivery_key = state.kafka_delivery_key
  AND state.fulfillment_channel IS NULL;

UPDATE order_current_state
SET fulfillment_channel = 'standard'
WHERE fulfillment_channel IS NULL;

ALTER TABLE order_current_state
    ALTER COLUMN fulfillment_channel SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'order_event_ledger_fulfillment_channel_check'
    ) THEN
        ALTER TABLE order_event_ledger
            ADD CONSTRAINT order_event_ledger_fulfillment_channel_check
            CHECK (fulfillment_channel IN ('standard', 'delivery', 'pickup')) NOT VALID;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION project_order_current_state()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO order_current_state (
        kafka_topic, order_id, customer_id, event_id, event_type, event_version,
        event_time, total_cents, status, fulfillment_channel, kafka_delivery_key,
        kafka_partition, kafka_offset, producer_run_id
    ) VALUES (
        NEW.kafka_topic, NEW.order_id, NEW.customer_id, NEW.event_id, NEW.event_type,
        NEW.event_version, NEW.event_time, NEW.total_cents, NEW.status,
        NEW.fulfillment_channel, NEW.kafka_delivery_key, NEW.kafka_partition,
        NEW.kafka_offset, NEW.producer_run_id
    )
    ON CONFLICT (kafka_topic, order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        event_id = EXCLUDED.event_id,
        event_type = EXCLUDED.event_type,
        event_version = EXCLUDED.event_version,
        event_time = EXCLUDED.event_time,
        total_cents = EXCLUDED.total_cents,
        status = EXCLUDED.status,
        fulfillment_channel = EXCLUDED.fulfillment_channel,
        kafka_delivery_key = EXCLUDED.kafka_delivery_key,
        kafka_partition = EXCLUDED.kafka_partition,
        kafka_offset = EXCLUDED.kafka_offset,
        producer_run_id = EXCLUDED.producer_run_id,
        updated_at = now()
    WHERE (EXCLUDED.event_time, EXCLUDED.kafka_partition, EXCLUDED.kafka_offset) >
          (order_current_state.event_time, order_current_state.kafka_partition,
           order_current_state.kafka_offset);
    RETURN NEW;
END;
$$;

INSERT INTO order_current_state (
    kafka_topic, order_id, customer_id, event_id, event_type, event_version,
    event_time, total_cents, status, fulfillment_channel, kafka_delivery_key,
    kafka_partition, kafka_offset, producer_run_id
)
SELECT DISTINCT ON (kafka_topic, order_id)
    kafka_topic, order_id, customer_id, event_id, event_type, event_version,
    event_time, total_cents, status, fulfillment_channel, kafka_delivery_key,
    kafka_partition, kafka_offset, producer_run_id
FROM order_event_ledger
ORDER BY kafka_topic, order_id, event_time DESC, kafka_partition DESC, kafka_offset DESC
ON CONFLICT (kafka_topic, order_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    event_id = EXCLUDED.event_id,
    event_type = EXCLUDED.event_type,
    event_version = EXCLUDED.event_version,
    event_time = EXCLUDED.event_time,
    total_cents = EXCLUDED.total_cents,
    status = EXCLUDED.status,
    fulfillment_channel = EXCLUDED.fulfillment_channel,
    kafka_delivery_key = EXCLUDED.kafka_delivery_key,
    kafka_partition = EXCLUDED.kafka_partition,
    kafka_offset = EXCLUDED.kafka_offset,
    producer_run_id = EXCLUDED.producer_run_id,
    updated_at = now()
WHERE (EXCLUDED.event_time, EXCLUDED.kafka_partition, EXCLUDED.kafka_offset) >
      (order_current_state.event_time, order_current_state.kafka_partition,
       order_current_state.kafka_offset);
