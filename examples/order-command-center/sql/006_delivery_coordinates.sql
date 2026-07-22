ALTER TABLE order_event_ledger
    ADD COLUMN IF NOT EXISTS kafka_topic TEXT;

ALTER TABLE order_event_ledger
    ADD COLUMN IF NOT EXISTS kafka_partition INTEGER;

ALTER TABLE order_event_ledger
    ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;

UPDATE order_event_ledger
SET
    kafka_topic = kafka_metadata::jsonb ->> 'topic',
    kafka_partition = (kafka_metadata::jsonb ->> 'partition')::INTEGER,
    kafka_offset = (kafka_metadata::jsonb ->> 'offset')::BIGINT
WHERE kafka_topic IS NULL OR kafka_partition IS NULL OR kafka_offset IS NULL;

ALTER TABLE order_event_ledger
    ALTER COLUMN kafka_topic SET NOT NULL;

ALTER TABLE order_event_ledger
    ALTER COLUMN kafka_partition SET NOT NULL;

ALTER TABLE order_event_ledger
    ALTER COLUMN kafka_offset SET NOT NULL;

ALTER TABLE order_current_state
    ADD COLUMN IF NOT EXISTS kafka_partition INTEGER;

ALTER TABLE order_current_state
    ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;

UPDATE order_current_state AS state
SET
    kafka_partition = ledger.kafka_partition,
    kafka_offset = ledger.kafka_offset
FROM order_event_ledger AS ledger
WHERE ledger.kafka_delivery_key = state.kafka_delivery_key
  AND (state.kafka_partition IS NULL OR state.kafka_offset IS NULL);

ALTER TABLE order_current_state
    ALTER COLUMN kafka_partition SET NOT NULL;

ALTER TABLE order_current_state
    ALTER COLUMN kafka_offset SET NOT NULL;

CREATE INDEX IF NOT EXISTS order_event_ledger_topic_run_coordinate_idx
    ON order_event_ledger (kafka_topic, producer_run_id, kafka_partition, kafka_offset);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'order_event_ledger_event_status_check'
    ) THEN
        ALTER TABLE order_event_ledger
            ADD CONSTRAINT order_event_ledger_event_status_check
            CHECK (
                (event_type = 'order.created' AND status = 'created') OR
                (event_type = 'order.paid' AND status = 'paid') OR
                (event_type = 'order.packed' AND status = 'packed')
            ) NOT VALID;
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
        event_time, total_cents, status, kafka_delivery_key, kafka_partition,
        kafka_offset, producer_run_id
    ) VALUES (
        NEW.kafka_topic, NEW.order_id, NEW.customer_id, NEW.event_id, NEW.event_type,
        NEW.event_version, NEW.event_time, NEW.total_cents, NEW.status,
        NEW.kafka_delivery_key, NEW.kafka_partition, NEW.kafka_offset, NEW.producer_run_id
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

-- Rebuild legacy current state with numeric Kafka ordering. The earlier
-- migration used the delivery key text as a tie-breaker, which is incorrect
-- once offsets cross a lexical boundary such as 9 -> 10.
INSERT INTO order_current_state (
    kafka_topic, order_id, customer_id, event_id, event_type, event_version,
    event_time, total_cents, status, kafka_delivery_key, kafka_partition,
    kafka_offset, producer_run_id
)
SELECT DISTINCT ON (kafka_topic, order_id)
    kafka_topic, order_id, customer_id, event_id, event_type, event_version,
    event_time, total_cents, status, kafka_delivery_key, kafka_partition,
    kafka_offset, producer_run_id
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
    kafka_delivery_key = EXCLUDED.kafka_delivery_key,
    kafka_partition = EXCLUDED.kafka_partition,
    kafka_offset = EXCLUDED.kafka_offset,
    producer_run_id = EXCLUDED.producer_run_id,
    updated_at = now()
WHERE (EXCLUDED.event_time, EXCLUDED.kafka_partition, EXCLUDED.kafka_offset) >
      (order_current_state.event_time, order_current_state.kafka_partition,
       order_current_state.kafka_offset);
