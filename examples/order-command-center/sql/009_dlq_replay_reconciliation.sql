ALTER TABLE order_dlq_replay_audit
    DROP CONSTRAINT IF EXISTS order_dlq_replay_audit_event_type_check;

ALTER TABLE order_dlq_replay_audit
    ADD CONSTRAINT order_dlq_replay_audit_event_type_check
    CHECK (event_type IN ('requested', 'published', 'failed', 'reconciled'));
