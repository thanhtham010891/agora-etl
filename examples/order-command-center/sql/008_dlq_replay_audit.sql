CREATE TABLE IF NOT EXISTS order_dlq_replay_requests (
    replay_id TEXT PRIMARY KEY,
    dlq_record_id BIGINT NOT NULL,
    kafka_topic TEXT NOT NULL,
    producer_run_id TEXT NOT NULL UNIQUE,
    corrected_event_id TEXT NOT NULL,
    corrected_payload JSONB NOT NULL,
    corrected_payload_sha256 TEXT NOT NULL,
    change_ticket TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('publishing', 'published', 'failed')),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    failure_detail TEXT
);

CREATE INDEX IF NOT EXISTS order_dlq_replay_requests_dlq_requested_idx
    ON order_dlq_replay_requests (dlq_record_id, requested_at DESC, replay_id DESC);

CREATE TABLE IF NOT EXISTS order_dlq_replay_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    replay_id TEXT NOT NULL REFERENCES order_dlq_replay_requests (replay_id),
    event_type TEXT NOT NULL CHECK (event_type IN ('requested', 'published', 'failed', 'reconciled')),
    details JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS order_dlq_replay_audit_replay_recorded_idx
    ON order_dlq_replay_audit (replay_id, recorded_at, audit_id);

CREATE OR REPLACE FUNCTION reject_dlq_replay_audit_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'order_dlq_replay_audit is append-only';
END;
$$;

DROP TRIGGER IF EXISTS order_dlq_replay_audit_immutable ON order_dlq_replay_audit;
CREATE TRIGGER order_dlq_replay_audit_immutable
BEFORE UPDATE OR DELETE ON order_dlq_replay_audit
FOR EACH ROW EXECUTE FUNCTION reject_dlq_replay_audit_mutation();
