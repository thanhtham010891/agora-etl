CREATE TABLE IF NOT EXISTS public.order_projection (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    status TEXT NOT NULL,
    total_cents BIGINT NOT NULL,
    currency TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    source TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL
);
