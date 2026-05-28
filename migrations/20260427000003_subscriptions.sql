CREATE TABLE IF NOT EXISTS subscriptions (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    callback_url TEXT NOT NULL,
    from_ledger  BIGINT NOT NULL,
    -- last ledger successfully acked by the client
    acked_ledger BIGINT NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'active', -- active | cancelled
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    event_id        UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    ledger          BIGINT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending | delivered | failed
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_delivery_queue_sub_status
    ON delivery_queue(subscription_id, status, next_attempt_at);
