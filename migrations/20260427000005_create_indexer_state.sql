CREATE TABLE IF NOT EXISTS indexer_state (
    id              TEXT PRIMARY KEY DEFAULT 'singleton',
    current_ledger  BIGINT      NOT NULL DEFAULT 0,
    latest_ledger   BIGINT      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
