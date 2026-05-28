CREATE TABLE IF NOT EXISTS contract_abis (
    contract_id TEXT PRIMARY KEY,
    abi         JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE events ADD COLUMN IF NOT EXISTS event_data_decoded JSONB;
