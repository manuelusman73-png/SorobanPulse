-- normalization_rules: per-contract transformation pipeline
CREATE TABLE IF NOT EXISTS normalization_rules (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id TEXT NOT NULL,
    pointer     TEXT NOT NULL,          -- JSON Pointer (RFC 6901) to the target field
    transform   TEXT NOT NULL,          -- one of: divide_by_decimals, hex_to_decimal, base64_decode
    params      JSONB NOT NULL DEFAULT '{}',  -- transform-specific params (e.g. {"decimals": 7})
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_normalization_rules_contract_id ON normalization_rules(contract_id);

-- normalized event data stored alongside the original
ALTER TABLE events ADD COLUMN IF NOT EXISTS event_data_normalized JSONB;
