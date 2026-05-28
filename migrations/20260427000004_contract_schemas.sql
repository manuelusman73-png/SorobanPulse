-- Create contract_schemas table for storing JSON Schema definitions per contract
CREATE TABLE IF NOT EXISTS contract_schemas (
    contract_id TEXT PRIMARY KEY,
    schema JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add schema_valid column to events table
ALTER TABLE events
ADD COLUMN IF NOT EXISTS schema_valid BOOLEAN DEFAULT NULL;

-- Index for querying events with schema validation failures
CREATE INDEX IF NOT EXISTS idx_events_schema_valid ON events(schema_valid) WHERE schema_valid = false;

-- Add comment explaining the schema_valid column
COMMENT ON COLUMN events.schema_valid IS 'NULL if no schema registered, TRUE if valid, FALSE if validation failed';
