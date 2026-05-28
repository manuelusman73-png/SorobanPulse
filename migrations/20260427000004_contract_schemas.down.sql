-- Drop schema_valid index
DROP INDEX IF EXISTS idx_events_schema_valid;

-- Drop schema_valid column from events table
ALTER TABLE events
DROP COLUMN IF EXISTS schema_valid;

-- Drop contract_schemas table
DROP TABLE IF EXISTS contract_schemas;
