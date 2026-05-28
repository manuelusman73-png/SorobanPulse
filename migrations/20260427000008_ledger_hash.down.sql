DROP INDEX IF EXISTS idx_events_ledger_hash;
ALTER TABLE events DROP COLUMN IF EXISTS ledger_hash;
