ALTER TABLE events ADD COLUMN IF NOT EXISTS ledger_hash TEXT;
CREATE INDEX IF NOT EXISTS idx_events_ledger_hash ON events(ledger_hash);
