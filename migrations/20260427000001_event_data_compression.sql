-- Set event_data column storage to EXTENDED (enables both compression and out-of-line storage).
-- On PostgreSQL 14+, also set the compression method to lz4 for better performance than pglz.
-- This migration is idempotent and does not require a table rewrite.

DO $$
BEGIN
    -- EXTENDED storage is the default for JSONB but we set it explicitly for clarity.
    ALTER TABLE events ALTER COLUMN event_data SET STORAGE EXTENDED;

    -- lz4 compression is available from PostgreSQL 14+.
    IF (SELECT current_setting('server_version_num')::int) >= 140000 THEN
        ALTER TABLE events ALTER COLUMN event_data SET COMPRESSION lz4;
    END IF;
END;
$$;
