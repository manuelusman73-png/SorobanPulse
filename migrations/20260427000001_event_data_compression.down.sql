-- Revert event_data compression to the PostgreSQL default (pglz).
DO $$
BEGIN
    IF (SELECT current_setting('server_version_num')::int) >= 140000 THEN
        ALTER TABLE events ALTER COLUMN event_data SET COMPRESSION pglz;
    END IF;
END;
$$;
