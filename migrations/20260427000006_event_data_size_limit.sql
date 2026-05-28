-- Defense-in-depth: reject event_data larger than 64 KiB at the DB level.
-- The application enforces MAX_EVENT_DATA_BYTES before INSERT; this constraint
-- is a secondary guard against direct writes that bypass the application layer.
ALTER TABLE events
    ADD CONSTRAINT event_data_size_limit
    CHECK (octet_length(event_data::text) <= 65536);
