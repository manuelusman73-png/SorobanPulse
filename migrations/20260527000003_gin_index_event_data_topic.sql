-- Add GIN index on event_data for efficient JSONB topic filtering
CREATE INDEX IF NOT EXISTS idx_events_event_data_topic_gin 
ON events USING GIN (event_data -> 'topic');
