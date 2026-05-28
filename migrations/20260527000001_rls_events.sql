-- Row Level Security policy on events table (defense-in-depth for multi-tenant isolation).
-- Application-level tenant filtering is the primary control; RLS is a second layer.
-- Enable RLS only when the app passes `app.current_tenant_id` via SET LOCAL.

ALTER TABLE events ENABLE ROW LEVEL SECURITY;

-- Superusers / replication bypass RLS by default in Postgres, so this only
-- affects the application role. Adjust the role name to match your deployment.
CREATE POLICY tenant_isolation ON events
    USING (
        tenant_id IS NULL                              -- single-tenant rows visible to all
        OR current_setting('app.current_tenant_id', TRUE) = ''   -- setting not configured → bypass
        OR tenant_id = current_setting('app.current_tenant_id', TRUE)
    );
