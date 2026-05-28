# Database Connection Pool Exhaustion Runbook

## Symptom
The Soroban Pulse database connection pool has reached its maximum capacity. The `soroban_pulse_db_pool_size` metric equals `soroban_pulse_db_pool_max`, and new requests may be queued or rejected with connection timeout errors.

## Likely Causes
1. **Slow database queries**: Long-running queries are holding connections open
2. **Connection leaks**: Connections are not being properly returned to the pool
3. **Insufficient pool size**: `DB_MAX_CONNECTIONS` is too low for the current load
4. **Database performance degradation**: The database is slow, causing queries to take longer
5. **Spike in traffic**: Sudden increase in API requests consuming all available connections

## Investigation Steps

### 1. Check current pool status
```bash
promtool query instant 'soroban_pulse_db_pool_size'
promtool query instant 'soroban_pulse_db_pool_idle'
promtool query instant 'soroban_pulse_db_pool_max'
```

### 2. Connect to the database and check active connections
```bash
psql $DATABASE_URL

# Check total connections
SELECT count(*) as total_connections FROM pg_stat_activity;

# Check connections by state
SELECT state, count(*) FROM pg_stat_activity GROUP BY state;

# Check long-running queries
SELECT pid, usename, application_name, state, query_start, query 
FROM pg_stat_activity 
WHERE state != 'idle' 
ORDER BY query_start ASC;
```

### 3. Check for connection leaks in the application
```bash
# Review recent logs for connection errors
kubectl logs -l app=soroban-pulse -c soroban-pulse --tail=100 | grep -i "connection\|pool\|timeout"
```

### 4. Monitor query performance
```bash
# Check slow queries
SELECT query, mean_exec_time, calls, max_exec_time 
FROM pg_stat_statements 
WHERE mean_exec_time > 100 
ORDER BY mean_exec_time DESC LIMIT 10;

# Check table sizes
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) 
FROM pg_tables 
WHERE schemaname = 'public' 
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## Resolution Steps

### Immediate actions
1. **Increase pool size** (temporary fix):
   ```bash
   kubectl set env deployment/soroban-pulse DB_MAX_CONNECTIONS=20
   kubectl rollout restart deployment/soroban-pulse
   ```

2. **Kill long-running queries** (if safe):
   ```bash
   psql $DATABASE_URL -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query_start < now() - interval '10 minutes';"
   ```

### Long-term fixes

**If queries are slow:**
- Add indexes on frequently queried columns
- Run `ANALYZE` to update table statistics
- Review and optimize slow queries
- Consider partitioning large tables

**If there are connection leaks:**
- Review application code for proper connection cleanup
- Check for unclosed transactions
- Implement connection pooling middleware (e.g., PgBouncer)

**If traffic is spiking:**
- Scale the application horizontally by adding more replicas
- Implement rate limiting to prevent overload
- Use caching to reduce database queries

**If the database is slow:**
- Scale the database vertically (more CPU/RAM)
- Upgrade to a faster storage backend
- Consider read replicas for read-heavy workloads

## Prevention
- Set `DB_MAX_CONNECTIONS` based on expected peak load
- Monitor connection pool utilization regularly
- Set up alerts for pool exhaustion
- Implement query timeouts to prevent long-running queries
- Use connection pooling middleware for better resource management
- Load test the application to determine optimal pool size
