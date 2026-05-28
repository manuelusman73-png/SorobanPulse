# Indexer Lag Runbook

## Symptom
The Soroban Pulse indexer is falling behind the latest ledger on the Stellar network. The `soroban_pulse_indexer_lag_ledgers` metric exceeds the warning threshold (100 ledgers) or critical threshold (500 ledgers).

## Likely Causes
1. **RPC endpoint issues**: The Soroban RPC endpoint is slow, timing out, or returning errors
2. **Database performance degradation**: Slow INSERT/UPDATE queries or connection pool exhaustion
3. **Network latency**: High latency between the indexer and RPC endpoint
4. **Indexer process overload**: CPU or memory constraints on the indexer pod
5. **Advisory lock contention**: In multi-replica setups, the standby replica may not have acquired the lock

## Investigation Steps

### 1. Check indexer logs
```bash
kubectl logs -l app=soroban-pulse -c soroban-pulse --tail=100 | grep -i "error\|lag\|rpc"
```

### 2. Verify RPC endpoint health
```bash
# Test RPC connectivity
curl -s https://soroban-testnet.stellar.org/health | jq .

# Check RPC error rate
promtool query instant 'rate(soroban_pulse_rpc_errors_total[5m])'
```

### 3. Check database performance
```bash
# Connect to the database
psql $DATABASE_URL

# Check for slow queries
SELECT query, mean_exec_time, calls FROM pg_stat_statements 
WHERE mean_exec_time > 100 
ORDER BY mean_exec_time DESC LIMIT 10;

# Check connection pool status
SELECT count(*) FROM pg_stat_activity;
```

### 4. Check pod resource usage
```bash
kubectl top pod -l app=soroban-pulse
kubectl describe pod -l app=soroban-pulse | grep -A 5 "Limits\|Requests"
```

### 5. Check advisory lock status (multi-replica only)
```bash
psql $DATABASE_URL -c "SELECT * FROM pg_locks WHERE locktype = 'advisory';"
```

## Resolution Steps

### For RPC errors
- Check the RPC endpoint status page
- Switch to a backup RPC endpoint by updating `STELLAR_RPC_URL`
- Increase `INDEXER_POLL_TIMEOUT_SECS` if the endpoint is slow but functional

### For database performance
- Increase `DB_MAX_CONNECTIONS` if the pool is exhausted
- Run `ANALYZE` on the events table to update statistics
- Check for missing indexes on frequently queried columns
- Consider scaling the database vertically or horizontally

### For resource constraints
- Increase CPU/memory limits in the Kubernetes deployment
- Check for memory leaks with `pprof` profiling
- Scale horizontally by adding more replicas

### For advisory lock issues
- Verify the leader replica is healthy: `kubectl logs -l app=soroban-pulse,replica=leader`
- Restart the leader replica to force failover
- Check `INDEXER_LOCK_RETRY_SECS` configuration

## Prevention
- Set up alerts for RPC error rate and database latency
- Monitor database query performance regularly
- Use connection pooling and set appropriate pool sizes
- Implement circuit breakers for RPC calls
- Test failover scenarios regularly in multi-replica setups
