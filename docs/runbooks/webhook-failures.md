# Webhook Failures Runbook

## Symptom
Webhook delivery failures are occurring. The `soroban_pulse_webhook_failures_total` metric is increasing, indicating that webhooks are not being successfully delivered to subscribers after all retry attempts are exhausted.

## Likely Causes
1. **Subscriber endpoint is down**: The webhook subscriber's endpoint is unreachable or returning errors
2. **Network connectivity issues**: Network partition or firewall blocking webhook delivery
3. **Subscriber endpoint is slow**: The endpoint is timing out before the indexer's timeout expires
4. **Invalid webhook configuration**: Incorrect subscriber URL or authentication credentials
5. **Webhook payload is too large**: The payload exceeds the subscriber's size limits
6. **Rate limiting**: The subscriber is rate-limiting webhook requests

## Investigation Steps

### 1. Check webhook failure rate
```bash
promtool query instant 'rate(soroban_pulse_webhook_failures_total[5m])'
```

### 2. Review indexer logs for webhook errors
```bash
kubectl logs -l app=soroban-pulse -c soroban-pulse --tail=200 | grep -i "webhook\|delivery\|subscriber"
```

### 3. Check subscriber endpoint health
```bash
# Test connectivity to subscriber endpoint
curl -v -X POST https://subscriber-endpoint.example.com/webhooks \
  -H "Content-Type: application/json" \
  -d '{"test": true}'
```

### 4. Check webhook configuration
```bash
# Query the database for webhook subscriptions
psql $DATABASE_URL -c "SELECT id, url, status, last_error FROM webhooks LIMIT 10;"
```

### 5. Monitor network connectivity
```bash
# From the indexer pod
kubectl exec -it <pod-name> -- bash
curl -v https://subscriber-endpoint.example.com/health
```

## Resolution Steps

### If subscriber endpoint is down
- Contact the subscriber to restore their endpoint
- Temporarily disable the webhook subscription
- Update the webhook URL to a working endpoint

### If network connectivity is the issue
- Check firewall rules and security groups
- Verify DNS resolution of the subscriber endpoint
- Check for network latency or packet loss

### If the endpoint is slow
- Increase the webhook timeout in the indexer configuration
- Ask the subscriber to optimize their endpoint performance
- Implement async webhook processing on the subscriber side

### If webhook configuration is invalid
- Verify the subscriber URL is correct
- Check authentication credentials (API keys, tokens)
- Test the webhook manually with `curl`

### If payload is too large
- Reduce the amount of data in the webhook payload
- Implement payload compression
- Ask the subscriber to increase their size limits

### If rate limiting is occurring
- Reduce the webhook delivery frequency
- Implement exponential backoff for retries
- Contact the subscriber to increase rate limits

## Prevention
- Monitor webhook failure rate continuously
- Set up alerts for webhook delivery failures
- Implement webhook retry logic with exponential backoff
- Test webhook endpoints regularly
- Implement webhook signature verification for security
- Document webhook payload format and size limits
- Provide webhook testing tools for subscribers
