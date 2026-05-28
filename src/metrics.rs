use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use sqlx::PgPool;

// The local module is also named `metrics`, which shadows the external crate
// of the same name. Use an explicit extern-crate alias to disambiguate.
extern crate metrics as m;

/// SLO-aligned histogram buckets for HTTP request duration (seconds).
const HTTP_DURATION_BUCKETS: &[f64] = &[0.05, 0.1, 0.2, 0.5, 1.0, 5.0];

/// Initialize the Prometheus metrics exporter
pub fn init_metrics() -> PrometheusHandle {
    PrometheusBuilder::new()
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(
                "soroban_pulse_http_request_duration_seconds".to_string(),
            ),
            HTTP_DURATION_BUCKETS,
        )
        .expect("Failed to set histogram buckets")
        .install_recorder()
        .expect("Failed to install Prometheus exporter")
}

/// Record events indexed
pub fn record_events_indexed(count: u64) {
    m::counter!("soroban_pulse_events_indexed_total").increment(count);
}

/// Update the current ledger being processed
pub fn update_current_ledger(ledger: u64) {
    m::gauge!("soroban_pulse_indexer_current_ledger").set(ledger as f64);
}

/// Update the latest ledger from RPC
pub fn update_latest_ledger(ledger: u64) {
    m::gauge!("soroban_pulse_indexer_latest_ledger").set(ledger as f64);
}

/// Update the indexer lag
pub fn update_indexer_lag(lag: u64) {
    m::gauge!("soroban_pulse_indexer_lag_ledgers").set(lag as f64);
}

/// Set the is_leader gauge: 1.0 when this replica holds the advisory lock, 0.0 otherwise.
pub fn record_indexer_is_leader(is_leader: bool) {
    m::gauge!("soroban_pulse_indexer_is_leader").set(if is_leader { 1.0 } else { 0.0 });
}

/// Record an RPC error
pub fn record_rpc_error() {
    m::counter!("soroban_pulse_rpc_errors_total").increment(1);
}

/// Record a validation failure
pub fn record_validation_failure() {
    m::counter!("soroban_pulse_events_validation_failed_total").increment(1);
}

/// Record an oversized event that was skipped due to exceeding MAX_EVENT_DATA_BYTES.
pub fn record_oversized_event() {
    m::counter!("soroban_pulse_events_oversized_total").increment(1);
}

/// Record a duplicate event
pub fn record_duplicate_event() {
    m::counter!("soroban_pulse_events_duplicate_total").increment(1);
}

/// Record an XDR validation failure (issue #267)
pub fn record_xdr_invalid() {
    m::counter!("soroban_pulse_events_xdr_invalid_total").increment(1);
}

/// Record an invalid contract ID (issue #370)
pub fn record_invalid_contract_id() {
    m::counter!("soroban_pulse_events_invalid_contract_id_total").increment(1);
}

/// Record an archive integrity failure (issue #371)
pub fn record_archive_integrity_failure() {
    m::counter!("soroban_pulse_archive_integrity_failures_total").increment(1);
}

/// Update re-encryption progress gauge (issue #372)
pub fn update_reencrypt_progress(remaining: u64) {
    m::gauge!("soroban_pulse_reencrypt_progress").set(remaining as f64);
}

/// Record a re-encryption error (issue #372)
pub fn record_reencrypt_error() {
    m::counter!("soroban_pulse_reencrypt_errors_total").increment(1);
}

/// Record a bloom filter hit (pre-filtered duplicate) (issue #266)
pub fn record_bloom_filter_hit() {
    m::counter!("soroban_pulse_bloom_filter_hits_total").increment(1);
}

/// Update the bloom filter size gauge (number of set bits) (issue #369)
pub fn update_bloom_filter_size(size: u64) {
    m::gauge!("soroban_pulse_bloom_filter_size").set(size as f64);
}

/// Record a normalizer error (issue #368)
pub fn record_normalizer_error() {
    m::counter!("soroban_pulse_normalizer_errors_total").increment(1);
}

/// Record a Kinesis publish failure (issue #265)
pub fn record_kinesis_publish_failure() {
    m::counter!("soroban_pulse_kinesis_publish_failures_total").increment(1);
}

/// Record a Pub/Sub publish failure (issue #264)
pub fn record_pubsub_publish_failure() {
    m::counter!("soroban_pulse_pubsub_publish_failures_total").increment(1);
}

/// Record a rate-limited request rejection (429 Too Many Requests)
pub fn record_rate_limit_rejected() {
    m::counter!("soroban_pulse_rate_limit_rejected_total").increment(1);
}

/// Record a persistent webhook delivery failure (all retries exhausted)
pub fn record_webhook_failure() {
    m::counter!("soroban_pulse_webhook_failures_total").increment(1);
}

/// Record a Redis queue publish failure (all retries exhausted)
pub fn record_queue_publish_failure() {
    m::counter!("soroban_pulse_redis_publish_failures_total").increment(1);
}

/// Record an event dropped because the Redis in-memory buffer is full
pub fn record_redis_dropped() {
    m::counter!("soroban_pulse_redis_dropped_total").increment(1);
}

/// Record a successful Redis reconnection after a connection loss
pub fn record_redis_reconnect() {
    m::counter!("soroban_pulse_redis_reconnect_total").increment(1);
}

/// Update the Redis in-memory buffer size gauge
pub fn update_redis_buffer_size(size: usize) {
    m::gauge!("soroban_pulse_redis_buffer_size").set(size as f64);
}

/// Record an RPC failover event (primary URL failed, switched to fallback)
pub fn record_rpc_failover() {
    m::counter!("soroban_pulse_rpc_failover_total").increment(1);
}

/// Update the active RPC endpoint label gauge (1.0 = active)
pub fn set_rpc_active_endpoint(endpoint: &str) {
    m::gauge!("soroban_pulse_rpc_active_endpoint", "url" => endpoint.to_string()).set(1.0);
}

/// Record a Kinesis ProvisionedThroughputExceededException (throttled record)
pub fn record_kinesis_throttled() {
    m::counter!("soroban_pulse_kinesis_throttled_total").increment(1);
}

/// Record an email notification failure
pub fn record_email_failure() {
    m::counter!("soroban_pulse_email_failures_total").increment(1);
}

/// Record a Lua script timeout
pub fn record_lua_timeout() {
    m::counter!("soroban_pulse_lua_timeout_total").increment(1);
}

pub fn record_replay_job() {
    m::counter!("soroban_pulse_replay_jobs_total").increment(1);
}

/// Record events pruned
pub fn increment_events_pruned(count: u64) {
    m::counter!("soroban_pulse_events_pruned_total").increment(count);
}

/// Record HTTP request duration
pub fn record_http_request_duration(
    duration: std::time::Duration,
    method: &str,
    route: &str,
    status: &str,
) {
    m::histogram!(
        "soroban_pulse_http_request_duration_seconds",
        "method" => method.to_string(),
        "route" => route.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Update the active SSE connections count
pub fn update_sse_connections(count: usize) {
    m::gauge!("soroban_pulse_sse_active_connections").set(count as f64);
}

/// Record SSE multi-stream contract IDs per connection (histogram)
pub fn record_sse_multi_contract_ids(count: u64) {
    m::histogram!("soroban_pulse_sse_multi_contract_ids").record(count as f64);
}

/// Update DB connection pool metrics
pub fn update_db_pool_metrics(pool: &PgPool) {
    m::gauge!("soroban_pulse_db_pool_size").set(pool.size() as f64);
    m::gauge!("soroban_pulse_db_pool_idle").set(pool.num_idle() as f64);
}

/// Update the process RSS memory gauge (Linux only).
/// Reads VmRSS from /proc/self/status.
#[cfg(target_os = "linux")]
pub fn update_process_memory_bytes() {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                if let Some(kb_str) = rest.split_whitespace().next() {
                    if let Ok(kb) = kb_str.parse::<u64>() {
                        m::gauge!("soroban_pulse_process_memory_bytes").set((kb * 1024) as f64);
                    }
                }
                break;
            }
        }
    }
}

/// Spawn a background task that updates process memory every 30 seconds (Linux only).
#[cfg(target_os = "linux")]
pub fn spawn_memory_collector() {
    tokio::spawn(async {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            update_process_memory_bytes();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_init_metrics() {
        let handle = init_metrics();
        // The handle should be valid - we can't easily test the internal state
        // but we can at least verify it doesn't panic
        assert!(true);
    }

    #[test]
    fn test_record_events_indexed() {
        // This should not panic
        record_events_indexed(42);
        record_events_indexed(0);
        assert!(true);
    }

    #[test]
    fn test_update_current_ledger() {
        // This should not panic
        update_current_ledger(12345);
        update_current_ledger(0);
        assert!(true);
    }

    #[test]
    fn test_update_latest_ledger() {
        // This should not panic
        update_latest_ledger(67890);
        update_latest_ledger(0);
        assert!(true);
    }

    #[test]
    fn test_update_indexer_lag() {
        // This should not panic
        update_indexer_lag(100);
        update_indexer_lag(0);
        assert!(true);
    }

    #[test]
    fn test_record_rpc_error() {
        // This should not panic
        record_rpc_error();
        assert!(true);
    }

    #[test]
    fn test_record_validation_failure() {
        // This should not panic
        record_validation_failure();
        assert!(true);
    }

    #[test]
    fn test_record_http_request_duration() {
        // This should not panic
        let duration = Duration::from_millis(150);
        record_http_request_duration(duration, "GET", "/events", "200");
        record_http_request_duration(Duration::ZERO, "POST", "/health", "500");
        assert!(true);
    }

    #[tokio::test]
    async fn test_update_db_pool_metrics() {
        // Create a test pool
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .min_connections(1)
            .connect_lazy("postgres://localhost/test")
            .unwrap();

        // This should not panic
        update_db_pool_metrics(&pool);
        assert!(true);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_memory_bytes_is_nonzero_on_linux() {
        update_process_memory_bytes();
        // After updating, the gauge should have been set to a positive value.
        // We can't easily read back a gauge value from the metrics crate without
        // a recorder, so we verify the /proc/self/status parse succeeds instead.
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let rss_kb: u64 = status
            .lines()
            .find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .expect("VmRSS not found in /proc/self/status");
        assert!(rss_kb > 0, "RSS should be non-zero");
    }
}
