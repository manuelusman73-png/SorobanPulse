/// Background task that periodically runs EXPLAIN on key queries and warns
/// if the query planner is not using the expected indexes.
/// Also queries pg_stat_user_indexes to expose per-index scan counts and
/// unused-index totals as Prometheus metrics.
extern crate metrics as m;

use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::watch;

/// Queries to check, paired with the index name expected to appear in the plan.
const CHECKS: &[(&str, &str, &str)] = &[
    (
        "main events query",
        "EXPLAIN (FORMAT JSON) SELECT id FROM events ORDER BY ledger DESC, id DESC LIMIT 20",
        "idx_events_ledger_desc",
    ),
    (
        "contract filter query",
        "EXPLAIN (FORMAT JSON) SELECT id FROM events WHERE contract_id = 'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM' ORDER BY ledger DESC LIMIT 20",
        "idx_events_contract_ledger",
    ),
    (
        "tx hash query",
        "EXPLAIN (FORMAT JSON) SELECT id FROM events WHERE tx_hash = 'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2' ORDER BY ledger DESC LIMIT 20",
        "idx_events_tx_ledger",
    ),
];

// ---------------------------------------------------------------------------
// pg_stat_user_indexes metrics
// ---------------------------------------------------------------------------

/// Per-index statistics row from pg_stat_user_indexes.
pub struct IndexScanStats {
    pub table: String,
    pub index: String,
    pub scan_count: i64,
}

/// Emit Prometheus metrics from a slice of index scan statistics.
/// Extracted as a pure function so it can be unit-tested without a DB.
pub fn emit_index_metrics(stats: &[IndexScanStats]) {
    let unused_count = stats.iter().filter(|s| s.scan_count == 0).count();
    m::gauge!("soroban_pulse_unused_indexes_total").set(unused_count as f64);

    for stat in stats {
        m::gauge!(
            "soroban_pulse_index_scan_count",
            "table" => stat.table.clone(),
            "index" => stat.index.clone()
        )
        .set(stat.scan_count as f64);
    }

    if unused_count > 0 {
        tracing::warn!(
            unused_indexes = unused_count,
            "Unused indexes detected (idx_scan = 0 since last stats reset); \
             consider dropping or rebuilding them"
        );
    }
}

/// Query pg_stat_user_indexes and emit scan-count metrics.
async fn collect_index_stats(pool: &PgPool) {
    let rows: Vec<(String, String, i64)> = match sqlx::query_as(
        "SELECT tablename, indexname, COALESCE(idx_scan, 0)::bigint
         FROM pg_stat_user_indexes
         WHERE schemaname = 'public'
         ORDER BY idx_scan ASC",
    )
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to query pg_stat_user_indexes");
            return;
        }
    };

    let stats: Vec<IndexScanStats> = rows
        .into_iter()
        .map(|(table, index, scan_count)| IndexScanStats {
            table,
            index,
            scan_count,
        })
        .collect();

    emit_index_metrics(&stats);
}

// ---------------------------------------------------------------------------
// Existing EXPLAIN-based checks
// ---------------------------------------------------------------------------

/// Run a single round of index usage checks.
async fn check_indexes(pool: &PgPool) {
    for (label, sql, expected_index) in CHECKS {
        match sqlx::query_scalar::<_, serde_json::Value>(sql)
            .fetch_one(pool)
            .await
        {
            Ok(plan) => {
                let plan_str = plan.to_string();
                let uses_index = plan_str.contains(expected_index)
                    || plan_str.contains("Index Scan")
                    || plan_str.contains("Index Only Scan")
                    || plan_str.contains("Bitmap Index Scan");
                let has_seq_scan = plan_str.contains("Seq Scan");

                if has_seq_scan && !uses_index {
                    tracing::warn!(
                        query = label,
                        expected_index = expected_index,
                        "Sequential scan detected — expected index not used"
                    );
                } else {
                    tracing::debug!(
                        query = label,
                        expected_index = expected_index,
                        "Index usage OK"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(query = label, error = %e, "Failed to run EXPLAIN for index check");
            }
        }
    }
}

/// Spawn the index monitoring background task.
///
/// Runs every `interval_hours` hours. Stops when `shutdown_rx` fires.
pub fn spawn(pool: PgPool, interval_hours: u64, mut shutdown_rx: watch::Receiver<bool>) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(interval_hours * 3600);
        // Run once shortly after startup, then on the configured interval.
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    tracing::debug!("Running index usage check");
                    check_indexes(&pool).await;
                    collect_index_stats(&pool).await;
                }
                _ = shutdown_rx.changed() => {
                    tracing::debug!("Index monitor shutting down");
                    break;
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stats(rows: &[(&str, &str, i64)]) -> Vec<IndexScanStats> {
        rows.iter()
            .map(|(table, index, scans)| IndexScanStats {
                table: table.to_string(),
                index: index.to_string(),
                scan_count: *scans,
            })
            .collect()
    }

    #[test]
    fn unused_count_all_zero() {
        let stats = make_stats(&[
            ("events", "idx_a", 0),
            ("events", "idx_b", 0),
        ]);
        let unused = stats.iter().filter(|s| s.scan_count == 0).count();
        assert_eq!(unused, 2);
    }

    #[test]
    fn unused_count_mixed() {
        let stats = make_stats(&[
            ("events", "idx_a", 0),
            ("events", "idx_b", 500),
            ("events", "idx_c", 0),
        ]);
        let unused = stats.iter().filter(|s| s.scan_count == 0).count();
        assert_eq!(unused, 2);
    }

    #[test]
    fn unused_count_none() {
        let stats = make_stats(&[
            ("events", "idx_a", 10),
            ("events", "idx_b", 200),
        ]);
        let unused = stats.iter().filter(|s| s.scan_count == 0).count();
        assert_eq!(unused, 0);
    }

    #[test]
    fn emit_index_metrics_does_not_panic_on_empty() {
        // Verify metric emission is safe with no rows.
        emit_index_metrics(&[]);
    }

    #[test]
    fn emit_index_metrics_does_not_panic_with_data() {
        let stats = make_stats(&[
            ("events", "idx_events_ledger_desc", 1234),
            ("events", "idx_old_unused", 0),
        ]);
        emit_index_metrics(&stats);
    }
}
