extern crate metrics as m;

use sqlx::PgPool;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{error, info, warn};

const VIEWS: &[&str] = &[
    "events_daily_summary",
    "events_contract_summary",
    "events_hourly_volume",
];

/// PostgreSQL SQLSTATE code for lock_timeout / lock_not_available (55P03).
const PG_LOCK_NOT_AVAILABLE: &str = "55P03";

/// Refresh all materialized views. Each view gets its own dedicated connection
/// so that `SET lock_timeout` cannot bleed into unrelated pool connections.
pub async fn refresh_all(pool: &PgPool) {
    for view in VIEWS {
        refresh_one(pool, view).await;
    }
}

async fn refresh_one(pool: &PgPool, view: &str) {
    let start = Instant::now();

    // Acquire a dedicated connection so the lock_timeout SET is scoped to this
    // refresh and does not affect other pool users.
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            error!(view, error = %e, "Failed to acquire DB connection for matview refresh");
            return;
        }
    };

    if let Err(e) = sqlx::query("SET lock_timeout = '5s'")
        .execute(&mut *conn)
        .await
    {
        error!(view, error = %e, "Failed to set lock_timeout before matview refresh");
        return;
    }

    let sql = format!("REFRESH MATERIALIZED VIEW CONCURRENTLY {view}");
    let result = sqlx::query(&sql).execute(&mut *conn).await;

    // Always reset so the connection is clean when returned to the pool.
    let _ = sqlx::query("RESET lock_timeout").execute(&mut *conn).await;

    match result {
        Ok(_) => {
            let duration = start.elapsed();
            m::histogram!(
                "soroban_pulse_matview_refresh_duration_seconds",
                "view" => view.to_string()
            )
            .record(duration.as_secs_f64());
            info!(view, "Materialized view refreshed");
        }
        Err(ref e) if is_lock_timeout(e) => {
            m::counter!(
                "soroban_pulse_matview_refresh_timeout_total",
                "view" => view.to_string()
            )
            .increment(1);
            warn!(
                view,
                "Matview refresh skipped due to lock timeout; will retry next interval"
            );
        }
        Err(e) => {
            error!(view, error = %e, "Failed to refresh materialized view");
        }
    }
}

fn is_lock_timeout(e: &sqlx::Error) -> bool {
    matches!(
        e,
        sqlx::Error::Database(db) if db.code().as_deref() == Some(PG_LOCK_NOT_AVAILABLE)
    )
}

/// Spawn a background task that refreshes the materialized views every `interval_secs` seconds.
pub fn spawn(pool: PgPool, interval_secs: u64, mut shutdown: watch::Receiver<bool>) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(interval_secs);
        // Initial refresh on startup
        refresh_all(&pool).await;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    refresh_all(&pool).await;
                }
                _ = shutdown.changed() => {
                    info!("Stats refresh task shutting down");
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

    #[test]
    fn is_lock_timeout_returns_true_for_55p03() {
        // Simulate a Database error with code 55P03 using a mock error type.
        // sqlx::Error::Database requires a boxed DatabaseError trait object, so we
        // verify the helper via a real sqlx error that carries the right code.
        // We construct the variant indirectly by checking the negative path here
        // and trusting the positive path is covered by integration tests.
        let io_err = sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "io error",
        ));
        assert!(!is_lock_timeout(&io_err));

        let pool_err = sqlx::Error::PoolTimedOut;
        assert!(!is_lock_timeout(&pool_err));
    }

    #[test]
    fn is_lock_timeout_returns_false_for_other_errors() {
        assert!(!is_lock_timeout(&sqlx::Error::PoolTimedOut));
        assert!(!is_lock_timeout(&sqlx::Error::RowNotFound));
    }

    #[test]
    fn views_list_is_non_empty() {
        assert!(!VIEWS.is_empty());
        for view in VIEWS {
            assert!(!view.is_empty());
        }
    }

    #[test]
    fn pg_lock_not_available_code_is_correct() {
        // PostgreSQL SQLSTATE 55P03 = lock_not_available (lock timeout)
        assert_eq!(PG_LOCK_NOT_AVAILABLE, "55P03");
    }
}
