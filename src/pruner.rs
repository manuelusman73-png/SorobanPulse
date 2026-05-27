use sqlx::PgPool;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};

pub fn start_pruning_task(
    pool: PgPool,
    retention_days: u64,
    pruning_interval_hours: u64,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    if retention_days == 0 {
        info!("Event pruning disabled (RETENTION_DAYS=0)");
        return;
    }

    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(pruning_interval_hours * 3600));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = prune_old_events(&pool, retention_days).await {
                        error!("Pruning task failed: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Pruning task shutting down");
                        break;
                    }
                }
            }
        }
    });
}

async fn prune_old_events(pool: &PgPool, retention_days: u64) -> Result<(), Box<dyn std::error::Error>> {
    let cutoff_date = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);

    let result = sqlx::query(
        "DELETE FROM events WHERE created_at < $1"
    )
    .bind(cutoff_date)
    .execute(pool)
    .await?;

    let deleted_count = result.rows_affected();
    if deleted_count > 0 {
        info!("Pruned {} events older than {} days", deleted_count, retention_days);
        crate::metrics::increment_events_pruned(deleted_count);

        // Reclaim space
        sqlx::query("VACUUM ANALYZE events")
            .execute(pool)
            .await?;
    }

    Ok(())
}
