//! Issue #372: Background re-encryption job for key rotation.
//! Migrates events encrypted with old key to new key in batches.

use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::encryption;
use crate::metrics;

/// Shared state for re-encryption job
#[derive(Clone)]
pub struct ReencryptState {
    pub is_running: Arc<AtomicBool>,
    pub rows_remaining: Arc<AtomicU64>,
}

impl ReencryptState {
    pub fn new() -> Self {
        Self {
            is_running: Arc::new(AtomicBool::new(false)),
            rows_remaining: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    pub fn set_running(&self, running: bool) {
        self.is_running.store(running, Ordering::Relaxed);
    }

    pub fn set_remaining(&self, count: u64) {
        self.rows_remaining.store(count, Ordering::Relaxed);
    }

    pub fn get_remaining(&self) -> u64 {
        self.rows_remaining.load(Ordering::Relaxed)
    }
}

/// Start a background re-encryption job.
/// Returns immediately; the job runs in the background.
pub fn start_reencrypt_job(
    pool: PgPool,
    new_key: [u8; 32],
    old_key: [u8; 32],
    batch_size: usize,
    state: ReencryptState,
) {
    if state.is_running() {
        warn!("Re-encryption job already running");
        return;
    }

    state.set_running(true);

    tokio::spawn(async move {
        if let Err(e) = run_reencrypt_job(&pool, new_key, old_key, batch_size, &state).await {
            error!(error = %e, "Re-encryption job failed");
        }
        state.set_running(false);
    });
}

/// Run the re-encryption job: fetch rows encrypted with old key, re-encrypt with new key.
async fn run_reencrypt_job(
    pool: &PgPool,
    new_key: [u8; 32],
    old_key: [u8; 32],
    batch_size: usize,
    state: &ReencryptState,
) -> anyhow::Result<()> {
    // Count total rows to re-encrypt
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE event_data->>'encrypted' = 'true'"
    )
    .fetch_one(pool)
    .await?;

    state.set_remaining(total as u64);
    info!(total, "Starting re-encryption job");

    let mut offset = 0;
    let mut reencrypted = 0;

    loop {
        let rows: Vec<(uuid::Uuid, serde_json::Value)> = sqlx::query_as(
            "SELECT id, event_data FROM events \
             WHERE event_data->>'encrypted' = 'true' \
             ORDER BY id \
             LIMIT $1 OFFSET $2"
        )
        .bind(batch_size as i64)
        .bind(offset as i64)
        .fetch_all(pool)
        .await?;

        if rows.is_empty() {
            break;
        }

        for (id, event_data) in rows {
            // Try to decrypt with old key, then re-encrypt with new key
            match encryption::decrypt(&new_key, Some(&old_key), &event_data) {
                Ok(decrypted) => {
                    match encryption::encrypt(&new_key, &decrypted) {
                        Ok(reencrypted_data) => {
                            if let Err(e) = sqlx::query(
                                "UPDATE events SET event_data = $1 WHERE id = $2"
                            )
                            .bind(&reencrypted_data)
                            .bind(id)
                            .execute(pool)
                            .await {
                                error!(id = %id, error = %e, "Failed to update re-encrypted event");
                                metrics::record_reencrypt_error();
                            } else {
                                reencrypted += 1;
                            }
                        }
                        Err(e) => {
                            error!(id = %id, error = %e, "Failed to re-encrypt event");
                            metrics::record_reencrypt_error();
                        }
                    }
                }
                Err(e) => {
                    error!(id = %id, error = %e, "Failed to decrypt event with old key");
                    metrics::record_reencrypt_error();
                }
            }
        }

        offset += batch_size;
        let remaining = (total as usize - offset).max(0);
        state.set_remaining(remaining as u64);
        metrics::update_reencrypt_progress(remaining as u64);

        info!(reencrypted, remaining, "Re-encryption progress");
    }

    info!(total_reencrypted = reencrypted, "Re-encryption job completed");
    Ok(())
}

/// Check if any rows are still encrypted with the old key.
pub async fn has_old_key_rows(pool: &PgPool) -> anyhow::Result<bool> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE event_data->>'encrypted' = 'true' LIMIT 1"
    )
    .fetch_one(pool)
    .await?;

    Ok(count > 0)
}
