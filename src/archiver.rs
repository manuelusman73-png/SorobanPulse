//! Background archival job: moves events older than `archive_after_days` from
//! PostgreSQL to S3-compatible object storage as gzip-compressed NDJSON files.
//! Issue #371: Verifies upload integrity via SHA-256 checksum before marking archived.
//!
//! Only compiled when the `archive` feature is enabled.

use aws_sdk_s3::primitives::ByteStream;
use chrono::{NaiveDate, Utc};
use flate2::{write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use sqlx::PgPool;
use std::io::Write;
use tracing::{error, info, warn};

use crate::config::Config;

/// Metadata returned by `GET /v1/events/archive`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ArchiveFile {
    /// S3 key of the archive file.
    pub key: String,
    /// Date the events were archived (YYYY-MM-DD).
    pub date: String,
    /// Size of the compressed file in bytes.
    pub size_bytes: i64,
}

/// Run the archival loop: every `interval` seconds, archive events older than
/// `archive_after_days` days.
pub async fn run_archiver(pool: PgPool, config: Config) {
    let Some(ref bucket) = config.archive_s3_bucket else {
        warn!("ARCHIVE_S3_BUCKET not set — archiver disabled");
        return;
    };
    let bucket = bucket.clone();
    let prefix = config.archive_s3_prefix.clone();
    let after_days = i64::from(config.archive_after_days);

    let aws_cfg = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&aws_cfg);

    info!(bucket = %bucket, prefix = %prefix, after_days, "Archiver started");

    loop {
        if let Err(e) = archive_once(&pool, &s3, &bucket, &prefix, after_days).await {
            error!(error = %e, "Archival run failed");
        }
        // Run once per day.
        tokio::time::sleep(tokio::time::Duration::from_secs(86_400)).await;
    }
}

/// One archival pass: find distinct dates with archivable events, process each.
async fn archive_once(
    pool: &PgPool,
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    after_days: i64,
) -> anyhow::Result<()> {
    let cutoff = Utc::now() - chrono::Duration::days(after_days);

    // Fetch distinct dates that have events older than the cutoff.
    let dates: Vec<NaiveDate> = sqlx::query_scalar(
        "SELECT DISTINCT DATE(timestamp) FROM events WHERE timestamp < $1 ORDER BY 1",
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await?;

    for date in dates {
        if let Err(e) = archive_date(pool, s3, bucket, prefix, date).await {
            error!(date = %date, error = %e, "Failed to archive date");
        }
    }
    Ok(())
}

/// Archive all events for a single calendar date, then delete them from the DB.
async fn archive_date(
    pool: &PgPool,
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    date: NaiveDate,
) -> anyhow::Result<()> {
    let day_start = date.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let day_end = date.and_hms_opt(23, 59, 59).unwrap().and_utc();

    let rows: Vec<crate::models::Event> = sqlx::query_as(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, 0::bigint AS total_count \
         FROM events WHERE timestamp BETWEEN $1 AND $2 ORDER BY ledger, id",
    )
    .bind(day_start)
    .bind(day_end)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(());
    }

    let count = rows.len();
    let gz_bytes = encode_ndjson_gz(&rows)?;

    // Compute SHA-256 of the archive before upload
    let mut hasher = Sha256::new();
    hasher.update(&gz_bytes);
    let local_hash = format!("{:x}", hasher.finalize());

    let key = format!(
        "{}/{}/{}/{}/events.ndjson.gz",
        prefix,
        date.format("%Y"),
        date.format("%m"),
        date.format("%d"),
    );

    let put_response = s3.put_object()
        .bucket(bucket)
        .key(&key)
        .body(ByteStream::from(gz_bytes))
        .content_type("application/x-ndjson")
        .content_encoding("gzip")
        .send()
        .await?;

    // Verify integrity: compare ETag with local SHA-256
    let etag = put_response.e_tag().unwrap_or("").trim_matches('"');
    if etag != local_hash {
        error!(
            date = %date,
            key = %key,
            local_hash = %local_hash,
            remote_etag = %etag,
            "Archive integrity check failed: ETag mismatch"
        );
        crate::metrics::record_archive_integrity_failure();
        return Err(anyhow::anyhow!("Archive integrity check failed: ETag mismatch"));
    }

    // Delete only after a successful verified upload.
    sqlx::query("DELETE FROM events WHERE timestamp BETWEEN $1 AND $2")
        .bind(day_start)
        .bind(day_end)
        .execute(pool)
        .await?;

    info!(date = %date, count, key = %key, "Archived events");
    Ok(())
}

/// Serialize events as NDJSON and gzip-compress the result.
fn encode_ndjson_gz(events: &[crate::models::Event]) -> anyhow::Result<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    for ev in events {
        let line = serde_json::to_string(ev)?;
        enc.write_all(line.as_bytes())?;
        enc.write_all(b"\n")?;
    }
    Ok(enc.finish()?)
}

/// List archive files available in S3 under the configured prefix.
pub async fn list_archive_files(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> anyhow::Result<Vec<ArchiveFile>> {
    let mut files = Vec::new();
    let mut continuation: Option<String> = None;

    loop {
        let mut req = s3.list_objects_v2().bucket(bucket).prefix(prefix);
        if let Some(ref tok) = continuation {
            req = req.continuation_token(tok);
        }
        let resp = req.send().await?;

        for obj in resp.contents() {
            let key = obj.key().unwrap_or_default().to_string();
            // Extract date from key: <prefix>/YYYY/MM/DD/events.ndjson.gz
            let date = key
                .trim_start_matches(prefix)
                .trim_start_matches('/')
                .split('/')
                .take(3)
                .collect::<Vec<_>>()
                .join("-");
            files.push(ArchiveFile {
                key: key.clone(),
                date,
                size_bytes: obj.size().unwrap_or(0),
            });
        }

        if resp.is_truncated().unwrap_or(false) {
            continuation = resp.next_continuation_token().map(str::to_string);
        } else {
            break;
        }
    }

    Ok(files)
}
