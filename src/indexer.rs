use chrono::DateTime;
use serde_json::json;
use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tracing::{error, info, instrument, span, warn, Level};

use crate::{
    bloom_filter::EventBloomFilter,
    config::{Config, HealthState, IndexerState},
    kinesis::KinesisPublisher,
    metrics,
    models::{GetEventsResult, LatestLedgerResult, RpcResponse, SorobanEvent},
    pubsub::PubSubPublisher,
    xdr_validation,
};

#[async_trait::async_trait]
pub trait RpcClient: Send + Sync {
    async fn get_latest_ledger(&self, rpc_url: &str) -> Result<u64, String>;
    async fn get_events(
        &self,
        rpc_url: &str,
        start_ledger: u64,
        cursor: Option<String>,
        event_types: &[String],
    ) -> Result<GetEventsResult, String>;
}

/// Postgres advisory lock key for the indexer singleton.
const INDEXER_LOCK_KEY: i64 = 0x536f726f62616e50; // "SorobanP"

#[derive(Debug, thiserror::Error)]
enum IndexerFetchError {
    #[error("{0}")]
    Rpc(String),
    #[error(transparent)]
    DbConnection(#[from] sqlx::Error),
}

pub struct SorobanRpcClient {
    client: reqwest::Client,
    /// Custom headers injected into every RPC request. Values are never logged.
    headers: reqwest::header::HeaderMap,
}

impl SorobanRpcClient {
    pub fn new(config: &Config) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(config.rpc_connect_timeout_secs))
            .timeout(Duration::from_secs(config.rpc_request_timeout_secs))
            .pool_max_idle_per_host(5)
            .pool_idle_timeout(Duration::from_secs(30))
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .expect("Failed to build HTTP client");

        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in &config.rpc_headers {
            let header_name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .unwrap_or_else(|_| panic!("Invalid RPC header name: {name}"));
            let header_value = reqwest::header::HeaderValue::from_str(value)
                .unwrap_or_else(|_| panic!("Invalid RPC header value for '{name}'"));
            headers.insert(header_name, header_value);
        }

        Self { client, headers }
    }
}

#[async_trait::async_trait]
impl RpcClient for SorobanRpcClient {
    async fn get_latest_ledger(&self, rpc_url: &str) -> Result<u64, String> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestLedger"
        });

        let span = span!(Level::INFO, "rpc_get_latest_ledger", url = %rpc_url);
        let _enter = span.enter();

        let resp: RpcResponse<LatestLedgerResult> = self
            .client
            .post(rpc_url)
            .headers(self.headers.clone())
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    warn!("RPC request timeout");
                }
                metrics::record_rpc_error();
                e.to_string()
            })?
            .json()
            .await
            .map_err(|e| e.to_string())?;

        match resp.result {
            Some(r) => {
                metrics::update_latest_ledger(r.sequence);
                Ok(r.sequence)
            }
            None => {
                if let Some(err) = resp.error {
                    warn!(code = err.code, error = %err.message, "RPC error");
                    metrics::record_rpc_error();
                }
                Err("RPC returned no result".to_string())
            }
        }
    }

    async fn get_events(
        &self,
        rpc_url: &str,
        start_ledger: u64,
        cursor: Option<String>,
        event_types: &[String],
    ) -> Result<GetEventsResult, String> {
        // Build filters: one filter object per type when event_types is non-empty.
        let filters: serde_json::Value = if event_types.is_empty() {
            json!([])
        } else {
            let filter_list: Vec<serde_json::Value> =
                event_types.iter().map(|t| json!({ "type": t })).collect();
            json!(filter_list)
        };

        let mut params = json!({
            "filters": filters,
            "pagination": { "limit": 100 }
        });

        if let Some(c) = &cursor {
            params["pagination"]["cursor"] = json!(c);
        } else {
            params["startLedger"] = json!(start_ledger);
        }

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getEvents",
            "params": params
        });

        let resp: RpcResponse<GetEventsResult> = self
            .client
            .post(rpc_url)
            .headers(self.headers.clone())
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    warn!("RPC request timeout");
                }
                metrics::record_rpc_error();
                e.to_string()
            })?
            .json()
            .await
            .map_err(|e| e.to_string())?;

        match resp.result {
            Some(r) => Ok(r),
            None => {
                if let Some(err) = resp.error {
                    warn!(code = err.code, error = %err.message, "RPC error");
                    metrics::record_rpc_error();
                    Err(err.message)
                } else {
                    Err("RPC returned no result".to_string())
                }
            }
        }
    }
}

/// Attempt to decode event_data using a registered ABI for the contract.
/// Returns `Some(decoded)` if an ABI is found and decoding succeeds, `None` otherwise.
async fn decode_event_with_abi(
    pool: &PgPool,
    contract_id: &str,
    event_data: &serde_json::Value,
) -> Option<serde_json::Value> {
    let abi: serde_json::Value =
        sqlx::query_scalar("SELECT abi FROM contract_abis WHERE contract_id = $1")
            .bind(contract_id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten()?;

    // The ABI is a JSON array of event definitions: [{name, inputs:[{name,type},...]}]
    // We match on the first topic (event name) and map positional values to named fields.
    let topic = event_data.get("topic")?.as_array()?;
    let event_name = topic.first()?.as_str()?;

    let events = abi.as_array()?;
    let def = events
        .iter()
        .find(|e| e.get("name").and_then(|n| n.as_str()) == Some(event_name))?;
    let inputs = def.get("inputs")?.as_array()?;

    let values = event_data.get("value")?.as_object()?;
    // Build decoded object: map input names to corresponding values
    let mut decoded = serde_json::Map::new();
    decoded.insert(
        "event".to_string(),
        serde_json::Value::String(event_name.to_string()),
    );
    for (i, input) in inputs.iter().enumerate() {
        let field_name = input
            .get("name")
            .and_then(|n| n.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("field_{i}"));
        let field_name = field_name.as_str();
        // Values may be keyed by index or by name in the raw data
        let val = values
            .get(field_name)
            .or_else(|| values.get(&i.to_string()))
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        decoded.insert(field_name.to_string(), val);
    }
    Some(serde_json::Value::Object(decoded))
}

pub struct Indexer<R: RpcClient> {
    pool: PgPool,
    rpc_client: R,
    config: Config,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    health_state: Option<Arc<HealthState>>,
    indexer_state: Option<Arc<IndexerState>>,
    event_tx: Option<broadcast::Sender<SorobanEvent>>,
    event_counter: AtomicU64,
    bloom_filter: Option<Arc<EventBloomFilter>>,
    kinesis_publisher: Option<Arc<dyn KinesisPublisher>>,
    pubsub_publisher: Option<Arc<dyn PubSubPublisher>>,
    #[cfg(feature = "kafka")]
    kafka_publisher: Option<Arc<dyn crate::kafka::KafkaPublisher>>,
    #[cfg(feature = "kafka")]
    kafka_topic: Option<String>,
    #[cfg(feature = "lua")]
    lua_transformer: Option<Arc<crate::lua_transform::LuaTransformer>>,
}

impl<R: RpcClient> Indexer<R> {
    pub fn new(
        pool: PgPool,
        config: Config,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
        rpc_client: R,
    ) -> Self {
        Self {
            pool,
            rpc_client,
            config,
            shutdown_rx,
            health_state: None,
            indexer_state: None,
            event_tx: None,
            event_counter: AtomicU64::new(0),
            bloom_filter: None,
            kinesis_publisher: None,
            pubsub_publisher: None,
            #[cfg(feature = "kafka")]
            kafka_publisher: None,
            #[cfg(feature = "kafka")]
            kafka_topic: None,
            #[cfg(feature = "lua")]
            lua_transformer: None,
        }
    }

    /// Set the health state for updating the last poll timestamp
    pub fn set_health_state(&mut self, health_state: Arc<HealthState>) {
        self.health_state = Some(health_state);
    }

    /// Set the indexer state for exposing operational metrics to the /status endpoint
    pub fn set_indexer_state(&mut self, indexer_state: Arc<IndexerState>) {
        self.indexer_state = Some(indexer_state);
    }

    /// Set the broadcast sender for real-time SSE streaming.
    pub fn set_event_tx(&mut self, event_tx: broadcast::Sender<SorobanEvent>) {
        self.event_tx = Some(event_tx);
    }

    /// Set the bloom filter for pre-filtering duplicate events (issue #266).
    pub fn set_bloom_filter(&mut self, bloom_filter: Arc<EventBloomFilter>) {
        self.bloom_filter = Some(bloom_filter);
    }

    /// Set the Kinesis publisher for streaming events (issue #265).
    pub fn set_kinesis_publisher(&mut self, publisher: Arc<dyn KinesisPublisher>) {
        self.kinesis_publisher = Some(publisher);
    }

    /// Set the Pub/Sub publisher for streaming events (issue #264).
    pub fn set_pubsub_publisher(&mut self, publisher: Arc<dyn PubSubPublisher>) {
        self.pubsub_publisher = Some(publisher);
    }

    /// Set the Kafka publisher and topic for event streaming.
    #[cfg(feature = "kafka")]
    pub fn set_kafka_publisher(
        &mut self,
        publisher: Arc<dyn crate::kafka::KafkaPublisher>,
        topic: String,
    ) {
        self.kafka_publisher = Some(publisher);
        self.kafka_topic = Some(topic);
    }

    /// Set the Lua transformer for event transformation.
    #[cfg(feature = "lua")]
    pub fn set_lua_transformer(&mut self, transformer: Arc<crate::lua_transform::LuaTransformer>) {
        self.lua_transformer = Some(transformer);
    }

    pub async fn run(&self) {
        // Attempt to acquire a Postgres session-level advisory lock.
        // Only one replica will hold this lock at a time; others poll until they can promote.
        let lock_conn = match self.pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "Failed to acquire DB connection for advisory lock");
                return;
            }
        };

        let retry_interval = Duration::from_secs(self.config.indexer_lock_retry_secs.max(1));
        let mut interval = tokio::time::interval(retry_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            // Respect shutdown signal while waiting to acquire the lock.
            let mut shutdown_rx = self.shutdown_rx.clone();
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Indexer shutting down before acquiring lock");
                        metrics::record_indexer_is_leader(false);
                        return;
                    }
                }
            }

            let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(INDEXER_LOCK_KEY)
                .fetch_one(&self.pool)
                .await
                .unwrap_or(false);

            if acquired {
                info!("Indexer lock acquired, starting indexing");
                if let Some(ref s) = self.indexer_state {
                    s.is_active_indexer.store(true, Ordering::Relaxed);
                }
                metrics::record_indexer_is_leader(true);
                break;
            }

            warn!(
                retry_secs = self.config.indexer_lock_retry_secs,
                "Indexer lock not acquired, running in standby mode — will retry"
            );
            if let Some(ref s) = self.indexer_state {
                s.is_active_indexer.store(false, Ordering::Relaxed);
            }
            metrics::record_indexer_is_leader(false);
        }

        // Run the actual indexing loop; release lock on exit.
        self.run_loop().await;

        // Explicitly release the advisory lock on graceful shutdown.
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(INDEXER_LOCK_KEY)
            .execute(&self.pool)
            .await;
        metrics::record_indexer_is_leader(false);
        if let Some(ref s) = self.indexer_state {
            s.is_active_indexer.store(false, Ordering::Relaxed);
        }

        drop(lock_conn);
    }

    async fn run_loop(&self) {
        let mut current_ledger = self.config.start_ledger;
        let mut consecutive_db_errors = 0u32;
        let mut rpc_backoff_ms = 1000u64; // Start with 1 second backoff

        // Restore persisted state so /status is accurate before the first poll.
        if let Some((persisted_current, persisted_latest)) = self.load_indexer_state().await {
            if persisted_current > 0 {
                if current_ledger == 0 {
                    current_ledger = persisted_current;
                }
                if let Some(ref s) = self.indexer_state {
                    s.current_ledger
                        .store(persisted_current, std::sync::atomic::Ordering::Relaxed);
                    s.latest_ledger
                        .store(persisted_latest, std::sync::atomic::Ordering::Relaxed);
                }
                metrics::update_current_ledger(persisted_current);
                info!(
                    current_ledger = persisted_current,
                    latest_ledger = persisted_latest,
                    "Restored persisted indexer state"
                );
            }
        }

        if current_ledger == 0 {
            loop {
                match self
                    .rpc_client
                    .get_latest_ledger(&self.config.stellar_rpc_url)
                    .await
                {
                    Ok(ledger) => {
                        current_ledger = ledger;
                        info!(ledger = current_ledger, "Starting from latest ledger");
                        metrics::update_current_ledger(current_ledger);
                        if let Some(ref s) = self.indexer_state {
                            s.current_ledger
                                .store(current_ledger, std::sync::atomic::Ordering::Relaxed);
                        }
                        rpc_backoff_ms = 1000; // Reset backoff on success
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, backoff_ms = rpc_backoff_ms, "Failed to get latest ledger, retrying with exponential backoff");
                        sleep(Duration::from_millis(rpc_backoff_ms)).await;
                        // Exponential backoff with max 60 seconds
                        rpc_backoff_ms = std::cmp::min(rpc_backoff_ms * 2, 60000);
                    }
                }
            }
        }

        loop {
            if *self.shutdown_rx.borrow() {
                info!("Indexer shutting down gracefully");
                break;
            }

            // Check pause state — sleep and re-check rather than blocking.
            if let Some(ref s) = self.indexer_state {
                if s.is_paused.load(std::sync::atomic::Ordering::Relaxed) {
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            }

            match self.fetch_and_store_events(current_ledger).await {
                Ok(latest) => {
                    consecutive_db_errors = 0;
                    // Update the last poll timestamp on success
                    if let Some(ref health_state) = self.health_state {
                        health_state.update_last_poll();
                    }
                    if latest > current_ledger {
                        current_ledger = latest;
                        metrics::update_current_ledger(current_ledger);
                        if let Some(ref s) = self.indexer_state {
                            s.current_ledger
                                .store(current_ledger, std::sync::atomic::Ordering::Relaxed);
                        }

                        // Calculate and update lag
                        let latest_ledger = self
                            .rpc_client
                            .get_latest_ledger(&self.config.stellar_rpc_url)
                            .await
                            .unwrap_or(0);
                        if latest_ledger > current_ledger {
                            if let Some(ref s) = self.indexer_state {
                                s.latest_ledger
                                    .store(latest_ledger, std::sync::atomic::Ordering::Relaxed);
                            }
                            let lag = latest_ledger - current_ledger;
                            metrics::update_indexer_lag(lag);

                            // Warn if lag exceeds threshold
                            if lag > self.config.indexer_lag_warn_threshold {
                                warn!(
                                    lag = lag,
                                    threshold = self.config.indexer_lag_warn_threshold,
                                    "Indexer is falling behind"
                                );
                            }
                        }
                    } else {
                        sleep(Duration::from_millis(self.config.indexer_poll_interval_ms)).await;
                    }
                }
                Err(IndexerFetchError::DbConnection(e)) => {
                    consecutive_db_errors += 1;
                    let backoff_secs = if consecutive_db_errors >= 5 { 60 } else { 10 };
                    if consecutive_db_errors == 5 {
                        error!(
                            consecutive = consecutive_db_errors,
                            "DB unavailable, backing off"
                        );
                    } else if consecutive_db_errors < 5 {
                        error!(error = %e, "Indexer error");
                    }
                    sleep(Duration::from_secs(backoff_secs)).await;
                }
                Err(IndexerFetchError::Rpc(msg)) => {
                    error!(error = %msg, "Indexer error");
                    sleep(Duration::from_millis(self.config.indexer_error_backoff_ms)).await;
                }
            }
        }
    }

    async fn get_latest_ledger(&self) -> Result<u64, String> {
        self.rpc_client
            .get_latest_ledger(&self.config.stellar_rpc_url)
            .await
    }

    /// Load the last persisted cursor from the database, if any.
    async fn load_checkpoint(&self) -> Option<String> {
        sqlx::query_scalar::<_, String>(
            "SELECT last_cursor FROM indexer_checkpoints WHERE id = 'singleton'",
        )
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()
    }

    /// Load the last persisted indexer state (current_ledger, latest_ledger) from the DB.
    async fn load_indexer_state(&self) -> Option<(u64, u64)> {
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT current_ledger, latest_ledger FROM indexer_state WHERE id = 'singleton'",
        )
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()
        .map(|(c, l)| (c as u64, l as u64))
    }

    /// Persist current_ledger and latest_ledger atomically inside an existing transaction.
    async fn save_indexer_state(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        current_ledger: u64,
        latest_ledger: u64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO indexer_state (id, current_ledger, latest_ledger, updated_at)
             VALUES ('singleton', $1, $2, NOW())
             ON CONFLICT (id) DO UPDATE
               SET current_ledger = EXCLUDED.current_ledger,
                   latest_ledger  = EXCLUDED.latest_ledger,
                   updated_at     = NOW()",
        )
        .bind(current_ledger as i64)
        .bind(latest_ledger as i64)
        .execute(&mut **tx)
        .await
        .map(|_| ())
    }

    /// Persist the cursor atomically alongside the event inserts.
    async fn save_checkpoint(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        cursor: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO indexer_checkpoints (id, last_cursor, updated_at)
             VALUES ('singleton', $1, NOW())
             ON CONFLICT (id) DO UPDATE SET last_cursor = EXCLUDED.last_cursor, updated_at = NOW()",
        )
        .bind(cursor)
        .execute(&mut **tx)
        .await
        .map(|_| ())
    }

    /// Public wrapper around `fetch_and_store_events` for integration/resilience tests.
    pub async fn fetch_and_store_events_pub(&self, start_ledger: u64) -> Result<u64, String> {
        self.fetch_and_store_events(start_ledger)
            .await
            .map_err(|e| e.to_string())
    }

    #[instrument(skip(self), fields(start_ledger = start_ledger))]
    async fn fetch_and_store_events(&self, start_ledger: u64) -> Result<u64, IndexerFetchError> {
        let cycle_start = std::time::Instant::now();
        // Resume from persisted cursor if available, otherwise start from ledger.
        let mut cursor: Option<String> = self.load_checkpoint().await;
        let mut latest_ledger = start_ledger;
        let mut total_fetched = 0;
        let mut total_inserted = 0;
        let mut total_skipped = 0;
        let mut total_pages = 0u32;
        let mut total_rpc_ms = 0u128;
        let mut total_db_ms = 0u128;
        let start_ledger_for_log = start_ledger;

        tracing::debug!(
            start_ledger = start_ledger,
            cursor = cursor.as_deref().unwrap_or("<none>"),
            "Indexer cycle starting",
        );

        loop {
            let rpc_start = std::time::Instant::now();
            let result = match self
                .rpc_client
                .get_events(
                    &self.config.stellar_rpc_url,
                    start_ledger,
                    cursor.clone(),
                    &self.config.indexer_event_types,
                )
                .await
            {
                Ok(r) => r,
                Err(msg) => {
                    warn!(error = %msg, "RPC error");
                    metrics::record_rpc_error();
                    return Err(IndexerFetchError::Rpc(msg));
                }
            };
            total_rpc_ms += rpc_start.elapsed().as_millis();
            total_pages += 1;

            latest_ledger = result.latest_ledger;
            let current_count = result.events.len();
            total_fetched += current_count;
            let schema_version = result.protocol_version.unwrap_or(1) as i32;
            let next_cursor = result.rpc_cursor.clone();

            // Store events and checkpoint atomically in one transaction.
            let mut db_tx = self.pool.begin().await?;
            let db_start = std::time::Instant::now();
            for event in result.events {
                // Apply Lua transformation if configured
                #[cfg(feature = "lua")]
                let event = if let Some(ref transformer) = self.lua_transformer {
                    match transformer.transform(event).await {
                        Ok(Some(transformed)) => transformed,
                        Ok(None) => {
                            // Script returned nil, skip this event
                            total_skipped += 1;
                            continue;
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                "Lua transformation failed, skipping event"
                            );
                            total_skipped += 1;
                            continue;
                        }
                    }
                } else {
                    event
                };

                match self
                    .store_event_in_tx(&mut db_tx, &event, schema_version)
                    .await
                {
                    Ok(rows) => {
                        total_inserted += rows;
                        if rows == 0 {
                            total_skipped += 1;
                            // duplicate — skipped via ON CONFLICT DO NOTHING
                        } else {
                            if let Some(ref tx) = self.event_tx {
                                let _ = tx.send(event.clone());
                            }
                            // Issue #265: publish to Kinesis
                            if let Some(ref publisher) = self.kinesis_publisher {
                                crate::kinesis::publish_event(publisher.as_ref(), &event).await;
                            }
                            // Issue #264: publish to Pub/Sub
                            if let Some(ref publisher) = self.pubsub_publisher {
                                crate::pubsub::publish_event(publisher.as_ref(), &event).await;
                            }
                            #[cfg(feature = "kafka")]
                            if let (Some(ref publisher), Some(ref topic)) =
                                (&self.kafka_publisher, &self.kafka_topic)
                            {
                                if let Err(e) = publisher.publish(topic, &event).await {
                                    tracing::warn!(error = %e, contract_id = %event.contract_id, "Kafka publish failed");
                                    crate::metrics::record_kafka_publish_error();
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "Failed to store event",
                        );
                    }
                }
            }
            // Persist the cursor that was just consumed so restarts resume correctly.
            if let Some(ref c) = next_cursor.as_ref().or(cursor.as_ref()) {
                let _ = Self::save_checkpoint(&mut db_tx, c).await;
            }
            // Persist ledger state so /status is accurate after a restart.
            let _ = Self::save_indexer_state(&mut db_tx, start_ledger, latest_ledger).await;
            db_tx.commit().await?;

            total_db_ms += db_start.elapsed().as_millis();
            cursor = next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        let cycle_ms = cycle_start.elapsed().as_millis();
        if total_inserted > 0 {
            info!(
                fetched = total_fetched,
                inserted = total_inserted,
                start_ledger = start_ledger_for_log,
                end_ledger = latest_ledger,
                pages = total_pages,
                cycle_ms = cycle_ms,
                rpc_ms = total_rpc_ms,
                db_ms = total_db_ms,
                "Indexed ledger range",
            );
        } else {
            tracing::debug!(
                fetched = total_fetched,
                inserted = total_inserted,
                start_ledger = start_ledger_for_log,
                end_ledger = latest_ledger,
                pages = total_pages,
                cycle_ms = cycle_ms,
                rpc_ms = total_rpc_ms,
                db_ms = total_db_ms,
                "Indexed ledger range (no new events)",
            );
        }
        metrics::record_events_indexed(total_inserted as u64);

        if total_fetched > 0 {
            let duplicate_rate = (total_skipped as f64 / total_fetched as f64) * 100.0;
            tracing::debug!(
                duplicates = total_skipped,
                total_fetched = total_fetched,
                duplicate_rate = duplicate_rate,
                "Event deduplication stats"
            );
        }

        if latest_ledger > start_ledger {
            Ok(latest_ledger + 1)
        } else {
            Ok(start_ledger)
        }
    }
    fn validate_event_data(event: &SorobanEvent) -> bool {
        // Validate that value is an object or null
        match &event.value {
            serde_json::Value::Object(_) | serde_json::Value::Null => {}
            other => {
                warn!(
                    tx_hash = %event.tx_hash,
                    contract_id = %event.contract_id,
                    ledger = event.ledger,
                    event_type = %event.event_type,
                    value_type = match other {
                        serde_json::Value::Null => "null",
                        serde_json::Value::Bool(_) => "bool",
                        serde_json::Value::Number(_) => "number",
                        serde_json::Value::String(_) => "string",
                        serde_json::Value::Array(_) => "array",
                        serde_json::Value::Object(_) => "object",
                    },
                    "Invalid event_data.value: expected object or null",
                );
                metrics::record_validation_failure();
                return false;
            }
        }

        // Validate that topic is nested list (array) if present
        if let Some(ref topic) = event.topic {
            // topic is Vec<Value>, so it's always an array in JSON terms
            // but we might want to validate something about it?
            // The original code was trying to validate it was a JSON Array.
            // If it's Vec<Value>, it's already structured.
            let _ = topic;
        }

        // Issue #267: XDR/ScVal validation
        if !xdr_validation::validate_xdr(
            &event.tx_hash,
            &event.contract_id,
            event.ledger,
            &event.value,
            event.topic.as_ref(),
        ) {
            return false;
        }

        true
    }

    fn should_log_debug(&self) -> bool {
        let count = self.event_counter.fetch_add(1, Ordering::Relaxed);
        count % u64::from(self.config.log_sample_rate) == 0
    }

    #[instrument(skip(self, event), fields(tx_hash = %event.tx_hash, contract_id = %event.contract_id, ledger = event.ledger))]
    async fn store_event(
        &self,
        event: &SorobanEvent,
        schema_version: i32,
    ) -> Result<u64, anyhow::Error> {
        if self.should_log_debug() {
            tracing::debug!(
                tx_hash = %event.tx_hash,
                contract_id = %event.contract_id,
                ledger = event.ledger,
                event_type = %event.event_type,
                "Processing event"
            );
        }

        // Validate event_data structure
        if !Self::validate_event_data(event) {
            return Ok(0);
        }

        let ledger = match i64::try_from(event.ledger) {
            Ok(v) => v,
            Err(_) => {
                error!(
                    tx_hash = %event.tx_hash,
                    contract_id = %event.contract_id,
                    ledger = event.ledger,
                    event_type = %event.event_type,
                    "Ledger number overflows i64, skipping event",
                );
                return Ok(0);
            }
        };
        let timestamp = DateTime::parse_from_rfc3339(&event.ledger_closed_at)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .map_err(|_| {
                warn!(
                    tx_hash = %event.tx_hash,
                    contract_id = %event.contract_id,
                    ledger = event.ledger,
                    event_type = %event.event_type,
                    raw = %event.ledger_closed_at,
                    "Unparseable ledger_closed_at, skipping event",
                );
                anyhow::anyhow!("Unparseable ledger_closed_at: {}", event.ledger_closed_at)
            })?;

        let event_data = json!({
            "value": event.value,
            "topic": event.topic
        });

        let event_data = if let Some(ref key) = self.config.event_data_encryption_key {
            crate::encryption::encrypt(key, &event_data).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to encrypt event_data, storing plaintext");
                event_data
            })
        } else {
            event_data
        };

        // RETURNING (xmax = 0) distinguishes a true INSERT (xmax=0) from an UPDATE (xmax≠0).
        let inserted: bool = sqlx::query_scalar(
            r#"
            INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data, schema_version)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tx_hash, contract_id, event_type)
            DO UPDATE SET event_data = events.event_data || EXCLUDED.event_data
            RETURNING (xmax = 0)
            "#,
        )
        .bind(&event.contract_id)
        .bind(&event.event_type)
        .bind(&event.tx_hash)
        .bind(ledger)
        .bind(timestamp)
        .bind(event_data)
        .bind(schema_version)
        .fetch_one(&self.pool)
        .await?;

        Ok(u64::from(inserted))
    }

    async fn store_event_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        event: &SorobanEvent,
        schema_version: i32,
    ) -> Result<u64, anyhow::Error> {
        // Issue #266: bloom filter pre-filter
        if let Some(ref bloom) = self.bloom_filter {
            if bloom.check(&event.tx_hash, &event.contract_id, &event.event_type) {
                return Ok(0);
            }
        }

        // Multi-tenant contract filter: skip events whose contract_id is not in
        // the allow-list for this tenant.
        if self.config.multi_tenant {
            if let Some(ref tenant_id) = self.config.indexer_tenant_id {
                if let Some(allowed) = self.config.tenant_contract_filter.get(tenant_id) {
                    if !allowed.is_empty() && !allowed.contains(&event.contract_id) {
                        return Ok(0);
                    }
                }
            }
        }

        if !Self::validate_event_data(event) {
            return Ok(0);
        }
        let ledger = match i64::try_from(event.ledger) {
            Ok(v) => v,
            Err(_) => return Ok(0),
        };
        let timestamp = DateTime::parse_from_rfc3339(&event.ledger_closed_at)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .map_err(|_| anyhow::anyhow!("Unparseable ledger_closed_at"))?;
        let event_data = json!({ "value": event.value, "topic": event.topic });

        // Look up ABI for this contract and decode event_data if available.
        let event_data_decoded =
            decode_event_with_abi(&self.pool, &event.contract_id, &event_data).await;
        // Enforce size limit before INSERT.
        let serialized = serde_json::to_vec(&event_data)?;
        if serialized.len() > self.config.max_event_data_bytes {
            warn!(
                tx_hash = %event.tx_hash,
                contract_id = %event.contract_id,
                ledger = event.ledger,
                size_bytes = serialized.len(),
                limit_bytes = self.config.max_event_data_bytes,
                "event_data exceeds size limit, skipping",
            );
            metrics::record_oversized_event();
            return Ok(0);
        }

        let tenant_id = if self.config.multi_tenant {
            self.config.indexer_tenant_id.as_deref()
        } else {
            None
        };

        let result = sqlx::query(
            r#"INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data, ledger_hash, in_successful_call, event_data_decoded, tenant_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
               ON CONFLICT (tx_hash, contract_id, event_type) DO NOTHING"#,
        )
        .bind(&event.contract_id)
        .bind(&event.event_type)
        .bind(&event.tx_hash)
        .bind(ledger)
        .bind(timestamp)
        .bind(event_data)
        .bind(&event.ledger_hash)
        .bind(event.in_successful_call)
        .bind(event_data_decoded)
        .bind(tenant_id)
        .execute(&mut **tx)
        .await?;

        let rows = result.rows_affected();
        if rows > 0 {
            // Record in bloom filter after successful insert
            if let Some(ref bloom) = self.bloom_filter {
                bloom.set(&event.tx_hash, &event.contract_id, &event.event_type);
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use tokio::sync::watch;

    #[derive(Debug, Clone)]
    pub struct MockRpcClient {
        latest_ledger_responses: Arc<Mutex<VecDeque<Result<u64, String>>>>,
        get_events_responses: Arc<Mutex<VecDeque<Result<GetEventsResult, String>>>>,
    }

    impl MockRpcClient {
        pub fn new() -> Self {
            Self {
                latest_ledger_responses: Arc::new(Mutex::new(VecDeque::new())),
                get_events_responses: Arc::new(Mutex::new(VecDeque::new())),
            }
        }

        pub fn with_latest_ledger_responses(responses: Vec<Result<u64, String>>) -> Self {
            Self {
                latest_ledger_responses: Arc::new(Mutex::new(VecDeque::from(responses))),
                get_events_responses: Arc::new(Mutex::new(VecDeque::new())),
            }
        }

        pub fn with_get_events_responses(responses: Vec<Result<GetEventsResult, String>>) -> Self {
            Self {
                latest_ledger_responses: Arc::new(Mutex::new(VecDeque::new())),
                get_events_responses: Arc::new(Mutex::new(VecDeque::from(responses))),
            }
        }

        pub fn add_latest_ledger_response(&self, response: Result<u64, String>) {
            self.latest_ledger_responses
                .lock()
                .unwrap()
                .push_back(response);
        }

        pub fn add_get_events_response(&self, response: Result<GetEventsResult, String>) {
            self.get_events_responses
                .lock()
                .unwrap()
                .push_back(response);
        }
    }

    impl Default for MockRpcClient {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait::async_trait]
    impl RpcClient for MockRpcClient {
        async fn get_latest_ledger(&self, _rpc_url: &str) -> Result<u64, String> {
            let mut responses = self.latest_ledger_responses.lock().unwrap();
            responses.pop_front().unwrap_or(Ok(100))
        }

        async fn get_events(
            &self,
            _rpc_url: &str,
            _start_ledger: u64,
            _cursor: Option<String>,
            _event_types: &[String],
        ) -> Result<GetEventsResult, String> {
            let mut responses = self.get_events_responses.lock().unwrap();
            responses.pop_front().unwrap_or(Ok(GetEventsResult {
                events: vec![],
                latest_ledger: 100,
                rpc_cursor: None,
                protocol_version: None,
            }))
        }
    }

    fn make_event(ledger: u64) -> SorobanEvent {
        SorobanEvent {
            contract_id: "C1".into(),
            event_type: "contract".into(),
            tx_hash: "abc".into(),
            ledger,
            ledger_closed_at: "2026-03-24T00:00:00Z".into(),
            value: Value::Null,
            topic: None,
            ..Default::default()
        }
    }

    #[test]
    fn ledger_overflow_returns_err() {
        assert!(i64::try_from(make_event(u64::MAX).ledger).is_err());
    }

    #[test]
    fn validate_event_data_accepts_valid_object_value() {
        let mut event = make_event(1);
        event.value = json!({"key": "value"});
        event.topic = Some(vec![json!("topic1")]);
        assert!(Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_accepts_null_value() {
        let mut event = make_event(1);
        event.value = Value::Null;
        event.topic = None;
        assert!(Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_accepts_null_topic() {
        let mut event = make_event(1);
        event.value = json!({"key": "value"});
        event.topic = None;
        assert!(Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_accepts_array_topic() {
        let mut event = make_event(1);
        event.value = json!({"key": "value"});
        event.topic = Some(vec![json!("topic1"), json!("topic2")]);
        assert!(Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_rejects_string_value() {
        let mut event = make_event(1);
        event.value = Value::String("invalid".to_string());
        assert!(!Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_rejects_number_value() {
        let mut event = make_event(1);
        event.value = Value::Number(42.into());
        assert!(!Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_rejects_array_value() {
        let mut event = make_event(1);
        event.value = Value::Array(vec![]);
        assert!(!Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn invalid_event_data_is_skipped(pool: PgPool) {
        let indexer = indexer(pool.clone());
        let mut event = make_event(1);
        event.value = Value::String("invalid".to_string());

        let result = indexer.store_event(&event, 1).await.unwrap();
        assert_eq!(result, 0); // Event should be skipped

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    fn indexer(pool: PgPool) -> Indexer<MockRpcClient> {
        let (_, shutdown_rx) = watch::channel(false);
        Indexer::new(
            pool,
            Config {
                database_url: String::new(),
                database_replica_url: None,
                stellar_rpc_url: String::new(),
                rpc_headers: Vec::new(),
                start_ledger: 0,
                port: 3000,
                behind_proxy: false,
                start_ledger_fallback: true,
                indexer_lag_warn_threshold: 1000,
                rpc_connect_timeout_secs: 30,
                rpc_request_timeout_secs: 60,
                api_keys: Vec::new(),
                db_max_connections: 10,
                db_min_connections: 2,
                db_idle_timeout_secs: 600,
                db_max_lifetime_secs: 1800,
                db_test_before_acquire: true,
                allowed_origins: vec!["*".to_string()],
                rate_limit_per_minute: 60,
                indexer_stall_timeout_secs: 60,
                db_statement_timeout_ms: 5000,
                indexer_poll_interval_ms: 5000,
                indexer_error_backoff_ms: 10000,
                sse_keepalive_interval_ms: 15000,
                sse_max_connections: 1000,
                environment: crate::config::Environment::Development,
                max_body_size_bytes: 1024 * 1024,
                log_sample_rate: 1,
                webhook_url: None,
                webhook_secret: None,
                webhook_contract_filter: Vec::new(),
                indexer_event_types: Vec::new(),
                event_data_encryption_key: None,
                event_data_encryption_key_old: None,
                index_check_interval_hours: 24,
                health_check_timeout_ms: 2000,
                tls_cert_file: None,
                tls_key_file: None,
                bloom_filter_fp_rate: 0.001,
                bloom_filter_capacity: 100_000,
                kinesis_stream_name: None,
                aws_region: None,
                pubsub_project_id: None,
                pubsub_topic_id: None,
                max_event_data_bytes: 65536,
                indexer_lock_retry_secs: 30,

                #[cfg(feature = "kafka")]
                kafka_brokers: None,
                #[cfg(feature = "kafka")]
                kafka_topic: None,
                #[cfg(feature = "kafka")]
                kafka_batch_size: 16384,
                #[cfg(feature = "kafka")]
                kafka_linger_ms: 5,
            },
            shutdown_rx,
            MockRpcClient::new(),
        )
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn should_log_debug_respects_sample_rate(pool: PgPool) {
        let pool_clone = pool.clone();
        let _ = pool_clone; // pool available if needed
        let mut indexer = indexer(pool);

        // Test with sample_rate = 1 (should log everything)
        indexer.config.log_sample_rate = 1;
        for _ in 0..100 {
            assert!(indexer.should_log_debug());
        }

        // Test with sample_rate = 10 (should log every 10th)
        indexer.config.log_sample_rate = 10;
        indexer.event_counter.store(0, Ordering::SeqCst);
        let mut logs = 0;
        for _ in 0..100 {
            if indexer.should_log_debug() {
                logs += 1;
            }
        }
        assert_eq!(logs, 10);

        // Test with sample_rate = 3
        indexer.config.log_sample_rate = 3;
        indexer.event_counter.store(0, Ordering::SeqCst);
        let mut logs = 0;
        for _ in 0..100 {
            if indexer.should_log_debug() {
                logs += 1;
            }
        }
        // 0, 3, 6, ..., 99 -> 34 logs
        assert_eq!(logs, 34);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn duplicate_insert_yields_one_row(pool: PgPool) {
        let indexer = indexer(pool.clone());
        let event = make_event(1);

        indexer.store_event(&event, 1).await.unwrap();
        indexer.store_event(&event, 1).await.unwrap(); // must not error

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn same_tx_hash_different_event_type_both_stored(pool: PgPool) {
        let indexer = indexer(pool.clone());
        let mut e1 = make_event(1);
        let mut e2 = make_event(1);
        e2.event_type = "system".into();

        indexer.store_event(&e1, 1).await.unwrap();
        indexer.store_event(&e2, 1).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn checkpoint_is_saved_and_loaded(pool: PgPool) {
        let idx = indexer(pool.clone());

        // No checkpoint initially
        assert!(idx.load_checkpoint().await.is_none());

        // Save a checkpoint inside a transaction
        let mut tx = pool.begin().await.unwrap();
        Indexer::<MockRpcClient>::save_checkpoint(&mut tx, "1234567-0")
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Should now be readable
        assert_eq!(idx.load_checkpoint().await.as_deref(), Some("1234567-0"));

        // Overwrite with a newer cursor
        let mut tx = pool.begin().await.unwrap();
        Indexer::<MockRpcClient>::save_checkpoint(&mut tx, "1234568-0")
            .await
            .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(idx.load_checkpoint().await.as_deref(), Some("1234568-0"));
    }

    #[tokio::test]
    async fn mock_rpc_client_returns_configured_responses() {
        let mock_client = MockRpcClient::with_latest_ledger_responses(vec![
            Ok(42),
            Err("RPC error".to_string()),
            Ok(100),
        ]);

        assert_eq!(
            mock_client.get_latest_ledger("http://test").await.unwrap(),
            42
        );
        assert!(mock_client.get_latest_ledger("http://test").await.is_err());
        assert_eq!(
            mock_client.get_latest_ledger("http://test").await.unwrap(),
            100
        );
    }

    #[tokio::test]
    async fn mock_rpc_client_get_events_returns_configured_responses() {
        let test_event = make_event(1);
        let mock_client = MockRpcClient::with_get_events_responses(vec![
            Ok(GetEventsResult {
                events: vec![test_event.clone()],
                latest_ledger: 50,
                rpc_cursor: None,
                protocol_version: None,
            }),
            Err("Network error".to_string()),
        ]);

        let result1 = mock_client
            .get_events("http://test", 1, None, &[])
            .await
            .unwrap();
        assert_eq!(result1.events.len(), 1);
        assert_eq!(result1.latest_ledger, 50);

        let result2 = mock_client.get_events("http://test", 2, None, &[]).await;
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn get_events_passes_event_types_filter() {
        // Verify that the event_types parameter is forwarded to the RPC call.
        // We use a mock that records what was passed.
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        #[derive(Clone)]
        struct RecordingMock {
            called_with_types: Arc<Mutex<Vec<Vec<String>>>>,
        }

        #[async_trait::async_trait]
        impl RpcClient for RecordingMock {
            async fn get_latest_ledger(&self, _: &str) -> Result<u64, String> {
                Ok(100)
            }
            async fn get_events(
                &self,
                _: &str,
                _: u64,
                _: Option<String>,
                event_types: &[String],
            ) -> Result<GetEventsResult, String> {
                self.called_with_types
                    .lock()
                    .unwrap()
                    .push(event_types.to_vec());
                Ok(GetEventsResult {
                    events: vec![],
                    latest_ledger: 100,
                    rpc_cursor: None,
                })
            }
        }

        let recording = RecordingMock {
            called_with_types: Arc::new(Mutex::new(vec![])),
        };
        let types_ref = recording.called_with_types.clone();

        recording
            .get_events("http://test", 1, None, &["contract".to_string()])
            .await
            .unwrap();
        let calls = types_ref.lock().unwrap();
        assert_eq!(calls[0], vec!["contract"]);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn indexer_uses_mock_rpc_client(pool: PgPool) {
        let mock_client = MockRpcClient::new();
        mock_client.add_latest_ledger_response(Ok(100));

        let test_event = make_event(100);
        mock_client.add_get_events_response(Ok(GetEventsResult {
            events: vec![test_event],
            latest_ledger: 100,
            rpc_cursor: None,
            protocol_version: None,
        }));

        let (_, shutdown_rx) = watch::channel(false);
        let indexer = Indexer::new(
            pool,
            Config {
                database_url: String::new(),
                database_replica_url: None,
                stellar_rpc_url: String::new(),
                rpc_headers: Vec::new(),
                start_ledger: 100,
                port: 3000,
                behind_proxy: false,
                start_ledger_fallback: true,
                indexer_lag_warn_threshold: 1000,
                rpc_connect_timeout_secs: 30,
                rpc_request_timeout_secs: 60,
                api_keys: Vec::new(),
                db_max_connections: 10,
                db_min_connections: 2,
                db_idle_timeout_secs: 600,
                db_max_lifetime_secs: 1800,
                db_test_before_acquire: true,
                allowed_origins: vec!["*".to_string()],
                rate_limit_per_minute: 60,
                indexer_stall_timeout_secs: 60,
                db_statement_timeout_ms: 5000,
                indexer_poll_interval_ms: 5000,
                indexer_error_backoff_ms: 10000,
                sse_keepalive_interval_ms: 15000,
                sse_max_connections: 1000,
                environment: crate::config::Environment::Development,
                max_body_size_bytes: 1024 * 1024,
                log_sample_rate: 1,
                webhook_url: None,
                webhook_secret: None,
                webhook_contract_filter: Vec::new(),
                indexer_event_types: Vec::new(),
                event_data_encryption_key: None,
                event_data_encryption_key_old: None,
                index_check_interval_hours: 24,
                health_check_timeout_ms: 2000,
                tls_cert_file: None,
                tls_key_file: None,
                bloom_filter_fp_rate: 0.001,
                bloom_filter_capacity: 100_000,
                kinesis_stream_name: None,
                aws_region: None,
                pubsub_project_id: None,
                pubsub_topic_id: None,
                max_event_data_bytes: 65536,
                indexer_lock_retry_secs: 30,
            },
            shutdown_rx,
            mock_client,
        );

        // Test that the indexer can use the mock client
        let latest_ledger = indexer.get_latest_ledger().await.unwrap();
        assert_eq!(latest_ledger, 100);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn oversized_event_is_skipped(pool: PgPool) {
        let mut idx = indexer(pool.clone());
        idx.config.max_event_data_bytes = 10; // tiny limit

        let mut event = make_event(1);
        // value large enough to exceed 10 bytes when serialized
        event.value = json!({"k": "a very long string value that exceeds the limit"});

        let mut tx = pool.begin().await.unwrap();
        let rows = idx.store_event_in_tx(&mut tx, &event).await.unwrap();
        tx.commit().await.unwrap();

        assert_eq!(rows, 0, "oversized event must be skipped");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn event_within_size_limit_is_stored(pool: PgPool) {
        let idx = indexer(pool.clone()); // default 65536 limit

        let event = make_event(1); // tiny event, well within limit

        let mut tx = pool.begin().await.unwrap();
        let rows = idx.store_event_in_tx(&mut tx, &event).await.unwrap();
        tx.commit().await.unwrap();

        assert_eq!(rows, 1);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    /// Verify that a standby replica acquires the advisory lock after the leader releases it.
    ///
    /// Steps:
    /// 1. Acquire the lock on a dedicated connection (simulates the leader).
    /// 2. Start a standby indexer with a 1-second retry interval.
    /// 3. Release the lock from the leader connection.
    /// 4. Assert the standby promotes within a few retry cycles.
    #[sqlx::test(migrations = "./migrations")]
    async fn standby_promotes_after_leader_releases_lock(pool: PgPool) {
        use std::sync::atomic::{AtomicBool, Ordering as AO};

        // Step 1: acquire the lock on a separate connection to simulate the leader.
        let mut leader_conn = pool.acquire().await.unwrap();
        let held: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(INDEXER_LOCK_KEY)
            .fetch_one(&mut *leader_conn)
            .await
            .unwrap();
        assert!(held, "leader must acquire the lock");

        // Step 2: build a standby indexer with a 1-second retry interval.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let promoted = Arc::new(AtomicBool::new(false));
        let promoted_clone = promoted.clone();
        let indexer_state = Arc::new(crate::config::IndexerState::new());
        let indexer_state_clone = indexer_state.clone();

        let mut standby = indexer(pool.clone());
        standby.config.indexer_lock_retry_secs = 1;
        standby.config.indexer_poll_interval_ms = 100; // short poll so run_loop yields quickly
        standby.shutdown_rx = shutdown_rx;
        standby.indexer_state = Some(indexer_state_clone);

        // Run the standby in a background task; it will block in the retry loop.
        let run_handle = tokio::spawn(async move {
            standby.run().await;
            promoted_clone.store(true, AO::Relaxed);
        });

        // Give the standby one tick to attempt and fail.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !indexer_state.is_active_indexer.load(AO::Relaxed),
            "standby must not be active while leader holds the lock"
        );

        // Step 3: release the lock from the leader connection.
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(INDEXER_LOCK_KEY)
            .execute(&mut *leader_conn)
            .await
            .unwrap();
        drop(leader_conn);

        // Step 4: wait up to 3 seconds for the standby to promote.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            if indexer_state.is_active_indexer.load(AO::Relaxed) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("standby did not promote within 3 seconds after leader released the lock");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Clean up: send shutdown so the indexer exits.
        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), run_handle).await;
    }
}
