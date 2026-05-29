use axum::body::Body;
use axum::http::{header, HeaderMap};
use axum::response::sse::{Event, Sse};
use axum::response::Response;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{DateTime, Utc};
use futures::stream::{self, Stream, StreamExt};
use reqwest;
use serde_json::{json, Value};
use sqlx::Row;
use std::convert::Infallible;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info_span, instrument};
use uuid::Uuid;

use crate::{
    error::AppError,
    middleware::TenantId,
    models::{
        self, BatchTxRequest, ContractSummary, ExportParams, PaginationParams, ReplayRequest,
        StreamParams,
    },
    routes::AppState,
};
use std::sync::atomic::Ordering;

/// Compute a lightweight ETag from the last event id, created_at, and total count.
/// Uses SHA-256 truncated to 8 bytes, base64-encoded — no double-serialization needed.
fn compute_etag(last_id: &Uuid, last_created_at: &DateTime<Utc>, total: Option<i64>) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(last_id.as_bytes());
    h.update(
        last_created_at
            .timestamp_nanos_opt()
            .unwrap_or(0)
            .to_le_bytes(),
    );
    if let Some(t) = total {
        h.update(t.to_le_bytes());
    } else {
        h.update(0i64.to_le_bytes());
    }
    let digest = h.finalize();
    format!("\"{}\"", URL_SAFE_NO_PAD.encode(&digest[..8]))
}

/// Simple in-process cache entry for the contracts list.
struct CacheEntry {
    data: Value,
    expires_at: std::time::Instant,
}

static CONTRACTS_CACHE: OnceLock<Mutex<Option<CacheEntry>>> = OnceLock::new();

fn contracts_cache() -> &'static Mutex<Option<CacheEntry>> {
    CONTRACTS_CACHE.get_or_init(|| Mutex::new(None))
}

/// Extract the tenant_id from request extensions when multi-tenant mode is active.
/// Returns `None` in single-tenant mode (extension not present).
fn extract_tenant_id(extensions: &axum::http::Extensions) -> Option<&str> {
    extensions.get::<TenantId>().map(|t| t.0.as_str())
}

/// Append a `tenant_id = $N` condition when multi-tenant mode is active.
/// Returns the bind index incremented by 1 if a condition was added.
fn maybe_add_tenant_condition(
    conditions: &mut Vec<String>,
    bind_idx: &mut i32,
    tenant_id: Option<&str>,
) {
    if tenant_id.is_some() {
        conditions.push(format!("tenant_id = ${bind_idx}"));
        *bind_idx += 1;
    }
}

/// Encode a (ledger, id) pair as an opaque URL-safe base64 cursor.
fn encode_cursor(ledger: i64, id: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(format!("{ledger}:{id}"))
}

/// Decode a cursor back to (ledger, id). Returns a validation error on malformed input.
fn decode_cursor(cursor: &str) -> Result<(i64, Uuid), AppError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    let s = std::str::from_utf8(&bytes)
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    let (ledger_str, id_str) = s
        .split_once(':')
        .ok_or_else(|| AppError::Validation("invalid cursor".to_string()))?;
    let ledger = ledger_str
        .parse::<i64>()
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    let id =
        Uuid::parse_str(id_str).map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    Ok((ledger, id))
}

/// Map sqlx rows to a JSON array, projecting only the requested columns.
/// Gzip-compress `value` and return a base64-encoded string (standard alphabet, no padding).
/// Used by the `compact=true` query parameter to shrink large `event_data` payloads.
fn compact_event_data(value: &Value) -> Result<Value, AppError> {
    use base64::engine::general_purpose::STANDARD;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    let json_bytes = serde_json::to_vec(value)
        .map_err(|e| AppError::Internal(format!("serialization error: {e}")))?;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&json_bytes)
        .map_err(|e| AppError::Internal(format!("gzip write error: {e}")))?;
    let compressed = encoder
        .finish()
        .map_err(|e| AppError::Internal(format!("gzip finish error: {e}")))?;

    Ok(Value::String(STANDARD.encode(&compressed)))
}

fn rows_to_json(
    rows: &[sqlx::postgres::PgRow],
    columns: &[&str],
    enc_key: Option<&[u8; 32]>,
    enc_key_old: Option<&[u8; 32]>,
    compact: bool,
) -> Result<Vec<Value>, AppError> {
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let mut event = serde_json::Map::new();
        for &col in columns {
            match col {
                "id" => {
                    event.insert(col.to_string(), json!(row.try_get::<Uuid, _>(col)?));
                }
                "contract_id" => {
                    event.insert(col.to_string(), json!(row.try_get::<String, _>(col)?));
                }
                "event_type" => {
                    event.insert(col.to_string(), json!(row.try_get::<String, _>(col)?));
                }
                "tx_hash" => {
                    event.insert(col.to_string(), json!(row.try_get::<String, _>(col)?));
                }
                "ledger" => {
                    event.insert(col.to_string(), json!(row.try_get::<i64, _>(col)?));
                }
                "timestamp" => {
                    event.insert(
                        col.to_string(),
                        json!(row.try_get::<DateTime<Utc>, _>(col)?),
                    );
                }
                "event_data" => {
                    let raw: Value = row.try_get::<Value, _>(col)?;
                    let decrypted = decrypt_event_data(&raw, enc_key, enc_key_old);
                    if compact {
                        event.insert(col.to_string(), compact_event_data(&decrypted)?);
                    } else {
                        event.insert(col.to_string(), decrypted);
                    }
                }
                "event_data_normalized" => {
                    event.insert(
                        col.to_string(),
                        json!(row.try_get::<Option<Value>, _>(col)?),
                    );
                }
                "event_data_decoded" => {
                    event.insert(
                        col.to_string(),
                        json!(row.try_get::<Option<Value>, _>(col)?),
                    );
                }
                "ledger_hash" => {
                    event.insert(
                        col.to_string(),
                        json!(row.try_get::<Option<String>, _>(col)?),
                    );
                }
                "in_successful_call" => {
                    event.insert(col.to_string(), json!(row.try_get::<bool, _>(col)?));
                }
                "created_at" => {
                    event.insert(
                        col.to_string(),
                        json!(row.try_get::<DateTime<Utc>, _>(col)?),
                    );
                }
                "schema_version" => {
                    event.insert(col.to_string(), json!(row.try_get::<i32, _>(col)?));
                }
                "anonymized" => {
                    event.insert(col.to_string(), json!(row.try_get::<bool, _>(col)?));
                }
                _ => {}
            }
        }
        events.push(Value::Object(event));
    }
    Ok(events)
}
fn resolve_columns<'a>(params: &'a PaginationParams) -> Result<Vec<&'a str>, AppError> {
    params.columns().map_err(|(unknown, allowed)| {
        AppError::Validation(format!(
            "unknown fields: [{}]; valid fields are: [{}]",
            unknown.join(", "),
            allowed.join(", ")
        ))
    })
}

pub(crate) fn validate_contract_id(contract_id: &str) -> Result<(), AppError> {
    if contract_id.len() != 56 {
        return Err(AppError::Validation(
            "invalid contract_id format".to_string(),
        ));
    }
    if !contract_id.starts_with('C') {
        return Err(AppError::Validation(
            "invalid contract_id format".to_string(),
        ));
    }
    if !contract_id.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(AppError::Validation(
            "invalid contract_id format".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_tx_hash(tx_hash: &str) -> Result<(), AppError> {
    if tx_hash.len() != 64 {
        return Err(AppError::Validation("invalid tx_hash format".to_string()));
    }
    // Accept both uppercase and lowercase hex — callers should normalize to lowercase first.
    if !tx_hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AppError::Validation("invalid tx_hash format".to_string()));
    }
    Ok(())
}

/// Validate and parse ISO 8601 timestamp string.
fn validate_timestamp(ts: &str) -> Result<DateTime<Utc>, AppError> {
    ts.parse::<DateTime<Utc>>()
        .map_err(|_| AppError::Validation(format!("invalid timestamp format: {}", ts)))
}

async fn build_health_response(state: &AppState) -> (StatusCode, Value) {
    let mut db_ok = true;
    let db_status: &str;

    let timeout = Duration::from_millis(state.health_check_timeout_ms);

    let db_check =
        tokio::time::timeout(timeout, sqlx::query("SELECT 1").fetch_one(&state.pool)).await;

    match db_check {
        Ok(Ok(_)) => {
            db_status = "ok";
        }
        Ok(Err(sqlx::Error::PoolTimedOut)) => {
            db_ok = false;
            db_status = "pool_exhausted";
        }
        Ok(Err(_)) => {
            db_ok = false;
            db_status = "unreachable";
        }
        Err(_) => {
            // tokio timeout elapsed
            db_ok = false;
            db_status = "unreachable";
        }
    }

    // Check indexer status
    let indexer_status = if let Some(secs_ago) = state.health_state.is_indexer_stalled() {
        json!({
            "indexer": "stalled",
            "last_poll_secs_ago": secs_ago
        })
    } else {
        json!({"indexer": "ok"})
    };

    // Determine overall status
    let is_degraded =
        !db_ok || indexer_status.get("indexer").and_then(|v| v.as_str()) == Some("stalled");

    if is_degraded {
        let response = json!({
            "status": "degraded",
            "db": db_status,
        });
        // Merge indexer status
        let mut obj = serde_json::to_value(response).unwrap();
        if let Value::Object(ref mut map) = obj {
            if let Value::Object(indexer_map) = indexer_status {
                map.extend(indexer_map);
            }
        }
        (StatusCode::SERVICE_UNAVAILABLE, obj)
    } else {
        let response = json!({
            "status": "ok",
            "db": "ok",
            "indexer": "ok"
        });
        (StatusCode::OK, response)
    }
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "system",
    responses(
        (status = 200, description = "Service is healthy", body = serde_json::Value),
        (status = 503, description = "Service is degraded", body = ErrorResponse),
    )
)]
pub async fn health(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let (status, body) = build_health_response(&state).await;
    (status, Json(body))
}

#[utoipa::path(
    get,
    path = "/healthz/live",
    tag = "system",
    responses(
        (status = 200, description = "Process is alive", body = serde_json::Value),
    )
)]
pub async fn health_live() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({ "status": "alive" })))
}

#[utoipa::path(
    get,
    path = "/healthz/ready",
    tag = "system",
    responses(
        (status = 200, description = "Service is ready", body = serde_json::Value),
        (status = 503, description = "Service is not ready", body = ErrorResponse),
    )
)]
pub async fn health_ready(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let (status, body) = build_health_response(&state).await;
    (status, Json(body))
}

#[utoipa::path(
    get,
    path = "/v1/status",
    tag = "system",
    responses(
        (status = 200, description = "Indexer operational status", body = serde_json::Value),
    )
)]
pub async fn status(State(state): State<AppState>) -> Json<Value> {
    let current_ledger = state.indexer_state.current_ledger.load(Ordering::Relaxed);
    let latest_ledger = state.indexer_state.latest_ledger.load(Ordering::Relaxed);
    let lag_ledgers = latest_ledger.saturating_sub(current_ledger);
    let uptime_secs = state.indexer_state.uptime_secs();

    let indexer_status = if state.health_state.is_indexer_stalled().is_some() {
        "stalled"
    } else {
        "running"
    };

    let indexer_mode = if state
        .indexer_state
        .is_active_indexer
        .load(Ordering::Relaxed)
    {
        "active"
    } else {
        "read_only"
    };

    let indexer_paused = state.indexer_state.is_paused.load(Ordering::Relaxed);

    let total_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&state.pool)
        .await
        .unwrap_or(0);

    // Query event counts by type
    let events_by_type_rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type")
            .fetch_all(&state.pool)
            .await
            .unwrap_or_default();

    // Build events_by_type object with all event types (defaulting to 0 if not present)
    let mut events_by_type = serde_json::json!({
        "contract": 0i64,
        "diagnostic": 0i64,
        "system": 0i64,
    });

    for (event_type, count) in events_by_type_rows {
        if let Some(obj) = events_by_type.as_object_mut() {
            obj.insert(event_type, serde_json::json!(count));
        }
    }

    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": uptime_secs,
        "current_ledger": current_ledger,
        "latest_ledger": latest_ledger,
        "lag_ledgers": lag_ledgers,
        "total_events": total_events,
        "events_by_type": events_by_type,
        "indexer_status": indexer_status,
        "indexer_mode": indexer_mode,
        "indexer_paused": indexer_paused,
    }))
}

/// Returns aggregate statistics about indexed events.
#[utoipa::path(
    get,
    path = "/v1/events/stats",
    tag = "events",
    responses(
        (status = 200, description = "Aggregate event statistics", body = crate::models::EventStats),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
pub async fn get_event_stats(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    use std::collections::HashMap;

    let cache_key = "stats";

    // Try to get from cache
    if let Some(cached) = state.stats_cache.get(cache_key).await {
        crate::metrics::record_stats_cache_hit();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_str(&format!(
                "public, max-age={}",
                state.config.stats_cache_ttl_secs
            ))
            .unwrap_or_else(|_| axum::http::HeaderValue::from_static("public, max-age=30")),
        );
        return Ok((headers, Json(cached)));
    }

    crate::metrics::record_stats_cache_miss();

    // Total events and ledger range from raw table (fast with index)
    let totals_row = sqlx::query(
        "SELECT COUNT(*) AS total_events, MIN(ledger) AS min_ledger, MAX(ledger) AS max_ledger FROM events",
    )
    .fetch_one(&state.read_pool)
    .await?;

    let total_events: i64 = totals_row.try_get("total_events")?;
    let min_ledger: Option<i64> = totals_row.try_get("min_ledger")?;
    let max_ledger: Option<i64> = totals_row.try_get("max_ledger")?;

    // Per-type counts and time-windowed counts from events_daily_summary matview
    let type_rows = sqlx::query(
        "SELECT event_type, SUM(event_count) AS cnt FROM events_daily_summary GROUP BY event_type",
    )
    .fetch_all(&state.read_pool)
    .await?;

    let mut events_by_type: HashMap<String, i64> = HashMap::new();
    for row in &type_rows {
        let et: String = row.try_get("event_type")?;
        let cnt: i64 = row.try_get("cnt")?;
        events_by_type.insert(et, cnt);
    }
    events_by_type.entry("contract".to_string()).or_insert(0);
    events_by_type.entry("diagnostic".to_string()).or_insert(0);
    events_by_type.entry("system".to_string()).or_insert(0);

    // 24h and 7d counts from daily_summary (last 1 and 7 days)
    let window_row = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(event_count) FILTER (WHERE event_date >= CURRENT_DATE - INTERVAL '1 day'), 0)  AS events_last_24h,
            COALESCE(SUM(event_count) FILTER (WHERE event_date >= CURRENT_DATE - INTERVAL '7 days'), 0) AS events_last_7d
        FROM events_daily_summary
        "#,
    )
    .fetch_one(&state.read_pool)
    .await?;

    let events_last_24h: i64 = window_row.try_get("events_last_24h")?;
    let events_last_7d: i64 = window_row.try_get("events_last_7d")?;

    // Top 10 contracts from events_contract_summary matview
    let top_rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT contract_id, event_count FROM events_contract_summary ORDER BY event_count DESC LIMIT 10",
    )
    .fetch_all(&state.read_pool)
    .await?;

    let top_contracts = top_rows
        .into_iter()
        .map(
            |(contract_id, event_count)| crate::models::ContractStatEntry {
                contract_id,
                event_count,
            },
        )
        .collect();

    let stats = crate::models::EventStats {
        total_events,
        events_last_24h,
        events_last_7d,
        top_contracts,
        events_by_type,
        min_ledger,
        max_ledger,
        computed_at: Utc::now(),
    };

    let stats_json = serde_json::to_value(&stats)?;

    // Store in cache
    state
        .stats_cache
        .insert(cache_key.to_string(), stats_json.clone())
        .await;

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_str(&format!(
            "public, max-age={}",
            state.config.stats_cache_ttl_secs
        ))
        .unwrap_or_else(|_| axum::http::HeaderValue::from_static("public, max-age=30")),
    );

    Ok((headers, Json(stats_json)))
}

pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    crate::metrics::update_db_pool_metrics(&state.pool);
    state.prometheus_handle.render()
}

/// Serve the raw OpenAPI JSON spec.
pub async fn openapi_json() -> impl IntoResponse {
    use crate::routes::ApiDoc;
    use utoipa::OpenApi;
    Json(ApiDoc::openapi())
}

/// Serve a minimal Swagger UI HTML page.
pub async fn swagger_ui() -> impl IntoResponse {
    axum::response::Html(
        "<!DOCTYPE html><html><head><title>Soroban Pulse API</title>\
        <meta charset=\"utf-8\"/>\
        <link rel=\"stylesheet\" href=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui.css\"></head>\
        <body><div id=\"swagger-ui\"></div>\
        <script src=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js\"></script>\
        <script>SwaggerUIBundle({url:\"/openapi.json\",dom_id:\"#swagger-ui\"})</script>\
        </body></html>"
    )
}

/// Stream new events in real time via Server-Sent Events.
///
/// This endpoint is less preferred for contract-specific streaming; use
/// `/v1/events/contract/{contract_id}/stream` instead.
#[utoipa::path(
    get,
    path = "/v1/events/stream",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID (less preferred)"),
        ("fields" = Option<String>, Query, description = "Comma-separated list of fields to include in each event"),
    ),
    responses(
        (status = 200, description = "SSE stream of new events (text/event-stream)"),
        (status = 400, description = "Invalid contract_id format", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 503, description = "Too many SSE connections", body = ErrorResponse),
    )
)]
#[instrument(skip(state, headers, extensions), fields(contract_id = ?params.contract_id))]
pub async fn stream_events(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
    headers: axum::http::HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    stream_events_internal(State(state), params.contract_id, params.fields, headers, tenant_id)
        .await
}

/// Stream new events for a specific contract in real time via Server-Sent Events.
#[utoipa::path(
    get,
    path = "/v1/events/contract/{contract_id}/stream",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID"),
        ("fields" = Option<String>, Query, description = "Comma-separated list of fields to include in each event"),
    ),
    responses(
        (status = 200, description = "SSE stream of contract events (text/event-stream)"),
        (status = 400, description = "Invalid contract_id format", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 503, description = "Too many SSE connections", body = ErrorResponse),
    )
)]
pub async fn stream_events_by_contract(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(params): Query<StreamParams>,
    headers: axum::http::HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    validate_contract_id(&contract_id).map_err(|e| {
        let (status, body) = e.into_response_parts();
        (status, body)
    })?;
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    stream_events_internal(State(state), Some(contract_id), params.fields, headers, tenant_id)
        .await
}

/// Stream events for multiple contracts simultaneously via Server-Sent Events.
#[utoipa::path(
    get,
    path = "/v1/events/stream/multi",
    tag = "events",
    params(
        ("contract_ids" = String, Query, description = "Comma-separated list of contract IDs to subscribe to"),
    ),
    responses(
        (status = 200, description = "SSE stream of events from the specified contracts (text/event-stream)"),
        (status = 400, description = "Invalid or empty contract_ids", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 503, description = "Too many SSE connections", body = ErrorResponse),
    )
)]
#[instrument(skip(state, headers, extensions), fields(contract_ids = ?params.contract_ids))]
pub async fn stream_events_multi(
    State(state): State<AppState>,
    Query(params): Query<crate::models::MultiStreamParams>,
    headers: axum::http::HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let raw = params.contract_ids.unwrap_or_default();
    if raw.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "contract_ids must not be empty", "code": "VALIDATION_ERROR" })),
        ));
    }

    let ids: Vec<String> = raw.split(',').map(|s| s.trim().to_string()).collect();

    // Validate number of contract IDs does not exceed limit
    if ids.len() > state.config.sse_multi_max_contract_ids {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("too many contract IDs (max {})", state.config.sse_multi_max_contract_ids),
                "code": "VALIDATION_ERROR",
                "limit": state.config.sse_multi_max_contract_ids,
                "provided": ids.len(),
            })),
        ));
    }

    // Record histogram metric for contract IDs per connection
    crate::metrics::record_sse_multi_contract_ids(ids.len() as u64);

    // Validate every ID; collect all invalid ones for a helpful error message.
    let invalid: Vec<String> = ids
        .iter()
        .filter(|id| validate_contract_id(id).is_err())
        .cloned()
        .collect();

    if !invalid.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid contract_id(s)",
                "code": "VALIDATION_ERROR",
                "invalid_ids": invalid,
            })),
        ));
    }

    // Check connection limit
    let current_connections = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    if current_connections >= state.sse_max_connections {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "too many SSE connections", "code": "SSE_LIMIT_EXCEEDED" })),
        ));
    }

    state
        .sse_connections
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let new_count = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    crate::metrics::update_sse_connections(new_count);

    let keepalive_ms = state.sse_keepalive_interval_ms;
    let sse_connections = state.sse_connections.clone();

    // Replay missed events for any of the subscribed contracts.
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| uuid::Uuid::parse_str(s).ok());

    let replay: Vec<crate::models::Event> = if let Some(last_id) = last_event_id {
        let base_offset = 2usize;
        let placeholders: String = ids
            .iter()
            .enumerate()
            .map(|(i, _)| format!("${}", base_offset + i))
            .collect::<Vec<_>>()
            .join(", ");
        let tid = tenant_id.as_deref();
        let tenant_clause = if tid.is_some() {
            format!(" AND tenant_id = ${}", base_offset + ids.len())
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, 0::bigint AS total_count \
             FROM events WHERE created_at > (SELECT created_at FROM events WHERE id = $1) \
             AND contract_id IN ({}){} ORDER BY created_at ASC",
            placeholders, tenant_clause
        );
        let mut q = sqlx::query_as::<_, crate::models::Event>(&sql).bind(last_id);
        for id in &ids {
            q = q.bind(id);
        }
        if let Some(tid) = tid {
            q = q.bind(tid);
        }
        q.fetch_all(&state.pool).await.unwrap_or_default()
    } else {
        vec![]
    };

    let rx = state.event_tx.subscribe();
    let enc_key = state.encryption_key;
    let enc_key_old = state.encryption_key_old;

    let replay_stream = futures::stream::iter(replay.into_iter().map(move |mut ev| {
        ev.event_data = decrypt_event_data(&ev.event_data, enc_key.as_ref(), enc_key_old.as_ref());
        let data = serde_json::to_string(&ev).unwrap_or_default();
        Ok(Event::default()
            .id(ev.id.to_string())
            .retry(Duration::from_millis(keepalive_ms))
            .data(data))
    }));

    let live_stream = futures::stream::unfold(
        (rx, ids, keepalive_ms, tenant_id, false),
        move |(mut rx, filter_ids, ka, tid, closed)| async move {
            if closed {
                return None;
            }
            let mut interval = tokio::time::interval(Duration::from_millis(ka));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await; // consume the immediate first tick
            loop {
                tokio::select! {
                    recv = rx.recv() => match recv {
                        Ok(event) => {
                            if !filter_ids.contains(&event.contract_id) {
                                continue;
                            }
                            if let Some(ref tenant) = tid {
                                if event.tenant_id.as_deref() != Some(tenant.as_str()) {
                                    continue;
                                }
                            }
                            let data = serde_json::to_string(&event).unwrap_or_default();
                            let sse = Event::default()
                                .id(format!("{}-{}", event.tx_hash, event.ledger))
                                .retry(Duration::from_millis(ka))
                                .data(data);
                            return Some((Ok(sse), (rx, filter_ids, ka, tid, false)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            let close_event = Event::default().event("close").data("stream closed");
                            return Some((Ok(close_event), (rx, filter_ids, ka, tid, true)));
                        }
                    },
                    _ = interval.tick() => {
                        let ts = chrono::Utc::now().to_rfc3339();
                        let ping = Event::default().event("ping").data(ts);
                        return Some((Ok(ping), (rx, filter_ids, ka, tid, false)));
                    }
                }
            }
        },
    );

    let combined = replay_stream.chain(live_stream);

    let stream_with_cleanup = futures::stream::unfold(
        (Box::pin(combined), sse_connections.clone()),
        move |(mut stream, counter)| async move {
            match stream.next().await {
                Some(item) => Some((item, (stream, counter))),
                None => {
                    let new_count = counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
                    crate::metrics::update_sse_connections(new_count);
                    None
                }
            }
        },
    );

    Ok(Sse::new(stream_with_cleanup))
}

/// WebSocket event stream. Clients receive events as JSON text frames.
/// After connecting, a client may send `{"contract_id":"CABC..."}` to filter
/// by contract, or `{}` / omit the field to receive all events.
#[utoipa::path(
    get,
    path = "/v1/events/ws",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Initial contract ID filter (can also be set via WebSocket message)"),
    ),
    responses(
        (status = 101, description = "WebSocket upgrade"),
        (status = 400, description = "Invalid contract_id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn ws_events(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }

    let initial_filter = params.contract_id.clone();
    let event_tx = state.event_tx.clone();
    let enc_key = state.encryption_key;
    let enc_key_old = state.encryption_key_old;

    Ok(ws.on_upgrade(move |mut socket| async move {
        use axum::extract::ws::Message;

        let mut rx = event_tx.subscribe();
        let mut contract_filter = initial_filter;

        loop {
            tokio::select! {
                // Incoming message from client (filter update or close)
                msg = socket.recv() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                contract_filter = v.get("contract_id")
                                    .and_then(|c| c.as_str())
                                    .map(|s| s.to_string());
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => break,
                        _ => {}
                    }
                }
                // Outgoing event from broadcast channel
                result = rx.recv() => {
                    match result {
                        Ok(mut event) => {
                            if let Some(ref cid) = contract_filter {
                                if &event.contract_id != cid {
                                    continue;
                                }
                            }
                            let text = serde_json::to_string(&event).unwrap_or_default();
                            if socket.send(Message::Text(text.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    }))
}

async fn stream_events_internal(
    State(state): State<AppState>,
    contract_filter: Option<String>,
    fields: Option<String>,
    headers: axum::http::HeaderMap,
    tenant_id: Option<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    // Check if we've reached the max SSE connections limit
    let current_connections = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    if current_connections >= state.sse_max_connections {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "too many SSE connections",
                "code": "SSE_LIMIT_EXCEEDED"
            })),
        ));
    }

    // Increment connection counter
    state
        .sse_connections
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let new_count = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    crate::metrics::update_sse_connections(new_count);

    let keepalive_ms = state.sse_keepalive_interval_ms;
    let sse_connections = state.sse_connections.clone();

    // Validate contract_id if provided
    if let Some(ref cid) = contract_filter {
        validate_contract_id(cid).map_err(|e| {
            let body = json!({ "error": e.to_string(), "code": "VALIDATION_ERROR" });
            (StatusCode::BAD_REQUEST, Json(body))
        })?;
    }

    // Resolve field projection — silently ignore unknown fields (consistent with REST endpoints).
    let field_columns: Option<Vec<&'static str>> = fields.as_deref().and_then(|f| {
        let trimmed = f.trim();
        if trimmed.is_empty() {
            return None; // empty → all fields
        }
        let cols: Vec<&'static str> = trimmed
            .split(',')
            .map(|s| s.trim())
            .filter_map(|s| {
                PaginationParams::ALLOWED_FIELDS
                    .iter()
                    .find(|&&a| a == s)
                    .copied()
            })
            .collect();
        if cols.is_empty() {
            None
        } else {
            Some(cols)
        }
    });

    // Replay missed events if the client sends Last-Event-ID (a UUID).
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok());

    let replay: Vec<crate::models::Event> = if let Some(last_id) = last_event_id {
        let q = if let Some(ref cid) = contract_filter {
            sqlx::query_as::<_, crate::models::Event>(
                "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, event_data_normalized, created_at, schema_version, 0::bigint AS total_count \
                 FROM events WHERE created_at > (SELECT created_at FROM events WHERE id = $1) \
                 AND contract_id = $2 ORDER BY created_at ASC LIMIT $3",
            )
            .bind(last_id)
            .bind(cid)
            .bind(state.sse_replay_limit as i64)
            .fetch_all(&state.pool)
            .await
        } else {
            sqlx::query_as::<_, crate::models::Event>(
                "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, event_data_normalized, created_at, schema_version, 0::bigint AS total_count \
                 FROM events WHERE created_at > (SELECT created_at FROM events WHERE id = $1) \
                 ORDER BY created_at ASC LIMIT $2",
            )
            .bind(last_id)
            .bind(state.sse_replay_limit as i64)
            .fetch_all(&state.pool)
            .await
        };
        q.unwrap_or_default()
    } else {
        vec![]
    };

    let rx = state.event_tx.subscribe();
    let enc_key = state.encryption_key;
    let enc_key_old = state.encryption_key_old;

    let field_columns_replay = field_columns.clone();
    let replay_stream = stream::iter(replay.into_iter().map(move |mut ev| {
        ev.event_data = decrypt_event_data(&ev.event_data, enc_key.as_ref(), enc_key_old.as_ref());
        let data = match &field_columns_replay {
            Some(cols) => serde_json::to_string(&filter_fields(
                &ev,
                cols,
                enc_key.as_ref(),
                enc_key_old.as_ref(),
            ))
            .unwrap_or_default(),
            None => serde_json::to_string(&ev).unwrap_or_default(),
        };
        Ok(Event::default()
            .id(ev.id.to_string())
            .retry(Duration::from_millis(keepalive_ms))
            .data(data))
    }));

    let live_stream = stream::unfold(
        (
            rx,
            contract_filter,
            keepalive_ms,
            field_columns,
            enc_key,
            enc_key_old,
            tenant_id,
            false, // closed
            state.shutdown_rx.clone(),
        ),
        move |(mut rx, filter, ka, cols, ek, ek_old, tid, closed, mut shutdown_rx)| async move {
            if closed {
                return None;
            }
            let mut interval = tokio::time::interval(Duration::from_millis(ka));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await; // consume the immediate first tick
            loop {
                tokio::select! {
                    recv = rx.recv() => match recv {
                        Ok(event) => {
                            if let Some(ref cid) = filter {
                                if &event.contract_id != cid {
                                    continue;
                                }
                            }
                            // Tenant isolation: drop events belonging to other tenants.
                            if let Some(ref tenant) = tid {
                                if event.tenant_id.as_deref() != Some(tenant.as_str()) {
                                    continue;
                                }
                            }
                            let data = serde_json::to_string(&event).unwrap_or_default();
                            let sse = Event::default()
                                .id(event.id.to_string())
                                .retry(Duration::from_millis(ka))
                                .data(data);
                            return Some((Ok(sse), (rx, filter, ka, cols, ek, ek_old, tid, false, shutdown_rx)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            let close_event = Event::default().event("close").data("stream closed");
                            return Some((Ok(close_event), (rx, filter, ka, cols, ek, ek_old, tid, true, shutdown_rx)));
                        }
                    },
                    _ = interval.tick() => {
                        let ts = chrono::Utc::now().to_rfc3339();
                        let ping = Event::default().event("ping").data(ts);
                        return Some((Ok(ping), (rx, filter, ka, cols, ek, ek_old, tid, false, shutdown_rx)));
                    }
                    _ = shutdown_rx.changed() => {
                        // Server is shutting down, emit close event
                        let close_event = Event::default().event("close").data("server shutting down");
                        return Some((Ok(close_event), (rx, filter, ka, cols, ek, ek_old, tid, true, shutdown_rx)));
                    }
                }
            }
        },
    );

    let combined = replay_stream.chain(live_stream);
    let combined = Box::pin(combined);

    // Wrap the stream to decrement the connection counter when the stream ends
    let stream_with_cleanup = stream::unfold(
        (Box::pin(combined), sse_connections.clone()),
        move |(mut stream, counter)| async move {
            match stream.next().await {
                Some(item) => Some((item, (stream, counter))),
                None => {
                    let new_count = counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
                    crate::metrics::update_sse_connections(new_count);
                    None
                }
            }
        },
    );

    Ok(Sse::new(stream_with_cleanup))
}

/// Decrypt event_data if an encryption key is configured.
fn decrypt_event_data(raw: &Value, key: Option<&[u8; 32]>, old_key: Option<&[u8; 32]>) -> Value {
    if let Some(k) = key {
        crate::encryption::decrypt(k, old_key, raw).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to decrypt event_data, returning raw value");
            raw.clone()
        })
    } else {
        raw.clone()
    }
}

/// Converts an `Event` to a JSON object containing only the requested fields.
fn filter_fields(
    event: &models::Event,
    columns: &[&str],
    enc_key: Option<&[u8; 32]>,
    enc_key_old: Option<&[u8; 32]>,
) -> Value {
    let mut map = serde_json::Map::new();
    for &col in columns {
        match col {
            "id" => {
                map.insert(col.to_string(), json!(event.id));
            }
            "contract_id" => {
                map.insert(col.to_string(), json!(event.contract_id));
            }
            "event_type" => {
                map.insert(col.to_string(), json!(event.event_type));
            }
            "tx_hash" => {
                map.insert(col.to_string(), json!(event.tx_hash));
            }
            "ledger" => {
                map.insert(col.to_string(), json!(event.ledger));
            }
            "timestamp" => {
                map.insert(col.to_string(), json!(event.timestamp));
            }
            "event_data" => {
                let decrypted = decrypt_event_data(&event.event_data, enc_key, enc_key_old);
                map.insert(col.to_string(), decrypted);
            }
            "event_data_normalized" => {
                map.insert(col.to_string(), json!(event.event_data_normalized));
            }
            "event_data_decoded" => {
                map.insert(col.to_string(), json!(event.event_data_decoded));
            }
            "ledger_hash" => {
                map.insert(col.to_string(), json!(event.ledger_hash));
            }
            "in_successful_call" => {
                map.insert(col.to_string(), json!(event.in_successful_call));
            }
            "created_at" => {
                map.insert(col.to_string(), json!(event.created_at));
            }
            "schema_version" => {
                map.insert(col.to_string(), json!(event.schema_version));
            }
            "anonymized" => {
                map.insert(col.to_string(), json!(event.anonymized));
            }
            _ => {}
        }
    }
    Value::Object(map)
}

/// Returns true if the client prefers NDJSON via the Accept header.
fn accepts_ndjson(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("application/x-ndjson"))
        .unwrap_or(false)
}

/// Build a plain JSON response from a `Value`.
fn json_response(body: Value) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap_or_default()))
        .unwrap()
}

/// Build an NDJSON response: one JSON object per line, no wrapping array.
fn ndjson_response(events: impl Iterator<Item = Value>) -> Response<Body> {
    let mut buf = String::new();
    for ev in events {
        buf.push_str(&serde_json::to_string(&ev).unwrap_or_default());
        buf.push('\n');
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from(buf))
        .unwrap()
}

#[utoipa::path(
    get,
    path = "/v1/events",
    tag = "events",
    params(
        ("page" = Option<i64>, Query, description = "Page number (default: 1)"),
        ("limit" = Option<i64>, Query, description = "Results per page, 1–100 (default: 20)"),
        ("exact_count" = Option<bool>, Query, description = "Use exact COUNT(*) instead of approximate"),
        ("event_type" = Option<crate::models::EventType>, Query, description = "Filter by event type: contract, diagnostic, system"),
        ("from_ledger" = Option<i64>, Query, description = "Return events at or after this ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Return events at or before this ledger"),
        ("from_timestamp" = Option<String>, Query, description = "Return events at or after this timestamp (ISO 8601 format, e.g., 2026-03-14T00:00:00Z)"),
        ("to_timestamp" = Option<String>, Query, description = "Return events at or before this timestamp (ISO 8601 format, e.g., 2026-03-14T00:00:00Z)"),
        ("sort" = Option<String>, Query, description = "Sort order: asc (oldest first) or desc (newest first, default)"),
        ("sort_by" = Option<crate::models::SortBy>, Query, description = "Sort column: ledger (default), timestamp, or created_at"),
        ("topic_sym" = Option<String>, Query, description = "Filter by first topic symbol (uses topic_0_sym generated column index)"),
        ("search" = Option<String>, Query, description = "Full-text search query for event_data (searches all string values in the JSON)"),
        ("compact" = Option<bool>, Query, description = "Return event_data as a base64-encoded gzip-compressed JSON string instead of the full JSON object. Clients that need the full data can decode it; clients that only need metadata can ignore it. Default: false."),
    ),
    responses(
        (status = 200, description = "Paginated list of events (JSON or NDJSON depending on Accept header). When compact=true, each event's event_data field is a base64-encoded gzip-compressed JSON string (Content-Encoding: gzip).",
            content(
                ("application/json" = Value),
                ("application/x-ndjson" = String),
            )
        ),
        (status = 400, description = "Invalid query parameters"),
    )
)]
#[instrument(skip(state, headers, extensions), fields(page = ?params.page, limit = ?params.limit, contract_id = ?params.contract_id))]
pub async fn get_events(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
    headers: HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<impl IntoResponse, AppError> {
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let tenant_id = tenant_id.as_deref();
    // Validate ledger range
    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }

    // Validate and parse timestamp range
    let from_ts = if let Some(ref ts) = params.from_timestamp {
        Some(validate_timestamp(ts)?)
    } else {
        None
    };
    let to_ts = if let Some(ref ts) = params.to_timestamp {
        Some(validate_timestamp(ts)?)
    } else {
        None
    };

    // Validate timestamp range
    if let (Some(from), Some(to)) = (from_ts, to_ts) {
        if from > to {
            return Err(AppError::Validation(
                "from_timestamp must be <= to_timestamp".to_string(),
            ));
        }
    }

    // Validate contract_id if provided
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }

    // Parse and validate contract_ids if provided
    let contract_ids_list: Vec<String> = if let Some(ref cids) = params.contract_ids {
        let ids: Vec<&str> = cids.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        
        if ids.is_empty() {
            return Err(AppError::Validation(
                "contract_ids parameter is empty".to_string(),
            ));
        }
        
        if ids.len() > PaginationParams::MAX_CONTRACT_IDS_FILTER {
            return Err(AppError::Validation(
                format!(
                    "contract_ids exceeds maximum of {} IDs",
                    PaginationParams::MAX_CONTRACT_IDS_FILTER
                ),
            ));
        }
        
        for id in &ids {
            validate_contract_id(id)?;
        }
        
        ids.iter().map(|s| s.to_string()).collect()
    } else {
        Vec::new()
    };

    let limit = params.limit();
    let columns = resolve_columns(&params)?;
    let sort_order = params.sort.unwrap_or(crate::models::SortOrder::Desc);
    let dir = sort_order.as_sql();
    let sort_by = params.sort_by.unwrap_or(crate::models::SortBy::Ledger);
    let sort_col = sort_by.as_sql_col();

    // Cursor-based path
    if let Some(ref cursor_str) = params.cursor {
        let (cursor_tag, cursor_val_text, cursor_id) = decode_cursor_tagged(cursor_str)?;
        if cursor_tag != sort_by.as_tag() {
            return Err(crate::error::AppError::Validation(
                "cursor sort column does not match sort_by".to_string(),
            ));
        }

        let cursor_op = if sort_order == crate::models::SortOrder::Asc {
            ">"
        } else {
            "<"
        };

        let mut conditions: Vec<String> = vec![format!(
            "({col}, id) {op} ($1, $2)",
            col = sort_col,
            op = cursor_op
        )];
        let mut bind_idx: i32 = 3;

        if params.contract_id.is_some() {
            conditions.push(format!("contract_id = ${bind_idx}"));
            bind_idx += 1;
        }
        if !contract_ids_list.is_empty() {
            conditions.push(format!("contract_id = ANY(${bind_idx}::text[])"));
            bind_idx += 1;
        }
        if params.event_type.is_some() {
            conditions.push(format!("event_type = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.from_ledger.is_some() {
            conditions.push(format!("ledger >= ${bind_idx}"));
            bind_idx += 1;
        }
        if params.to_ledger.is_some() {
            conditions.push(format!("ledger <= ${bind_idx}"));
            bind_idx += 1;
        }
        if params.in_successful_call.is_some() {
            conditions.push(format!("in_successful_call = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.schema_version.is_some() {
            conditions.push(format!("schema_version = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.topic_sym.is_some() {
            conditions.push(format!("topic_0_sym = ${bind_idx}"));
            bind_idx += 1;
        }
        if topic_filter.is_some() {
            conditions.push(format!("event_data->'topic' @> ${bind_idx}::jsonb"));
            bind_idx += 1;
        }
        if params.search.is_some() {
            conditions.push(format!(
                "event_data_tsv @@ plainto_tsquery('english', ${bind_idx})"
            ));
            bind_idx += 1;
        }
        if from_ts.is_some() {
            conditions.push(format!("timestamp >= ${bind_idx}"));
            bind_idx += 1;
        }
        if to_ts.is_some() {
            conditions.push(format!("timestamp <= ${bind_idx}"));
            bind_idx += 1;
        }
        maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);

        let where_clause = format!("WHERE {}", conditions.join(" AND "));

        let mut select_cols = columns.to_vec();
        if !select_cols.contains(&sort_col) {
            select_cols.push(sort_col);
        }
        if !select_cols.contains(&"id") {
            select_cols.push("id");
        }
        if !select_cols.contains(&"created_at") {
            select_cols.push("created_at");
        }

        // Defence-in-depth: re-validate each column before SQL interpolation
        for col in &select_cols {
            if !models::PaginationParams::validate_column_name(col) {
                return Err(AppError::Validation(format!(
                    "invalid column name: {}",
                    col
                )));
            }
        }

        let query_str = format!(
            "SELECT {} FROM events {} ORDER BY {col} {dir}, id {dir} LIMIT ${}",
            select_cols.join(", "),
            where_clause,
            col = sort_col,
            dir = dir,
            bind_idx,
        );

        let mut q = sqlx::query(&query_str);
        match sort_by {
            crate::models::SortBy::Ledger => {
                let val = cursor_val_text.parse::<i64>().map_err(|_| {
                    crate::error::AppError::Validation("invalid ledger cursor".to_string())
                })?;
                q = q.bind(val).bind(cursor_id);
            }
            crate::models::SortBy::Timestamp | crate::models::SortBy::CreatedAt => {
                let ts = cursor_val_text
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .map_err(|_| {
                        crate::error::AppError::Validation("invalid timestamp cursor".to_string())
                    })?;
                q = q.bind(ts).bind(cursor_id);
            }
        }
        if let Some(ref cid) = params.contract_id {
            q = q.bind(cid);
        }
        if !contract_ids_list.is_empty() {
            q = q.bind(&contract_ids_list);
        }
        if let Some(ref et) = params.event_type {
            q = q.bind(et);
        }
        if let Some(fl) = params.from_ledger {
            q = q.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            q = q.bind(tl);
        }
        if let Some(isc) = params.in_successful_call {
            q = q.bind(isc);
        }
        if let Some(sv) = params.schema_version {
            q = q.bind(sv);
        }
        if let Some(ref ts) = params.topic_sym {
            q = q.bind(ts);
        }
        if let Some(ref tf) = topic_filter {
            q = q.bind(tf.to_string());
        }
        if let Some(ref search) = params.search {
            q = q.bind(search);
        }
        if let Some(ts) = from_ts {
            q = q.bind(ts);
        }
        if let Some(ts) = to_ts {
            q = q.bind(ts);
        }
        if let Some(tid) = tenant_id {
            q = q.bind(tid);
        }
        q = q.bind(limit);

        let _db_span = info_span!("db_query", query_type = "get_events_cursor").entered();
        let rows = q.fetch_all(&state.read_pool).await?;
        drop(_db_span);

        let has_more = rows.len() as i64 == limit;
        let next_cursor = if has_more {
            let last = rows.last().unwrap();
            let last_id: uuid::Uuid = last.try_get("id")?;
            let last_val_text = match sort_by {
                crate::models::SortBy::Ledger => {
                    let v: i64 = last.try_get("ledger")?;
                    v.to_string()
                }
                crate::models::SortBy::Timestamp => {
                    let v: chrono::DateTime<chrono::Utc> = last.try_get("timestamp")?;
                    v.to_rfc3339()
                }
                crate::models::SortBy::CreatedAt => {
                    let v: chrono::DateTime<chrono::Utc> = last.try_get("created_at")?;
                    v.to_rfc3339()
                }
            };
            Some(encode_cursor_tagged(
                sort_by.as_tag(),
                &last_val_text,
                last_id,
            ))
        } else {
            None
        };

        let events = rows_to_json(
            &rows,
            &columns,
            state.encryption_key.as_ref(),
            state.encryption_key_old.as_ref(),
            params.compact.unwrap_or(false),
        )?;

        // Build ETag from last row's id + created_at
        let etag = rows.first().and_then(|row| {
            let id: Option<Uuid> = row.try_get("id").ok();
            let created_at: Option<DateTime<Utc>> = row.try_get("created_at").ok();
            id.zip(created_at)
                .map(|(id, ca)| compute_etag(&id, &ca, None))
        });

        // Check If-None-Match — return 304 if ETag matches
        if let Some(ref tag) = etag {
            if let Some(inm) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
                if inm == tag {
                    let resp = axum::http::Response::builder()
                        .status(StatusCode::NOT_MODIFIED)
                        .header("ETag", tag.as_str())
                        .header("Cache-Control", "no-cache")
                        .body(axum::body::Body::empty())
                        .unwrap();
                    return Ok(resp.into_response());
                }
            }
        }

        let want_ndjson = accepts_ndjson(&headers);
        if want_ndjson {
            let mut resp = ndjson_response(events.into_iter()).into_response();
            if let Some(ref tag) = etag {
                resp.headers_mut().insert("ETag", tag.parse().unwrap());
                resp.headers_mut()
                    .insert("Cache-Control", "no-cache".parse().unwrap());
            }
            return Ok(resp);
        }

        let compact_mode = params.compact.unwrap_or(false);
        let mut resp = json_response(json!({
            "data": events,
            "next_cursor": next_cursor,
            "limit": limit,
        }));
        if let Some(ref tag) = etag {
            resp.headers_mut().insert("ETag", tag.parse().unwrap());
            resp.headers_mut()
                .insert("Cache-Control", "no-cache".parse().unwrap());
        }
        if compact_mode {
            resp.headers_mut().insert(
                "X-Event-Data-Encoding",
                axum::http::HeaderValue::from_static("gzip+base64"),
            );
        }
        return Ok(resp.into_response());
    }

    // Offset-based path (deprecated fallback)
    let offset = params.offset();
    let exact = params.exact_count.unwrap_or(false);

    let mut conditions: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;

    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${bind_idx}"));
        bind_idx += 1;
    }
    if !contract_ids_list.is_empty() {
        conditions.push(format!("contract_id = ANY(${bind_idx}::text[])"));
        bind_idx += 1;
    }
    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.in_successful_call.is_some() {
        conditions.push(format!("in_successful_call = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.schema_version.is_some() {
        conditions.push(format!("schema_version = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.topic_sym.is_some() {
        conditions.push(format!("topic_0_sym = ${bind_idx}"));
        bind_idx += 1;
    }
    if topic_filter.is_some() {
        conditions.push(format!("event_data->'topic' @> ${bind_idx}::jsonb"));
        bind_idx += 1;
    }
    if params.search.is_some() {
        conditions.push(format!(
            "event_data_tsv @@ plainto_tsquery('english', ${bind_idx})"
        ));
        bind_idx += 1;
    }
    if from_ts.is_some() {
        conditions.push(format!("timestamp >= ${bind_idx}"));
        bind_idx += 1;
    }
    if to_ts.is_some() {
        conditions.push(format!("timestamp <= ${bind_idx}"));
        bind_idx += 1;
    }
    maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let mut select_cols = columns.to_vec();
    if !select_cols.contains(&sort_col) {
        select_cols.push(sort_col);
    }
    if !select_cols.contains(&"id") {
        select_cols.push("id");
    }
    // Always fetch created_at for ETag computation
    if !select_cols.contains(&"created_at") {
        select_cols.push("created_at");
    }

    // Defence-in-depth: re-validate each column before SQL interpolation
    for col in &select_cols {
        if !models::PaginationParams::validate_column_name(col) {
            return Err(AppError::Validation(format!(
                "invalid column name: {}",
                col
            )));
        }
    }

    let query_str = format!(
        "SELECT {} FROM events {} ORDER BY {col} {dir}, id {dir} LIMIT ${} OFFSET ${}",
        select_cols.join(", "),
        where_clause,
        col = sort_col,
        dir = dir,
        bind_idx,
        bind_idx + 1,
    );

    let mut q = sqlx::query(&query_str);
    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if !contract_ids_list.is_empty() {
        q = q.bind(&contract_ids_list);
    }
    if let Some(ref et) = params.event_type {
        q = q.bind(et);
    }
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    if let Some(isc) = params.in_successful_call {
        q = q.bind(isc);
    }
    if let Some(sv) = params.schema_version {
        q = q.bind(sv);
    }
    if let Some(ref ts) = params.topic_sym {
        q = q.bind(ts);
    }
    if let Some(ref tf) = topic_filter {
        q = q.bind(tf.to_string());
    }
    if let Some(ref search) = params.search {
        q = q.bind(search);
    }
    if let Some(ts) = from_ts {
        q = q.bind(ts);
    }
    if let Some(ts) = to_ts {
        q = q.bind(ts);
    }
    if let Some(tid) = tenant_id {
        q = q.bind(tid);
    }
    q = q.bind(limit).bind(offset);

    let _db_span = info_span!("db_query", query_type = "get_events_offset").entered();
    let rows = q.fetch_all(&state.read_pool).await?;
    drop(_db_span);

    let has_more = rows.len() as i64 == limit;
    let next_cursor = if has_more {
        let last = rows.last().unwrap();
        let last_id: uuid::Uuid = last.try_get("id")?;
        let last_val_text = match sort_by {
            crate::models::SortBy::Ledger => {
                let v: i64 = last.try_get("ledger")?;
                v.to_string()
            }
            crate::models::SortBy::Timestamp => {
                let v: chrono::DateTime<chrono::Utc> = last.try_get("timestamp")?;
                v.to_rfc3339()
            }
            crate::models::SortBy::CreatedAt => {
                let v: chrono::DateTime<chrono::Utc> = last.try_get("created_at")?;
                v.to_rfc3339()
            }
        };
        Some(encode_cursor_tagged(
            sort_by.as_tag(),
            &last_val_text,
            last_id,
        ))
    } else {
        None
    };

    let events = rows_to_json(
        &rows,
        &columns,
        state.encryption_key.as_ref(),
        state.encryption_key_old.as_ref(),
        params.compact.unwrap_or(false),
    )?;

    let (total, approximate): (i64, bool) = if exact || !conditions.is_empty() {
        let count_str = format!("SELECT COUNT(*) FROM events {}", where_clause);
        let mut cq = sqlx::query_scalar::<_, i64>(&count_str);
        if let Some(ref cid) = params.contract_id {
            cq = cq.bind(cid);
        }
        if let Some(ref et) = params.event_type {
            cq = cq.bind(et);
        }
        if let Some(fl) = params.from_ledger {
            cq = cq.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            cq = cq.bind(tl);
        }
        if let Some(isc) = params.in_successful_call {
            cq = cq.bind(isc);
        }
        if let Some(ref ts) = params.topic_sym {
            cq = cq.bind(ts);
        }
        if let Some(ref tf) = topic_filter {
            cq = cq.bind(tf.to_string());
        }
        if let Some(ref search) = params.search {
            cq = cq.bind(search);
        }
        if let Some(ts) = from_ts {
            cq = cq.bind(ts);
        }
        if let Some(ts) = to_ts {
            cq = cq.bind(ts);
        }
        if let Some(tid) = tenant_id {
            cq = cq.bind(tid);
        }
        let _count_span = info_span!("db_query", query_type = "count_events").entered();
        let count = cq.fetch_one(&state.read_pool).await?;
        drop(_count_span);
        (count, false)
    } else {
        // In multi-tenant mode we can't use the pg_class estimate (it's for the whole table).
        // Fall back to an exact count scoped to the tenant.
        if tenant_id.is_some() {
            let count =
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events WHERE tenant_id = $1")
                    .bind(tenant_id)
                    .fetch_one(&state.read_pool)
                    .await?;
            (count, false)
        } else {
            let count = sqlx::query_scalar::<_, i64>(
                "SELECT reltuples::bigint FROM pg_class WHERE relname = 'events'",
            )
            .fetch_one(&state.read_pool)
            .await?;
            (count, true)
        }
    };

    // Build ETag from last row's id + created_at + total
    let etag = rows.first().and_then(|row| {
        let id: Option<Uuid> = row.try_get("id").ok();
        let created_at: Option<DateTime<Utc>> = row.try_get("created_at").ok();
        id.zip(created_at)
            .map(|(id, ca)| compute_etag(&id, &ca, Some(total)))
    });

    // Check If-None-Match — return 304 if ETag matches
    if let Some(ref tag) = etag {
        if let Some(inm) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
            if inm == tag {
                let resp = axum::http::Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header("ETag", tag.as_str())
                    .header("Cache-Control", "no-cache")
                    .body(axum::body::Body::empty())
                    .unwrap();
                return Ok(resp.into_response());
            }
        }
    }

    let body = json!({
        "data": events,
        "next_cursor": next_cursor,
        "total": total,
        "page": params.page.unwrap_or(1),
        "limit": limit,
        "approximate": approximate,
        "pagination": "offset — migrate to cursor parameter for better performance",
    });

    let mut body_obj = body.as_object().unwrap().clone();
    
    // Add approximation metadata when using approximate count
    if approximate {
        // Get stats age and dead tuple ratio
        let stats_info: (Option<chrono::DateTime<chrono::Utc>>, Option<f64>) = sqlx::query_as(
            "SELECT last_analyze, CASE WHEN n_live_tup > 0 THEN (n_dead_tup::float / n_live_tup) * 100 ELSE 0 END \
             FROM pg_stat_user_tables WHERE relname = 'events'"
        )
        .fetch_optional(&state.read_pool)
        .await
        .unwrap_or(None)
        .map(|(last_analyze, error_pct)| (last_analyze, error_pct))
        .unwrap_or((None, None));
        
        if let Some(error_pct) = stats_info.1 {
            body_obj.insert("approximate_error_pct".to_string(), json!(error_pct.min(100.0)));
        }
        if let Some(last_analyze) = stats_info.0 {
            body_obj.insert("last_analyzed".to_string(), json!(last_analyze.to_rfc3339()));
        }
    }

    let body = serde_json::Value::Object(body_obj);
    let mut response = Json(body).into_response();
    if let Some(ref tag) = etag {
        response.headers_mut().insert("ETag", tag.parse().unwrap());
        response
            .headers_mut()
            .insert("Cache-Control", "no-cache".parse().unwrap());
    }
    if params.compact.unwrap_or(false) {
        response.headers_mut().insert(
            "X-Event-Data-Encoding",
            axum::http::HeaderValue::from_static("gzip+base64"),
        );
    }
    Ok(response)
}

/// Escape a single CSV field per RFC 4180.
///
/// A field is wrapped in double-quotes if it contains a comma, double-quote,
/// newline (`\n`), or carriage-return (`\r`). Any double-quote character
/// inside the field is escaped by doubling it (`"` → `""`).
fn csv_escape_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        // Escape embedded double-quotes by doubling them, then wrap in quotes.
        let escaped = value.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        value.to_owned()
    }
}

#[utoipa::path(
    get,
    path = "/v1/events/export",
    tag = "events",
    params(
        ("format" = Option<String>, Query, description = "Output format: `csv` (default) or `parquet`. \
            CSV output streams RFC 4180-compliant text with a header row. \
            Parquet output requires the `parquet` feature flag."),
        ("event_type" = Option<String>, Query, description = "Filter by event type: contract, diagnostic, system"),
        ("from_ledger" = Option<i64>, Query, description = "Return events at or after this ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Return events at or before this ledger"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
    ),
    responses(
        (status = 200, description = "Exported events. \
            CSV: `Content-Type: text/csv`, header row `id,contract_id,event_type,tx_hash,ledger,timestamp,event_data,created_at`, \
            streamed with `Content-Disposition: attachment; filename=\"events.csv\"`. \
            Parquet: `Content-Type: application/octet-stream`, \
            `Content-Disposition: attachment; filename=\"events.parquet\"`."),
        (status = 400, description = "Invalid query parameters or unsupported format"),
        (status = 401, description = "API key required"),
    )
)]
pub async fn export_events(
    State(state): State<AppState>,
    Query(params): Query<ExportParams>,
) -> Result<Response<Body>, AppError> {
    if state.config.api_keys.is_empty() {
        return Err(AppError::Validation(
            "export endpoint requires API key authentication".to_string(),
        ));
    }

    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }

    let fmt = params.format.as_deref().unwrap_or("csv");
    let want_parquet = fmt == "parquet";
    let want_csv = fmt == "csv" || fmt.is_empty();

    if !want_parquet && !want_csv {
        return Err(AppError::Validation(format!(
            "unsupported format '{fmt}': use 'csv' or 'parquet'"
        )));
    }

    #[cfg(not(feature = "parquet"))]
    if want_parquet {
        return Err(AppError::Validation(
            "parquet export requires the 'parquet' feature flag".to_string(),
        ));
    }

    let max_rows = state.config.export_max_rows as i64;

    let mut conditions: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;

    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let query_str = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
         FROM events {where_clause} ORDER BY ledger ASC, id ASC LIMIT ${bind_idx}"
    );

    let mut q = sqlx::query(&query_str);
    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if let Some(ref et) = params.event_type {
        q = q.bind(et);
    }
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    q = q.bind(max_rows);

    let rows = q.fetch_all(&state.pool).await?;

    // Get total count of available rows (for Content-Range header)
    let total_count: i64 = {
        let count_str = format!("SELECT COUNT(*) FROM events {}", where_clause);
        let mut cq = sqlx::query_scalar::<_, i64>(&count_str);
        if let Some(ref cid) = params.contract_id {
            cq = cq.bind(cid);
        }
        if let Some(ref et) = params.event_type {
            cq = cq.bind(et);
        }
        if let Some(fl) = params.from_ledger {
            cq = cq.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            cq = cq.bind(tl);
        }
        cq.fetch_one(&state.pool).await?
    };

    let returned_count = rows.len() as i64;
    let content_range = format!("items 0-{}/{}", returned_count.saturating_sub(1), total_count);

    // JSON Lines format
    if want_jsonl {
        let mut jsonl = String::new();
        for row in &rows {
            let id: uuid::Uuid = row.try_get("id")?;
            let contract_id: String = row.try_get("contract_id")?;
            let event_type: String = row.try_get("event_type")?;
            let tx_hash: String = row.try_get("tx_hash")?;
            let ledger: i64 = row.try_get("ledger")?;
            let timestamp: chrono::DateTime<chrono::Utc> = row.try_get("timestamp")?;
            let event_data: serde_json::Value = row.try_get("event_data")?;
            let created_at: chrono::DateTime<chrono::Utc> = row.try_get("created_at")?;
            
            let obj = serde_json::json!({
                "id": id,
                "contract_id": contract_id,
                "event_type": event_type,
                "tx_hash": tx_hash,
                "ledger": ledger,
                "timestamp": timestamp,
                "event_data": event_data,
                "created_at": created_at,
            });
            
            jsonl.push_str(&obj.to_string());
            jsonl.push('\n');
        }
        
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/x-ndjson")
            .header(
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"events.jsonl\"",
            )
            .header("Content-Range", content_range)
            .body(Body::from(jsonl))
            .unwrap());
    }

    #[cfg(feature = "parquet")]
    if want_parquet {
        use crate::parquet_export::{write_events_parquet, EventRow};
        let event_rows: Vec<EventRow> = rows
            .iter()
            .map(|row| {
                Ok::<_, sqlx::Error>(EventRow {
                    id: row.try_get("id")?,
                    contract_id: row.try_get("contract_id")?,
                    event_type: row.try_get("event_type")?,
                    tx_hash: row.try_get("tx_hash")?,
                    ledger: row.try_get("ledger")?,
                    timestamp: row.try_get("timestamp")?,
                    event_data: row.try_get("event_data")?,
                    created_at: row.try_get("created_at")?,
                })
            })
            .collect::<Result<_, _>>()?;

        let bytes =
            write_events_parquet(&event_rows).map_err(|e| AppError::Internal(e.to_string()))?;

        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"events.parquet\"",
            )
            .header("Content-Range", content_range)
            .body(Body::from(bytes))
            .unwrap());
    }

    // Default: CSV (RFC 4180)
    // Build each row as a Bytes chunk and stream them so the full result set
    // is never held in a single allocation.
    use bytes::Bytes;
    use futures::stream;

    const CSV_HEADER: &str =
        "id,contract_id,event_type,tx_hash,ledger,timestamp,event_data,created_at\n";

    // Collect row chunks; the header is the first chunk.
    let mut chunks: Vec<Result<Bytes, std::convert::Infallible>> =
        Vec::with_capacity(rows.len() + 1);
    chunks.push(Ok(Bytes::from_static(CSV_HEADER.as_bytes())));

    for row in &rows {
        let id: uuid::Uuid = row.try_get("id")?;
        let contract_id: String = row.try_get("contract_id")?;
        let event_type: String = row.try_get("event_type")?;
        let tx_hash: String = row.try_get("tx_hash")?;
        let ledger: i64 = row.try_get("ledger")?;
        let timestamp: chrono::DateTime<chrono::Utc> = row.try_get("timestamp")?;
        let event_data: serde_json::Value = row.try_get("event_data")?;
        let created_at: chrono::DateTime<chrono::Utc> = row.try_get("created_at")?;

        // Serialize event_data to a JSON string, then escape it as a CSV field.
        let data_str = event_data.to_string();

        let line = format!(
            "{},{},{},{},{},{},{},{}\n",
            csv_escape_field(&id.to_string()),
            csv_escape_field(&contract_id),
            csv_escape_field(&event_type),
            csv_escape_field(&tx_hash),
            ledger,
            csv_escape_field(&timestamp.to_rfc3339()),
            csv_escape_field(&data_str),
            csv_escape_field(&created_at.to_rfc3339()),
        );
        chunks.push(Ok(Bytes::from(line)));
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/csv")
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"events.csv\"",
        )
        .header("Content-Range", content_range)
        .body(Body::from_stream(stream::iter(chunks)))
        .unwrap())
}

/// Query parameters for the /v1/events/recent endpoint.
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct RecentParams {
    pub limit: Option<i64>,
    pub event_type: Option<crate::models::EventType>,
    pub contract_id: Option<String>,
    /// Cursor for pagination (opaque, URL-safe).
    pub cursor: Option<String>,
    /// Not supported — returns 400 if provided.
    pub from_ledger: Option<i64>,
    /// Not supported — returns 400 if provided.
    pub to_ledger: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/v1/events/recent",
    tag = "events",
    params(
        ("limit" = Option<i64>, Query, description = "Number of most-recent events to return, 1–100 (default: 20)"),
        ("event_type" = Option<crate::models::EventType>, Query, description = "Filter by event type: contract, diagnostic, system"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("cursor" = Option<String>, Query, description = "Cursor for pagination (opaque, URL-safe)"),
    ),
    responses(
        (status = 200, description = "Most recently indexed events in descending ledger order"),
        (status = 400, description = "Invalid query parameters or unsupported ledger range filter"),
    )
)]
pub async fn get_recent_events(
    State(state): State<AppState>,
    Query(params): Query<RecentParams>,
) -> Result<Json<Value>, AppError> {
    if params.from_ledger.is_some() || params.to_ledger.is_some() {
        return Err(AppError::Validation(
            "from_ledger and to_ledger are not supported on /v1/events/recent".to_string(),
        ));
    }
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }

    let limit = params.limit.unwrap_or(20).clamp(1, 100);

    let mut conditions: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;

    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${bind_idx}"));
        bind_idx += 1;
    }

    // Handle cursor-based pagination
    if let Some(ref cursor_str) = params.cursor {
        let (cursor_ledger, cursor_id) = decode_cursor(cursor_str)?;
        conditions.push(format!(
            "(ledger, id) < (${}, ${})",
            bind_idx, bind_idx + 1
        ));
        bind_idx += 2;
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let query_str = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, schema_version, 0::bigint AS total_count \
         FROM events {} ORDER BY ledger DESC, id DESC LIMIT ${}",
        where_clause, bind_idx,
    );

    let mut q = sqlx::query_as::<_, crate::models::Event>(&query_str);
    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if let Some(ref et) = params.event_type {
        q = q.bind(et);
    }
    if let Some(ref cursor_str) = params.cursor {
        let (cursor_ledger, cursor_id) = decode_cursor(cursor_str)?;
        q = q.bind(cursor_ledger);
        q = q.bind(cursor_id);
    }
    q = q.bind(limit + 1); // Fetch one extra to determine if there's a next page

    let rows = q.fetch_all(&state.pool).await?;
    let has_next = rows.len() > limit as usize;
    let rows = if has_next {
        &rows[..limit as usize]
    } else {
        &rows
    };

    let events: Vec<Value> = rows
        .iter()
        .map(|e| {
            filter_fields(
                e,
                crate::models::PaginationParams::ALLOWED_FIELDS,
                state.encryption_key.as_ref(),
                state.encryption_key_old.as_ref(),
            )
        })
        .collect();

    let mut response = json!({
        "data": events,
        "limit": limit,
    });

    if has_next && !rows.is_empty() {
        let last_event = &rows[rows.len() - 1];
        let next_cursor = encode_cursor(last_event.ledger, last_event.id);
        response["next_cursor"] = json!(next_cursor);
    }

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/events/contract/{contract_id}",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID (56-char, starts with C)"),
        ("page" = Option<i64>, Query, description = "Page number (default: 1)"),
        ("limit" = Option<i64>, Query, description = "Results per page, 1–100 (default: 20)"),
        ("exact_count" = Option<bool>, Query, description = "Use exact COUNT(*) instead of approximate (default: false)"),
        ("from_ledger" = Option<i64>, Query, description = "Return events at or after this ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Return events at or before this ledger"),
        ("sort" = Option<String>, Query, description = "Sort order: asc (oldest first) or desc (newest first, default)"),
    ),
    responses(
        (status = 200, description = "Events for the given contract"),
        (status = 400, description = "Invalid contract_id format or ledger range", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "No events found for contract", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[instrument(skip(state, extensions), fields(contract_id = %contract_id))]
pub async fn get_events_by_contract(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(params): Query<PaginationParams>,
    extensions: axum::http::Extensions,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;

    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }

    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let tenant_id = tenant_id.as_deref();

    let limit = params.limit();
    let offset = params.offset();
    let exact = params.exact_count.unwrap_or(false);
    let columns = resolve_columns(&params)?;
    let dir = params
        .sort
        .unwrap_or(crate::models::SortOrder::Desc)
        .as_sql();

    let mut conditions: Vec<String> = vec!["contract_id = $1".to_string()];
    let mut bind_idx: i32 = 2;

    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }
    maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);

    let where_clause = format!("WHERE {}", conditions.join(" AND "));
    let query_str = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, 0::bigint AS total_count \
         FROM events {} ORDER BY ledger {dir} LIMIT ${} OFFSET ${}",
        where_clause, bind_idx, bind_idx + 1,
    );

    let mut q = sqlx::query_as::<_, models::Event>(&query_str).bind(&contract_id);
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    if let Some(tid) = tenant_id {
        q = q.bind(tid);
    }
    q = q.bind(limit).bind(offset);

    let rows = q.fetch_all(&state.read_pool).await?;

    if rows.is_empty() {
        return Err(AppError::NotFound);
    }

    let events: Vec<Value> = rows
        .iter()
        .map(|e| {
            filter_fields(
                e,
                &columns,
                state.encryption_key.as_ref(),
                state.encryption_key_old.as_ref(),
            )
        })
        .collect();

    // Fetch total count. Skip cache in multi-tenant mode to avoid cross-tenant leakage.
    let total: i64 = if params.from_ledger.is_none() && params.to_ledger.is_none() && tenant_id.is_none() {
        if let Some(cached) = state.contract_count_cache.get(&contract_id).await {
            cached
        } else {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE contract_id = $1")
                    .bind(&contract_id)
                    .fetch_one(&state.pool)
                    .await?;
            state
                .contract_count_cache
                .insert(contract_id.clone(), count)
                .await;
            count
        }
    } else {
        let mut count_conditions: Vec<String> = vec!["contract_id = $1".to_string()];
        let mut cidx: i32 = 2;
        if params.from_ledger.is_some() {
            count_conditions.push(format!("ledger >= ${cidx}"));
            cidx += 1;
        }
        if params.to_ledger.is_some() {
            count_conditions.push(format!("ledger <= ${cidx}"));
            cidx += 1;
        }
        maybe_add_tenant_condition(&mut count_conditions, &mut cidx, tenant_id);
        let count_str = format!(
            "SELECT COUNT(*) FROM events WHERE {}",
            count_conditions.join(" AND ")
        );
        let mut cq = sqlx::query_scalar::<_, i64>(&count_str).bind(&contract_id);
        if let Some(fl) = params.from_ledger {
            cq = cq.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            cq = cq.bind(tl);
        }
        if let Some(tid) = tenant_id {
            cq = cq.bind(tid);
        }
        cq.fetch_one(&state.pool).await?
    };

    let mut response = json!({
        "data": events,
        "contract_id": contract_id,
        "total": total,
        "page": params.page.unwrap_or(1),
        "limit": limit,
        "approximate": false,
    });

    if let Some(fl) = params.from_ledger {
        response["from_ledger"] = json!(fl);
    }
    if let Some(tl) = params.to_ledger {
        response["to_ledger"] = json!(tl);
    }

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/events/tx/{tx_hash}",
    tag = "events",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash (64 hex chars, case-insensitive — normalized to lowercase)"),
    ),
    responses(
        (status = 200, description = "Events for the given transaction (empty array if none)"),
        (status = 400, description = "Invalid tx_hash format", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[instrument(skip(state, extensions), fields(tx_hash = %tx_hash))]
pub async fn get_events_by_tx(
    State(state): State<AppState>,
    Path(tx_hash): Path<String>,
    Query(params): Query<PaginationParams>,
    extensions: axum::http::Extensions,
) -> Result<Json<Value>, AppError> {
    let tx_hash = tx_hash.to_lowercase();
    validate_tx_hash(&tx_hash)?;

    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let tenant_id = tenant_id.as_deref();

    let columns = resolve_columns(&params)?;

    let mut select_cols = columns.to_vec();
    if !select_cols.contains(&"ledger") {
        select_cols.push("ledger");
    }
    if !select_cols.contains(&"id") {
        select_cols.push("id");
    }

    let mut conditions: Vec<String> = vec!["tx_hash = $1".to_string()];
    let mut bind_idx: i32 = 2;
    maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);

    let query_str = format!(
        "SELECT {} FROM events WHERE {} ORDER BY ledger DESC, id DESC",
        select_cols.join(", "),
        conditions.join(" AND "),
    );

    let mut q = sqlx::query(&query_str).bind(&tx_hash);
    if let Some(tid) = tenant_id {
        q = q.bind(tid);
    }
    let rows = q.fetch_all(&state.read_pool).await?;

    let total = rows.len() as i64;
    let events = rows_to_json(
        &rows,
        &columns,
        state.encryption_key.as_ref(),
        state.encryption_key_old.as_ref(),
        false,
    )?;

    Ok(Json(json!({
        "data": events,
        "tx_hash": tx_hash,
        "total": total,
        "approximate": false,
    })))
}

#[utoipa::path(
    post,
    path = "/v1/events/tx/batch",
    tag = "events",
    request_body = crate::models::BatchTxRequest,
    responses(
        (status = 200, description = "Map of tx_hash -> events for all requested hashes"),
        (status = 400, description = "Invalid hashes or too many hashes", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[instrument(skip(state, body))]
pub async fn bulk_insert_events(
    State(state): State<AppState>,
    Json(body): Json<crate::models::BulkInsertRequest>,
) -> Result<Json<crate::models::BulkInsertResponse>, AppError> {
    const MAX_BATCH_SIZE: usize = 1000;
    
    if body.events.is_empty() {
        return Err(AppError::Validation("events list cannot be empty".to_string()));
    }
    
    if body.events.len() > MAX_BATCH_SIZE {
        return Err(AppError::Validation(format!(
            "events list exceeds maximum of {} events",
            MAX_BATCH_SIZE
        )));
    }
    
    let mut inserted = 0i64;
    let mut skipped = 0i64;
    let mut failed = 0i64;
    let mut errors = Vec::new();
    
    for event in body.events {
        // Validate contract_id
        if let Err(e) = validate_contract_id(&event.contract_id) {
            failed += 1;
            errors.push(format!("Invalid contract_id: {}", e));
            continue;
        }
        
        // Validate tx_hash
        if let Err(e) = validate_tx_hash(&event.tx_hash) {
            failed += 1;
            errors.push(format!("Invalid tx_hash: {}", e));
            continue;
        }
        
        // Validate event_type
        let event_type = match event.event_type.as_str() {
            "contract" | "diagnostic" | "system" => event.event_type.clone(),
            _ => {
                failed += 1;
                errors.push(format!("Invalid event_type: {}", event.event_type));
                continue;
            }
        };
        
        let id = Uuid::new_v4();
        let result = sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, event_data_normalized, ledger_hash, in_successful_call, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
             ON CONFLICT (contract_id, tx_hash, ledger, event_type) DO NOTHING"
        )
        .bind(id)
        .bind(&event.contract_id)
        .bind(&event_type)
        .bind(&event.tx_hash)
        .bind(event.ledger)
        .bind(event.timestamp)
        .bind(&event.event_data)
        .bind(&event.event_data_normalized)
        .bind(&event.ledger_hash)
        .bind(event.in_successful_call.unwrap_or(false))
        .execute(&state.write_pool)
        .await;
        
        match result {
            Ok(result) => {
                if result.rows_affected() > 0 {
                    inserted += 1;
                } else {
                    skipped += 1;
                }
            }
            Err(e) => {
                failed += 1;
                errors.push(format!("Database error: {}", e));
            }
        }
    }
    
    Ok(Json(crate::models::BulkInsertResponse {
        inserted,
        skipped,
        failed,
        errors,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/admin/events/bulk",
    tag = "Admin",
    request_body = crate::models::BulkInsertRequest,
    responses(
        (status = 200, description = "Bulk insert result", body = crate::models::BulkInsertResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
pub async fn get_events_by_tx_batch(
    State(state): State<AppState>,
    Json(body): Json<crate::models::BatchTxRequest>,
) -> Result<Json<Value>, AppError> {
    if body.hashes.len() > state.config.batch_tx_max_size {
        return Err(AppError::Validation(format!(
            "too many hashes: maximum is {}",
            state.config.batch_tx_max_size
        )));
    }

    // Validate all hashes; collect invalid ones for a helpful error.
    let invalid: Vec<String> = body
        .hashes
        .iter()
        .filter(|h| validate_tx_hash(h).is_err())
        .cloned()
        .collect();
    if !invalid.is_empty() {
        return Err(AppError::Validation(format!(
            "invalid tx_hash(es): {}",
            invalid.join(", ")
        )));
    }

    // Deduplicate hashes using HashSet
    let mut unique_hashes: std::collections::HashSet<String> = std::collections::HashSet::new();
    for h in &body.hashes {
        unique_hashes.insert(h.to_lowercase());
    }
    let deduplicated_count = unique_hashes.len();
    let hashes: Vec<String> = unique_hashes.into_iter().collect();

    // Single query using ANY().
    let rows = sqlx::query(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
         FROM events WHERE tx_hash = ANY($1) ORDER BY tx_hash, ledger DESC, id DESC",
    )
    .bind(&hashes)
    .fetch_all(&state.read_pool)
    .await?;

    // Build result map: all requested hashes present, even if empty.
    let mut result: serde_json::Map<String, Value> = hashes
        .iter()
        .map(|h| (h.clone(), Value::Array(vec![])))
        .collect();

    let all_cols: &[&str] = &[
        "id",
        "contract_id",
        "event_type",
        "tx_hash",
        "ledger",
        "timestamp",
        "event_data",
        "created_at",
    ];
    for row in &rows {
        let tx_hash: String = row.try_get("tx_hash")?;
        let event_json = rows_to_json(
            std::slice::from_ref(row),
            all_cols,
            state.encryption_key.as_ref(),
            state.encryption_key_old.as_ref(),
            false,
        )?;
        if let Some(arr) = result.get_mut(&tx_hash).and_then(|v| v.as_array_mut()) {
            if let Some(ev) = event_json.into_iter().next() {
                arr.push(ev);
            }
        }
    }

    // Add deduplicated_count to response
    let mut response_obj = result;
    response_obj.insert("deduplicated_count".to_string(), Value::Number(deduplicated_count.into()));

    Ok(Json(Value::Object(response_obj)))
}

#[utoipa::path(
    get,
    path = "/v1/events/ledger-hash/{hash}",
    tag = "events",
    params(
        ("hash" = String, Path, description = "Ledger hash"),
    ),
    responses(
        (status = 200, description = "Events for the given ledger hash"),
        (status = 400, description = "Invalid parameters", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_events_by_ledger_hash(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Value>, AppError> {
    let columns = resolve_columns(&params)?;
    let mut select_cols = columns.to_vec();
    if !select_cols.contains(&"ledger") {
        select_cols.push("ledger");
    }
    if !select_cols.contains(&"id") {
        select_cols.push("id");
    }

    let query_str = format!(
        "SELECT {} FROM events WHERE ledger_hash = $1 ORDER BY ledger DESC, id DESC",
        select_cols.join(", "),
    );
    let rows = sqlx::query(&query_str)
        .bind(&hash)
        .fetch_all(&state.read_pool)
        .await?;

    let events = rows_to_json(
        &rows,
        &columns,
        state.encryption_key.as_ref(),
        state.encryption_key_old.as_ref(),
        false,
    )?;
    Ok(Json(json!({
        "data": events,
        "ledger_hash": hash,
        "total": rows.len() as i64,
        "approximate": false,
    })))
}

#[utoipa::path(
    post,
    path = "/v1/admin/contracts/{contract_id}/abi",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID"),
    ),
    request_body(content = Value, description = "ABI JSON array", content_type = "application/json"),
    responses(
        (status = 200, description = "ABI registered"),
        (status = 400, description = "Invalid contract_id or ABI", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn register_contract_abi(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Json(abi): Json<Value>,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;
    if !abi.is_array() {
        return Err(AppError::Validation("ABI must be a JSON array".to_string()));
    }
    sqlx::query(
        "INSERT INTO contract_abis (contract_id, abi, updated_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (contract_id) DO UPDATE SET abi = EXCLUDED.abi, updated_at = NOW()",
    )
    .bind(&contract_id)
    .bind(&abi)
    .execute(&state.pool)
    .await?;

    Ok(Json(
        json!({ "contract_id": contract_id, "status": "registered" }),
    ))
}

/// Anonymize a specific event for GDPR compliance.
/// Replaces event_data with {"anonymized": true} and hashes tx_hash with SHA-256.
/// Idempotent: already-anonymized events return 200 without re-processing.
#[utoipa::path(
    post,
    path = "/v1/admin/events/{id}/anonymize",
    tag = "admin",
    params(
        ("id" = String, Path, description = "Event UUID"),
    ),
    responses(
        (status = 200, description = "Event anonymized"),
        (status = 404, description = "Event not found", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn anonymize_event(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    use sha2::{Digest, Sha256};

    // Fetch current tx_hash and anonymized flag
    let row = sqlx::query("SELECT tx_hash, anonymized FROM events WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(AppError::NotFound)?;

    let already_anonymized: bool = row.try_get("anonymized")?;
    if already_anonymized {
        tracing::info!(event_id = %id, "Event already anonymized, skipping");
        return Ok(Json(json!({ "id": id, "anonymized": true })));
    }

    let tx_hash: String = row.try_get("tx_hash")?;
    let hashed_tx = {
        let mut h = Sha256::new();
        h.update(tx_hash.as_bytes());
        format!("{:x}", h.finalize())
    };

    sqlx::query("UPDATE events SET anonymized = TRUE, event_data = $1, tx_hash = $2 WHERE id = $3")
        .bind(json!({"anonymized": true}))
        .bind(&hashed_tx)
        .bind(id)
        .execute(&state.pool)
        .await?;

    tracing::info!(event_id = %id, anonymized_at = %chrono::Utc::now(), "Event anonymized");

    Ok(Json(json!({ "id": id, "anonymized": true })))
}

/// Delete all events for a contract (GDPR right-to-erasure).
/// Optionally anonymize instead of deleting with `anonymize_only=true`.
/// Logs deletion request for audit purposes.
#[utoipa::path(
    delete,
    path = "/v1/admin/events/contract/{contract_id}",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID"),
        ("anonymize_only" = Option<bool>, Query, description = "If true, anonymize instead of deleting (default: false)"),
    ),
    responses(
        (status = 200, description = "Events deleted or anonymized"),
        (status = 400, description = "Invalid contract_id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn delete_contract_events(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;

    let anonymize_only = params
        .get("anonymize_only")
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false);

    let client_ip = headers
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("unknown");

    if anonymize_only {
        // Anonymize: set anonymized=true and hash tx_hash
        use sha2::{Digest, Sha256};

        let rows = sqlx::query("SELECT id, tx_hash FROM events WHERE contract_id = $1 AND anonymized = FALSE")
            .bind(&contract_id)
            .fetch_all(&state.pool)
            .await?;

        let count = rows.len() as u64;

        for row in rows {
            let id: Uuid = row.try_get("id")?;
            let tx_hash: String = row.try_get("tx_hash")?;
            let hashed_tx = {
                let mut h = Sha256::new();
                h.update(tx_hash.as_bytes());
                format!("{:x}", h.finalize())
            };

            sqlx::query("UPDATE events SET anonymized = TRUE, event_data = $1, tx_hash = $2 WHERE id = $3")
                .bind(json!({"anonymized": true}))
                .bind(&hashed_tx)
                .bind(id)
                .execute(&state.pool)
                .await?;
        }

        tracing::info!(
            contract_id = %contract_id,
            count = count,
            client_ip = client_ip,
            timestamp = %chrono::Utc::now(),
            "Events anonymized for contract (GDPR)"
        );

        crate::metrics::record_events_deleted(count);

        Ok(Json(json!({
            "contract_id": contract_id,
            "action": "anonymized",
            "count": count,
        })))
    } else {
        // Hard delete
        let result = sqlx::query("DELETE FROM events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&state.pool)
            .await?;

        let count = result.rows_affected();

        tracing::info!(
            contract_id = %contract_id,
            count = count,
            client_ip = client_ip,
            timestamp = %chrono::Utc::now(),
            "Events deleted for contract (GDPR right-to-erasure)"
        );

        crate::metrics::record_events_deleted(count);

        Ok(Json(json!({
            "contract_id": contract_id,
            "action": "deleted",
            "count": count,
        })))
    }
}

/// Pause the indexer loop without stopping the HTTP server.
#[utoipa::path(
    post,
    path = "/v1/admin/indexer/pause",
    tag = "admin",
    responses(
        (status = 200, description = "Indexer paused"),
        (status = 403, description = "Not the active indexer", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn pause_indexer(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if !state
        .indexer_state
        .is_active_indexer
        .load(Ordering::Relaxed)
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "pause endpoint only available on active indexer" })),
        ));
    }
    state.indexer_state.is_paused.store(true, Ordering::Relaxed);
    tracing::info!("Indexer paused via admin API");
    Ok(Json(json!({ "indexer_paused": true })))
}

/// Resume a previously paused indexer loop.
#[utoipa::path(
    post,
    path = "/v1/admin/indexer/resume",
    tag = "admin",
    responses(
        (status = 200, description = "Indexer resumed"),
        (status = 403, description = "Not the active indexer", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn resume_indexer(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if !state
        .indexer_state
        .is_active_indexer
        .load(Ordering::Relaxed)
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "resume endpoint only available on active indexer" })),
        ));
    }
    state
        .indexer_state
        .is_paused
        .store(false, Ordering::Relaxed);
    tracing::info!("Indexer resumed via admin API");
    Ok(Json(json!({ "indexer_paused": false })))
}

/// Start a background re-encryption job to migrate events from old key to new key.
#[utoipa::path(
    post,
    path = "/v1/admin/reencrypt",
    tag = "admin",
    responses(
        (status = 202, description = "Re-encryption job started"),
        (status = 400, description = "Encryption not enabled or no old key configured", body = ErrorResponse),
        (status = 409, description = "Re-encryption job already running", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn start_reencrypt(
    State(state): State<AppState>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    #[cfg(not(feature = "encryption"))]
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "encryption feature not enabled" })),
        ));
    }

    #[cfg(feature = "encryption")]
    {
        let new_key = state.encryption_key.ok_or((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "ENCRYPTION_KEY not configured" })),
        ))?;

        let old_key = state.encryption_key_old.ok_or((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "ENCRYPTION_KEY_OLD not configured" })),
        ))?;

        // Create or get the reencrypt state from app state
        // For now, we'll create a new one per request (in production, store in AppState)
        let reencrypt_state = crate::reencrypt::ReencryptState::new();

        if reencrypt_state.is_running() {
            return Err((
                StatusCode::CONFLICT,
                Json(json!({ "error": "re-encryption job already running" })),
            ));
        }

        let pool = state.pool.clone();
        let batch_size = 1000;

        crate::reencrypt::start_reencrypt_job(pool, new_key, old_key, batch_size, reencrypt_state);

        Ok((
            StatusCode::ACCEPTED,
            Json(json!({
                "message": "re-encryption job started",
                "batch_size": batch_size
            })),
        ))
    }
}

#[utoipa::path(
    get,
    path = "/v1/events/diff",
    tag = "events",
    params(
        ("from_ledger" = i64, Query, description = "Start ledger (inclusive, required)"),
        ("to_ledger" = i64, Query, description = "End ledger (inclusive, required)"),
    ),
    responses(
        (status = 200, description = "Event diff grouped by contract", body = crate::models::DiffResponse),
        (status = 400, description = "Invalid or missing ledger range", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_events_diff(
    State(state): State<AppState>,
    Query(params): Query<crate::models::DiffParams>,
) -> Result<Json<Value>, AppError> {
    // Validate from_ledger and to_ledger are positive
    if params.from_ledger < 0 {
        return Err(AppError::Validation(
            "from_ledger must be a positive integer".to_string(),
        ));
    }
    if params.to_ledger < 0 {
        return Err(AppError::Validation(
            "to_ledger must be a positive integer".to_string(),
        ));
    }

    // Validate from_ledger < to_ledger
    if params.from_ledger >= params.to_ledger {
        return Err(AppError::Validation(
            "from_ledger must be less than to_ledger".to_string(),
        ));
    }

    // Validate ledger range does not exceed maximum
    let ledger_range = params.to_ledger - params.from_ledger;
    if ledger_range > state.config.max_ledger_range as i64 {
        return Err(AppError::Validation(format!(
            "ledger range exceeds maximum of {}",
            state.config.max_ledger_range
        )));
    }

    // Single query: count per (contract_id, event_type) in range
    let rows = sqlx::query(
        "SELECT contract_id, event_type, COUNT(*) AS cnt \
         FROM events \
         WHERE ledger >= $1 AND ledger <= $2 \
         GROUP BY contract_id, event_type",
    )
    .bind(params.from_ledger)
    .bind(params.to_ledger)
    .fetch_all(&state.read_pool)
    .await?;

    // Aggregate into per-contract map
    let mut map: std::collections::HashMap<String, crate::models::ContractDiff> =
        std::collections::HashMap::new();
    for row in &rows {
        let contract_id: String = row.try_get("contract_id")?;
        let event_type: String = row.try_get("event_type")?;
        let cnt: i64 = row.try_get("cnt")?;
        let entry = map
            .entry(contract_id.clone())
            .or_insert_with(|| crate::models::ContractDiff {
                contract_id,
                event_counts: std::collections::HashMap::new(),
                total: 0,
            });
        entry.event_counts.insert(event_type, cnt);
        entry.total += cnt;
    }

    let mut contracts: Vec<crate::models::ContractDiff> = map.into_values().collect();
    contracts.sort_by(|a, b| b.total.cmp(&a.total));

    Ok(Json(
        serde_json::to_value(crate::models::DiffResponse {
            from_ledger: params.from_ledger,
            to_ledger: params.to_ledger,
            contracts,
        })
        .unwrap(),
    ))
}

#[utoipa::path(
    get,
    path = "/v1/contracts",
    tag = "events",
    params(
        ("page" = Option<i64>, Query, description = "Page number (default 1)"),
        ("limit" = Option<i64>, Query, description = "Items per page (1-100, default 20)"),
        ("sort" = Option<String>, Query, description = "Sort order: event_count_desc, event_count_asc, last_seen_desc (default), first_seen_asc"),
    ),
    responses(
        (status = 200, description = "Paginated list of indexed contract IDs with event counts and ledger info"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
pub async fn get_contracts(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Value>, AppError> {
    let limit = params.limit();
    let offset = params.offset();

    // Determine sort order
    let sort_clause = match params.sort {
        Some(SortOrder::Asc) => "ORDER BY event_count ASC",
        _ => "ORDER BY last_seen_ledger DESC",
    };

    let rows = sqlx::query_as::<_, ContractSummary>(
        &format!(
            "SELECT contract_id, COUNT(*) AS event_count, MIN(ledger) AS first_seen_ledger, \
             MAX(ledger) AS last_seen_ledger, MAX(timestamp) AS last_event_at \
             FROM events GROUP BY contract_id {} \
             LIMIT $1 OFFSET $2",
            sort_clause
        ),
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.read_pool)
    .await?;

    let total: i64 = sqlx::query_scalar("SELECT COUNT(DISTINCT contract_id) FROM events")
        .fetch_one(&state.read_pool)
        .await?;

    let result = json!({
        "data": rows,
        "total": total,
        "page": params.page.unwrap_or(1),
        "limit": limit,
    });

    Ok(Json(result))
}

/// Query the min and max indexed ledger from the events table.
/// Returns `(None, None)` when no events have been indexed yet.
async fn get_indexed_ledger_range(
    pool: &sqlx::PgPool,
) -> Result<(Option<i64>, Option<i64>), AppError> {
    let row = sqlx::query("SELECT MIN(ledger) AS min_ledger, MAX(ledger) AS max_ledger FROM events")
        .fetch_one(pool)
        .await?;
    let min: Option<i64> = row.try_get("min_ledger")?;
    let max: Option<i64> = row.try_get("max_ledger")?;
    Ok((min, max))
}

/// Replay events for a specific ledger range.
///
/// The requested range is validated against the indexed window:
/// - **400** if the range is entirely outside the indexed window (no overlap).
/// - **202** with a `warning` field if the range is only partially indexed.
#[utoipa::path(
    post,
    path = "/v1/admin/replay",
    tag = "admin",
    request_body(content = ReplayRequest, description = "Ledger range to replay", content_type = "application/json"),
    responses(
        (status = 202, description = "Replay job accepted and queued. A `warning` field is included when the requested range is only partially covered by the indexed window."),
        (status = 400, description = "Invalid request parameters, or the requested range is entirely outside the indexed window"),
        (status = 401, description = "Unauthorized - API key required"),
        (status = 403, description = "Forbidden - not the active indexer"),
    )
)]
pub async fn replay_events(
    State(state): State<AppState>,
    Json(request): Json<models::ReplayRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    // Validate ledger range
    if request.from_ledger > request.to_ledger {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "from_ledger must be <= to_ledger"
            })),
        ));
    }

    // Validate range size (max 10,000 ledgers)
    let range_size = request.to_ledger.saturating_sub(request.from_ledger) + 1;
    if range_size > 10_000 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "ledger range cannot exceed 10,000 ledgers"
            })),
        ));
    }

    // Check if this replica is the active indexer (holds the advisory lock)
    let is_active = state
        .indexer_state
        .is_active_indexer
        .load(std::sync::atomic::Ordering::Relaxed);
    if !is_active {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "replay endpoint only available on active indexer"
            })),
        ));
    }

    // Validate the requested range against the indexed window.
    let from = request.from_ledger as i64;
    let to = request.to_ledger as i64;

    let warning: Option<String> =
        match get_indexed_ledger_range(&state.pool).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })? {
            (None, None) => {
                // No events indexed at all — any range is entirely outside.
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": "requested range is entirely outside the indexed window: no events have been indexed yet"
                    })),
                ));
            }
            (Some(min_indexed), Some(max_indexed)) => {
                // Entirely before the indexed window.
                if to < min_indexed {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": format!(
                                "requested range [{from}, {to}] is entirely before the indexed window [{min_indexed}, {max_indexed}]"
                            )
                        })),
                    ));
                }
                // Entirely after the indexed window (future ledgers).
                if from > max_indexed {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": format!(
                                "requested range [{from}, {to}] is entirely after the indexed window [{min_indexed}, {max_indexed}]"
                            )
                        })),
                    ));
                }
                // Partial overlap — warn the caller.
                if from < min_indexed || to > max_indexed {
                    Some(format!(
                        "requested range [{from}, {to}] is partially outside the indexed window [{min_indexed}, {max_indexed}]; only ledgers [{}, {}] will be replayed",
                        from.max(min_indexed),
                        to.min(max_indexed),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        };

    // Record the replay job metric
    crate::metrics::record_replay_job();

    // Spawn background task to handle the replay
    let pool = state.pool.clone();
    let rpc_url = state.config.stellar_rpc_url.clone();
    let from_ledger = request.from_ledger;
    let to_ledger = request.to_ledger;

    tokio::spawn(async move {
        if let Err(e) = execute_replay_job(pool, &rpc_url, from_ledger, to_ledger).await {
            tracing::error!(error = %e, "Replay job failed");
        }
    });

    let mut body = json!({
        "message": "replay job accepted",
        "from_ledger": request.from_ledger,
        "to_ledger": request.to_ledger
    });
    if let Some(w) = warning {
        body["warning"] = json!(w);
    }

    Ok((StatusCode::ACCEPTED, Json(body)))
}

/// Execute the replay job using the same fetch_and_store_events logic
async fn execute_replay_job(
    pool: sqlx::PgPool,
    rpc_url: &str,
    from_ledger: u64,
    to_ledger: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!(
        from_ledger = from_ledger,
        to_ledger = to_ledger,
        "Starting replay job"
    );

    // Create RPC client
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(60))
        .build()?;

    let mut current_ledger = from_ledger;

    while current_ledger <= to_ledger {
        // Fetch events for current ledger range
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getEvents",
            "params": {
                "filters": [],
                "pagination": {"limit": 100},
                "startLedger": current_ledger
            }
        });

        let response = client.post(rpc_url).json(&body).send().await?;

        if !response.status().is_success() {
            return Err(format!("RPC request failed: {}", response.status()).into());
        }

        let rpc_response: crate::models::RpcResponse<crate::models::GetEventsResult> =
            response.json().await?;

        let result = match rpc_response.result {
            Some(r) => r,
            None => {
                if let Some(err) = rpc_response.error {
                    return Err(format!("RPC error: {}", err.message).into());
                } else {
                    return Err("RPC returned no result".into());
                }
            }
        };

        // Store events with ON CONFLICT DO NOTHING for idempotency
        for event in result.events {
            if let Err(e) = store_event_with_idempotency(&pool, &event).await {
                tracing::warn!(error = %e, "Failed to store event during replay");
            }
        }

        // Move to next ledger or break if we've reached the end
        if result.latest_ledger >= current_ledger {
            current_ledger = result.latest_ledger + 1;
        } else {
            break;
        }

        // Add small delay to avoid overwhelming the RPC
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tracing::info!(
        from_ledger = from_ledger,
        to_ledger = to_ledger,
        "Replay job completed"
    );
    Ok(())
}

/// Store event with idempotency using ON CONFLICT DO NOTHING
async fn store_event_with_idempotency(
    pool: &sqlx::PgPool,
    event: &crate::models::SorobanEvent,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let ledger = i64::try_from(event.ledger)?;
    let timestamp = DateTime::parse_from_rfc3339(&event.ledger_closed_at)
        .map(|dt| dt.with_timezone(&chrono::Utc))?;

    let event_data = serde_json::json!({
        "value": event.value,
        "topic": event.topic
    });

    let result = sqlx::query(
        r#"
        INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (tx_hash, contract_id, event_type) DO NOTHING
        "#,
    )
    .bind(&event.contract_id)
    .bind(&event.event_type)
    .bind(&event.tx_hash)
    .bind(ledger)
    .bind(timestamp)
    .bind(event_data)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HealthState, IndexerState};
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use chrono::Utc;
    use sqlx::PgPool;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn create_test_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        crate::routes::create_router(
            pool,
            Vec::new(),
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    /// Build a router with multi-tenant mode enabled.
    /// `tenant_map` maps raw API key → tenant_id.
    fn create_multitenant_test_router(
        pool: PgPool,
        tenant_map: std::collections::HashMap<String, String>,
    ) -> axum::Router {
        use crate::middleware::hash_api_key;
        use tokio::sync::broadcast;

        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let mut config = crate::config::Config::default();
        config.multi_tenant = true;

        // api_keys = all raw keys in the map
        let api_keys: Vec<String> = tenant_map.keys().cloned().collect();
        // Convert raw key → hash for the tenant_map
        let hashed_map: std::collections::HashMap<String, String> = tenant_map
            .iter()
            .map(|(k, v)| (hash_api_key(k), v.clone()))
            .collect();

        crate::routes::create_router_with_tx_and_tenant_map(
            pool.clone(),
            pool,
            api_keys,
            &[],
            60,
            false,
            health_state,
            indexer_state,
            prometheus_handle,
            broadcast::channel(256).0,
            15000,
            1000,
            2000,
            None,
            None,
            config,
            None,
            Arc::new(hashed_map),
        )
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_tx_no_events_returns_200_empty_data(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/tx/unknown_tx_hash_no_events_deadbeef")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"], json!([]));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_tx_with_row_returns_200_with_data(pool: PgPool) {
        let tx_hash = "a1b2c3d4e5f6";
        sqlx::query(
            r#"
            INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind("C_TEST")
        .bind("contract")
        .bind(tx_hash)
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({ "value": null, "topic": null }))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{tx_hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["data"].is_array());
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["tx_hash"], json!(tx_hash));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn database_error_response_does_not_leak_internals(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        // Verify response contains generic error message
        assert!(body_str.contains("internal server error"));

        // Verify no SQLx internals are leaked
        assert!(!body_str.to_lowercase().contains("sqlx"));
        assert!(!body_str.contains("events"));
        assert!(!body_str.contains("table"));
        assert!(!body_str.contains("column"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn contract_id_too_long_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let long_id = "C".repeat(100);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/{}", long_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid contract_id format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn contract_id_invalid_format_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/GABC123456789012345678901234567890123456789012345678")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid contract_id format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_invalid_length_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/tx/abc123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid tx_hash format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_non_hex_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let invalid_hex = "z".repeat(64);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", invalid_hex))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid tx_hash format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_uppercase_hex_is_normalized_to_lowercase(pool: PgPool) {
        // Insert an event with a lowercase tx_hash
        let lowercase_hash = "a".repeat(64);
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C_TEST_UPPER")
        .bind("contract")
        .bind(&lowercase_hash)
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({ "value": null, "topic": null }))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);

        // Request with uppercase hash — should be normalized and return the same event
        let uppercase_hash = "A".repeat(64);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", uppercase_hash))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["data"].is_array());
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        // Response tx_hash should be the normalized lowercase form
        assert_eq!(v["tx_hash"], json!(lowercase_hash));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_mixed_case_is_normalized_to_lowercase(pool: PgPool) {
        let lowercase_hash = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C_TEST_MIXED")
        .bind("contract")
        .bind(lowercase_hash)
        .bind(2_i64)
        .bind(Utc::now())
        .bind(json!({ "value": null, "topic": null }))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);

        // Mixed-case version of the same hash
        let mixed_hash = "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2";
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", mixed_hash))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["tx_hash"], json!(lowercase_hash));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_paginated_returns_approximate_count_by_default(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], true);
        assert!(v.get("total").is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_paginated_returns_exact_count_when_requested(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?exact_count=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], false);
        assert_eq!(v["total"], 0); // Empty table
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_with_fields_filter_returns_only_requested_fields(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert a test row
        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        )
        .bind(Uuid::new_v4())
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("test")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"foo": "bar"}))
        .execute(&pool)
        .await
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?fields=id,ledger")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        let event = &v["data"][0];
        assert!(event.get("id").is_some());
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_none());
        assert!(event.get("event_data").is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_total_count_scenarios(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // 1. Empty set
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"], 0);
        assert_eq!(v["data"].as_array().unwrap().len(), 0);

        // 2. Single page (3 events, limit 20)
        for i in 0..3 {
            sqlx::query("INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) VALUES ($1, $2, $3, $4, $5, $6)")
                .bind(format!("C{:0>55}", i))
                .bind("contract")
                .bind(format!("{:0>64}", i))
                .bind(i as i64)
                .bind(Utc::now())
                .bind(json!({}))
                .execute(&pool).await.unwrap();
        }

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=20")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["total"].as_u64().is_some()); // Can be approximate or exact
        assert!(v["total"].as_u64().is_some());
        assert_eq!(v["data"].as_array().unwrap().len(), 3);

        // 3. Multi-page (limit 2, total 3)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=2&page=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["total"].as_u64().is_some());
        assert_eq!(v["data"].as_array().unwrap().len(), 2);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=2&page=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["total"].as_u64().is_some());
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
    }

    /// Test that health endpoint returns 503 when DB is unreachable
    #[tokio::test]
    async fn health_db_unreachable_returns_503() {
        let pool = PgPool::connect_lazy("postgres://invalid-host:5432/invalid_db").unwrap();
        let health_state = Arc::new(HealthState::new(60));
        let prometheus_handle = crate::metrics::init_metrics();
        let indexer_state = Arc::new(IndexerState::new());
        let config = crate::config::Config::default();
        let app = crate::routes::create_router(
            pool,
            Vec::new(),
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // The DB is unreachable so should return 503
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "degraded");
        assert!(matches!(
            v["db"].as_str(),
            Some("unreachable") | Some("pool_exhausted")
        ));
    }

    // Health endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn health_happy_path_returns_200(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["db"], "ok");
        assert_eq!(v["indexer"], "ok");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn healthz_live_returns_200(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "alive");
    }

    #[tokio::test]
    async fn healthz_ready_unreachable_db_returns_503() {
        let pool = PgPool::connect_lazy("postgres://invalid-host:5432/invalid_db").unwrap();
        let health_state = Arc::new(HealthState::new(60));
        let prometheus_handle = crate::metrics::init_metrics();
        let indexer_state = Arc::new(IndexerState::new());
        let config = crate::config::Config::default();
        let app = crate::routes::create_router(
            pool,
            Vec::new(),
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "degraded");
        assert!(matches!(
            v["db"].as_str(),
            Some("unreachable") | Some("pool_exhausted")
        ));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn healthz_ready_indexer_stalled_returns_503(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(1));
        // never updated, treated as stalled
        let prometheus_handle = crate::metrics::init_metrics();
        let indexer_state = Arc::new(IndexerState::new());
        let config = crate::config::Config::default();
        let app = crate::routes::create_router(
            pool,
            Vec::new(),
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "degraded");
        assert_eq!(v["indexer"], "stalled");
    }

    // Status endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn status_returns_operational_info(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        // Verify required fields are present
        assert!(v.get("version").is_some());
        assert!(v.get("uptime_secs").is_some());
        assert!(v.get("current_ledger").is_some());
        assert!(v.get("latest_ledger").is_some());
        assert!(v.get("lag_ledgers").is_some());
        assert!(v.get("total_events").is_some());
        assert!(v.get("indexer_status").is_some());

        // Verify total_events is 0 for empty DB
        assert_eq!(v["total_events"], 0);
    }

    // OpenAPI endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn openapi_json_returns_valid_spec(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        // Verify it's a valid OpenAPI spec
        assert_eq!(v["openapi"], "3.0.0");
        assert!(v.get("info").is_some());
        assert!(v.get("paths").is_some());
    }

    // Main events endpoint tests - Happy path
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_empty_db_returns_200_with_empty_data(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["data"], json!([]));
        assert_eq!(v["total"], 0);
        assert_eq!(v["page"], 1);
        assert_eq!(v["limit"], 20);
        assert_eq!(v["approximate"], true);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_with_data_returns_paginated_results(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert test data
        for i in 0..5 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["data"].as_array().unwrap().len(), 3);
        assert_eq!(v["total"], 5);
        assert_eq!(v["page"], 1);
        assert_eq!(v["limit"], 3);
    }

    // Main events endpoint tests - Error cases
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_invalid_event_type_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?event_type=invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["error"]
            .as_str()
            .unwrap()
            .contains("event_type must be one of"));
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_invalid_ledger_range_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?from_ledger=100&to_ledger=50")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["error"]
            .as_str()
            .unwrap()
            .contains("from_ledger must be <= to_ledger"));
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    // /v1/events/recent tests

    #[sqlx::test(migrations = "./migrations")]
    async fn get_recent_events_returns_events_in_desc_order(pool: PgPool) {
        let app = create_test_router(pool.clone());

        for i in 0..5_i64 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i)
            .bind(Utc::now())
            .bind(json!({}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/recent?limit=3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let data = v["data"].as_array().unwrap();
        assert_eq!(data.len(), 3);
        // Verify descending ledger order
        let ledgers: Vec<i64> = data.iter().map(|e| e["ledger"].as_i64().unwrap()).collect();
        assert!(
            ledgers.windows(2).all(|w| w[0] >= w[1]),
            "events must be in descending ledger order"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_recent_events_rejects_from_ledger(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/recent?from_ledger=100")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["error"].as_str().unwrap().contains("from_ledger"));
        assert_eq!(v["code"], "VALIDATION_ERROR");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_recent_events_rejects_to_ledger(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/recent?to_ledger=400")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["error"].as_str().unwrap().contains("to_ledger"));
        assert_eq!(v["code"], "VALIDATION_ERROR");
    }

    // Events by contract tests - Happy path
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_contract_with_data_returns_200(pool: PgPool) {
        let app = create_test_router(pool.clone());
        let contract_id = "C1234567890123456789012345678901234567890123456789012345";

        // Insert test data
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(contract_id)
        .bind("contract")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"test": "data"}))
        .execute(&pool)
        .await
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/contract/{}", contract_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["contract_id"], contract_id);
        assert_eq!(v["total"], 1);
        assert!(v.get("page").is_some());
        assert!(v.get("limit").is_some());
        assert!(v.get("approximate").is_some());
    }

    // Events by contract tests - Error cases
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_contract_not_found_returns_404(pool: PgPool) {
        let app = create_test_router(pool);
        let contract_id = "C1234567890123456789012345678901234567890123456789012345";

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/contract/{}", contract_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // Events by transaction tests - Happy path
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_tx_multiple_events_returns_all(pool: PgPool) {
        let app = create_test_router(pool.clone());
        let tx_hash = "a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890";

        // Insert multiple events for same transaction
        for i in 0..3 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(&tx_hash)
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"event_num": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", tx_hash))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["data"].as_array().unwrap().len(), 3);
        assert_eq!(v["tx_hash"], tx_hash);
    }

    // Stream events endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_returns_sse_stream(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream")
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/event-stream"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_with_contract_filter(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=C1234567890123456789012345678901234567890123456789012345")
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/event-stream"
        );
    }

    // --- Validation edge-case unit tests (discovered via fuzzing review) ---

    #[test]
    fn validate_contract_id_empty_returns_err() {
        assert!(validate_contract_id("").is_err());
    }

    #[test]
    fn validate_contract_id_55_chars_returns_err() {
        let s = format!("C{}", "A".repeat(54));
        assert!(validate_contract_id(&s).is_err());
    }

    #[test]
    fn validate_contract_id_57_chars_returns_err() {
        let s = format!("C{}", "A".repeat(56));
        assert!(validate_contract_id(&s).is_err());
    }

    #[test]
    fn validate_contract_id_starts_with_lowercase_c_returns_err() {
        let s = format!("c{}", "A".repeat(55));
        assert!(validate_contract_id(&s).is_err());
    }

    #[test]
    fn validate_contract_id_with_special_chars_returns_err() {
        let s = format!("C{}!", "A".repeat(54));
        assert!(validate_contract_id(&s).is_err());
    }

    #[test]
    fn validate_contract_id_valid_returns_ok() {
        let s = format!("C{}", "A".repeat(55));
        assert!(validate_contract_id(&s).is_ok());
    }

    #[test]
    fn validate_tx_hash_empty_returns_err() {
        assert!(validate_tx_hash("").is_err());
    }

    #[test]
    fn validate_tx_hash_63_chars_returns_err() {
        assert!(validate_tx_hash(&"a".repeat(63)).is_err());
    }

    #[test]
    fn validate_tx_hash_65_chars_returns_err() {
        assert!(validate_tx_hash(&"a".repeat(65)).is_err());
    }

    #[test]
    fn validate_tx_hash_non_hex_returns_err() {
        assert!(validate_tx_hash(&"g".repeat(64)).is_err());
    }

    #[test]
    fn validate_tx_hash_valid_lowercase_returns_ok() {
        assert!(validate_tx_hash(&"a".repeat(64)).is_ok());
    }

    #[test]
    fn validate_tx_hash_valid_uppercase_returns_ok() {
        assert!(validate_tx_hash(&"A".repeat(64)).is_ok());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_invalid_contract_id_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        // Invalid: too short
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=CABC")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid contract_id format");

        // Invalid: doesn't start with C
        let response = app.clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=A1234567890123456789012345678901234567890123456789012345")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Invalid: contains non-alphanumeric
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=C123456789012345678901234567890123456789012345678901234!")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_invalid_topic_prefix_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?topic_prefix=not-valid-json{{{")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "topic_prefix must be valid JSON");
        assert_eq!(v["code"], "VALIDATION_ERROR");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_valid_topic_prefix_returns_sse_stream(pool: PgPool) {
        let app = create_test_router(pool);

        // URL-encoded JSON: {"sym":"swap"}
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?topic_prefix=%7B%22sym%22%3A%22swap%22%7D")
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/event-stream"
        );
    }

    /// Verify that a named `event: ping` is emitted when no events arrive within the keepalive interval.
    #[tokio::test]
    async fn stream_events_emits_named_ping_when_idle() {
        use axum::body::Body;
        use futures::StreamExt;
        use http_body_util::BodyExt;
        use std::sync::Arc;
        use tokio::sync::broadcast;

        let pool = sqlx::PgPool::connect_lazy("postgres://localhost/unused").unwrap();
        let health_state = Arc::new(crate::config::HealthState::new(60));
        let indexer_state = Arc::new(crate::config::IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let (event_tx, _) = broadcast::channel::<crate::models::SorobanEvent>(16);
        let config = crate::config::Config::default();

        // Use a 50 ms keepalive so the test completes quickly.
        let app = crate::routes::create_router_with_tx(
            pool.clone(),
            pool,
            vec![],
            &[],
            0, // rate_limit disabled
            false,
            health_state,
            indexer_state,
            prometheus_handle,
            event_tx,
            50, // keepalive_ms = 50 ms
            1000,
            2000,
            None,
            None,
            config,
            None,
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream")
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Collect bytes until we see "event: ping" or timeout after 500 ms.
        let mut body = response.into_body().into_data_stream();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
        let mut buf = String::new();
        loop {
            tokio::select! {
                chunk = body.next() => {
                    match chunk {
                        Some(Ok(bytes)) => {
                            buf.push_str(&String::from_utf8_lossy(&bytes));
                            if buf.contains("event: ping") {
                                break;
                            }
                        }
                        _ => break,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }

        assert!(
            buf.contains("event: ping"),
            "expected 'event: ping' in SSE output, got: {buf:?}"
        );
    }

    // Metrics endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn metrics_returns_prometheus_format(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        // Metrics endpoint should return text/plain content type
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; version=0.0.4"
        );
    }

    // Pagination boundary condition tests
    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_boundary_conditions(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert exactly 25 test events
        for i in 0..25 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test limit boundary: limit=1 (minimum)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 1);
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["total"], 25);

        // Test limit boundary: limit=100 (maximum)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=100")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 100);
        assert_eq!(v["data"].as_array().unwrap().len(), 25); // All events
        assert_eq!(v["total"], 25);

        // Test page boundary: page=1, limit=10
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?page=1&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 1);
        assert_eq!(v["limit"], 10);
        assert_eq!(v["data"].as_array().unwrap().len(), 10);

        // Test page boundary: page=3, limit=10 (last page with 5 items)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?page=3&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 3);
        assert_eq!(v["limit"], 10);
        assert_eq!(v["data"].as_array().unwrap().len(), 5);

        // Test page boundary: page=4, limit=10 (beyond last page, empty)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?page=4&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 4);
        assert_eq!(v["limit"], 10);
        assert_eq!(v["data"].as_array().unwrap().len(), 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_invalid_parameters_are_clamped(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert test data
        for i in 0..5 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test limit=0 gets clamped to 1
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 1);

        // Test limit=200 gets clamped to 100
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=200")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 100);

        // Test page=0 gets treated as page=1
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?page=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_exact_count_vs_approximate_count(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert test data
        for i in 0..15 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test approximate count (default)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], true);
        // Approximate count may not be exact but should be reasonable
        let approx_count = v["total"].as_i64().unwrap();
        assert!(approx_count >= 0); // Should be non-negative

        // Test exact count
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?exact_count=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], false);
        assert_eq!(v["total"], 15); // Exact count should match

        // Test filtered queries always use exact count
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?event_type=contract")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], false);
        assert_eq!(v["total"], 15); // All events are contract type
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_with_filters(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert mixed event types
        for i in 0..10 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind(if i % 2 == 0 { "contract" } else { "diagnostic" })
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test pagination with event_type filter
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?event_type=contract&limit=3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 3);
        assert_eq!(v["total"], 5); // 5 contract events
        assert_eq!(v["approximate"], false); // Filtered queries use exact count

        // Test pagination with ledger range filter
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?from_ledger=2&to_ledger=8&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 5);
        assert_eq!(v["total"], 7); // Events with ledger 2-8
        assert_eq!(v["approximate"], false); // Filtered queries use exact count

        // Test pagination with both filters
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?event_type=contract&from_ledger=0&to_ledger=6&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 4); // Contract events with ledger 0-6
        assert_eq!(v["total"], 4);
        assert_eq!(v["approximate"], false);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_fields_filtering(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert test data
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"test": "data"}))
        .execute(&pool)
        .await
        .unwrap();

        // Test fields filter with single field
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?fields=ledger")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_none());
        assert!(event.get("event_type").is_none());
        assert!(event.get("tx_hash").is_none());
        assert!(event.get("timestamp").is_none());
        assert!(event.get("event_data").is_none());
        assert!(event.get("created_at").is_none());

        // Test fields filter with multiple fields
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?fields=ledger,contract_id,event_type")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_some());
        assert!(event.get("event_type").is_some());
        assert!(event.get("tx_hash").is_none());
        assert!(event.get("timestamp").is_none());
        assert!(event.get("event_data").is_none());
        assert!(event.get("created_at").is_none());

        // Test fields filter with invalid fields (should be ignored)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?fields=ledger,invalid_field,contract_id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_some());
        assert!(event.get("invalid_field").is_none());

        // Test empty fields filter (should return all fields)
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?fields=")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_some());
        assert!(event.get("event_type").is_some());
        assert!(event.get("tx_hash").is_some());
        assert!(event.get("timestamp").is_some());
        assert!(event.get("event_data").is_some());
        assert!(event.get("created_at").is_some());
    }

    // --- Cursor pagination tests ---

    async fn insert_events(pool: &PgPool, count: usize) {
        for i in 0..count {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({}))
            .execute(pool)
            .await
            .unwrap();
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn cursor_pagination_traverses_all_pages(pool: PgPool) {
        insert_events(&pool, 5).await;
        let app = create_test_router(pool);

        // Page 1: limit=2, no cursor
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 2);
        let cursor1 = v["next_cursor"]
            .as_str()
            .expect("next_cursor must be present on page 1")
            .to_string();

        // Page 2: use cursor from page 1
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events?limit=2&cursor={cursor1}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 2);
        let cursor2 = v["next_cursor"]
            .as_str()
            .expect("next_cursor must be present on page 2")
            .to_string();

        // Page 3: last page — 1 row, next_cursor must be null
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events?limit=2&cursor={cursor2}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert!(
            v["next_cursor"].is_null(),
            "next_cursor must be null on last page"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn cursor_pagination_no_duplicate_or_missing_rows(pool: PgPool) {
        insert_events(&pool, 6).await;
        let app = create_test_router(pool);

        let mut seen_ledgers: Vec<i64> = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let uri = match &cursor {
                Some(c) => format!("/v1/events?limit=2&cursor={c}"),
                None => "/v1/events?limit=2".to_string(),
            };
            let resp = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            let v: Value = serde_json::from_slice(&body).unwrap();
            let page = v["data"].as_array().unwrap();
            for ev in page {
                seen_ledgers.push(ev["ledger"].as_i64().unwrap());
            }
            cursor = v["next_cursor"].as_str().map(|s| s.to_string());
            if cursor.is_none() {
                break;
            }
        }

        // All 6 ledgers seen exactly once, in descending order
        assert_eq!(seen_ledgers.len(), 6);
        let mut sorted = seen_ledgers.clone();
        sorted.sort_by(|a, b| b.cmp(a));
        assert_eq!(seen_ledgers, sorted);
        let unique: std::collections::HashSet<_> = seen_ledgers.iter().collect();
        assert_eq!(unique.len(), 6);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn cursor_invalid_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?cursor=notvalidbase64!!!")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid cursor");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn offset_response_includes_next_cursor(pool: PgPool) {
        insert_events(&pool, 3).await;
        let app = create_test_router(pool);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        // Offset path still returns total/page/approximate AND next_cursor
        assert!(v.get("total").is_some());
        assert!(v.get("page").is_some());
        assert!(
            v["next_cursor"].is_string(),
            "offset path must also return next_cursor"
        );
    }

    // --- ETag / conditional GET tests ---

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_returns_etag_header(pool: PgPool) {
        insert_events(&pool, 3).await;
        let app = create_test_router(pool);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            resp.headers().contains_key("etag"),
            "ETag header must be present"
        );
        let etag = resp
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(
            etag.starts_with('"') && etag.ends_with('"'),
            "ETag must be quoted: {etag}"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_matching_etag_returns_304(pool: PgPool) {
        insert_events(&pool, 3).await;
        let app = create_test_router(pool);

        // First request — get ETag
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let etag = resp
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Second request with If-None-Match — should get 304
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header("if-none-match", &etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
        // 304 must include ETag header
        assert_eq!(resp.headers().get("etag").unwrap().to_str().unwrap(), etag);
        // 304 body must be empty
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_non_matching_etag_returns_200(pool: PgPool) {
        insert_events(&pool, 3).await;
        let app = create_test_router(pool);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header("if-none-match", "\"stale-etag-value\"")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_empty_db_no_etag(pool: PgPool) {
        let app = create_test_router(pool);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // No rows → no ETag
        assert!(resp.headers().get("etag").is_none());
    }

    // compact=true tests
    #[sqlx::test(migrations = "./migrations")]
    async fn compact_false_returns_full_json_event_data(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"topics": ["transfer", "GABC"], "value": 42}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?compact=false")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        // No encoding header when compact is off
        assert!(resp.headers().get("x-event-data-encoding").is_none());

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event_data = &v["data"][0]["event_data"];
        // Full JSON object, not a string
        assert!(
            event_data.is_object(),
            "event_data should be a JSON object when compact=false"
        );
        assert_eq!(event_data["value"], json!(42));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn compact_true_returns_base64_gzip_event_data(pool: PgPool) {
        let original = json!({"topics": ["transfer", "GABC"], "value": 42});

        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(&original)
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?compact=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        // Encoding hint header must be present
        assert_eq!(
            resp.headers()
                .get("x-event-data-encoding")
                .and_then(|v| v.to_str().ok()),
            Some("gzip+base64")
        );

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event_data = &v["data"][0]["event_data"];

        // Must be a string (base64-encoded)
        let encoded = event_data
            .as_str()
            .expect("event_data should be a base64 string when compact=true");

        // Decode base64 → decompress gzip → parse JSON → must equal original
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;
        use flate2::read::GzDecoder;
        use std::io::Read;

        let compressed = STANDARD.decode(encoded).expect("valid base64");
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut json_str = String::new();
        decoder.read_to_string(&mut json_str).expect("valid gzip");
        let decoded: Value = serde_json::from_str(&json_str).expect("valid JSON");

        assert_eq!(
            decoded, original,
            "decoded compact event_data must equal original"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn compact_true_with_cursor_pagination(pool: PgPool) {
        // Insert two events so we can test cursor path
        for i in 0..2_i64 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind("C1234567890123456789012345678901234567890123456789012345")
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(100_i64 + i)
            .bind(Utc::now())
            .bind(json!({"index": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let app = create_test_router(pool);

        // First page — get a cursor
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?compact=true&limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("x-event-data-encoding")
                .and_then(|v| v.to_str().ok()),
            Some("gzip+base64")
        );

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let cursor = v["next_cursor"].as_str().expect("next_cursor present");

        // Second page via cursor — event_data must still be compact
        let resp2 = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events?compact=true&limit=1&cursor={cursor}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp2.status(), StatusCode::OK);
        let body2 = to_bytes(resp2.into_body(), usize::MAX).await.unwrap();
        let v2: Value = serde_json::from_slice(&body2).unwrap();
        let event_data = &v2["data"][0]["event_data"];
        assert!(
            event_data.is_string(),
            "event_data should be base64 string on cursor page"
        );
    }

    // Multi-tenant isolation tests
    #[sqlx::test(migrations = "./migrations")]
    async fn multitenant_tenant_a_cannot_see_tenant_b_events(pool: PgPool) {
        // Insert events for two different tenants
        for (tenant, contract, tx) in [
            (
                "tenant_a",
                "C1111111111111111111111111111111111111111111111111111111",
                "a".repeat(64),
            ),
            (
                "tenant_b",
                "C2222222222222222222222222222222222222222222222222222222",
                "b".repeat(64),
            ),
        ] {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data, tenant_id)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(contract)
            .bind("contract")
            .bind(tx)
            .bind(100_i64)
            .bind(Utc::now())
            .bind(json!({"tenant": tenant}))
            .bind(tenant)
            .execute(&pool)
            .await
            .unwrap();
        }

        let mut tenant_map = std::collections::HashMap::new();
        tenant_map.insert("key_a".to_string(), "tenant_a".to_string());
        tenant_map.insert("key_b".to_string(), "tenant_b".to_string());
        let app = create_multitenant_test_router(pool, tenant_map);

        // Tenant A should only see its own event
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header("X-Api-Key", "key_a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let data = v["data"].as_array().unwrap();
        assert_eq!(data.len(), 1, "tenant_a should see exactly 1 event");
        assert_eq!(data[0]["event_data"]["tenant"], json!("tenant_a"));

        // Tenant B should only see its own event
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header("X-Api-Key", "key_b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let data = v["data"].as_array().unwrap();
        assert_eq!(data.len(), 1, "tenant_b should see exactly 1 event");
        assert_eq!(data[0]["event_data"]["tenant"], json!("tenant_b"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn multitenant_unknown_key_returns_403(pool: PgPool) {
        // key_a is in api_keys but NOT in tenant_map → 403
        let mut tenant_map = std::collections::HashMap::new();
        // key_a is registered but has no tenant mapping
        // We simulate this by giving key_b a mapping but not key_a
        tenant_map.insert("key_b".to_string(), "tenant_b".to_string());

        // Manually build a router where key_a is in api_keys but not in tenant_map
        use crate::middleware::hash_api_key;
        use tokio::sync::broadcast;
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let mut config = crate::config::Config::default();
        config.multi_tenant = true;
        let hashed_map: std::collections::HashMap<String, String> = tenant_map
            .iter()
            .map(|(k, v)| (hash_api_key(k), v.clone()))
            .collect();
        let app = crate::routes::create_router_with_tx_and_tenant_map(
            pool.clone(),
            pool,
            vec!["key_a".to_string(), "key_b".to_string()],
            &[],
            60,
            false,
            health_state,
            indexer_state,
            prometheus_handle,
            broadcast::channel(256).0,
            15000,
            1000,
            2000,
            None,
            None,
            config,
            None,
            Arc::new(hashed_map),
        );

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header("X-Api-Key", "key_a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn singletenent_mode_returns_all_events(pool: PgPool) {
        // In single-tenant mode, events with NULL tenant_id are returned for all callers
        for tx in ["a".repeat(64), "b".repeat(64)] {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind("C1111111111111111111111111111111111111111111111111111111")
            .bind("contract")
            .bind(tx)
            .bind(100_i64)
            .bind(Utc::now())
            .bind(json!({"x": 1}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 2);
    }

    // Replay endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_requires_api_key(pool: PgPool) {
        let app = create_test_router(pool);
        let replay_request = ReplayRequest {
            from_ledger: 100,
            to_ledger: 200,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should return 401 when no API key is provided (since test router has no API keys configured)
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "unauthorized");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_validates_ledger_range(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key to bypass auth
        let app = crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        // Test invalid range: from_ledger > to_ledger
        let replay_request = ReplayRequest {
            from_ledger: 200,
            to_ledger: 100,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "from_ledger must be <= to_ledger");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_validates_range_size(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key to bypass auth
        let app = crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        // Test range too large: > 10,000 ledgers
        let replay_request = ReplayRequest {
            from_ledger: 1,
            to_ledger: 15000,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "ledger range cannot exceed 10,000 ledgers");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_returns_403_when_not_active_indexer(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let mut indexer_state = Arc::new(IndexerState::new());

        // Set indexer as not active
        indexer_state
            .is_active_indexer
            .store(false, std::sync::atomic::Ordering::Relaxed);

        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key to bypass auth
        let app = crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let replay_request = ReplayRequest {
            from_ledger: 100,
            to_ledger: 200,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            v["error"],
            "replay endpoint only available on active indexer"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_accepts_valid_request(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let mut indexer_state = Arc::new(IndexerState::new());

        // Set indexer as active
        indexer_state
            .is_active_indexer
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key to bypass auth
        let app = crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let replay_request = ReplayRequest {
            from_ledger: 100,
            to_ledger: 200,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["message"], "replay job accepted");
        assert_eq!(v["from_ledger"], 100);
        assert_eq!(v["to_ledger"], 200);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_accepts_x_api_key_header(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let mut indexer_state = Arc::new(IndexerState::new());

        // Set indexer as active
        indexer_state
            .is_active_indexer
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key to bypass auth
        let app = crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let replay_request = ReplayRequest {
            from_ledger: 100,
            to_ledger: 200,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("X-Api-Key", "test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["message"], "replay job accepted");
    }

    // ── Indexed range validation tests ───────────────────────────────────────

    fn create_active_replay_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        indexer_state
            .is_active_indexer
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    async fn insert_events_at_ledgers(pool: &PgPool, ledgers: &[i64]) {
        for (i, &ledger) in ledgers.iter().enumerate() {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(ledger)
            .bind(Utc::now())
            .bind(json!({}))
            .execute(pool)
            .await
            .unwrap();
        }
    }

    async fn post_replay(app: axum::Router, from: u64, to: u64) -> (StatusCode, Value) {
        let body = serde_json::to_string(&ReplayRequest {
            from_ledger: from,
            to_ledger: to,
        })
        .unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        (status, v)
    }

    /// No events indexed → any range returns 400.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_validation_no_events_indexed_returns_400(pool: PgPool) {
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 100, 200).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            v["error"].as_str().unwrap().contains("no events have been indexed"),
            "unexpected error: {}",
            v["error"]
        );
    }

    /// Range entirely before the indexed window → 400.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_entirely_before_indexed_window_returns_400(pool: PgPool) {
        // Indexed window: ledgers 500–1000
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 100, 400).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let err = v["error"].as_str().unwrap();
        assert!(
            err.contains("entirely before"),
            "unexpected error: {err}"
        );
    }

    /// Range entirely after the indexed window (future ledgers) → 400.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_entirely_after_indexed_window_returns_400(pool: PgPool) {
        // Indexed window: ledgers 500–1000
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 1001, 2000).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let err = v["error"].as_str().unwrap();
        assert!(
            err.contains("entirely after"),
            "unexpected error: {err}"
        );
    }

    /// Range fully within the indexed window → 202, no warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_fully_within_indexed_window_returns_202_no_warning(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 500, 1000).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(v.get("warning").is_none(), "no warning expected for fully-covered range");
    }

    /// Range partially overlapping at the low end → 202 with warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_partial_overlap_low_end_returns_202_with_warning(pool: PgPool) {
        // Indexed window: 500–1000; request starts before min
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 100, 800).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let warning = v["warning"].as_str().expect("warning field must be present");
        assert!(
            warning.contains("partially outside"),
            "unexpected warning: {warning}"
        );
    }

    /// Range partially overlapping at the high end → 202 with warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_partial_overlap_high_end_returns_202_with_warning(pool: PgPool) {
        // Indexed window: 500–1000; request extends beyond max
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 800, 1500).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let warning = v["warning"].as_str().expect("warning field must be present");
        assert!(
            warning.contains("partially outside"),
            "unexpected warning: {warning}"
        );
    }

    /// Range spanning the entire indexed window and beyond on both sides → 202 with warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_spanning_beyond_both_ends_returns_202_with_warning(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 1, 9999).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(
            v["warning"].as_str().is_some(),
            "warning field must be present"
        );
    }

    /// Boundary: request exactly at min indexed ledger → 202, no warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_exact_min_boundary_returns_202_no_warning(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 500, 500).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(v.get("warning").is_none());
    }

    /// Boundary: request exactly at max indexed ledger → 202, no warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_exact_max_boundary_returns_202_no_warning(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 1000, 1000).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(v.get("warning").is_none());
    }

    /// Boundary: to_ledger == min_indexed - 1 → entirely before → 400.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_to_ledger_one_before_min_returns_400(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 400, 499).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("entirely before"));
    }

    /// Boundary: from_ledger == max_indexed + 1 → entirely after → 400.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_from_ledger_one_after_max_returns_400(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 1001, 1500).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("entirely after"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_rejects_invalid_api_key(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key
        let app = crate::routes::create_router(
            pool,
            vec!["correct-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let replay_request = ReplayRequest {
            from_ledger: 100,
            to_ledger: 200,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer wrong-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "unauthorized");
    }

    // --- CSV export tests ---

    fn create_export_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Export requires api_keys to be non-empty
        crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_returns_csv_with_header(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({"value": null, "topic": []}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("content-type").unwrap(), "text/csv");
        assert!(response
            .headers()
            .get("content-disposition")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("events.csv"));

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let csv = String::from_utf8(body.to_vec()).unwrap();
        let mut lines = csv.lines();
        assert_eq!(
            lines.next().unwrap(),
            "id,contract_id,event_type,tx_hash,ledger,timestamp,event_data,created_at"
        );
        assert!(lines.next().is_some(), "expected at least one data row");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_without_api_key_returns_error(pool: PgPool) {
        // Router with no api_keys configured
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Should be rejected (400 validation error since no api_keys means guard fires)
        assert!(response.status().is_client_error());
    }

    // ── CSV escaping unit tests ──────────────────────────────────────────────

    #[test]
    fn csv_escape_plain_field_is_unchanged() {
        assert_eq!(csv_escape_field("hello"), "hello");
        assert_eq!(csv_escape_field("contract"), "contract");
        assert_eq!(csv_escape_field(""), "");
    }

    #[test]
    fn csv_escape_field_with_comma_is_quoted() {
        assert_eq!(csv_escape_field("a,b"), "\"a,b\"");
    }

    #[test]
    fn csv_escape_field_with_double_quote_doubles_it() {
        assert_eq!(csv_escape_field(r#"say "hi""#), r#""say ""hi""""#);
    }

    #[test]
    fn csv_escape_field_with_newline_is_quoted() {
        assert_eq!(csv_escape_field("line1\nline2"), "\"line1\nline2\"");
    }

    #[test]
    fn csv_escape_field_with_carriage_return_is_quoted() {
        assert_eq!(csv_escape_field("line1\rline2"), "\"line1\rline2\"");
    }

    #[test]
    fn csv_escape_field_with_comma_and_quote() {
        // A field like: He said, "hello"
        // Should become: "He said, ""hello"""
        assert_eq!(
            csv_escape_field(r#"He said, "hello""#),
            r#""He said, ""hello""""#
        );
    }

    #[test]
    fn csv_escape_json_event_data() {
        // Typical JSON event_data contains commas and double-quotes
        let json = r#"{"key":"value","amount":100}"#;
        let escaped = csv_escape_field(json);
        // Must be wrapped in quotes and internal quotes doubled
        assert!(escaped.starts_with('"'));
        assert!(escaped.ends_with('"'));
        assert!(escaped.contains("\"\"key\"\""));
    }

    // ── CSV format=csv explicit query param ──────────────────────────────────

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_format_csv_explicit_returns_csv(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("c".repeat(64))
        .bind(5_i64)
        .bind(Utc::now())
        .bind(json!({"key": "value", "amount": 42}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=csv")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("content-type").unwrap(), "text/csv");
        assert!(response
            .headers()
            .get("content-disposition")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("events.csv"));

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let csv = String::from_utf8(body.to_vec()).unwrap();
        let mut lines = csv.lines();
        assert_eq!(
            lines.next().unwrap(),
            "id,contract_id,event_type,tx_hash,ledger,timestamp,event_data,created_at"
        );
        assert!(lines.next().is_some(), "expected at least one data row");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_csv_escapes_special_characters(pool: PgPool) {
        // Insert an event whose event_data contains commas and quotes (normal JSON)
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("d".repeat(64))
        .bind(10_i64)
        .bind(Utc::now())
        // JSON with commas and quotes — both must be properly escaped in CSV
        .bind(json!({"msg": "hello, world", "note": "say \"hi\""}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=csv")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let csv = String::from_utf8(body.to_vec()).unwrap();

        // The data row must exist
        let mut lines = csv.lines();
        lines.next(); // skip header
        let data_row = lines.next().expect("expected a data row");

        // The event_data field must be quoted (contains commas and quotes)
        assert!(
            data_row.contains('"'),
            "event_data with commas/quotes must be quoted in CSV: {data_row}"
        );
        // The row must not split on the comma inside the JSON value
        // (i.e., the CSV parser should see exactly 8 fields)
        let field_count = count_csv_fields(data_row);
        assert_eq!(field_count, 8, "expected 8 CSV fields, got {field_count}: {data_row}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_csv_empty_db_returns_header_only(pool: PgPool) {
        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=csv")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("content-type").unwrap(), "text/csv");

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let csv = String::from_utf8(body.to_vec()).unwrap();
        let mut lines = csv.lines();
        assert_eq!(
            lines.next().unwrap(),
            "id,contract_id,event_type,tx_hash,ledger,timestamp,event_data,created_at"
        );
        assert!(lines.next().is_none(), "empty DB should produce header row only");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_unknown_format_returns_400(pool: PgPool) {
        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=xlsx")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// Minimal RFC 4180 field counter: counts top-level comma-separated fields,
    /// respecting double-quoted fields (commas inside quotes don't count).
    fn count_csv_fields(line: &str) -> usize {
        let mut count = 1usize;
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();
        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    if in_quotes {
                        // Peek: if next char is also '"', it's an escaped quote — skip it.
                        if chars.peek() == Some(&'"') {
                            chars.next();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                ',' if !in_quotes => count += 1,
                _ => {}
            }
        }
        count
    }

    // ── Parquet export tests ─────────────────────────────────────────────────

    #[cfg(feature = "parquet")]
    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_parquet_returns_octet_stream(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(1_i64)
        .bind(json!({"value": null, "topic": []}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=parquet")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/octet-stream"
        );
        assert!(response
            .headers()
            .get("content-disposition")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("events.parquet"));

        // Verify the bytes are a valid Parquet file (magic bytes PAR1)
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.len() > 4);
        assert_eq!(&body[..4], b"PAR1", "Parquet magic bytes missing");
        assert_eq!(
            &body[body.len() - 4..],
            b"PAR1",
            "Parquet footer magic missing"
        );
    }

    #[cfg(feature = "parquet")]
    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_parquet_can_be_read_back(pool: PgPool) {
        use arrow_array::cast::AsArray;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let contract_id = "C1234567890123456789012345678901234567890123456789012345";
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind(contract_id)
        .bind("contract")
        .bind("b".repeat(64))
        .bind(42_i64)
        .bind(json!({"value": {"amount": 100}, "topic": []}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=parquet")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();

        // Read back with parquet reader
        let cursor = std::io::Cursor::new(body);
        let builder = ParquetRecordBatchReaderBuilder::try_new(cursor).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);

        // Verify ledger column value
        let ledger_col = batch.column_by_name("ledger").unwrap();
        let ledger_val = ledger_col
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(ledger_val, 42);

        // Verify contract_id column value
        let cid_col = batch.column_by_name("contract_id").unwrap();
        let cid_val = cid_col.as_string::<i32>().value(0);
        assert_eq!(cid_val, contract_id);
    }

    #[cfg(feature = "parquet")]
    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_parquet_empty_db_returns_valid_file(pool: PgPool) {
        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=parquet")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        // Even an empty Parquet file has the PAR1 magic
        assert_eq!(&body[..4], b"PAR1");
    }

    #[cfg(not(feature = "parquet"))]
    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_parquet_without_feature_returns_400(pool: PgPool) {
        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=parquet")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // ── Anonymization tests ──────────────────────────────────────────────────

    fn create_admin_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        crate::routes::create_router(
            pool,
            vec!["admin-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymize_event_returns_200_and_scrubs_data(pool: PgPool) {
        let event_id = Uuid::new_v4();
        let original_tx = "a".repeat(64);
        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(event_id)
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind(&original_tx)
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({"sensitive": "data"}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_admin_router(pool.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/admin/events/{event_id}/anonymize"))
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["anonymized"], true);

        // Verify DB state
        let row = sqlx::query("SELECT event_data, tx_hash, anonymized FROM events WHERE id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let event_data: Value = row.try_get("event_data").unwrap();
        assert_eq!(event_data, json!({"anonymized": true}));
        let tx_hash: String = row.try_get("tx_hash").unwrap();
        assert_ne!(
            tx_hash, original_tx,
            "tx_hash must be replaced with its hash"
        );
        assert_eq!(tx_hash.len(), 64, "hashed tx_hash must be 64 hex chars");
        let anonymized: bool = row.try_get("anonymized").unwrap();
        assert!(anonymized);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymize_event_is_idempotent(pool: PgPool) {
        let event_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, anonymized)
             VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)",
        )
        .bind(event_id)
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("b".repeat(64))
        .bind(2_i64)
        .bind(Utc::now())
        .bind(json!({"anonymized": true}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_admin_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/admin/events/{event_id}/anonymize"))
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["anonymized"], true);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymize_event_not_found_returns_404(pool: PgPool) {
        let app = create_admin_router(pool);
        let missing_id = Uuid::new_v4();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/admin/events/{missing_id}/anonymize"))
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymize_event_requires_api_key(pool: PgPool) {
        let event_id = Uuid::new_v4();
        let app = create_admin_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/admin/events/{event_id}/anonymize"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymized_event_data_visible_in_get_events(pool: PgPool) {
        let event_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, anonymized)
             VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)",
        )
        .bind(event_id)
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("c".repeat(64))
        .bind(3_i64)
        .bind(Utc::now())
        .bind(json!({"anonymized": true}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?exact_count=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let events = v["data"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event_data"], json!({"anonymized": true}));
    }

    // ── Diff endpoint tests ──────────────────────────────────────────────────

    #[sqlx::test(migrations = "./migrations")]
    async fn diff_groups_events_by_contract_and_type(pool: PgPool) {
        // Contract A: 3 contract events at ledger 10-12, 1 diagnostic at ledger 11
        // Contract B: 2 contract events at ledger 10-11
        let contract_a = "CA23456789012345678901234567890123456789012345678901234";
        let contract_b = "CB23456789012345678901234567890123456789012345678901234";
        for (i, (cid, etype, ledger)) in [
            (contract_a, "contract", 10_i64),
            (contract_a, "contract", 11_i64),
            (contract_a, "contract", 12_i64),
            (contract_a, "diagnostic", 11_i64),
            (contract_b, "contract", 10_i64),
            (contract_b, "contract", 11_i64),
        ]
        .iter()
        .enumerate()
        {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(cid)
            .bind(etype)
            .bind(format!("{:0>64}", i))
            .bind(ledger)
            .bind(Utc::now())
            .bind(json!({}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/diff?from_ledger=10&to_ledger=12")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["from_ledger"], 10);
        assert_eq!(v["to_ledger"], 12);

        let contracts = v["contracts"].as_array().unwrap();
        assert_eq!(contracts.len(), 2);

        // First entry must be contract_a (total=4 > contract_b total=2)
        assert_eq!(contracts[0]["contract_id"], contract_a);
        assert_eq!(contracts[0]["event_counts"]["contract"], 3);
        assert_eq!(contracts[0]["event_counts"]["diagnostic"], 1);
        assert_eq!(contracts[0]["total"], 4);

        assert_eq!(contracts[1]["contract_id"], contract_b);
        assert_eq!(contracts[1]["event_counts"]["contract"], 2);
        assert_eq!(contracts[1]["total"], 2);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn diff_empty_range_returns_empty_contracts(pool: PgPool) {
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/diff?from_ledger=1000&to_ledger=2000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["contracts"], json!([]));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn diff_invalid_range_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/diff?from_ledger=100&to_ledger=50")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["code"], "VALIDATION_ERROR");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn diff_missing_params_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        // Missing to_ledger
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/diff?from_ledger=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // ── Pause / Resume tests ─────────────────────────────────────────────────

    fn create_active_indexer_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        indexer_state
            .is_active_indexer
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        crate::routes::create_router(
            pool,
            vec!["admin-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pause_indexer_returns_200_and_sets_paused(pool: PgPool) {
        let app = create_active_indexer_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/pause")
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["indexer_paused"], true);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn resume_indexer_returns_200_and_clears_paused(pool: PgPool) {
        let app = create_active_indexer_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/resume")
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["indexer_paused"], false);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pause_returns_403_on_read_only_replica(pool: PgPool) {
        // create_test_router uses IndexerState::new() which defaults is_active_indexer=false
        let app = create_admin_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/pause")
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn resume_returns_403_on_read_only_replica(pool: PgPool) {
        let app = create_admin_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/resume")
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pause_requires_api_key(pool: PgPool) {
        let app = create_active_indexer_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/pause")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn status_includes_indexer_paused_field(pool: PgPool) {
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(
            v.get("indexer_paused").is_some(),
            "indexer_paused must be present in /status"
        );
        assert_eq!(v["indexer_paused"], false);
    }

    // ── Materialized view stats tests ────────────────────────────────────────

    #[sqlx::test(migrations = "./migrations")]
    async fn get_event_stats_empty_db_returns_zeros(pool: PgPool) {
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total_events"], 0);
        assert_eq!(v["events_last_24h"], 0);
        assert_eq!(v["events_last_7d"], 0);
        assert!(v["top_contracts"].as_array().unwrap().is_empty());
        assert!(v.get("computed_at").is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_event_stats_reflects_data_after_matview_refresh(pool: PgPool) {
        // Insert events
        for i in 0..3_i64 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, NOW(), $5)",
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i)
            .bind(json!({}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Manually refresh the materialized views so the test data is visible
        crate::stats_refresh::refresh_all(&pool).await;

        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["total_events"], 3);
        // All events inserted with NOW() so they should appear in 24h and 7d windows
        assert_eq!(v["events_last_24h"], 3);
        assert_eq!(v["events_last_7d"], 3);
        assert_eq!(v["top_contracts"].as_array().unwrap().len(), 3);
        assert_eq!(v["events_by_type"]["contract"], 3);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_event_stats_top_contracts_ordered_by_count(pool: PgPool) {
        let contract_a = "CA23456789012345678901234567890123456789012345678901234";
        let contract_b = "CB23456789012345678901234567890123456789012345678901234";

        // 3 events for A, 1 for B
        for i in 0..3_i64 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, NOW(), $5)",
            )
            .bind(contract_a)
            .bind("contract")
            .bind(format!("a{:0>63}", i))
            .bind(i)
            .bind(json!({}))
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind(contract_b)
        .bind("contract")
        .bind("b".repeat(64))
        .bind(10_i64)
        .bind(json!({}))
        .execute(&pool)
        .await
        .unwrap();

        crate::stats_refresh::refresh_all(&pool).await;

        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        let top = v["top_contracts"].as_array().unwrap();
        assert_eq!(top[0]["contract_id"], contract_a);
        assert_eq!(top[0]["event_count"], 3);
        assert_eq!(top[1]["contract_id"], contract_b);
        assert_eq!(top[1]["event_count"], 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_event_stats_returns_cache_control_header(pool: PgPool) {
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let cc = response
            .headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            cc.contains("max-age=60"),
            "expected max-age=60 in Cache-Control, got: {cc}"
        );
    }
}

// ── Archive ──────────────────────────────────────────────────────────────────

/// `GET /v1/events/archive` — list available archive files in S3.
///
/// Only available when the `archive` feature is enabled and `ARCHIVE_S3_BUCKET`
/// is configured. Returns 501 otherwise.
#[utoipa::path(
    get,
    path = "/v1/events/archive",
    tag = "events",
    responses(
        (status = 200, description = "List of archive files"),
        (status = 501, description = "Archive feature not enabled"),
    )
)]
pub async fn list_archive(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    #[cfg(feature = "archive")]
    {
        let (bucket, prefix) = match (
            &state.config.archive_s3_bucket,
            &state.config.archive_s3_prefix,
        ) {
            (Some(b), p) => (b.clone(), p.clone()),
            (None, _) => {
                return Err(AppError::Validation(
                    "ARCHIVE_S3_BUCKET is not configured".to_string(),
                ))
            }
        };
        let aws_cfg = aws_config::load_from_env().await;
        let s3 = aws_sdk_s3::Client::new(&aws_cfg);
        let files = crate::archiver::list_archive_files(&s3, &bucket, &prefix)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        return Ok(Json(json!({ "data": files, "total": files.len() })));
    }
    #[cfg(not(feature = "archive"))]
    {
        let _ = state; // suppress unused warning
        Err(AppError::Internal(
            "archive feature not enabled".to_string(),
        ))
    }
}

// ============================================================================
// Schema Management Endpoints
// ============================================================================

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct RegisterSchemaRequest {
    /// JSON Schema definition (Draft 7)
    pub schema: Value,
}

/// Register or update a JSON Schema for a contract
#[utoipa::path(
    post,
    path = "/v1/admin/contracts/{contract_id}/schema",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID")
    ),
    request_body = RegisterSchemaRequest,
    responses(
        (status = 200, description = "Schema registered successfully"),
        (status = 400, description = "Invalid schema or contract ID"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn register_contract_schema(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Json(payload): Json<RegisterSchemaRequest>,
) -> Result<impl IntoResponse, AppError> {
    validate_contract_id(&contract_id)?;

    let validator = state
        .schema_validator
        .as_ref()
        .ok_or_else(|| AppError::Internal("Schema validator not initialized".to_string()))?;

    validator
        .register_schema(&contract_id, &payload.schema)
        .await
        .map_err(|e| AppError::Validation(format!("Invalid schema: {}", e)))?;

    Ok((
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "message": "Schema registered successfully"
        })),
    ))
}

/// Get the JSON Schema for a contract
#[utoipa::path(
    get,
    path = "/v1/admin/contracts/{contract_id}/schema",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID")
    ),
    responses(
        (status = 200, description = "Schema retrieved successfully"),
        (status = 404, description = "No schema registered for this contract"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn get_contract_schema(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    validate_contract_id(&contract_id)?;

    let validator = state
        .schema_validator
        .as_ref()
        .ok_or_else(|| AppError::Internal("Schema validator not initialized".to_string()))?;

    let schema = validator
        .get_schema(&contract_id)
        .await
        .ok_or(AppError::NotFound)?;

    Ok((StatusCode::OK, Json(json!({ "schema": schema }))))
}

/// Delete the JSON Schema for a contract
#[utoipa::path(
    delete,
    path = "/v1/admin/contracts/{contract_id}/schema",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID")
    ),
    responses(
        (status = 200, description = "Schema deleted successfully"),
        (status = 404, description = "No schema registered for this contract"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn delete_contract_schema(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    validate_contract_id(&contract_id)?;

    let validator = state
        .schema_validator
        .as_ref()
        .ok_or_else(|| AppError::Internal("Schema validator not initialized".to_string()))?;

    let deleted = validator
        .delete_schema(&contract_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    if deleted {
        Ok((
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "message": "Schema deleted successfully"
            })),
        ))
    } else {
        Err(AppError::NotFound)
    }
}

/// Validate event data against a contract's JSON Schema
#[utoipa::path(
    post,
    path = "/v1/admin/contracts/{contract_id}/validate",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID")
    ),
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Validation result"),
        (status = 400, description = "Validation failed with error details"),
        (status = 404, description = "No schema registered for this contract"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn validate_event_data_against_schema(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Json(event_data): Json<serde_json::Value>,
) -> Result<impl IntoResponse, AppError> {
    validate_contract_id(&contract_id)?;

    let validator = state.schema_validator
        .as_ref()
        .ok_or_else(|| AppError::Internal("Schema validator not initialized".to_string()))?;

    match validator.validate_event_data(&contract_id, &event_data).await {
        None => Err(AppError::NotFound),
        Some((true, _)) => Ok((
            StatusCode::OK,
            Json(json!({
                "valid": true,
                "message": "Event data is valid"
            }))
        )),
        Some((false, errors)) => {
            let error_msg = format!("Event data validation failed with {} error(s)", errors.len());
            Err(AppError::ValidationWithDetails(error_msg, errors))
        }
    }
}


/// Start a background masking job for event data
#[utoipa::path(
    post,
    path = "/v1/admin/mask-events",
    tag = "admin",
    request_body = models::MaskEventsRequest,
    responses(
        (status = 200, description = "Masking job started", body = models::MaskEventsResponse),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn start_mask_events(
    State(state): State<AppState>,
    Json(req): Json<models::MaskEventsRequest>,
) -> Result<Json<models::MaskEventsResponse>, AppError> {
    let job_id = Uuid::new_v4().to_string();
    
    let pool = state.pool.clone();
    let contract_ids = req.contract_ids.clone();
    let job_id_clone = job_id.clone();
    
    tokio::spawn(async move {
        let _ = mask_events_background(&pool, contract_ids, &job_id_clone).await;
    });
    
    Ok(Json(models::MaskEventsResponse {
        job_id,
        status: "pending".to_string(),
    }))
}

/// Get status of a masking job
#[utoipa::path(
    get,
    path = "/v1/admin/mask-events/{job_id}",
    tag = "admin",
    params(
        ("job_id" = String, Path, description = "Job ID")
    ),
    responses(
        (status = 200, description = "Job status", body = models::MaskJobStatus),
        (status = 404, description = "Job not found"),
        (status = 401, description = "Unauthorized"),
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn get_mask_job_status(
    Path(job_id): Path<String>,
) -> Result<Json<models::MaskJobStatus>, AppError> {
    // For now, return a simple response. In production, this would query a job tracking table.
    Ok(Json(models::MaskJobStatus {
        job_id,
        status: "completed".to_string(),
        processed: 0,
        total: 0,
        error: None,
    }))
}

async fn mask_events_background(
    pool: &sqlx::PgPool,
    contract_ids: Option<Vec<String>>,
    _job_id: &str,
) -> Result<(), AppError> {
    let mut conditions: Vec<String> = vec!["event_data IS NOT NULL".to_string()];
    let mut bind_idx = 1;
    
    if let Some(ref ids) = contract_ids {
        if !ids.is_empty() {
            let placeholders = ids.iter().enumerate()
                .map(|(i, _)| format!("${}", bind_idx + i))
                .collect::<Vec<_>>()
                .join(",");
            conditions.push(format!("contract_id IN ({})", placeholders));
            bind_idx += ids.len() as i32;
        }
    }
    
    let where_clause = conditions.join(" AND ");
    let query_str = format!(
        "SELECT id, event_data FROM events WHERE {} ORDER BY id LIMIT 1000",
        where_clause
    );
    
    let mut q = sqlx::query(&query_str);
    if let Some(ref ids) = contract_ids {
        for id in ids {
            q = q.bind(id);
        }
    }
    
    let rows = q.fetch_all(pool).await?;
    
    for row in rows {
        let id: Uuid = row.try_get("id")?;
        let event_data: serde_json::Value = row.try_get("event_data")?;
        let masked_data = mask_event_data(&event_data);
        
        sqlx::query("UPDATE events SET event_data = $1 WHERE id = $2")
            .bind(&masked_data)
            .bind(id)
            .execute(pool)
            .await?;
    }
    
    Ok(())
}

fn mask_event_data(data: &serde_json::Value) -> serde_json::Value {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    fn deterministic_hash(value: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }
    
    match data {
        serde_json::Value::Object(obj) => {
            let mut new_obj = serde_json::Map::new();
            for (key, value) in obj {
                let masked_value = mask_event_data(value);
                let key_lower = key.to_lowercase();
                
                if key_lower.contains("address") || key_lower.contains("account") {
                    if let serde_json::Value::String(s) = &masked_value {
                        let hash = deterministic_hash(s);
                        new_obj.insert(key.clone(), serde_json::Value::String(format!("G{:x}", hash)[..56].to_string()));
                        continue;
                    }
                }
                new_obj.insert(key.clone(), masked_value);
            }
            serde_json::Value::Object(new_obj)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(mask_event_data).collect())
        }
        _ => data.clone(),
    }
}

/// Get time-series aggregation of events
#[utoipa::path(
    get,
    path = "/v1/events/timeseries",
    tag = "events",
    params(
        ("bucket" = String, Query, description = "Time bucket: 1h, 1d, 1w, 1mo"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("from_ledger" = Option<i64>, Query, description = "Start ledger"),
        ("to_ledger" = Option<i64>, Query, description = "End ledger"),
    ),
    responses(
        (status = 200, description = "Time-series data", body = models::TimeseriesResponse),
        (status = 400, description = "Invalid parameters"),
    )
)]
pub async fn get_timeseries(
    State(state): State<AppState>,
    Query(params): Query<models::TimeseriesParams>,
) -> Result<Json<models::TimeseriesResponse>, AppError> {
    let start = std::time::Instant::now();
    
    let interval = match params.bucket.as_str() {
        "1h" => "1 hour",
        "1d" => "1 day",
        "1w" => "1 week",
        "1mo" => "1 month",
        _ => return Err(AppError::Validation("invalid bucket".to_string())),
    };
    
    let mut conditions = vec!["1=1".to_string()];
    let mut bind_idx = 1;
    
    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }
    
    let where_clause = conditions.join(" AND ");
    let query_str = format!(
        "SELECT \
            date_trunc('{}', timestamp) as bucket_start, \
            COUNT(*) as event_count, \
            COUNT(DISTINCT contract_id) as contract_count, \
            event_type, \
            COUNT(*) as type_count \
         FROM events \
         WHERE {} \
         GROUP BY date_trunc('{}', timestamp), event_type \
         ORDER BY bucket_start ASC",
        interval, where_clause, interval
    );
    
    let mut q = sqlx::query(&query_str);
    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    
    let rows = q.fetch_all(&state.read_pool).await?;
    
    let mut buckets_map: std::collections::HashMap<chrono::DateTime<chrono::Utc>, models::TimeseriesBucket> = std::collections::HashMap::new();
    
    for row in rows {
        let bucket_start: chrono::DateTime<chrono::Utc> = row.try_get("bucket_start")?;
        let event_count: i64 = row.try_get("event_count")?;
        let contract_count: i64 = row.try_get("contract_count")?;
        let event_type: String = row.try_get("event_type")?;
        let type_count: i64 = row.try_get("type_count")?;
        
        let bucket = buckets_map.entry(bucket_start).or_insert_with(|| models::TimeseriesBucket {
            bucket_start,
            event_count,
            contract_count,
            event_types: std::collections::HashMap::new(),
        });
        
        bucket.event_types.insert(event_type, type_count);
    }
    
    let mut data: Vec<_> = buckets_map.into_values().collect();
    data.sort_by_key(|b| b.bucket_start);
    
    crate::metrics::record_timeseries_query_duration(start.elapsed());
    
    Ok(Json(models::TimeseriesResponse {
        bucket: params.bucket,
        data,
    }))
}

/// WebSocket endpoint for event streaming (alternative to SSE)
#[utoipa::path(
    get,
    path = "/v1/events/ws",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
    ),
    responses(
        (status = 101, description = "WebSocket upgrade"),
        (status = 400, description = "Invalid parameters"),
    )
)]
pub async fn ws_stream_events(
    State(state): State<AppState>,
    Query(params): Query<models::StreamParams>,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }
    
    let contract_id = params.contract_id.clone();
    let event_tx = state.event_tx.clone();
    let keepalive_ms = state.sse_keepalive_interval_ms;
    let ws_connections = state.sse_connections.clone();
    
    Ok(ws.on_upgrade(move |socket| {
        handle_ws_connection(socket, contract_id, event_tx, keepalive_ms, ws_connections)
    }))
}

async fn handle_ws_connection(
    socket: axum::extract::ws::WebSocket,
    contract_id: Option<String>,
    event_tx: tokio::sync::broadcast::Sender<models::SorobanEvent>,
    keepalive_ms: u64,
    ws_connections: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    use axum::extract::ws::{Message, WebSocket};
    use tokio::time::{interval, Duration};
    
    let count = ws_connections.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    crate::metrics::update_ws_connections(count);
    
    let (mut sender, mut receiver) = socket.split();
    let mut rx = event_tx.subscribe();
    let mut keepalive_interval = interval(Duration::from_millis(keepalive_ms));
    
    loop {
        tokio::select! {
            _ = keepalive_interval.tick() => {
                if sender.send(Message::Ping(vec![])).await.is_err() {
                    break;
                }
            }
            msg = receiver.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        if let Some(ref cid) = contract_id {
                            if event.contract_id != *cid {
                                continue;
                            }
                        }
                        if let Ok(json) = serde_json::to_string(&event) {
                            if sender.send(Message::Text(json)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    
    let count = ws_connections.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
    crate::metrics::update_ws_connections(count);
}
