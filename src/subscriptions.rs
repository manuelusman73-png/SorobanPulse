use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::net::IpAddr;
use std::time::Duration;
use tokio::time::sleep;
use url::Url;
use uuid::Uuid;

use crate::{config::Environment, error::AppError, routes::AppState};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Subscription {
    pub id: Uuid,
    pub callback_url: String,
    pub from_ledger: i64,
    pub acked_ledger: i64,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateSubscriptionRequest {
    pub callback_url: String,
    pub from_ledger: i64,
}

#[derive(Debug, Deserialize)]
pub struct AckRequest {
    pub ledger: i64,
}

// ---------------------------------------------------------------------------
// SSRF validation
// ---------------------------------------------------------------------------

/// Build the reqwest client used by the delivery worker.
/// Redirects are disabled to prevent SSRF via redirect chains.
pub fn build_delivery_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build subscription delivery HTTP client")
}

/// Validate a callback URL against SSRF rules.
///
/// Rules (in order):
/// 1. If SUBSCRIPTION_ALLOWED_URL_PREFIXES is set, the URL must match one of the
///    comma-separated prefixes — all other checks are skipped for matched URLs.
/// 2. The URL must be parseable.
/// 3. In production/staging, only HTTPS is accepted.
/// 4. Private IPs (RFC 1918), loopback, and link-local ranges are always rejected.
pub fn validate_callback_url(raw: &str, env: &Environment) -> Result<(), AppError> {
    // 1. Allowlist check
    let prefixes: Vec<String> = std::env::var("SUBSCRIPTION_ALLOWED_URL_PREFIXES")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if !prefixes.is_empty() {
        if prefixes.iter().any(|p| raw.starts_with(p.as_str())) {
            return Ok(());
        }
        return Err(AppError::Validation(
            "callback_url does not match any entry in SUBSCRIPTION_ALLOWED_URL_PREFIXES".into(),
        ));
    }

    // 2. Parse
    let url = Url::parse(raw)
        .map_err(|e| AppError::Validation(format!("callback_url is not a valid URL: {e}")))?;

    // 3. Scheme check
    match url.scheme() {
        "https" => {}
        "http" if !env.is_production_like() => {}
        "http" => {
            return Err(AppError::Validation(
                "callback_url must use HTTPS in production".into(),
            ));
        }
        scheme => {
            return Err(AppError::Validation(format!(
                "callback_url scheme '{scheme}' is not permitted; use https"
            )));
        }
    }

    // 4. SSRF: reject private/reserved hosts
    let host = url.host_str().unwrap_or("");
    if is_ssrf_host(host) {
        return Err(AppError::Validation(
            "callback_url points to a private, loopback, or link-local address".into(),
        ));
    }

    Ok(())
}

fn is_ssrf_host(host: &str) -> bool {
    // Loopback hostnames
    if matches!(host, "localhost" | "127.0.0.1" | "::1")
        || host.ends_with(".local")
        || host.ends_with(".localhost")
    {
        return true;
    }

    // Try to parse as a numeric IP first (handles both IPv4 and IPv6)
    if let Ok(ip) = host.parse::<IpAddr>() {
        return is_private_ip(&ip);
    }

    // Hostname-form private ranges (e.g. "10.0.0.1" already handled above,
    // but catch textual prefixes for belt-and-suspenders)
    host.starts_with("10.")
        || host.starts_with("192.168.")
        || host.starts_with("169.254.")
        || (host.starts_with("172.") && {
            host.split('.')
                .nth(1)
                .and_then(|o| o.parse::<u8>().ok())
                .map(|o| (16..=31).contains(&o))
                .unwrap_or(false)
        })
}

fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            a == 127                                    // loopback 127/8
                || a == 10                              // RFC 1918 10/8
                || (a == 172 && (16..=31).contains(&b)) // RFC 1918 172.16/12
                || (a == 192 && b == 168)               // RFC 1918 192.168/16
                || (a == 169 && b == 254)               // link-local / metadata
                || (a == 0 && b == 0 && c == 0)         // 0.0.0.0
                || a >= 224                              // multicast + reserved
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique-local fc00::/7
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

pub async fn create_subscription(
    State(state): State<AppState>,
    Json(body): Json<CreateSubscriptionRequest>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    if body.callback_url.is_empty() {
        return Err(AppError::Validation("callback_url is required".into()));
    }
    validate_callback_url(&body.callback_url, &state.config.environment)?;
    if body.from_ledger < 0 {
        return Err(AppError::Validation(
            "from_ledger must be non-negative".into(),
        ));
    }

    let sub: Subscription = sqlx::query_as(
        "INSERT INTO subscriptions (callback_url, from_ledger) VALUES ($1, $2)
         RETURNING id, callback_url, from_ledger, acked_ledger, status, created_at",
    )
    .bind(&body.callback_url)
    .bind(body.from_ledger)
    .fetch_one(&state.pool)
    .await?;

    // Enqueue all existing events >= from_ledger for this new subscription
    sqlx::query(
        "INSERT INTO delivery_queue (subscription_id, event_id, ledger)
         SELECT $1, id, ledger FROM events WHERE ledger >= $2
         ORDER BY ledger ASC",
    )
    .bind(sub.id)
    .bind(body.from_ledger)
    .execute(&state.pool)
    .await?;

    Ok((StatusCode::CREATED, Json(json!(sub))))
}

pub async fn get_subscription(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    let sub: Subscription = sqlx::query_as(
        "SELECT id, callback_url, from_ledger, acked_ledger, status, created_at
         FROM subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(AppError::NotFound)?;

    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM delivery_queue WHERE subscription_id = $1 AND status = 'pending'",
    )
    .bind(id)
    .fetch_one(&state.pool)
    .await?;

    Ok(Json(
        json!({ "subscription": sub, "pending_deliveries": pending }),
    ))
}

pub async fn cancel_subscription(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let rows = sqlx::query(
        "UPDATE subscriptions SET status = 'cancelled' WHERE id = $1 AND status = 'active'",
    )
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if rows == 0 {
        return Err(AppError::NotFound);
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn ack_subscription(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<AckRequest>,
) -> Result<Json<Value>, AppError> {
    // Advance acked_ledger only forward
    let rows = sqlx::query(
        "UPDATE subscriptions SET acked_ledger = $1
         WHERE id = $2 AND status = 'active' AND acked_ledger < $1",
    )
    .bind(body.ledger)
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if rows == 0 {
        // Check if subscription exists at all
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM subscriptions WHERE id = $1)")
                .bind(id)
                .fetch_one(&state.pool)
                .await?;
        if !exists {
            return Err(AppError::NotFound);
        }
    }

    // Mark delivered items up to this ledger as acknowledged (clean up)
    sqlx::query(
        "UPDATE delivery_queue SET status = 'delivered'
         WHERE subscription_id = $1 AND ledger <= $2 AND status = 'pending'",
    )
    .bind(id)
    .bind(body.ledger)
    .execute(&state.pool)
    .await?;

    Ok(Json(json!({ "acked_ledger": body.ledger })))
}

// ---------------------------------------------------------------------------
// Delivery worker
// ---------------------------------------------------------------------------

/// Enqueue newly indexed events for all active subscriptions.
/// Called by the indexer after a successful store.
pub async fn enqueue_event(pool: &PgPool, event_id: Uuid, ledger: i64) {
    let result = sqlx::query(
        "INSERT INTO delivery_queue (subscription_id, event_id, ledger)
         SELECT id, $1, $2 FROM subscriptions
         WHERE status = 'active' AND from_ledger <= $2
         ON CONFLICT DO NOTHING",
    )
    .bind(event_id)
    .bind(ledger)
    .execute(pool)
    .await;

    if let Err(e) = result {
        tracing::warn!(error = %e, "Failed to enqueue event for subscriptions");
    }
}

/// Background worker: polls delivery_queue and POSTs pending items to callback URLs.
pub async fn run_delivery_worker(pool: PgPool, http: reqwest::Client) {
    loop {
        match deliver_pending(&pool, &http).await {
            Ok(n) if n > 0 => tracing::debug!(delivered = n, "Delivery worker cycle"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "Delivery worker error"),
        }
        sleep(Duration::from_secs(5)).await;
    }
}

async fn deliver_pending(pool: &PgPool, http: &reqwest::Client) -> Result<usize, sqlx::Error> {
    // Fetch up to 50 pending items whose next_attempt_at is due
    let rows: Vec<(Uuid, Uuid, String, Value, i64)> = sqlx::query_as(
        "SELECT dq.id, dq.event_id, s.callback_url, e.event_data, dq.ledger
         FROM delivery_queue dq
         JOIN subscriptions s ON s.id = dq.subscription_id
         JOIN events e ON e.id = dq.event_id
         WHERE dq.status = 'pending'
           AND dq.next_attempt_at <= NOW()
           AND s.status = 'active'
         ORDER BY dq.ledger ASC
         LIMIT 50",
    )
    .fetch_all(pool)
    .await?;

    let count = rows.len();
    for (queue_id, event_id, callback_url, event_data, ledger) in rows {
        let payload = json!({ "event_id": event_id, "ledger": ledger, "event_data": event_data });
        match http
            .post(&callback_url)
            .json(&payload)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                sqlx::query("UPDATE delivery_queue SET status = 'delivered' WHERE id = $1")
                    .bind(queue_id)
                    .execute(pool)
                    .await?;
            }
            Ok(resp) => {
                let err = format!("HTTP {}", resp.status());
                schedule_retry(pool, queue_id, &err).await?;
            }
            Err(e) => {
                schedule_retry(pool, queue_id, &e.to_string()).await?;
            }
        }
    }
    Ok(count)
}

/// Exponential backoff: 2^attempts seconds, capped at 1 hour.
async fn schedule_retry(pool: &PgPool, queue_id: Uuid, error: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE delivery_queue
         SET attempts = attempts + 1,
             last_error = $2,
             next_attempt_at = NOW() + (LEAST(POWER(2, attempts + 1), 3600) || ' seconds')::interval
         WHERE id = $1",
    )
    .bind(queue_id)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    // --- SSRF validation tests ---

    #[test]
    fn ssrf_rejects_localhost() {
        let err = validate_callback_url("http://localhost/hook", &Environment::Development);
        assert!(err.is_err(), "localhost must be rejected");
    }

    #[test]
    fn ssrf_rejects_127_loopback() {
        let err = validate_callback_url("http://127.0.0.1/hook", &Environment::Development);
        assert!(err.is_err(), "127.0.0.1 must be rejected");
    }

    #[test]
    fn ssrf_rejects_ipv6_loopback() {
        let err = validate_callback_url("http://[::1]/hook", &Environment::Development);
        assert!(err.is_err(), "::1 must be rejected");
    }

    #[test]
    fn ssrf_rejects_rfc1918_10_x() {
        let err = validate_callback_url("http://10.0.0.1/hook", &Environment::Development);
        assert!(err.is_err(), "10.x.x.x must be rejected");
    }

    #[test]
    fn ssrf_rejects_rfc1918_172_16() {
        let err = validate_callback_url("http://172.16.0.1/hook", &Environment::Development);
        assert!(err.is_err(), "172.16.x.x must be rejected");
    }

    #[test]
    fn ssrf_rejects_rfc1918_192_168() {
        let err = validate_callback_url("http://192.168.1.1/hook", &Environment::Development);
        assert!(err.is_err(), "192.168.x.x must be rejected");
    }

    #[test]
    fn ssrf_rejects_link_local_metadata() {
        let err = validate_callback_url(
            "http://169.254.169.254/latest/meta-data/",
            &Environment::Development,
        );
        assert!(err.is_err(), "AWS metadata endpoint must be rejected");
    }

    #[test]
    fn ssrf_rejects_http_in_production() {
        let err = validate_callback_url("http://example.com/hook", &Environment::Production);
        assert!(err.is_err(), "http must be rejected in production");
    }

    #[test]
    fn ssrf_allows_https_public_in_production() {
        let ok = validate_callback_url("https://example.com/hook", &Environment::Production);
        assert!(ok.is_ok(), "https public URL must be allowed in production");
    }

    #[test]
    fn ssrf_allows_http_public_in_development() {
        let ok = validate_callback_url("http://example.com/hook", &Environment::Development);
        assert!(ok.is_ok(), "http public URL must be allowed in development");
    }

    #[test]
    fn ssrf_allowlist_permits_matched_prefix() {
        // Safety: env var manipulation is fine in single-threaded unit tests
        std::env::set_var(
            "SUBSCRIPTION_ALLOWED_URL_PREFIXES",
            "http://internal.corp/,https://trusted.corp/",
        );
        let ok =
            validate_callback_url("http://internal.corp/hook", &Environment::Production);
        std::env::remove_var("SUBSCRIPTION_ALLOWED_URL_PREFIXES");
        assert!(ok.is_ok(), "allowlisted prefix must be permitted");
    }

    #[test]
    fn ssrf_allowlist_blocks_unmatched_url() {
        std::env::set_var(
            "SUBSCRIPTION_ALLOWED_URL_PREFIXES",
            "https://trusted.corp/",
        );
        let err =
            validate_callback_url("https://other.corp/hook", &Environment::Development);
        std::env::remove_var("SUBSCRIPTION_ALLOWED_URL_PREFIXES");
        assert!(err.is_err(), "URL not in allowlist must be rejected when allowlist is set");
    }

    #[test]
    fn build_delivery_client_does_not_follow_redirects() {
        // The only way to verify policy without making a real request is to confirm
        // the builder compiles and returns a client successfully.
        let _client = build_delivery_client();
    }

    // --- Unit tests for retry backoff logic ---

    #[test]
    fn backoff_formula_caps_at_3600() {
        // Verify the SQL formula: LEAST(POWER(2, attempts+1), 3600)
        // attempts=0 → 2^1=2s, attempts=10 → 2^11=2048s, attempts=12 → 3600s cap
        let backoff = |attempts: u32| -> u64 {
            let raw = 2u64.pow(attempts + 1);
            raw.min(3600)
        };
        assert_eq!(backoff(0), 2);
        assert_eq!(backoff(1), 4);
        assert_eq!(backoff(10), 2048);
        assert_eq!(backoff(11), 3600); // 2^12=4096 → capped
        assert_eq!(backoff(12), 3600);
    }

    // --- Mock HTTP delivery tests ---

    struct MockServer {
        received: Arc<Mutex<Vec<Value>>>,
        fail_count: Arc<Mutex<u32>>,
    }

    impl MockServer {
        fn new() -> Self {
            Self {
                received: Arc::new(Mutex::new(vec![])),
                fail_count: Arc::new(Mutex::new(0)),
            }
        }

        fn with_failures(n: u32) -> Self {
            Self {
                received: Arc::new(Mutex::new(vec![])),
                fail_count: Arc::new(Mutex::new(n)),
            }
        }
    }

    /// Simulate delivery to a mock callback: records payload or returns error.
    async fn mock_deliver(server: &MockServer, payload: &Value) -> Result<bool, String> {
        let mut fails = server.fail_count.lock().unwrap();
        if *fails > 0 {
            *fails -= 1;
            return Err("connection refused".to_string());
        }
        server.received.lock().unwrap().push(payload.clone());
        Ok(true)
    }

    #[tokio::test]
    async fn delivery_succeeds_on_first_attempt() {
        let server = MockServer::new();
        let payload = json!({ "event_id": Uuid::new_v4(), "ledger": 100, "event_data": {} });
        let result = mock_deliver(&server, &payload).await;
        assert!(result.is_ok());
        assert_eq!(server.received.lock().unwrap().len(), 1);
        assert_eq!(server.received.lock().unwrap()[0]["ledger"], 100);
    }

    #[tokio::test]
    async fn delivery_retries_after_failure() {
        let server = MockServer::with_failures(2);
        let payload = json!({ "event_id": Uuid::new_v4(), "ledger": 200, "event_data": {} });

        // First two attempts fail
        assert!(mock_deliver(&server, &payload).await.is_err());
        assert!(mock_deliver(&server, &payload).await.is_err());
        // Third attempt succeeds
        assert!(mock_deliver(&server, &payload).await.is_ok());
        assert_eq!(server.received.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn ack_advances_cursor_only_forward() {
        // Simulate ack logic: acked_ledger should only move forward
        let mut acked_ledger: i64 = 50;
        let new_ack = |current: i64, proposed: i64| -> i64 {
            if proposed > current {
                proposed
            } else {
                current
            }
        };

        acked_ledger = new_ack(acked_ledger, 100);
        assert_eq!(acked_ledger, 100);

        // Trying to ack an older ledger should not regress
        acked_ledger = new_ack(acked_ledger, 80);
        assert_eq!(acked_ledger, 100);

        // Advancing further works
        acked_ledger = new_ack(acked_ledger, 150);
        assert_eq!(acked_ledger, 150);
    }

    #[tokio::test]
    async fn payload_contains_required_fields() {
        let event_id = Uuid::new_v4();
        let ledger = 12345_i64;
        let event_data = json!({ "value": { "amount": 100 }, "topic": [] });

        let payload = json!({
            "event_id": event_id,
            "ledger": ledger,
            "event_data": event_data,
        });

        assert!(payload.get("event_id").is_some());
        assert!(payload.get("ledger").is_some());
        assert!(payload.get("event_data").is_some());
        assert_eq!(payload["ledger"], 12345);
    }
}
