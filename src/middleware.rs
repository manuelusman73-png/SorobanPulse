use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
    Json,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use subtle::ConstantTimeEq;

/// Middleware to extract request_id from headers and store in thread-local
pub async fn request_id_middleware(req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    crate::error::set_request_id(request_id);
    next.run(req).await
}

/// The resolved tenant for the current request.
/// Injected as a request extension by `auth_middleware` when multi-tenant mode
/// is enabled.  Handlers read this via `req.extensions().get::<TenantId>()`.
#[derive(Clone, Debug)]
pub struct TenantId(pub String);

#[derive(Clone)]
pub struct AuthState {
    pub api_keys: Vec<String>,
    /// key_hash → tenant_id mapping; populated only in multi-tenant mode.
    pub tenant_map: Arc<std::collections::HashMap<String, String>>,
    pub multi_tenant: bool,
}

/// SHA-256 hex digest of a raw API key — used as the lookup key in tenant_map.
pub fn hash_api_key(key: &str) -> String {
    let mut h = Sha256::new();
    h.update(key.as_bytes());
    format!("{:x}", h.finalize())
}

pub async fn auth_middleware(
    State(state): State<Arc<AuthState>>,
    mut req: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let path = req.uri().path();

    // Exclude /health and /healthz/*
    if path == "/health" || path.starts_with("/healthz/") {
        return Ok(next.run(req).await);
    }

    if !state.api_keys.is_empty() {
        let auth_header = req
            .headers()
            .get("Authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));

        let api_key_header = req.headers().get("X-Api-Key").and_then(|h| h.to_str().ok());

        let provided_key = auth_header.or(api_key_header);

        // Use constant-time comparison to prevent timing attacks
        let is_valid = provided_key.map_or(false, |key| {
            state.api_keys.iter().any(|expected| {
                let provided_bytes = key.as_bytes();
                let expected_bytes = expected.as_bytes();
                provided_bytes.ct_eq(expected_bytes).into()
            })
        });

        if !is_valid {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "unauthorized" })),
            ));
        }

        // In multi-tenant mode, resolve and inject the tenant_id extension.
        if state.multi_tenant {
            let key = provided_key.unwrap_or("");
            let key_hash = hash_api_key(key);
            match state.tenant_map.get(&key_hash) {
                Some(tid) => {
                    req.extensions_mut().insert(TenantId(tid.clone()));
                }
                None => {
                    return Err((
                        StatusCode::FORBIDDEN,
                        Json(serde_json::json!({
                            "error": "api key is not associated with a tenant"
                        })),
                    ));
                }
            }
        }
    }

    Ok(next.run(req).await)
}

pub async fn security_headers_middleware(req: Request, next: Next) -> Response {
    let path = req.uri().path().to_owned();
    let mut response = next.run(req).await;

    // Set standard security headers for all responses
    response
        .headers_mut()
        .insert("X-Content-Type-Options", "nosniff".parse().unwrap());

    response
        .headers_mut()
        .insert("X-Frame-Options", "DENY".parse().unwrap());

    response.headers_mut().insert(
        "Referrer-Policy",
        "strict-origin-when-cross-origin".parse().unwrap(),
    );

    // Set CSP header with different policies for /docs vs other routes
    let csp = if path == "/docs" {
        // For Swagger UI, allow unpkg.com for scripts and styles
        "default-src 'self'; script-src 'self' https://unpkg.com; style-src 'self' https://unpkg.com; img-src 'self' data:; connect-src 'self';"
    } else {
        // Restrictive CSP for all other routes
        "default-src 'self';"
    };

    response
        .headers_mut()
        .insert("Content-Security-Policy", csp.parse().unwrap());

    response
}

pub async fn cache_middleware(req: Request, next: Next) -> Response {
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();

    let mut response = next.run(req).await;

    // Add cache headers based on endpoint
    let cache_control = if path.ends_with("/tx/") || (path.contains("/tx/") && !path.contains("?"))
    {
        // GET /v1/events/tx/:hash - immutable, cache for 1 hour
        "public, max-age=3600, immutable"
    } else if path == "/v1/events" || path == "/events" {
        // GET /v1/events - check for filters
        if query.contains("to_ledger") {
            // With to_ledger filter - cache for 60 seconds
            "public, max-age=60"
        } else {
            // No filters - cache for 5 seconds with stale-while-revalidate
            "public, max-age=5, stale-while-revalidate=10"
        }
    } else if path.contains("/contract/") {
        // GET /v1/events/contract/:id - cache for 5 seconds with stale-while-revalidate
        "public, max-age=5, stale-while-revalidate=10"
    } else {
        // Default - no caching
        return response;
    };

    response.headers_mut().insert(
        "Cache-Control",
        cache_control
            .parse()
            .unwrap_or_else(|_| "no-cache".parse().unwrap()),
    );

    // Add ETag header based on response body hash
    let (mut parts, body) = response.into_parts();
    parts.headers.insert(
        "Cache-Control",
        cache_control
            .parse()
            .unwrap_or_else(|_| "no-cache".parse().unwrap()),
    );
    if let Ok(body_bytes) = axum::body::to_bytes(body, usize::MAX).await {
        let mut hasher = Sha256::new();
        hasher.update(&body_bytes);
        let hash = format!("{:x}", hasher.finalize());
        let etag = format!("\"{}\"", &hash[..16]);
        parts.headers.insert(
            "ETag",
            etag.parse()
                .unwrap_or_else(|_| "\"unknown\"".parse().unwrap()),
        );
        Response::from_parts(parts, axum::body::Body::from(body_bytes))
    } else {
        Response::from_parts(parts, axum::body::Body::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{HeaderValue, Request, StatusCode};
    use axum::response::Response;
    use axum::{routing::get, Router};
    use tower::ServiceExt;

    async fn setup_app(api_keys: Vec<String>) -> Router {
        let auth_state = Arc::new(AuthState {
            api_keys,
            tenant_map: Arc::new(std::collections::HashMap::new()),
            multi_tenant: false,
        });
        Router::new()
            .route("/test", get(|| async { "OK" }))
            .route("/health", get(|| async { "OK" }))
            .route("/healthz/live", get(|| async { "OK" }))
            .route_layer(axum::middleware::from_fn_with_state(
                auth_state,
                auth_middleware,
            ))
    }

    #[tokio::test]
    async fn test_auth_bypassed_when_no_key_configured() {
        let app = setup_app(vec![]).await;

        let response: Response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_success_with_bearer_token() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("Authorization", "Bearer secret123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_success_with_x_api_key() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("X-Api-Key", "secret123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_failure_with_invalid_key() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("Authorization", "Bearer wrongkey")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_failure_with_missing_key() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_success_with_secondary_key() {
        let app = setup_app(vec!["primary".to_string(), "secondary".to_string()]).await;

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("Authorization", "Bearer secondary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_failure_with_correct_prefix_wrong_suffix() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("Authorization", "Bearer secret999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_health_endpoints_bypass_auth() {
        let app = setup_app(vec!["secret123".to_string()]).await;

        let response: Response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn setup_security_test_app() -> Router {
        Router::new()
            .route("/test", get(|| async { "OK" }))
            .route("/docs", get(|| async { "Swagger UI" }))
            .layer(axum::middleware::from_fn(security_headers_middleware))
    }

    #[tokio::test]
    async fn test_security_headers_on_regular_route() {
        let app = setup_security_test_app().await;

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify X-Content-Type-Options header
        assert_eq!(
            response.headers().get("X-Content-Type-Options"),
            Some(&HeaderValue::from_static("nosniff"))
        );

        // Verify X-Frame-Options header
        assert_eq!(
            response.headers().get("X-Frame-Options"),
            Some(&HeaderValue::from_static("DENY"))
        );

        // Verify Referrer-Policy header
        assert_eq!(
            response.headers().get("Referrer-Policy"),
            Some(&HeaderValue::from_static("strict-origin-when-cross-origin"))
        );

        // Verify restrictive CSP header for regular routes
        assert_eq!(
            response.headers().get("Content-Security-Policy"),
            Some(&HeaderValue::from_static("default-src 'self';"))
        );
    }

    #[tokio::test]
    async fn test_security_headers_on_docs_route() {
        let app = setup_security_test_app().await;

        let response = app
            .oneshot(Request::builder().uri("/docs").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify standard security headers are still present
        assert_eq!(
            response.headers().get("X-Content-Type-Options"),
            Some(&HeaderValue::from_static("nosniff"))
        );

        assert_eq!(
            response.headers().get("X-Frame-Options"),
            Some(&HeaderValue::from_static("DENY"))
        );

        assert_eq!(
            response.headers().get("Referrer-Policy"),
            Some(&HeaderValue::from_static("strict-origin-when-cross-origin"))
        );

        // Verify permissive CSP header for /docs route that allows unpkg.com
        let expected_csp = "default-src 'self'; script-src 'self' https://unpkg.com; style-src 'self' https://unpkg.com; img-src 'self' data:; connect-src 'self';";
        assert_eq!(
            response.headers().get("Content-Security-Policy"),
            Some(&HeaderValue::from_static(expected_csp))
        );
    }

    #[tokio::test]
    async fn test_all_security_headers_present() {
        let app = setup_security_test_app().await;

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let headers = response.headers();

        // Verify all required security headers are present
        assert!(headers.contains_key("X-Content-Type-Options"));
        assert!(headers.contains_key("X-Frame-Options"));
        assert!(headers.contains_key("Referrer-Policy"));
        assert!(headers.contains_key("Content-Security-Policy"));

        // Verify header values are not empty
        assert!(!headers.get("X-Content-Type-Options").unwrap().is_empty());
        assert!(!headers.get("X-Frame-Options").unwrap().is_empty());
        assert!(!headers.get("Referrer-Policy").unwrap().is_empty());
        assert!(!headers.get("Content-Security-Policy").unwrap().is_empty());
    }
}
