use std::env;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

/// Load an optional TOML config file. Returns an empty table if the file is
/// absent or CONFIG_FILE is not set — never an error.
fn load_config_file() -> toml::Table {
    let path = match env::var("CONFIG_FILE") {
        Ok(p) if !p.is_empty() => p,
        _ => return toml::Table::new(),
    };
    match std::fs::read_to_string(&path) {
        Ok(contents) => contents.parse::<toml::Table>().unwrap_or_else(|e| {
            eprintln!("Warning: failed to parse config file '{path}': {e}");
            toml::Table::new()
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => toml::Table::new(),
        Err(e) => {
            eprintln!("Warning: could not read config file '{path}': {e}");
            toml::Table::new()
        }
    }
}

/// Return the env var value if set, otherwise fall back to the TOML table.
fn env_or_file(key: &str, file: &toml::Table) -> Option<String> {
    env::var(key).ok().filter(|v| !v.is_empty()).or_else(|| {
        file.get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    })
}

/// Like `env_or_file` but returns a default string when neither source has the key.
fn env_or_file_or(key: &str, file: &toml::Table, default: &str) -> String {
    env_or_file(key, file).unwrap_or_else(|| default.to_string())
}

/// Shared operational state updated by the indexer and read by the /status handler.
pub struct IndexerState {
    pub current_ledger: AtomicU64,
    pub latest_ledger: AtomicU64,
    /// True when this replica holds the advisory lock and is actively indexing.
    pub is_active_indexer: AtomicBool,
    /// True when the indexer loop has been paused via the admin API.
    pub is_paused: AtomicBool,
    started_at: u64,
}

impl IndexerState {
    pub fn new() -> Self {
        let started_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            current_ledger: AtomicU64::new(0),
            latest_ledger: AtomicU64::new(0),
            is_active_indexer: AtomicBool::new(false),
            is_paused: AtomicBool::new(false),
            started_at,
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(self.started_at)
    }
}

/// Deployment environment — controls strictness of defaults.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Environment {
    Development,
    Staging,
    Production,
}

impl Environment {
    fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "production" | "prod" => Self::Production,
            "staging" | "stage" => Self::Staging,
            _ => Self::Development,
        }
    }

    /// Returns `true` for staging and production.
    pub fn is_production_like(&self) -> bool {
        matches!(self, Self::Staging | Self::Production)
    }
}

/// Shared state for health checks, accessible between the indexer and HTTP handlers
#[derive(Clone)]
pub struct HealthState {
    /// Unix timestamp of the last successful indexer poll
    pub last_indexer_poll: Arc<AtomicU64>,
    /// Timeout in seconds after which the indexer is considered stalled
    pub indexer_stall_timeout_secs: u64,
}

impl HealthState {
    pub fn new(indexer_stall_timeout_secs: u64) -> Self {
        Self {
            last_indexer_poll: Arc::new(AtomicU64::new(0)),
            indexer_stall_timeout_secs,
        }
    }

    /// Update the last poll timestamp to the current time
    pub fn update_last_poll(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_indexer_poll.store(now, Ordering::SeqCst);
    }

    /// Check if the indexer is stalled (no successful poll within the timeout)
    /// Returns Some(seconds_ago) if stalled, None if OK
    pub fn is_indexer_stalled(&self) -> Option<u64> {
        let last_poll = self.last_indexer_poll.load(Ordering::SeqCst);
        if last_poll == 0 {
            // No poll ever completed
            return Some(0);
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let elapsed = now.saturating_sub(last_poll);
        if elapsed > self.indexer_stall_timeout_secs {
            Some(elapsed)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    /// Optional read replica URL. When set, HTTP handlers use this pool; indexer uses primary.
    pub database_replica_url: Option<String>,
    pub stellar_rpc_url: String,
    /// Custom headers to inject into every RPC request (name, value). Values are never logged.
    pub rpc_headers: Vec<(String, String)>,
    pub start_ledger: u64,
    pub start_ledger_fallback: bool,
    pub port: u16,
    pub api_keys: Vec<String>,
    pub db_max_connections: u32,
    pub db_min_connections: u32,
    pub db_idle_timeout_secs: u64,
    pub db_max_lifetime_secs: u64,
    pub db_test_before_acquire: bool,
    pub behind_proxy: bool,
    pub rpc_connect_timeout_secs: u64,
    pub rpc_request_timeout_secs: u64,
    pub allowed_origins: Vec<String>,
    pub rate_limit_per_minute: u32,
    pub indexer_lag_warn_threshold: u64,
    pub indexer_stall_timeout_secs: u64,
    pub db_statement_timeout_ms: u64,
    pub indexer_poll_interval_ms: u64,
    pub indexer_error_backoff_ms: u64,
    pub sse_keepalive_interval_ms: u64,
    pub sse_max_connections: usize,
    pub environment: Environment,
    pub max_body_size_bytes: usize,
    pub log_sample_rate: u32,
    pub webhook_url: Option<String>,
    pub webhook_secret: Option<String>,
    pub webhook_contract_filter: Vec<String>,
    /// Event types to index (empty = all types)
    pub indexer_event_types: Vec<String>,
    /// AES-GCM encryption key for event_data (32 bytes, hex-encoded)
    pub event_data_encryption_key: Option<[u8; 32]>,
    /// Previous encryption key for rotation
    pub event_data_encryption_key_old: Option<[u8; 32]>,
    /// How often the index usage monitor runs (hours)
    pub index_check_interval_hours: u64,
    /// Health check timeout in milliseconds
    pub health_check_timeout_ms: u64,
    /// TLS certificate file path
    pub tls_cert_file: Option<String>,
    /// TLS key file path
    pub tls_key_file: Option<String>,
    // Issue #266: Bloom filter deduplication
    pub bloom_filter_fp_rate: f64,
    pub bloom_filter_capacity: usize,
    // Issue #265: AWS Kinesis streaming
    pub kinesis_stream_name: Option<String>,
    pub aws_region: Option<String>,
    // Issue #264: GCP Pub/Sub streaming
    pub pubsub_project_id: Option<String>,
    pub pubsub_topic_id: Option<String>,
    /// Maximum allowed size of serialized event_data in bytes.
    pub max_event_data_bytes: usize,
    /// Maximum rows for CSV/JSON export.
    pub export_max_rows: u64,
    /// Maximum number of entries in the contract count cache.
    pub contract_count_cache_size: u64,
    /// TTL in seconds for contract count cache entries.
    pub contract_count_cache_ttl_secs: u64,
    /// How often standby replicas retry the advisory lock (seconds).
    pub indexer_lock_retry_secs: u64,
    /// Enable multi-tenant mode. When true, every API key must be mapped to a
    /// tenant_id in the api_key_tenants table and all queries are scoped to that
    /// tenant.  Disabled by default to preserve single-tenant behaviour.
    pub multi_tenant: bool,
    /// Per-tenant contract filter for the indexer.
    /// Format: "tenant1:CABC...,CDEF...;tenant2:CXYZ..."
    /// When set, the indexer only stores events whose contract_id appears in the
    /// allow-list for the matching tenant.  Empty = index all contracts.
    pub tenant_contract_filter: std::collections::HashMap<String, Vec<String>>,
    /// The tenant_id this indexer instance stamps on every event it inserts.
    /// Only meaningful when multi_tenant = true.  Set via INDEXER_TENANT_ID env var.
    pub indexer_tenant_id: Option<String>,
    // Email notification fields
    pub email_smtp_host: Option<String>,
    pub email_smtp_port: u16,
    pub email_smtp_user: Option<String>,
    pub email_smtp_password: Option<String>,
    pub email_from: Option<String>,
    pub email_to: Vec<String>,
    pub email_contract_filter: Vec<String>,
    // Redis stream fields
    pub redis_url: Option<String>,
    pub redis_stream_key: Option<String>,
    // Stats refresh
    pub stats_refresh_interval_secs: u64,
    // Issue #327: Event retention and pruning
    pub retention_days: u64,
    pub pruning_interval_hours: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost/soroban_pulse".to_string(),
            database_replica_url: None,
            stellar_rpc_url: "https://soroban-testnet.stellar.org".to_string(),
            rpc_headers: Vec::new(),
            start_ledger: 0,
            start_ledger_fallback: false,
            port: 3000,
            api_keys: Vec::new(),
            db_max_connections: 10,
            db_min_connections: 2,
            db_idle_timeout_secs: 600,
            db_max_lifetime_secs: 1800,
            db_test_before_acquire: true,
            behind_proxy: false,
            rpc_connect_timeout_secs: 5,
            rpc_request_timeout_secs: 30,
            allowed_origins: vec!["*".to_string()],
            rate_limit_per_minute: 60,
            indexer_lag_warn_threshold: 100,
            indexer_stall_timeout_secs: 60,
            db_statement_timeout_ms: 5000,
            indexer_poll_interval_ms: 5000,
            indexer_error_backoff_ms: 10000,
            sse_keepalive_interval_ms: 15000,
            sse_max_connections: 1000,
            environment: Environment::Development,
            max_body_size_bytes: 1024 * 1024, // 1 MB default
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
            bloom_filter_capacity: 1_000_000,
            kinesis_stream_name: None,
            aws_region: None,
            pubsub_project_id: None,
            pubsub_topic_id: None,
            max_event_data_bytes: 65536,
            export_max_rows: 10_000,
            contract_count_cache_size: 1000,
            contract_count_cache_ttl_secs: 30,
            indexer_lock_retry_secs: 30,
            multi_tenant: false,
            tenant_contract_filter: std::collections::HashMap::new(),
            indexer_tenant_id: None,
            email_smtp_host: None,
            email_smtp_port: 587,
            email_smtp_user: None,
            email_smtp_password: None,
            email_from: None,
            email_to: Vec::new(),
            email_contract_filter: Vec::new(),
            redis_url: None,
            redis_stream_key: None,
            stats_refresh_interval_secs: 3600,
            retention_days: 90,
            pruning_interval_hours: 24,
        }
    }
}

fn validate_rpc_url_checked(raw: &str, errors: &mut Vec<String>) -> Option<String> {
    let url = match Url::parse(raw) {
        Ok(u) => u,
        Err(e) => {
            errors.push(format!(
                "  STELLAR_RPC_URL={raw:?} is not a valid URL: {e}. \
                 Expected a valid HTTPS URL (e.g., https://soroban-testnet.stellar.org)."
            ));
            return None;
        }
    };

    let allow_insecure = env::var("ALLOW_INSECURE_RPC")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
        .unwrap_or(false);

    match url.scheme() {
        "https" => {}
        "http" if allow_insecure => {}
        "http" => {
            errors.push(format!(
                "  STELLAR_RPC_URL={raw:?} uses http. \
                 Set ALLOW_INSECURE_RPC=true to permit insecure connections, \
                 or use an https URL (e.g., https://soroban-testnet.stellar.org)."
            ));
            return None;
        }
        scheme => {
            errors.push(format!(
                "  STELLAR_RPC_URL={raw:?} has disallowed scheme '{scheme}'. \
                 Only https is permitted (e.g., https://soroban-testnet.stellar.org)."
            ));
            return None;
        }
    }

    if !allow_insecure {
        let host = url.host_str().unwrap_or("");
        let is_loopback =
            host == "localhost" || host == "127.0.0.1" || host == "::1" || host.ends_with(".local");
        let is_private = host.starts_with("10.")
            || host.starts_with("192.168.")
            || host.starts_with("169.254.")
            || (host.starts_with("172.") && {
                host.split('.')
                    .nth(1)
                    .and_then(|o| o.parse::<u8>().ok())
                    .map(|o| (16..=31).contains(&o))
                    .unwrap_or(false)
            });
        if is_loopback || is_private {
            errors.push(format!(
                "  STELLAR_RPC_URL={raw:?} points to a non-routable host '{host}'. \
                 Set ALLOW_INSECURE_RPC=true to allow this in development."
            ));
            return None;
        }
    }

    let mut safe = url.clone();
    let _ = safe.set_username("");
    let _ = safe.set_password(None);
    Some(safe.to_string())
}

/// Read DATABASE_URL from DATABASE_URL_FILE if set, otherwise fall back to DATABASE_URL.
fn resolve_database_url_checked(errors: &mut Vec<String>) -> String {
    if let Ok(file_path) = env::var("DATABASE_URL_FILE") {
        match std::fs::read_to_string(&file_path) {
            Ok(contents) => return contents.trim().to_string(),
            Err(e) => {
                errors.push(format!(
                    "  DATABASE_URL_FILE={file_path:?} could not be read: {e}. \
                     Ensure the file exists and is readable."
                ));
                return String::new();
            }
        }
    }
    match env::var("DATABASE_URL") {
        Ok(v) if !v.is_empty() => v,
        _ => {
            errors.push(
                "  DATABASE_URL is not set. \
                 Expected a PostgreSQL connection string \
                 (e.g., postgres://user:pass@localhost/soroban_pulse)."
                    .to_string(),
            );
            String::new()
        }
    }
}

/// Parse STELLAR_RPC_HEADERS: semicolon-separated "Name: Value" pairs.
fn parse_rpc_headers_checked(errors: &mut Vec<String>) -> Vec<(String, String)> {
    let raw = match env::var("STELLAR_RPC_HEADERS") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return Vec::new(),
    };
    let mut headers = Vec::new();
    for pair in raw.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        match pair.split_once(':') {
            Some((name, value)) => {
                let name = name.trim().to_string();
                let value = value.trim().to_string();
                if name.is_empty() {
                    errors.push(format!(
                        "  STELLAR_RPC_HEADERS: header name is empty in {pair:?}. \
                         Expected 'Name: Value' format (e.g., X-API-Key: mykey)."
                    ));
                } else {
                    headers.push((name, value));
                }
            }
            None => {
                errors.push(format!(
                    "  STELLAR_RPC_HEADERS: invalid entry {pair:?}. \
                     Expected 'Name: Value' format (e.g., X-API-Key: mykey)."
                ));
            }
        }
    }
    headers
}

/// Parse INDEXER_EVENT_TYPES: comma-separated list of event types.
fn parse_indexer_event_types_checked(errors: &mut Vec<String>) -> Vec<String> {
    let raw = match env::var("INDEXER_EVENT_TYPES") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return Vec::new(),
    };
    let valid = ["contract", "diagnostic", "system"];
    let mut types = Vec::new();
    for t in raw
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
    {
        if valid.contains(&t.as_str()) {
            types.push(t);
        } else {
            errors.push(format!(
                "  INDEXER_EVENT_TYPES={t:?} is not a valid event type. \
                 Valid values are: contract, diagnostic, system."
            ));
        }
    }
    types
}

/// Parse a 64-hex-char string into a 32-byte key, collecting errors instead of panicking.
fn parse_hex_key_checked(var: &str, value: &str, errors: &mut Vec<String>) -> Option<[u8; 32]> {
    if value.len() != 64 {
        errors.push(format!(
            "  {var} must be exactly 64 hex characters (32 bytes), got {} chars.",
            value.len()
        ));
        return None;
    }
    let mut key = [0u8; 32];
    for (i, chunk) in value.as_bytes().chunks(2).enumerate() {
        let hex = match std::str::from_utf8(chunk) {
            Ok(s) => s,
            Err(_) => {
                errors.push(format!("  {var} contains invalid UTF-8 at byte {i}."));
                return None;
            }
        };
        match u8::from_str_radix(hex, 16) {
            Ok(b) => key[i] = b,
            Err(_) => {
                errors.push(format!(
                    "  {var} contains non-hex character at byte {i} ({hex:?}). \
                     Expected a 64-character lowercase hex string."
                ));
                return None;
            }
        }
    }
    Some(key)
}

/// Parse a required integer env var, pushing a descriptive error on failure.
fn parse_int<T>(var: &str, raw: &str, example: &str, errors: &mut Vec<String>) -> Option<T>
where
    T: std::str::FromStr,
{
    raw.parse::<T>().ok().or_else(|| {
        errors.push(format!(
            "  {var}={raw:?} is not a valid integer. Expected a positive integer (e.g., {example})."
        ));
        None
    })
}

/// Parse a required bool env var ("true"/"false"), pushing a descriptive error on failure.
fn parse_bool(var: &str, raw: &str, errors: &mut Vec<String>) -> Option<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Some(true),
        "false" | "0" | "no" | "n" => Some(false),
        _ => {
            errors.push(format!(
                "  {var}={raw:?} is not a valid boolean. Expected true or false (e.g., true)."
            ));
            None
        }
    }
}

/// Parse an integer that must fall within [min, max], pushing a descriptive error on failure.
fn parse_int_range<T>(
    var: &str,
    raw: &str,
    min: T,
    max: T,
    example: &str,
    errors: &mut Vec<String>,
) -> Option<T>
where
    T: std::str::FromStr + PartialOrd + std::fmt::Display,
{
    match raw.parse::<T>() {
        Ok(v) if v >= min && v <= max => Some(v),
        Ok(v) => {
            errors.push(format!(
                "  {var}={v} is out of range. Expected a value between {min} and {max} (e.g., {example})."
            ));
            None
        }
        Err(_) => {
            errors.push(format!(
                "  {var}={raw:?} is not a valid integer. Expected a value between {min} and {max} (e.g., {example})."
            ));
            None
        }
    }
}

/// Parse TENANT_CONTRACT_FILTER env var.
///
/// Format: `"tenant1:CABC...,CDEF...;tenant2:CXYZ..."`
/// Each semicolon-separated segment is `tenant_id:contract1,contract2,...`.
/// Returns an empty map when the variable is unset or empty.
fn parse_tenant_contract_filter() -> std::collections::HashMap<String, Vec<String>> {
    let raw = match std::env::var("TENANT_CONTRACT_FILTER") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return std::collections::HashMap::new(),
    };
    let mut map = std::collections::HashMap::new();
    for segment in raw.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if let Some((tenant, contracts_str)) = segment.split_once(':') {
            let tenant = tenant.trim().to_string();
            let contracts: Vec<String> = contracts_str
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
            if !tenant.is_empty() && !contracts.is_empty() {
                map.insert(tenant, contracts);
            }
        }
    }
    map
}

impl Config {
    /// Returns the DATABASE_URL with credentials stripped — safe to log.
    pub fn safe_db_url(&self) -> String {
        Url::parse(&self.database_url)
            .map(|mut u| {
                let _ = u.set_username("");
                let _ = u.set_password(None);
                u.to_string()
            })
            .unwrap_or_else(|_| "<unparseable>".to_string())
    }

    /// Returns only header names (no values) — safe to log.
    pub fn safe_rpc_headers(&self) -> Vec<&str> {
        self.rpc_headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect()
    }

    pub fn from_env() -> Self {
        let file = load_config_file();
        let mut errors: Vec<String> = Vec::new();

        let environment =
            Environment::from_str(&env_or_file_or("ENVIRONMENT", &file, "development"));

        let behind_proxy = env_or_file("BEHIND_PROXY", &file)
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
            .unwrap_or(false);

        let start_ledger = parse_int::<u64>(
            "START_LEDGER",
            &env_or_file_or("START_LEDGER", &file, "0"),
            "0",
            &mut errors,
        )
        .unwrap_or(0);

        let start_ledger_fallback = env_or_file("START_LEDGER_FALLBACK", &file)
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
            .unwrap_or(false);

        let port = parse_int::<u16>(
            "PORT",
            &env_or_file_or("PORT", &file, "3000"),
            "3000",
            &mut errors,
        )
        .unwrap_or(3000);

        let allowed_origins: Vec<String> = env_or_file_or("ALLOWED_ORIGINS", &file, "*")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if environment.is_production_like() && allowed_origins.iter().any(|o| o == "*") {
            errors.push(format!(
                "  ALLOWED_ORIGINS=* is not permitted in {environment:?}. \
                 Set explicit origins (e.g., https://app.example.com) or use ENVIRONMENT=development."
            ));
        }

        let database_url = resolve_database_url_checked(&mut errors);

        let stellar_rpc_url = {
            let raw = env_or_file_or(
                "STELLAR_RPC_URL",
                &file,
                "https://soroban-testnet.stellar.org",
            );
            validate_rpc_url_checked(&raw, &mut errors).unwrap_or_else(|| raw.clone())
        };

        let db_max_connections = parse_int::<u32>(
            "DB_MAX_CONNECTIONS",
            &env_or_file_or("DB_MAX_CONNECTIONS", &file, "10"),
            "10",
            &mut errors,
        )
        .unwrap_or(10);

        let db_min_connections = parse_int::<u32>(
            "DB_MIN_CONNECTIONS",
            &env_or_file_or("DB_MIN_CONNECTIONS", &file, "2"),
            "2",
            &mut errors,
        )
        .unwrap_or(2);

        let db_idle_timeout_secs = parse_int::<u64>(
            "DB_IDLE_TIMEOUT_SECS",
            &env_or_file_or("DB_IDLE_TIMEOUT_SECS", &file, "600"),
            "600",
            &mut errors,
        )
        .unwrap_or(600);

        let db_max_lifetime_secs = parse_int::<u64>(
            "DB_MAX_LIFETIME_SECS",
            &env_or_file_or("DB_MAX_LIFETIME_SECS", &file, "1800"),
            "1800",
            &mut errors,
        )
        .unwrap_or(1800);

        let db_test_before_acquire = parse_bool(
            "DB_TEST_BEFORE_ACQUIRE",
            &env_or_file_or("DB_TEST_BEFORE_ACQUIRE", &file, "true"),
            &mut errors,
        )
        .unwrap_or(true);

        let rpc_connect_timeout_secs = parse_int::<u64>(
            "RPC_CONNECT_TIMEOUT_SECS",
            &env_or_file_or("RPC_CONNECT_TIMEOUT_SECS", &file, "5"),
            "5",
            &mut errors,
        )
        .unwrap_or(5);

        let rpc_request_timeout_secs = parse_int::<u64>(
            "RPC_REQUEST_TIMEOUT_SECS",
            &env_or_file_or("RPC_REQUEST_TIMEOUT_SECS", &file, "30"),
            "30",
            &mut errors,
        )
        .unwrap_or(30);

        let rate_limit_per_minute = parse_int::<u32>(
            "RATE_LIMIT_PER_MINUTE",
            &env_or_file_or("RATE_LIMIT_PER_MINUTE", &file, "60"),
            "60",
            &mut errors,
        )
        .unwrap_or(60);

        let indexer_lag_warn_threshold = parse_int::<u64>(
            "INDEXER_LAG_WARN_THRESHOLD",
            &env_or_file_or("INDEXER_LAG_WARN_THRESHOLD", &file, "100"),
            "100",
            &mut errors,
        )
        .unwrap_or(100);

        let indexer_stall_timeout_secs = parse_int::<u64>(
            "INDEXER_STALL_TIMEOUT_SECS",
            &env_or_file_or("INDEXER_STALL_TIMEOUT_SECS", &file, "60"),
            "60",
            &mut errors,
        )
        .unwrap_or(60);

        let db_statement_timeout_ms = parse_int::<u64>(
            "DB_STATEMENT_TIMEOUT_MS",
            &env_or_file_or("DB_STATEMENT_TIMEOUT_MS", &file, "5000"),
            "5000",
            &mut errors,
        )
        .unwrap_or(5000);

        let indexer_poll_interval_ms = parse_int_range::<u64>(
            "INDEXER_POLL_INTERVAL_MS",
            &env_or_file_or("INDEXER_POLL_INTERVAL_MS", &file, "5000"),
            100,
            60000,
            "5000",
            &mut errors,
        )
        .unwrap_or(5000);

        let indexer_error_backoff_ms = parse_int_range::<u64>(
            "INDEXER_ERROR_BACKOFF_MS",
            &env_or_file_or("INDEXER_ERROR_BACKOFF_MS", &file, "10000"),
            100,
            60000,
            "10000",
            &mut errors,
        )
        .unwrap_or(10000);

        // SSE_KEEPALIVE_SECS (seconds) takes precedence over SSE_KEEPALIVE_INTERVAL_MS (ms).
        let sse_keepalive_interval_ms = if let Some(secs_str) = env_or_file("SSE_KEEPALIVE_SECS", &file) {
            parse_int_range::<u64>("SSE_KEEPALIVE_SECS", &secs_str, 1, 60, "15", &mut errors)
                .map(|s| s * 1000)
                .unwrap_or(15000)
        } else {
            parse_int_range::<u64>(
                "SSE_KEEPALIVE_INTERVAL_MS",
                &env_or_file_or("SSE_KEEPALIVE_INTERVAL_MS", &file, "15000"),
                1000,
                60000,
                "15000",
                &mut errors,
            )
            .unwrap_or(15000)
        };

        let sse_max_connections = parse_int_range::<usize>(
            "SSE_MAX_CONNECTIONS",
            &env_or_file_or("SSE_MAX_CONNECTIONS", &file, "1000"),
            1,
            usize::MAX,
            "1000",
            &mut errors,
        )
        .unwrap_or(1000);

        let max_body_size_bytes = parse_int::<usize>(
            "MAX_BODY_SIZE_BYTES",
            &env_or_file_or("MAX_BODY_SIZE_BYTES", &file, "1048576"),
            "1048576",
            &mut errors,
        )
        .unwrap_or(1024 * 1024);

        let log_sample_rate = parse_int_range::<u32>(
            "LOG_SAMPLE_RATE",
            &env_or_file_or("LOG_SAMPLE_RATE", &file, "1"),
            1,
            u32::MAX,
            "1",
            &mut errors,
        )
        .unwrap_or(1);

        let contract_count_cache_size = parse_int::<u64>(
            "CONTRACT_COUNT_CACHE_SIZE",
            &env_or_file_or("CONTRACT_COUNT_CACHE_SIZE", &file, "1000"),
            "1000",
            &mut errors,
        )
        .unwrap_or(1000);

        let contract_count_cache_ttl_secs = parse_int::<u64>(
            "CONTRACT_COUNT_CACHE_TTL_SECS",
            &env_or_file_or("CONTRACT_COUNT_CACHE_TTL_SECS", &file, "30"),
            "30",
            &mut errors,
        )
        .unwrap_or(30);

        let indexer_lock_retry_secs = parse_int::<u64>(
            "INDEXER_LOCK_RETRY_SECS",
            &env_or_file_or("INDEXER_LOCK_RETRY_SECS", &file, "30"),
            "30",
            &mut errors,
        )
        .unwrap_or(30);

        let export_max_rows = parse_int::<u64>(
            "EXPORT_MAX_ROWS",
            &env_or_file_or("EXPORT_MAX_ROWS", &file, "10000"),
            "10000",
            &mut errors,
        )
        .unwrap_or(10_000);

        let health_check_timeout_ms = parse_int::<u64>(
            "HEALTH_CHECK_TIMEOUT_MS",
            &env_or_file_or("HEALTH_CHECK_TIMEOUT_MS", &file, "2000"),
            "2000",
            &mut errors,
        )
        .unwrap_or(2000);

        let index_check_interval_hours = parse_int::<u64>(
            "INDEX_CHECK_INTERVAL_HOURS",
            &env_or_file_or("INDEX_CHECK_INTERVAL_HOURS", &file, "24"),
            "24",
            &mut errors,
        )
        .unwrap_or(24);

        let archive_after_days = parse_int::<u32>(
            "ARCHIVE_AFTER_DAYS",
            &env_or_file_or("ARCHIVE_AFTER_DAYS", &file, "30"),
            "30",
            &mut errors,
        )
        .unwrap_or(30);

        let event_data_encryption_key = env_or_file("EVENT_DATA_ENCRYPTION_KEY", &file)
            .and_then(|v| parse_hex_key_checked("EVENT_DATA_ENCRYPTION_KEY", &v, &mut errors));

        let event_data_encryption_key_old = env_or_file("EVENT_DATA_ENCRYPTION_KEY_OLD", &file)
            .and_then(|v| parse_hex_key_checked("EVENT_DATA_ENCRYPTION_KEY_OLD", &v, &mut errors));

        let rpc_headers = parse_rpc_headers_checked(&mut errors);
        let indexer_event_types = parse_indexer_event_types_checked(&mut errors);

        // Report all errors at once.
        if !errors.is_empty() {
            eprintln!(
                "Configuration errors ({} found):\n{}",
                errors.len(),
                errors.join("\n")
            );
            panic!("Startup aborted due to configuration errors. Fix the above and restart.");
        }

        Self {
            database_url,
            database_replica_url: env::var("DATABASE_REPLICA_URL")
                .ok()
                .filter(|s| !s.is_empty()),
            stellar_rpc_url,
            rpc_headers,
            start_ledger,
            start_ledger_fallback,
            port,
            api_keys: {
                let mut keys = Vec::new();
                if let Some(key) = env_or_file("API_KEY", &file) {
                    keys.push(key);
                }
                if let Some(key) = env_or_file("API_KEY_SECONDARY", &file) {
                    keys.push(key);
                }
                keys
            },
            db_max_connections,
            db_min_connections,
            db_idle_timeout_secs,
            db_max_lifetime_secs,
            db_test_before_acquire,
            behind_proxy,
            rpc_connect_timeout_secs,
            rpc_request_timeout_secs,
            allowed_origins,
            rate_limit_per_minute,
            indexer_lag_warn_threshold,
            indexer_stall_timeout_secs,
            db_statement_timeout_ms,
            indexer_poll_interval_ms,
            indexer_error_backoff_ms,
            sse_keepalive_interval_ms,
            sse_max_connections,
            environment,
            max_body_size_bytes,
            log_sample_rate,
            webhook_url: env::var("WEBHOOK_URL").ok().filter(|s| !s.is_empty()),
            webhook_secret: env::var("WEBHOOK_SECRET").ok().filter(|s| !s.is_empty()),
            webhook_contract_filter: env::var("WEBHOOK_CONTRACT_FILTER")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            indexer_event_types,
            event_data_encryption_key,
            event_data_encryption_key_old,
            index_check_interval_hours,
            health_check_timeout_ms,
            tls_cert_file: env::var("TLS_CERT_FILE").ok().filter(|s| !s.is_empty()),
            tls_key_file: env::var("TLS_KEY_FILE").ok().filter(|s| !s.is_empty()),
            bloom_filter_fp_rate: env_or_file_or("BLOOM_FILTER_FP_RATE", &file, "0.001")
                .parse()
                .unwrap_or(0.001),
            bloom_filter_capacity: env_or_file_or("BLOOM_FILTER_CAPACITY", &file, "1000000")
                .parse()
                .unwrap_or(1_000_000),
            kinesis_stream_name: env::var("KINESIS_STREAM_NAME").ok().filter(|s| !s.is_empty()),
            aws_region: env::var("AWS_REGION").ok().filter(|s| !s.is_empty()),
            pubsub_project_id: env::var("PUBSUB_PROJECT_ID").ok().filter(|s| !s.is_empty()),
            pubsub_topic_id: env::var("PUBSUB_TOPIC_ID").ok().filter(|s| !s.is_empty()),
            max_event_data_bytes: parse_int::<usize>(
                "MAX_EVENT_DATA_BYTES",
                &env_or_file_or("MAX_EVENT_DATA_BYTES", &file, "65536"),
                "65536",
                &mut errors,
            )
            .unwrap_or(65536),
            export_max_rows,
            contract_count_cache_size,
            contract_count_cache_ttl_secs,
            indexer_lock_retry_secs,
            multi_tenant: env_or_file("MULTI_TENANT", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(false),
            tenant_contract_filter: parse_tenant_contract_filter(),
            indexer_tenant_id: env_or_file("INDEXER_TENANT_ID", &file),
            email_smtp_host: env_or_file("EMAIL_SMTP_HOST", &file),
            email_smtp_port: env_or_file("EMAIL_SMTP_PORT", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(587),
            email_smtp_user: env_or_file("EMAIL_SMTP_USER", &file),
            email_smtp_password: env_or_file("EMAIL_SMTP_PASSWORD", &file),
            email_from: env_or_file("EMAIL_FROM", &file),
            email_to: env_or_file("EMAIL_TO", &file)
                .map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
                .unwrap_or_default(),
            email_contract_filter: env_or_file("EMAIL_CONTRACT_FILTER", &file)
                .map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
                .unwrap_or_default(),
            redis_url: env_or_file("REDIS_URL", &file),
            redis_stream_key: env_or_file("REDIS_STREAM_KEY", &file),
            stats_refresh_interval_secs: env_or_file("STATS_REFRESH_INTERVAL_SECS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(3600),
            retention_days: env_or_file("RETENTION_DAYS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(90),
            pruning_interval_hours: env_or_file("PRUNING_INTERVAL_HOURS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(24),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_environment_from_str() {
        assert_eq!(Environment::from_str("production"), Environment::Production);
        assert_eq!(Environment::from_str("prod"), Environment::Production);
        assert_eq!(Environment::from_str("staging"), Environment::Staging);
        assert_eq!(Environment::from_str("stage"), Environment::Staging);
        assert_eq!(
            Environment::from_str("development"),
            Environment::Development
        );
        assert_eq!(Environment::from_str("dev"), Environment::Development);
        assert_eq!(Environment::from_str("unknown"), Environment::Development);
    }

    #[test]
    fn test_environment_is_production_like() {
        assert!(!Environment::Development.is_production_like());
        assert!(Environment::Staging.is_production_like());
        assert!(Environment::Production.is_production_like());
    }

    #[test]
    fn test_indexer_state_new() {
        let state = IndexerState::new();
        assert_eq!(
            state
                .current_ledger
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            state
                .latest_ledger
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(state.started_at > 0);
    }

    #[test]
    fn test_indexer_state_uptime() {
        let state = IndexerState::new();
        let uptime1 = state.uptime_secs();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let uptime2 = state.uptime_secs();
        assert!(uptime2 >= uptime1);
    }

    #[test]
    fn test_health_state_new() {
        let health_state = HealthState::new(60);
        assert_eq!(health_state.indexer_stall_timeout_secs, 60);
        assert_eq!(
            health_state
                .last_indexer_poll
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[test]
    fn test_health_state_update_and_check() {
        let health_state = HealthState::new(60);
        assert_eq!(health_state.is_indexer_stalled(), Some(0));
        health_state.update_last_poll();
        assert_eq!(health_state.is_indexer_stalled(), None);
        let health_state_short = HealthState::new(0);
        health_state_short.update_last_poll();
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(health_state_short.is_indexer_stalled().is_some());
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.database_url, "postgres://localhost/soroban_pulse");
        assert_eq!(
            config.stellar_rpc_url,
            "https://soroban-testnet.stellar.org"
        );
        assert_eq!(config.port, 3000);
        assert_eq!(config.start_ledger, 0);
        assert!(!config.start_ledger_fallback);
        assert_eq!(config.environment, Environment::Development);
    }

    #[test]
    fn test_config_safe_db_url() {
        let mut config = Config::default();
        config.database_url = "postgres://user:password@localhost/db".to_string();
        let safe_url = config.safe_db_url();
        assert!(!safe_url.contains("password"));
        assert!(safe_url.contains("localhost"));
    }

    #[test]
    fn test_config_safe_db_url_unparseable() {
        let mut config = Config::default();
        config.database_url = "not-a-url".to_string();
        assert_eq!(config.safe_db_url(), "<unparseable>");
    }

    #[test]
    fn test_safe_rpc_headers_returns_names_only() {
        let mut config = Config::default();
        config.rpc_headers = vec![
            ("X-API-Key".to_string(), "secret".to_string()),
            ("Authorization".to_string(), "Bearer token".to_string()),
        ];
        let safe = config.safe_rpc_headers();
        assert_eq!(safe, vec!["X-API-Key", "Authorization"]);
    }

    #[test]
    fn startup_log_fields_do_not_contain_credentials() {
        let mut config = Config::default();
        config.database_url = "postgres://admin:supersecret@db.example.com/mydb".to_string();
        let safe_db = config.safe_db_url();
        assert!(!safe_db.contains("supersecret"));
        assert!(!safe_db.contains("admin"));
    }

    // --- parse_rpc_headers_checked ---

    #[test]
    fn test_parse_rpc_headers_empty() {
        env::remove_var("STELLAR_RPC_HEADERS");
        let mut errors = Vec::new();
        let headers = parse_rpc_headers_checked(&mut errors);
        assert!(headers.is_empty());
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_rpc_headers_single() {
        env::set_var("STELLAR_RPC_HEADERS", "X-API-Key: mykey");
        let mut errors = Vec::new();
        let headers = parse_rpc_headers_checked(&mut errors);
        env::remove_var("STELLAR_RPC_HEADERS");
        assert_eq!(
            headers,
            vec![("X-API-Key".to_string(), "mykey".to_string())]
        );
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_rpc_headers_multiple() {
        env::set_var("STELLAR_RPC_HEADERS", "X-API-Key: mykey; X-Custom: value");
        let mut errors = Vec::new();
        let headers = parse_rpc_headers_checked(&mut errors);
        env::remove_var("STELLAR_RPC_HEADERS");
        assert_eq!(headers.len(), 2);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_rpc_headers_invalid_format_collects_error() {
        env::set_var("STELLAR_RPC_HEADERS", "NoColonHere");
        let mut errors = Vec::new();
        let headers = parse_rpc_headers_checked(&mut errors);
        env::remove_var("STELLAR_RPC_HEADERS");
        assert!(headers.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("STELLAR_RPC_HEADERS"));
        assert!(errors[0].contains("Name: Value"));
    }

    // --- parse_indexer_event_types_checked ---

    #[test]
    fn test_parse_indexer_event_types_empty() {
        env::remove_var("INDEXER_EVENT_TYPES");
        let mut errors = Vec::new();
        assert!(parse_indexer_event_types_checked(&mut errors).is_empty());
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_indexer_event_types_single() {
        env::set_var("INDEXER_EVENT_TYPES", "contract");
        let mut errors = Vec::new();
        let types = parse_indexer_event_types_checked(&mut errors);
        env::remove_var("INDEXER_EVENT_TYPES");
        assert_eq!(types, vec!["contract"]);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_indexer_event_types_multiple() {
        env::set_var("INDEXER_EVENT_TYPES", "contract,diagnostic");
        let mut errors = Vec::new();
        let types = parse_indexer_event_types_checked(&mut errors);
        env::remove_var("INDEXER_EVENT_TYPES");
        assert_eq!(types, vec!["contract", "diagnostic"]);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_indexer_event_types_invalid_collects_error() {
        env::set_var("INDEXER_EVENT_TYPES", "contract,invalid");
        let mut errors = Vec::new();
        let types = parse_indexer_event_types_checked(&mut errors);
        env::remove_var("INDEXER_EVENT_TYPES");
        assert_eq!(types, vec!["contract"]);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("INDEXER_EVENT_TYPES"));
        assert!(errors[0].contains("invalid"));
    }

    // --- parse_int / parse_bool / parse_int_range ---

    #[test]
    fn parse_int_valid_returns_value() {
        let mut errors = Vec::new();
        assert_eq!(
            parse_int::<u32>("PORT", "3000", "3000", &mut errors),
            Some(3000)
        );
        assert!(errors.is_empty());
    }

    #[test]
    fn parse_int_invalid_collects_descriptive_error() {
        let mut errors = Vec::new();
        let result = parse_int::<u32>("DB_MAX_CONNECTIONS", "abc", "10", &mut errors);
        assert!(result.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("DB_MAX_CONNECTIONS"));
        assert!(errors[0].contains("\"abc\""));
        assert!(errors[0].contains("e.g., 10"));
    }

    #[test]
    fn parse_bool_valid_values() {
        let mut e = Vec::new();
        assert_eq!(parse_bool("X", "true", &mut e), Some(true));
        assert_eq!(parse_bool("X", "false", &mut e), Some(false));
        assert_eq!(parse_bool("X", "1", &mut e), Some(true));
        assert_eq!(parse_bool("X", "0", &mut e), Some(false));
        assert!(e.is_empty());
    }

    #[test]
    fn parse_bool_invalid_collects_descriptive_error() {
        let mut errors = Vec::new();
        let result = parse_bool("DB_TEST_BEFORE_ACQUIRE", "yes_please", &mut errors);
        assert!(result.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("DB_TEST_BEFORE_ACQUIRE"));
        assert!(errors[0].contains("\"yes_please\""));
        assert!(errors[0].contains("true or false"));
    }

    #[test]
    fn parse_int_range_out_of_range_collects_descriptive_error() {
        let mut errors = Vec::new();
        let result = parse_int_range::<u64>(
            "INDEXER_POLL_INTERVAL_MS",
            "50",
            100,
            60000,
            "5000",
            &mut errors,
        );
        assert!(result.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("INDEXER_POLL_INTERVAL_MS"));
        assert!(errors[0].contains("50"));
        assert!(errors[0].contains("100"));
        assert!(errors[0].contains("60000"));
    }

    // --- parse_hex_key_checked ---

    #[test]
    fn parse_hex_key_valid() {
        let mut errors = Vec::new();
        let key = parse_hex_key_checked("MY_KEY", &"ab".repeat(32), &mut errors);
        assert!(key.is_some());
        assert!(errors.is_empty());
    }

    #[test]
    fn parse_hex_key_wrong_length_collects_error() {
        let mut errors = Vec::new();
        let key = parse_hex_key_checked("MY_KEY", "tooshort", &mut errors);
        assert!(key.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("MY_KEY"));
        assert!(errors[0].contains("64 hex characters"));
    }

    #[test]
    fn parse_hex_key_non_hex_collects_error() {
        let mut errors = Vec::new();
        let key = parse_hex_key_checked("MY_KEY", &"zz".repeat(32), &mut errors);
        assert!(key.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("MY_KEY"));
        assert!(errors[0].contains("non-hex"));
    }

    // --- multiple errors collected together ---

    #[test]
    fn multiple_invalid_vars_all_reported() {
        let mut errors = Vec::new();
        parse_int::<u32>("PORT", "bad", "3000", &mut errors);
        parse_int::<u32>("DB_MAX_CONNECTIONS", "also_bad", "10", &mut errors);
        parse_bool("DB_TEST_BEFORE_ACQUIRE", "nope", &mut errors);
        assert_eq!(errors.len(), 3);
        assert!(errors[0].contains("PORT"));
        assert!(errors[1].contains("DB_MAX_CONNECTIONS"));
        assert!(errors[2].contains("DB_TEST_BEFORE_ACQUIRE"));
    }

    // --- resolve_database_url_checked ---

    #[test]
    fn missing_database_url_collects_error() {
        env::remove_var("DATABASE_URL");
        env::remove_var("DATABASE_URL_FILE");
        let mut errors = Vec::new();
        let url = resolve_database_url_checked(&mut errors);
        assert!(url.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("DATABASE_URL"));
        assert!(errors[0].contains("postgres://"));
    }

    // --- config file helpers ---

    #[test]
    fn config_file_provides_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "PORT = \"9999\"\n").unwrap();
        env::remove_var("PORT");
        env::set_var("CONFIG_FILE", path.to_str().unwrap());
        let file = load_config_file();
        let port = env_or_file_or("PORT", &file, "3000");
        assert_eq!(port, "9999");
        env::remove_var("CONFIG_FILE");
    }

    #[test]
    fn env_var_takes_precedence_over_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "PORT = \"9999\"\n").unwrap();
        env::set_var("PORT", "7777");
        env::set_var("CONFIG_FILE", path.to_str().unwrap());
        let file = load_config_file();
        let port = env_or_file_or("PORT", &file, "3000");
        assert_eq!(port, "7777");
        env::remove_var("PORT");
        env::remove_var("CONFIG_FILE");
    }

    #[test]
    fn missing_config_file_is_not_an_error() {
        env::set_var("CONFIG_FILE", "/nonexistent/path/config.toml");
        let file = load_config_file();
        assert!(file.is_empty());
        env::remove_var("CONFIG_FILE");
    }
}
