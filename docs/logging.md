# Structured Logging Convention

This document defines the canonical field names and patterns for structured logging across Soroban Pulse.

## Field Naming

### Error Handling
- **`error`**: Always use `error` (not `message`, `msg`, or `err`) for error values
  ```rust
  error!(error = %e, "Database error");
  warn!(error = %e, "RPC error");
  ```

### Request/Operation Context
- **`correlation_id`**: Unique identifier for tracing a request through the system
  ```rust
  error!(correlation_id = %correlation_id, error = %e, "Request failed");
  ```

### Soroban-Specific Context
- **`contract_id`**: Soroban contract identifier
- **`tx_hash`**: Transaction hash
- **`ledger`**: Ledger sequence number
- **`event_type`**: Event type (contract, diagnostic, system)
  ```rust
  info!(contract_id = %contract_id, ledger = ledger, "Event indexed");
  ```

### Operational Metrics
- **`attempt`**: Retry attempt number
- **`skipped`**: Number of items skipped
- **`count`**: Total count of items
  ```rust
  warn!(attempt = attempt, "DB connection failed, retrying...");
  warn!(skipped = n, "Subscriber lagged, some events skipped");
  ```

## Instrumentation Patterns

### Async Functions
Prefer `#[instrument]` macro over manual span creation:

```rust
#[instrument(skip(self, pool))]
async fn fetch_and_store_events(&mut self, pool: &PgPool) -> Result<()> {
    // Automatically creates a span with function name
    // Use skip() for large types that shouldn't be logged
}
```

### Manual Spans
Only use manual spans when `#[instrument]` is not applicable:

```rust
let span = tracing::info_span!("operation", contract_id = %contract_id);
let _guard = span.enter();
```

## Reserved Field Names

Do **not** use these as explicit field names — they are reserved by the tracing framework:
- `message` — implicit field for the log message string
- `target` — implicit field for the module path
- `level` — implicit field for the log level

## JSON Output

When `RUST_LOG_FORMAT=json`, all structured fields appear in the JSON object:

```json
{
  "timestamp": "2026-05-27T00:20:50.776Z",
  "level": "ERROR",
  "message": "Database error",
  "error": "connection timeout",
  "correlation_id": "abc123",
  "target": "soroban_pulse::handlers"
}
```

## Linting

A Makefile target checks for violations:

```bash
make lint-logs
```

This grep-based check ensures no log call uses `message` as an explicit field name.
