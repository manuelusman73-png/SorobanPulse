# Deployment Guide

## Resource Sizing

### Recommended defaults

| Resource | Request | Limit |
| -------- | ------- | ----- |
| CPU | `100m` | `1000m` |
| Memory | `256Mi` | `512Mi` |

These values are set in both `k8s/deployment.yaml` and `helm/soroban-pulse/values.yaml`. The Helm chart exposes the full `resources` block in `values.yaml` so you can override it without editing the template:

```bash
helm upgrade soroban-pulse ./helm/soroban-pulse \
  --set resources.requests.memory=512Mi \
  --set resources.limits.memory=1Gi \
  --set resources.limits.cpu=2000m
```

### Tuning guidance

- **CPU request (`100m`)** — the scheduler guarantee. Raise this if pods are consistently throttled (check `container_cpu_throttled_seconds_total`).
- **CPU limit (`1000m`)** — caps one full core per pod. Increase for CPU-bound workloads (e.g. high event compression activity).
- **Memory request (`256Mi`)** — raise to match observed steady-state RSS. Under-requesting causes the scheduler to place pods on nodes that later OOM.
- **Memory limit (`512Mi`)** — pods that exceed this are OOM-killed. The `PodMemoryNearLimit` Prometheus alert (see `docs/alerts.yml`) fires when usage crosses 90% of this limit, giving you time to act before a kill occurs.

### Memory alert

The `PodMemoryNearLimit` alert in `docs/alerts.yml` fires when `soroban_pulse_process_memory_bytes` exceeds 90% of the 512 MiB limit for more than 5 minutes. Respond by:

1. Checking for a memory leak (`kubectl top pod`, heap profiling).
2. Increasing the limit and request if usage is legitimately growing with load.
3. Scaling horizontally (`replicaCount` / HPA) to spread the load.

---

## Healthcheck Configuration

The `app` service in `docker-compose.yml` includes a Docker healthcheck that polls `GET /healthz/ready` every 10 seconds. The check is configured with:

- `interval: 10s` — time between checks
- `timeout: 5s` — maximum time for a single check to respond
- `retries: 5` — consecutive failures before the container is marked unhealthy
- `start_period: 30s` — grace period on startup to allow migrations to complete

The `db` service uses `pg_isready` as its healthcheck, and the `app` service declares `depends_on: db: condition: service_healthy`, so Docker Compose will not start the application until PostgreSQL is accepting connections.

`make docker-up` runs `docker-compose wait app` after starting the stack, blocking until the app container reports healthy.

---

## Direct TLS

By default, Soroban Pulse serves plain HTTP and relies on an external reverse proxy (nginx, Caddy, AWS ALB, etc.) for TLS termination. For simpler deployments — a single VPS, a development environment with self-signed certificates, or any setup where adding a proxy is impractical — the service can handle TLS directly.

### Enabling direct TLS

Set both `TLS_CERT_FILE` and `TLS_KEY_FILE` to PEM-encoded certificate and key files:

```bash
TLS_CERT_FILE=/etc/ssl/certs/soroban-pulse.crt
TLS_KEY_FILE=/etc/ssl/private/soroban-pulse.key
PORT=443
```

When both variables are set, the service starts an HTTPS listener using `axum-server` with `rustls`. The certificate and key files are validated at startup — if either file is missing or the TLS handshake configuration fails, the service panics with a descriptive error.

When only one of the two variables is set, the service logs a warning and falls back to plain HTTP.

**`BEHIND_PROXY` is automatically forced to `false` when direct TLS is enabled**, since there is no proxy in front of the service. Any explicit `BEHIND_PROXY=true` setting is overridden and a warning is logged.

### Self-signed certificate (development)

```bash
# Generate a self-signed cert valid for 365 days
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj '/CN=localhost'

TLS_CERT_FILE=cert.pem TLS_KEY_FILE=key.pem PORT=3443 cargo run
```

### Production recommendation

For production deployments, prefer a reverse proxy (nginx, Caddy, AWS ALB) for TLS termination. This allows:
- Automatic certificate renewal (e.g., Let's Encrypt via Caddy)
- HTTP/2 and connection multiplexing
- Load balancing across multiple replicas

Set `BEHIND_PROXY=true` when running behind a proxy so the service trusts `X-Forwarded-For` headers for rate limiting.

---

## Horizontal Scaling

Soroban Pulse supports running multiple replicas safely. Only one replica will run the indexer loop at a time; all others serve HTTP traffic in read-only mode.

### How it works

On startup, each replica attempts to acquire a **Postgres session-level advisory lock** (`pg_try_advisory_lock`). The first replica to acquire the lock becomes the active indexer and logs:

```
Indexer lock acquired, starting indexing
```

All other replicas fail to acquire the lock and log:

```
Indexer lock not acquired, running in read-only mode
```

They continue to serve all HTTP endpoints (`/v1/events`, `/health`, `/metrics`, etc.) against the shared database.

### Failover

The advisory lock is **session-scoped**: if the indexer replica crashes or its database connection is dropped, Postgres automatically releases the lock. The next replica to restart (or any replica that reconnects) will acquire the lock within one poll cycle and resume indexing.

On graceful shutdown (`SIGTERM` / `Ctrl-C`), the active indexer explicitly releases the lock via `pg_advisory_unlock` before exiting, allowing another replica to take over immediately.

### Example: 3-replica Docker Compose

```yaml
services:
  app:
    image: soroban-pulse:latest
    deploy:
      replicas: 3
    environment:
      DATABASE_URL: postgres://user:pass@db:5432/soroban_pulse
```

With 3 replicas running, exactly one will hold the advisory lock and index events. The other two serve HTTP only. If the indexer replica is killed, one of the remaining two will acquire the lock on its next startup.

---

## Migration Strategy

### How migrations are run

`db::run_migrations` is called automatically on every replica startup. To prevent race conditions during rolling deploys (where multiple replicas start simultaneously), the migration step is guarded by a **Postgres session-level advisory lock**:

1. The starting replica acquires `pg_advisory_lock(<id>)` on a dedicated connection.
2. It runs `sqlx::migrate!` against that connection.
3. It releases `pg_advisory_unlock(<id>)` — unconditionally, even if migration fails.

Because `pg_advisory_lock` blocks (rather than fails) when another session holds the lock, replicas queue up and each one either applies pending migrations or finds nothing to do. No replica proceeds to serve traffic until the lock is released and migrations are confirmed complete.

The lock is **session-scoped**: if the process crashes mid-migration, Postgres releases the lock automatically when the connection is dropped, allowing the next replica to retry.

### Migration rollback procedure

Each migration has a corresponding `.down.sql` file that reverses its changes. To roll back the most recent migration:

```bash
# Using the Makefile
make migrate-down

# Or directly with cargo
cargo sqlx migrate revert
```

This will:

1. Execute the most recent `.down.sql` file
2. Remove the migration entry from the `_sqlx_migrations` table
3. Leave the database in the pre-migration state

### Rollback scenarios

**Scenario 1: Deployment fails validation**

If a new version fails health checks or integration tests after deployment:

```bash
# 1. Roll back the application to the previous version
kubectl rollout undo deployment/soroban-pulse

# 2. Roll back the database migration
kubectl exec -it deployment/soroban-pulse -- make migrate-down
```

**Scenario 2: Migration causes performance degradation**

If a migration creates an index that causes lock contention or slow queries:

```bash
# 1. Roll back the migration immediately
make migrate-down

# 2. Investigate the issue in a staging environment
# 3. Modify the migration to use CONCURRENTLY or adjust timing
# 4. Re-apply when ready
make migrate
```

**Scenario 3: Data corruption detected**

If a migration inadvertently corrupts data:

```bash
# 1. Roll back the migration
make migrate-down

# 2. Restore from the most recent backup taken before the migration
./scripts/restore.sh s3://my-bucket/backups/soroban_pulse_pre_migration.dump

# 3. Fix the migration script
# 4. Test thoroughly in staging before re-applying
```

### Testing migrations and rollbacks

Before deploying to production, test both the up and down migrations:

```bash
# 1. Start a test database
docker-compose -f docker-compose.test.yml up -d

# 2. Apply the migration
DATABASE_URL=postgres://postgres:postgres@localhost/soroban_pulse_test make migrate

# 3. Verify the schema changes
psql $DATABASE_URL -c "\d events"

# 4. Roll back the migration
DATABASE_URL=postgres://postgres:postgres@localhost/soroban_pulse_test make migrate-down

# 5. Verify the schema is restored
psql $DATABASE_URL -c "\d events"

# 6. Clean up
docker-compose -f docker-compose.test.yml down
```

### Alternative: run migrations as a pre-deploy Job (Kubernetes)

For stricter separation of concerns, you can disable in-process migrations and run them as a Kubernetes `Job` that completes before the `Deployment` rollout begins:

```yaml
# k8s/migrate-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: soroban-pulse-migrate
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: migrate
          image: soroban-pulse:latest
          command: ["./migrate"] # separate migrate binary
          envFrom:
            - secretRef:
                name: soroban-pulse-secrets
```

Reference this Job in your `Deployment` rollout pipeline (e.g., Argo CD sync waves, Helm hooks) so it runs to completion before any application pods start.

---

Soroban Pulse ships three `.env.*.example` templates:

| File                      | Purpose                                                   |
| ------------------------- | --------------------------------------------------------- |
| `.env.example`            | Local development defaults                                |
| `.env.staging.example`    | Staging environment — testnet, JSON logs, restricted CORS |
| `.env.production.example` | Production — mainnet, strict CORS, higher pool sizing     |

Copy the appropriate template and fill in real values:

```bash
cp .env.staging.example .env.staging
cp .env.production.example .env.production
```

### Environment-specific behaviour (`ENVIRONMENT`)

Set the `ENVIRONMENT` variable to one of `development`, `staging`, or `production`.

| Behaviour                   | development | staging              | production           |
| --------------------------- | ----------- | -------------------- | -------------------- |
| `ALLOWED_ORIGINS=*` allowed | ✅          | ❌ panics at startup | ❌ panics at startup |
| `RUST_LOG_FORMAT` default   | `text`      | `json`               | `json`               |
| Recommended `API_KEY`       | optional    | required             | required             |

In staging and production, setting `ALLOWED_ORIGINS=*` will cause the service to **panic at startup** — you must list explicit origins.

### Key differences between environments

| Variable                | Development | Staging                       | Production                    |
| ----------------------- | ----------- | ----------------------------- | ----------------------------- |
| `STELLAR_RPC_URL`       | testnet     | testnet                       | mainnet                       |
| `ALLOWED_ORIGINS`       | `*`         | `https://staging.example.com` | `https://app.example.com,...` |
| `RUST_LOG`              | `debug`     | `info`                        | `warn`                        |
| `RATE_LIMIT_PER_MINUTE` | `60`        | `60`                          | `30`                          |
| `DB_MAX_CONNECTIONS`    | `10`        | `10`                          | `20`                          |
| `BEHIND_PROXY`          | `false`     | `true`                        | `true`                        |

---

## Stellar RPC Endpoints

### Official endpoints

| Network | URL |
| ------- | --- |
| Testnet | `https://soroban-testnet.stellar.org` |
| Mainnet | `https://mainnet.stellar.validationcloud.io/v1/<YOUR_API_KEY>` |

The official mainnet endpoint is provided by Validation Cloud and requires a free API key for sustained access. Register at [validationcloud.io](https://validationcloud.io).

### API key requirements

For low-volume or development use, the testnet endpoint accepts requests without an API key. For production mainnet traffic, an API key is required to avoid rate limiting. Set it in `STELLAR_RPC_URL` as a path segment (as shown above) or as a query parameter depending on the provider.

### Third-party RPC providers

For high-volume production deployments, consider a dedicated RPC provider:

| Provider | URL format |
| -------- | ---------- |
| QuickNode | `https://<endpoint>.stellar.quiknode.pro/<token>/` |
| Ankr | `https://rpc.ankr.com/stellar_soroban/<token>` |

### `ALLOW_INSECURE_RPC`

> ⚠️ **Never set this in production.**

Setting `ALLOW_INSECURE_RPC=true` disables the URL validation that rejects HTTP and loopback/private-network RPC URLs. It exists only for local development against a locally-run RPC node (e.g., `http://localhost:8000`). In staging and production this variable must be unset or absent.

---

## Secret Management

Secrets (database password, API key, RPC URL) should never be stored in plain `.env` files in production. Use one of the following patterns.

### Docker Secrets (recommended for Docker Compose / Swarm)

Mount the secret as a file and point `DATABASE_URL_FILE` at it:

```yaml
# docker-compose.yml
services:
  app:
    environment:
      DATABASE_URL_FILE: /run/secrets/database_url
    secrets:
      - database_url

secrets:
  database_url:
    file: ./secrets/database_url.txt
```

When `DATABASE_URL_FILE` is set it takes precedence over `DATABASE_URL`. The file is read once at startup and its contents are trimmed of whitespace.

### Kubernetes Secrets

Create a secret and mount it as an environment variable:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: soroban-pulse-secrets
stringData:
  DATABASE_URL: "postgres://user:pass@host:5432/db"
  API_KEY: "your-api-key"
---
# In your Deployment spec:
envFrom:
  - secretRef:
      name: soroban-pulse-secrets
```

Or mount as a file and use `DATABASE_URL_FILE`:

```yaml
volumes:
  - name: db-secret
    secret:
      secretName: soroban-pulse-secrets
volumeMounts:
  - name: db-secret
    mountPath: /run/secrets
    readOnly: true
env:
  - name: DATABASE_URL_FILE
    value: /run/secrets/DATABASE_URL
```

### AWS Secrets Manager

Use the [AWS Secrets Manager Agent](https://docs.aws.amazon.com/secretsmanager/latest/userguide/secrets-manager-agent.html) or an init container to write the secret to a file, then set `DATABASE_URL_FILE` to that path. Alternatively, use the [External Secrets Operator](https://external-secrets.io/) to sync secrets into Kubernetes Secrets automatically.

### HashiCorp Vault

Use the [Vault Agent Injector](https://developer.hashicorp.com/vault/docs/platform/k8s/injector) to render secrets into a file at `/vault/secrets/database_url`, then:

```bash
DATABASE_URL_FILE=/vault/secrets/database_url
```

### Secret hygiene

- No secrets are logged at any log level. The `DATABASE_URL` is consumed at startup and never emitted to logs. The `API_KEY` is stored in memory only and never traced.
- Rotate secrets by updating the secret store and restarting the service (or using a sidecar that signals the process).

---

## TLS Termination

Soroban Pulse speaks plain HTTP and **must never be exposed directly on port 80 or 443 without TLS in front of it**. All TLS termination must happen at a reverse proxy or load balancer layer.

Set `BEHIND_PROXY=true` in your environment so the service trusts `X-Forwarded-For` headers from the proxy and logs real client IPs.

---

### Option 1 — nginx (self-managed)

Install certbot and obtain a certificate, then use the config below.

```nginx
# /etc/nginx/sites-available/soroban-pulse
server {
    listen 80;
    server_name api.example.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name api.example.com;

    ssl_certificate     /etc/letsencrypt/live/api.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.example.com/privkey.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;
    ssl_ciphers         HIGH:!aNULL:!MD5;

    location / {
        proxy_pass         http://127.0.0.1:3000;
        proxy_set_header   Host              $host;
        proxy_set_header   X-Real-IP         $remote_addr;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/soroban-pulse /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

---

### Option 2 — Caddy (automatic HTTPS)

```caddyfile
# /etc/caddy/Caddyfile
api.example.com {
    reverse_proxy localhost:3000
}
```

```bash
sudo systemctl reload caddy
```

---

### Option 3 — AWS Application Load Balancer (ALB)

1. Create an ALB with an HTTPS listener on port 443.
2. Attach an ACM certificate to the listener.
3. Add a target group pointing to the EC2/ECS instance on port 3000.
4. Set the security group to allow inbound 443 from the internet and inbound 3000 **only from the ALB security group**.
5. Set `BEHIND_PROXY=true` so ALB-injected `X-Forwarded-For` headers are trusted.

---

## Database Backup and Recovery

### RTO / RPO targets

| Target                         | Goal                                      |
| ------------------------------ | ----------------------------------------- |
| RPO (Recovery Point Objective) | ≤ 1 hour (with hourly `pg_dump` schedule) |
| RTO (Recovery Time Objective)  | ≤ 30 minutes (restore from latest dump)   |

For stricter RPO, enable WAL archiving (see below).

### Backup Encryption

All backups are encrypted using GPG symmetric encryption (AES-256) before being stored or uploaded. This protects sensitive event data at rest in S3 or local storage.

#### Setting up encryption

1. Generate a strong passphrase and store it securely (e.g., AWS Secrets Manager, HashiCorp Vault):

```bash
# Generate a random 32-character passphrase
openssl rand -base64 32
```

2. Set the `BACKUP_ENCRYPTION_KEY` environment variable to this passphrase:

```bash
export BACKUP_ENCRYPTION_KEY="your-secure-passphrase-here"
```

3. Store the passphrase in your secrets manager and inject it at runtime:

```bash
# Example: AWS Secrets Manager
export BACKUP_ENCRYPTION_KEY=$(aws secretsmanager get-secret-value \
  --secret-id soroban-pulse/backup-key \
  --query SecretString --output text)
```

### pg_dump schedule (recommended for most deployments)

Use `scripts/backup.sh` to create an encrypted compressed custom-format dump:

```bash
# Dump to a local directory (encrypted)
DATABASE_URL=postgres://user:pass@localhost/soroban_pulse \
BACKUP_ENCRYPTION_KEY="your-passphrase" \
./scripts/backup.sh

# Dump and upload to S3 (encrypted)
DATABASE_URL=postgres://... \
BACKUP_ENCRYPTION_KEY="your-passphrase" \
BACKUP_DEST=s3://my-bucket/soroban-pulse \
./scripts/backup.sh
```

Backups are saved with a `.gpg` extension (e.g., `soroban_pulse_20260314T000000Z.dump.gpg`).

Schedule with cron (hourly example):

```cron
0 * * * * \
  DATABASE_URL=postgres://... \
  BACKUP_ENCRYPTION_KEY=$(aws secretsmanager get-secret-value --secret-id soroban-pulse/backup-key --query SecretString --output text) \
  BACKUP_DEST=s3://my-bucket/backups \
  /app/scripts/backup.sh >> /var/log/soroban-backup.log 2>&1
```

### Restoring from a dump

```bash
# From a local encrypted file
DATABASE_URL=postgres://... \
BACKUP_ENCRYPTION_KEY="your-passphrase" \
./scripts/restore.sh ./backups/soroban_pulse_20260314T000000Z.dump.gpg

# From S3 (encrypted)
DATABASE_URL=postgres://... \
BACKUP_ENCRYPTION_KEY="your-passphrase" \
./scripts/restore.sh s3://my-bucket/backups/soroban_pulse_20260314T000000Z.dump.gpg
```

The restore script prompts for confirmation before overwriting data.

### Automated backup verification

A weekly GitHub Actions workflow (`.github/workflows/backup-verify.yml`) automatically validates that backups are restorable:

1. Provisions two PostgreSQL containers (source and restore target)
2. Seeds the source database with test data
3. Runs `scripts/backup.sh` to create an encrypted dump
4. Restores the encrypted dump to the target database
5. Compares `COUNT(*) FROM events` between source and target — fails if they differ

The workflow runs every Sunday at 02:00 UTC (`cron: '0 2 * * 0'`) and can also be triggered manually via `workflow_dispatch`.

### WAL archiving (for sub-minute RPO)

Enable continuous archiving in `postgresql.conf`:

```ini
wal_level = replica
archive_mode = on
archive_command = 'aws s3 cp %p s3://my-bucket/wal/%f'
```

Use [pgBackRest](https://pgbackrest.org/) or [Barman](https://pgbarman.org/) for managed WAL archiving and point-in-time recovery.

### Managed database recommendations

For production workloads, prefer a managed PostgreSQL service to offload backup and HA concerns:

- **AWS RDS for PostgreSQL** — automated backups, Multi-AZ, point-in-time recovery up to 35 days.
- **Google Cloud SQL** — automated backups, read replicas, point-in-time recovery.
- **Supabase** — managed Postgres with daily backups on paid plans.

When using a managed service, disable the `db` service in `docker-compose.yml` and point `DATABASE_URL` at the managed endpoint.

### Testing backups against Docker Compose

```bash
# 1. Start the stack
docker-compose up -d db

# 2. Run a backup
DATABASE_URL=postgres://user:pass@localhost:5432/soroban_pulse \
  BACKUP_DEST=./backups ./scripts/backup.sh

# 3. Restore into a fresh database to verify
createdb soroban_pulse_verify
DATABASE_URL=postgres://user:pass@localhost:5432/soroban_pulse_verify \
  ./scripts/restore.sh ./backups/soroban_pulse_*.dump
```

---

## Environment Variables

| Variable            | Description                                                                       | Default       |
| ------------------- | --------------------------------------------------------------------------------- | ------------- |
| `ENVIRONMENT`       | Deployment environment (`development`/`staging`/`production`)                     | `development` |
| `BEHIND_PROXY`      | Trust `X-Forwarded-For` from upstream proxy/load balancer                         | `false`       |
| `DATABASE_URL_FILE` | Path to a file containing the database URL (takes precedence over `DATABASE_URL`) | —             |

See the root [README](../README.md) for all other variables.

---

## Security Checklist

- [ ] TLS termination is handled by nginx, Caddy, or a cloud load balancer
- [ ] Port 3000 is firewalled from public internet access
- [ ] `BEHIND_PROXY=true` is set when running behind a proxy
- [ ] Certificates are auto-renewed (certbot timer or Caddy/ACM managed)
- [ ] `ENVIRONMENT=production` is set in production
- [ ] `ALLOWED_ORIGINS` lists only known domains (no `*`)
- [ ] `API_KEY` is set and rotated regularly
- [ ] Secrets are managed via Docker Secrets, Kubernetes Secrets, or a vault — not plain `.env` files
- [ ] Database backups are scheduled and restore procedure is tested
- [ ] Autovacuum is configured to prevent table and index bloat

---

## Database Maintenance

### Table Bloat and VACUUM

Soroban Pulse uses an `ON CONFLICT DO NOTHING` pattern for the `events` table to ensure idempotency. While efficient for data integrity, this pattern creates **dead tuples** every time a duplicate event is encountered. In an append-heavy workload, these dead tuples can lead to "table bloat," where the table and its indexes consume far more disk space than necessary, eventually degrading query performance and increasing index scan times.

PostgreSQL's built-in **autovacuum** daemon handles the removal of dead tuples and the updating of query planner statistics (`ANALYZE`). For a high-traffic indexing service, the default autovacuum settings may be too conservative.

### Recommended Autovacuum Settings

We recommend the following settings in `postgresql.conf` to ensure the `events` table is vacuumed frequently enough to prevent significant bloat:

```ini
# Trigger vacuum when 1% of the table has changed (default is 20%)
autovacuum_vacuum_scale_factor = 0.01

# Trigger analyze when 0.5% of the table has changed (default is 10%)
autovacuum_analyze_scale_factor = 0.005

# Reduce the delay between vacuum rounds to increase throughput
autovacuum_vacuum_cost_delay = 10ms
```

### Managed Database Configuration (RDS, Cloud SQL)

If you are using a managed service that does not allow global `postgresql.conf` changes, you can apply these settings specifically to the `events` table:

```sql
ALTER TABLE events SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.005,
  autovacuum_vacuum_cost_delay = 10
);
```

### Manual Maintenance

In cases of extreme bloat (e.g., after a large re-indexing operation), you may want to run a manual vacuum using the provided Makefile target:

```bash
make vacuum
```

This executes `VACUUM ANALYZE events;` which cleans up dead tuples and updates statistics without taking an exclusive lock on the table (allowing application traffic to continue).

---

## Index Usage Monitoring

Soroban Pulse includes a background task that periodically runs `EXPLAIN` on the three key query patterns and logs a warning if the query planner is not using the expected index.

### Checked queries

| Query | Expected index |
|---|---|
| `GET /v1/events` (no filters) | `idx_events_ledger_desc` |
| `GET /v1/events/contract/:id` | `idx_events_contract_ledger` |
| `GET /v1/events/tx/:hash` | `idx_events_tx_ledger` |

### Configuration

| Variable | Description | Default |
|---|---|---|
| `INDEX_CHECK_INTERVAL_HOURS` | How often to run the check | `24` |

### Log output

- `DEBUG` — index is being used as expected.
- `WARN` — a sequential scan was detected where an index scan is expected. This indicates the query planner has chosen a different execution plan, which may degrade performance at scale. Investigate with `EXPLAIN ANALYZE` and consider running `ANALYZE events;` to refresh planner statistics.

---

## Event Data Compression

The `event_data` JSONB column uses PostgreSQL TOAST for out-of-line storage of large values. The migration `20260427000000_event_data_compression` sets the column storage to `EXTENDED` and, on PostgreSQL 14+, switches the compression algorithm to `lz4` for better compression throughput compared to the default `pglz`.

This change is applied automatically on startup via SQLx migrations and requires no manual intervention. No table rewrite is performed — only the column metadata is updated.

### Verifying the setting

```sql
SELECT attname, attstorage, attcompression
FROM pg_attribute
WHERE attrelid = 'events'::regclass AND attname = 'event_data';
```

Expected output on PostgreSQL 14+:

```
 attname    | attstorage | attcompression
------------+------------+----------------
 event_data | x          | l
```

(`x` = EXTENDED, `l` = lz4)
