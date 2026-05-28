# Contributing to Soroban Pulse

## Running Integration Tests

Integration tests require a live PostgreSQL instance. The easiest way is `make test-db`, which starts a throwaway container, runs the full suite, then tears it down:

```bash
make test-db   # only Docker required — no local Postgres needed
```

Under the hood this runs:

```bash
docker compose -f docker-compose.test.yml up -d --wait
DATABASE_URL=postgres://postgres:postgres@localhost/soroban_pulse_test cargo test
docker compose -f docker-compose.test.yml down
```

If you already have a Postgres instance running, set `DATABASE_URL` directly:

```bash
export DATABASE_URL=postgres://<user>:<password>@localhost/<dbname>
cargo test
```

`sqlx::test`-annotated tests create and tear down their own isolated database automatically — no manual schema setup needed.

## Getting Started

1. Fork the repo and create a branch from `main`.
2. Copy `.env.example` to `.env` and fill in your values. **Never commit `.env` or any `.env.*` file.**
3. Run `make docker-up` to start the full stack, or `make run` for the dev server only.

## Common Tasks

Run `make help` to see all available targets:

```
make build       # Compile the project
make test        # Run the full test suite (requires DATABASE_URL)
make lint        # Run clippy with warnings as errors
make fmt         # Format source code
make run         # Start the development server
make docker-up   # Start the full stack via Docker Compose
make docker-down # Tear down the Docker Compose stack
make migrate     # Run pending database migrations
make check-migrations # Check for duplicate migration timestamps
make clean       # Remove build artifacts
```

## Database Migrations

### Naming Convention

Migration files must follow the naming convention:

```
<YYYYMMDDHHMMSS>_<description>.sql
```

Where:
- `YYYYMMDDHHMMSS` is a unique timestamp (14 digits)
- `<description>` is a short, snake_case description of the migration

**Important:** Each migration must have a **unique timestamp prefix**. SQLx applies migrations in lexicographic order by filename. Duplicate timestamps cause non-deterministic apply order and can lead to schema inconsistencies between fresh and incrementally-migrated databases.

### Checking for Duplicates

Before committing a new migration, run:

```bash
make check-migrations
```

This ensures no two migration files share the same timestamp prefix. The CI pipeline also runs this check automatically.

### Example

```
20260527024126_add_composite_indexes.sql
20260527024127_add_webhook_failures_table.sql
```

Not:

```
20260527000000_add_composite_indexes.sql
20260527000000_add_webhook_failures_table.sql  # ❌ Duplicate timestamp!
```

## Fuzzing

Fuzz targets live in `fuzz/fuzz_targets/` and cover the primary input-validation boundary:

| Target | What it tests |
|--------|---------------|
| `fuzz_validate_contract_id` | `validate_contract_id` — no panics, deterministic, valid inputs accepted |
| `fuzz_validate_tx_hash` | `validate_tx_hash` — no panics, deterministic, valid inputs accepted |
| `fuzz_pagination_params` | `PaginationParams` deserialization — no panics, `limit`/`offset` in range |

Requires a nightly toolchain and `cargo-fuzz`:

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
```

Run all fuzz targets locally (60 seconds each):

```bash
make fuzz
```

Or run a single target:

```bash
cd fuzz
cargo fuzz run fuzz_validate_contract_id -- -max_total_time=60
cargo fuzz run fuzz_validate_tx_hash     -- -max_total_time=60
cargo fuzz run fuzz_pagination_params   -- -max_total_time=60
```

To run indefinitely (until a crash is found):

```bash
cargo fuzz run fuzz_validate_contract_id
```

Corpus and crash artifacts are stored under `fuzz/corpus/` and `fuzz/artifacts/` respectively (both git-ignored). The CI pipeline runs fuzz tests for 60 seconds per target on every push and PR, and stores corpus artifacts for 30 days to accumulate interesting inputs over time.

## Pre-commit Hooks

This project uses [lefthook](https://github.com/evilmartians/lefthook) to run `cargo check`, `cargo fmt --check`, and `cargo clippy` before every commit.

Install lefthook and register the hooks once:

```bash
# macOS
brew install lefthook

# Linux / other (via cargo)
cargo install lefthook

# Register hooks in your local clone
lefthook install
```

Hooks typically complete in under 30 seconds on a typical change. If a hook fails, fix the reported issue and re-commit.

## Security: Never Log Sensitive Values

Never log passwords, API keys, tokens, or other credentials at any log level. This includes:

- `DATABASE_URL` — use `config.safe_db_url()` which strips credentials before logging
- `STELLAR_RPC_URL` — already sanitized by `validate_rpc_url()` before being stored in `Config`; the stored `config.stellar_rpc_url` is safe to log
- `API_KEY` / `API_KEY_SECONDARY` — never log these values
- Any request header that may contain `Authorization` or `X-Api-Key` values

When adding new log statements, double-check that no field contains a raw secret. If in doubt, strip credentials before logging (see `Config::safe_db_url()` for the pattern).

## Code Style

- Formatting is enforced by `rustfmt` using the project's `rustfmt.toml` (`max_width = 100`, `edition = "2021"`).
- Run `make fmt` before pushing, or let the pre-commit hook handle it.
- CI will reject any PR where `cargo fmt --check` fails.

## Commit Message Format

This project uses [Conventional Commits](https://www.conventionalcommits.org/) to structure commit messages. This enables automated changelog generation and semantic versioning.

### Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- `feat:` A new feature
- `fix:` A bug fix
- `perf:` A performance improvement
- `docs:` Documentation only
- `chore:` Build, CI, or dependency updates
- `refactor:` Code refactoring without feature changes
- `test:` Adding or updating tests

### Breaking Changes

Prefix the footer with `BREAKING CHANGE:` to indicate a breaking change:

```
feat(api): change event response format

BREAKING CHANGE: The /v1/events response now returns event_data as base64-encoded gzip by default.
Use ?compact=false to get uncompressed JSON.
```

### Examples

```
feat(indexer): add Lua script transformation support

- Load Lua scripts from configurable path
- Transform events before storage
- Skip events by returning nil

Closes #123
```

```
fix(handlers): prevent PII leakage in export endpoint

Filter payments by patientId in addition to clinicId.

Closes #456
```

```
perf(db): add GIN index on event_data JSONB column

Improves topic filtering query performance by 10x.
```

## Pull Requests

- Keep PRs focused — one logical change per PR.
- Ensure `make test` and `make lint` pass locally before opening a PR.
- Write a clear PR description referencing the relevant issue (e.g. `Closes #75`).
- Use Conventional Commits for all commit messages in the PR.

## Releases

See [RELEASE.md](RELEASE.md) for the complete release process, including:
- Version numbering (Semantic Versioning)
- Updating CHANGELOG.md
- Creating git tags and GitHub releases
- Docker image publishing

Only maintainers can cut releases. If you'd like to propose a release, open an issue or contact the maintainers.
