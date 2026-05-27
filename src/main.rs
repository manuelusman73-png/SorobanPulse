#![deny(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions, // e.g. AppError, AppState — idiomatic in Rust
    clippy::missing_errors_doc,      // internal handlers; not a public library
    clippy::missing_panics_doc,      // panics only on misconfiguration at startup
    clippy::wildcard_imports,        // used sparingly in test modules only
)]
mod bloom_filter;
mod config;
mod db;
mod email;
mod encryption;
mod error;
mod handlers;
mod index_monitor;
mod indexer;
mod kafka;
mod kinesis;
#[cfg(feature = "lua")]
mod lua_transform;
mod metrics;
mod middleware;
mod models;
mod normalizer;

#[cfg(feature = "parquet")]
mod parquet_export;

mod pruner;
mod pubsub;
mod queue_publisher;
mod routes;
mod rpc_client;
mod schema_validator;
mod stats_refresh;
mod subscriptions;
mod webhook;
mod xdr_validation;

#[cfg(feature = "archive")]
mod archiver;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry_otlp::WithExportConfig;
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetryLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let log_format = std::env::var("RUST_LOG_FORMAT").unwrap_or_else(|_| "text".to_string());

    let registry = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()));

    #[cfg(feature = "otel")]
    let registry = {
        let otel_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:4317".to_string());

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(otel_endpoint),
            )
            .install_simple()
            .expect("Failed to initialize OpenTelemetry tracer");

        registry.with(OpenTelemetryLayer::new(tracer))
    };

    if log_format == "json" {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        registry.with(tracing_subscriber::fmt::layer()).init();
    }

    // Initialize metrics exporter
    let prometheus_handle = metrics::init_metrics();

    #[cfg(target_os = "linux")]
    metrics::spawn_memory_collector();

    let config = config::Config::from_env();

    info!(
        rpc_url = %config.stellar_rpc_url,
        rpc_headers = ?config.safe_rpc_headers(),
        start_ledger = config.start_ledger,
        port = config.port,
        db_url = %config.safe_db_url(),
        db_max_connections = config.db_max_connections,
        db_min_connections = config.db_min_connections,
        indexer_event_types = ?config.indexer_event_types,
        "Resolved configuration",
    );

    let pool = {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match db::create_pool(
                &config.database_url,
                config.db_max_connections,
                config.db_min_connections,
                config.db_statement_timeout_ms,
                config.db_idle_timeout_secs,
                config.db_max_lifetime_secs,
                config.db_test_before_acquire,
            )
            .await
            {
                Ok(p) => break p,
                Err(e) => {
                    if attempt >= 3 {
                        tracing::error!(error = %e, "Failed to connect to database after 3 attempts");
                        std::process::exit(1);
                    }
                    tracing::warn!(attempt = attempt, "DB connection failed, retrying...");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    };

    let _ = db::run_migrations(&pool).await;

    // Create read pool: use replica URL if configured, otherwise reuse primary pool.
    let read_pool = if let Some(ref replica_url) = config.database_replica_url {
        info!("DATABASE_REPLICA_URL set — HTTP handlers will use read replica");
        match db::create_pool(
            replica_url,
            config.db_max_connections,
            config.db_min_connections,
            config.db_statement_timeout_ms,
            config.db_idle_timeout_secs,
            config.db_max_lifetime_secs,
            config.db_test_before_acquire,
        )
        .await
        {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "Failed to connect to read replica, falling back to primary");
                pool.clone()
            }
        }
    } else {
        pool.clone()
    };

    info!("Migrations applied successfully");
    info!(environment = ?config.environment, "Running environment");

    // Initialize schema validator and load schemas
    let schema_validator = Arc::new(schema_validator::SchemaValidator::new(pool.clone()));
    if let Err(e) = schema_validator.load_schemas().await {
        warn!(error = %e, "Failed to load schemas from database");
    } else {
        info!("Schema validator initialized");
    }

    // Create shared health state for indexer and HTTP handlers
    let health_state = Arc::new(config::HealthState::new(config.indexer_stall_timeout_secs));

    // Create shared indexer state for /status endpoint
    let indexer_state = Arc::new(config::IndexerState::new());

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut shutdown_rx_axum = shutdown_rx.clone();

    // Broadcast channel for real-time SSE streaming (capacity 256 events)
    let (event_tx, _) = tokio::sync::broadcast::channel::<models::SorobanEvent>(256);

    // Spawn webhook delivery task if WEBHOOK_URL is configured.
    if let Some(ref webhook_url) = config.webhook_url {
        let webhook_rx = event_tx.subscribe();
        let webhook_url = webhook_url.clone();
        let webhook_secret = config.webhook_secret.clone();
        let webhook_contract_filter = config.webhook_contract_filter.clone();
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to build webhook HTTP client");

        info!(url = %webhook_url, "Webhook delivery enabled");

        tokio::spawn(async move {
            let mut rx = webhook_rx;
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        // Apply contract filter if configured
                        if !webhook_contract_filter.is_empty()
                            && !webhook_contract_filter.contains(&event.contract_id)
                        {
                            continue;
                        }
                        let client = http_client.clone();
                        let url = webhook_url.clone();
                        let secret = webhook_secret.clone();
                        tokio::spawn(webhook::deliver(client, url, secret, event));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(
                            skipped = n,
                            "Webhook subscriber lagged, some events skipped"
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    // Spawn email notification task if EMAIL_SMTP_HOST is configured.
    if let Some(ref smtp_host) = config.email_smtp_host {
        if let Some(ref from) = config.email_from {
            if !config.email_to.is_empty() {
                let email_rx = event_tx.subscribe();
                let notifier = email::EmailNotifier::new(
                    smtp_host.clone(),
                    config.email_smtp_port,
                    config.email_smtp_user.clone(),
                    config.email_smtp_password.clone(),
                    from.clone(),
                    config.email_to.clone(),
                    config.email_contract_filter.clone(),
                );

                info!(
                    smtp_host = %smtp_host,
                    recipients = config.email_to.len(),
                    "Email notifications enabled"
                );

                notifier.spawn(email_rx);
            } else {
                warn!("EMAIL_SMTP_HOST is set but EMAIL_TO is empty — email notifications disabled");
            }
        } else {
            warn!("EMAIL_SMTP_HOST is set but EMAIL_FROM is not — email notifications disabled");
        }
    }

    // Spawn Redis publisher if configured
    if let (Some(redis_url), Some(redis_stream_key)) = (&config.redis_url, &config.redis_stream_key) {
        let redis_rx = event_tx.subscribe();
        let redis_url = redis_url.clone();
        let redis_stream_key = redis_stream_key.clone();

        info!(stream_key = %redis_stream_key, "Redis publisher enabled");

        tokio::spawn(async move {
            queue_publisher::spawn_redis_publisher(redis_url, redis_stream_key, redis_rx).await;
        });
    }

    // Spawn background indexer with health state
    let rpc_client = indexer::SorobanRpcClient::new(&config);
    let mut indexer = indexer::Indexer::new(
        pool.clone(),
        config.clone(),
        shutdown_rx.clone(),
        rpc_client,
    );
    indexer.set_health_state(health_state.clone());
    indexer.set_indexer_state(indexer_state.clone());
    indexer.set_event_tx(event_tx.clone());

    // Issue #266: Initialize and seed bloom filter
    {
        let bloom = std::sync::Arc::new(bloom_filter::EventBloomFilter::new(
            config.bloom_filter_capacity,
            config.bloom_filter_fp_rate,
        ));
        match bloom_filter::seed_from_db(&bloom, &pool, 100_000).await {
            Ok(n) => info!(seeded = n, "Bloom filter seeded from DB"),
            Err(e) => tracing::warn!(error = %e, "Failed to seed bloom filter from DB"),
        }
        indexer.set_bloom_filter(bloom);
    }

    // Issue #265: Initialize Kinesis publisher if configured
    #[cfg(feature = "kinesis")]
    if let (Some(stream_name), Some(region)) = (
        config.kinesis_stream_name.clone(),
        config.aws_region.clone(),
    ) {
        let publisher = kinesis::aws::AwsKinesisPublisher::from_env(stream_name, region).await;
        indexer.set_kinesis_publisher(std::sync::Arc::new(publisher));
        info!("Kinesis publisher enabled");
    }

    // Issue #264: Initialize Pub/Sub publisher if configured
    #[cfg(feature = "pubsub")]
    if let (Some(project_id), Some(topic_id)) = (
        config.pubsub_project_id.clone(),
        config.pubsub_topic_id.clone(),
    ) {
        match pubsub::gcp::GcpPubSubPublisher::from_env(project_id, topic_id).await {
            Ok(publisher) => {
                indexer.set_pubsub_publisher(std::sync::Arc::new(publisher));
                info!("Pub/Sub publisher enabled");
            }
            Err(e) => tracing::warn!(error = %e, "Failed to initialize Pub/Sub publisher"),
        }
    }

    #[cfg(feature = "kafka")]
    if let (Some(brokers), Some(topic)) = (&config.kafka_brokers, &config.kafka_topic) {
        match crate::kafka::RdKafkaProducer::new(
            brokers,
            config.kafka_batch_size,
            config.kafka_linger_ms,
        ) {
            Ok(producer) => {
                info!(brokers = %brokers, topic = %topic, "Kafka publishing enabled");
                indexer.set_kafka_publisher(std::sync::Arc::new(producer), topic.clone());
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to create Kafka producer — Kafka publishing disabled");
            }
        }
    }

    // Initialize Lua transformer if configured
    #[cfg(feature = "lua")]
    if let Some(ref script_path) = config.event_transform_script {
        match soroban_pulse::lua_transform::LuaTransformer::new(
            std::path::Path::new(script_path),
            config.event_transform_timeout_ms,
        ) {
            Ok(transformer) => {
                info!(
                    script_path = %script_path,
                    timeout_ms = config.event_transform_timeout_ms,
                    "Lua event transformer enabled"
                );
                indexer.set_lua_transformer(std::sync::Arc::new(transformer));
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to initialize Lua transformer — transformation disabled");
            }
        }
    }

    let indexer_handle = tokio::spawn(async move {
        indexer.run().await;
    });

    // Spawn index usage monitoring background task
    index_monitor::spawn(
        pool.clone(),
        config.index_check_interval_hours,
        shutdown_rx.clone(),
    );

    // Spawn materialized-view refresh background task
    stats_refresh::spawn(
        pool.clone(),
        config.stats_refresh_interval_secs,
        shutdown_rx.clone(),
    );

    // Spawn event pruning background task
    pruner::start_pruning_task(
        pool.clone(),
        config.retention_days,
        config.pruning_interval_hours,
        shutdown_rx.clone(),
    );

    async fn shutdown_signal() {
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }

        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }

        tracing::info!("Graceful shutdown initiated, draining requests...");

        tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            tracing::info!("Graceful shutdown timeout reached (30s), forcing exit");
            std::process::exit(0);
        });
    }

    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    info!(origins = ?config.allowed_origins, "Allowed CORS origins");
    info!(
        rate_limit = config.rate_limit_per_minute,
        "Rate limit per IP"
    );

    // Clone pool before it is moved into the router for the tenant map load.
    let pool_for_tenant_map = pool.clone();

    let tenant_map = if config.multi_tenant {
        match routes::load_tenant_map(&pool_for_tenant_map).await {
            Ok(map) => {
                info!(count = map.len(), "Loaded tenant map from database");
                std::sync::Arc::new(map)
            }
            Err(e) => {
                error!(error = %e, "Failed to load tenant map — aborting startup");
                return Err(e.into());
            }
        }
    } else {
        std::sync::Arc::new(std::collections::HashMap::new())
    };

    let router = routes::create_router_with_tx_and_tenant_map(
        pool,
        read_pool,
        config.api_keys.clone(),
        &config.allowed_origins,
        config.rate_limit_per_minute,
        config.behind_proxy,
        health_state,
        indexer_state,
        prometheus_handle,
        event_tx,
        config.sse_keepalive_interval_ms,
        config.sse_max_connections,
        2000,
        config.event_data_encryption_key,
        config.event_data_encryption_key_old,
        config.clone(),
        Some(schema_validator),
        tenant_map,
    );

    info!(addr = %addr, "Soroban Pulse listening");

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        error!(addr = %addr, "Address already in use");
        e
    })?;

    info!(
        behind_proxy = config.behind_proxy,
        "Running server - trusting X-Forwarded-For"
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx_axum.changed().await;
        })
        .await?;
    let _ = indexer_handle.await;

    #[cfg(feature = "otel")]
    global::shutdown_tracer_provider();

    Ok(())
}
