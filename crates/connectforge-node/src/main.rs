mod config;
mod startup;
mod telemetry;

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use clap::Parser;
use config::{Cli, Command, DemoArgs, ServeArgs};
use connectforge_api::{build_schema, ConnectorRegistry, DynStore};
use connectforge_core::{LogConfig, LogEngine, SinkConfig, SinkRuntime, SourceRuntime};
use connectforge_infra::{
    BroadcastEventBus, FailingSink, FileLogStore, GeneratorSource, MemoryCheckpointStore,
    MemoryDeadLetterSink, MemoryLogStore,
};
use connectforge_types::{
    ConnectorId, ConnectorKind, ConnectorStatus, DeliveryGuarantee, Offset, PartitionId,
    RetentionPolicy, TopicName,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    telemetry::init_tracing();

    let cli = Cli::parse();
    match cli
        .command
        .unwrap_or(Command::Serve(ServeArgs::parse_from(["serve"])))
    {
        Command::Serve(args) => serve(args).await,
        Command::Demo(args) => demo(args).await,
    }
}

fn build_store(data_dir: Option<std::path::PathBuf>) -> anyhow::Result<DynStore> {
    match data_dir {
        Some(dir) => {
            tracing::info!(path = %dir.display(), "using durable segmented file store");
            let store =
                FileLogStore::open(&dir).map_err(|e| anyhow::anyhow!("open file store: {e}"))?;
            Ok(Arc::new(store))
        }
        None => {
            tracing::info!("using in-memory store (non-durable)");
            Ok(Arc::new(MemoryLogStore::new()))
        }
    }
}

async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let metrics = telemetry::install_metrics()?;

    let cfg = LogConfig {
        max_topics: args.max_topics,
        ..LogConfig::default()
    };
    let store = build_store(args.data_dir)?;
    let bus = Arc::new(BroadcastEventBus::new(args.subscriber_buffer));
    let engine = Arc::new(LogEngine::new(cfg, Arc::new(store.clone()), bus));
    let registry = Arc::new(ConnectorRegistry::new());

    // A background connector supervisor drives a demo source→sink pipeline and
    // keeps the registry (statuses + dead-letter queue) observable via GraphQL.
    tokio::spawn(connector_supervisor(
        engine.clone(),
        store.clone(),
        registry.clone(),
    ));

    let schema = build_schema(engine, registry);
    let app = startup::build_app(schema, metrics);

    let listener = tokio::net::TcpListener::bind(args.addr)
        .await
        .with_context(|| format!("bind {}", args.addr))?;
    tracing::info!(addr = %args.addr, "connectforge listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(startup::shutdown_signal())
        .await
        .context("server error")?;
    Ok(())
}

/// The shared store type held by the connector runtimes. It is a double `Arc`
/// (`Arc<Arc<dyn LogStore>>`) so that the erased [`DynStore`] — which is itself
/// `Arc<dyn LogStore>` and therefore `Sized` — satisfies the `St: LogStore`
/// bound via the blanket `impl LogStore for Arc<T>`.
type ConnStore = Arc<DynStore>;

/// Continuously drive a demo source→sink connector pipeline, refreshing the
/// registry so the `connectors` and `deadLetters` queries return live data.
async fn connector_supervisor(
    engine: Arc<LogEngine<DynStore, BroadcastEventBus>>,
    store: DynStore,
    registry: Arc<ConnectorRegistry>,
) {
    if let Err(e) = run_supervisor(&engine, store, &registry).await {
        tracing::warn!(error = %e, "connector supervisor stopped");
    }
}

async fn run_supervisor(
    engine: &LogEngine<DynStore, BroadcastEventBus>,
    store: DynStore,
    registry: &ConnectorRegistry,
) -> anyhow::Result<()> {
    let topic = TopicName::new("connector-demo").map_err(|e| anyhow::anyhow!("{e}"))?;
    // Idempotent: ignore "already exists" on restart.
    let _ = engine
        .create_topic(topic.clone(), 1, RetentionPolicy::default())
        .await;

    let conn_store: ConnStore = Arc::new(store);
    let source_id = ConnectorId::new("stream-source").map_err(|e| anyhow::anyhow!("{e}"))?;
    let sink_id = ConnectorId::new("logging-sink").map_err(|e| anyhow::anyhow!("{e}"))?;

    // An effectively unbounded source that streams 200 records per tick.
    let source = Arc::new(GeneratorSource::new(u64::MAX, 200, 1_000, 0x00C0_FFEE));
    let source_rt = SourceRuntime::new(
        source_id.clone(),
        topic.clone(),
        PartitionId(0),
        conn_store.clone(),
        source,
    );

    let checkpoints = Arc::new(MemoryCheckpointStore::new());
    let dlq = Arc::new(MemoryDeadLetterSink::new());
    // Dead-letter every 500th offset to exercise the DLQ path.
    let sink = Arc::new(FailingSink::new(500));
    let sink_rt = SinkRuntime::new(
        sink_id,
        topic.clone(),
        PartitionId(0),
        SinkConfig::default(),
        conn_store,
        sink,
        checkpoints,
        dlq.clone(),
    );

    loop {
        source_rt.run_once().await?;
        while sink_rt.run_once().await?.polled > 0 {}

        registry.upsert_status(ConnectorStatus {
            id: source_id.clone(),
            kind: ConnectorKind::Source,
            guarantee: DeliveryGuarantee::AtLeastOnce,
            running: true,
            processed: source_rt.produced(),
            dead_lettered: 0,
            checkpoint: Offset(0),
        });
        registry.upsert_status(sink_rt.status().await?);
        registry.set_dead_letters(dlq.records());

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn demo(args: DemoArgs) -> anyhow::Result<()> {
    let store = build_store(args.data_dir)?;
    let bus = Arc::new(BroadcastEventBus::new(4_096));
    let engine = Arc::new(LogEngine::new(
        LogConfig::default(),
        Arc::new(store.clone()),
        bus,
    ));

    let topic =
        TopicName::new(args.topic.clone()).map_err(|e| anyhow::anyhow!("invalid topic: {e}"))?;
    engine
        .create_topic(topic.clone(), 1, RetentionPolicy::default())
        .await
        .map_err(|e| anyhow::anyhow!("create topic: {e}"))?;

    let conn_store: ConnStore = Arc::new(store);

    // Source stage: ingest `records` synthetic records into partition 0.
    let source = Arc::new(GeneratorSource::new(
        args.records,
        500,
        args.keys,
        0x00C0_FFEE,
    ));
    let source_rt = SourceRuntime::new(
        ConnectorId::new("demo-source").map_err(|e| anyhow::anyhow!("{e}"))?,
        topic.clone(),
        PartitionId(0),
        conn_store.clone(),
        source,
    );

    let start = Instant::now();
    while source_rt
        .run_once()
        .await
        .map_err(|e| anyhow::anyhow!("source: {e}"))?
        > 0
    {}
    let produced = source_rt.produced();

    // Sink stage: deliver at-least-once, dead-lettering every `fail_every`-th
    // offset, and checkpointing progress so a restart resumes cleanly.
    let checkpoints = Arc::new(MemoryCheckpointStore::new());
    let dlq = Arc::new(MemoryDeadLetterSink::new());
    let sink = Arc::new(FailingSink::new(args.fail_every));
    let sink_rt = SinkRuntime::new(
        ConnectorId::new("demo-sink").map_err(|e| anyhow::anyhow!("{e}"))?,
        topic.clone(),
        PartitionId(0),
        SinkConfig::default(),
        conn_store,
        sink,
        checkpoints,
        dlq.clone(),
    );

    let mut delivered = 0u64;
    let mut dead = 0u64;
    loop {
        let report = sink_rt
            .run_once()
            .await
            .map_err(|e| anyhow::anyhow!("sink: {e}"))?;
        if report.polled == 0 {
            break;
        }
        delivered += report.delivered;
        dead += report.dead_lettered;
    }
    let wall = start.elapsed();

    let throughput = produced as f64 / wall.as_secs_f64();
    println!("ConnectForge connector demo");
    println!("  topic               : {}", args.topic);
    println!("  guarantee           : at-least-once");
    println!("  records sourced     : {produced}");
    println!("  records delivered   : {delivered}");
    println!("  records dead-lettered: {dead}");
    println!("  dlq size            : {}", dlq.len());
    println!("  wall time           : {:.3}s", wall.as_secs_f64());
    println!("  pipeline throughput : {throughput:.0} records/sec");
    Ok(())
}
