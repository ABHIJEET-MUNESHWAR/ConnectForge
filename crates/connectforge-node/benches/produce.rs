//! Criterion micro-benchmark for the ConnectForge produce (append) hot path.
//!
//! Measures single-record append latency against the in-memory store so the
//! numbers reflect engine + store overhead without disk noise. See the README
//! "Benchmarks" section for the complexity analysis.

use std::sync::Arc;

use connectforge_core::{LogConfig, LogEngine};
use connectforge_infra::{BroadcastEventBus, MemoryLogStore};
use connectforge_types::{ProduceRecord, RetentionPolicy, TopicName};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use tokio::runtime::Runtime;

fn produce_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("produce");
    group.throughput(criterion::Throughput::Elements(1));

    group.bench_function("produce_single_record", |b| {
        b.iter_batched(
            || {
                let store = Arc::new(MemoryLogStore::new());
                let bus = Arc::new(BroadcastEventBus::new(1024));
                let engine = Arc::new(LogEngine::new(LogConfig::default(), store, bus));
                let topic = TopicName::new("bench").unwrap();
                rt.block_on(async {
                    engine
                        .create_topic(topic.clone(), 1, RetentionPolicy::default())
                        .await
                        .unwrap();
                });
                let record = ProduceRecord::new(None, b"benchmark-payload".to_vec()).unwrap();
                (engine, topic, record)
            },
            |(engine, topic, record)| {
                rt.block_on(async move {
                    let _ = engine.produce(&topic, vec![record]).await;
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, produce_benchmark);
criterion_main!(benches);
