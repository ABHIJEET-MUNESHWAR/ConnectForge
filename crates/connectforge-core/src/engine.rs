//! The `LogEngine`: the transactional heart of the broker.
//!
//! It applies a CQRS-style split — [`LogEngine::produce`] is the write/command
//! path (validate → rate-limit → durably append → publish), while
//! [`LogEngine::fetch`] is the read/query path (pull records by offset). Every
//! storage call is wrapped in a timeout + jittered retry so a transient adapter
//! failure never surfaces to the caller.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::Utc;
use connectforge_resilience::{retry_if, with_timeout, RateLimiter, RetryPolicy};
use connectforge_types::{
    FetchResult, LogStats, Offset, PartitionId, ProduceRecord, ProduceResult, RetentionPolicy,
    TopicConfig, TopicName,
};

use crate::config::LogConfig;
use crate::error::CoreError;
use crate::event::LogEvent;
use crate::ports::{EventBus, EventStream, LogStore};

/// FNV-1a 64-bit — mirrors [`TopicConfig::partition_for`] for hot-path routing
/// without re-materializing a `TopicConfig`.
fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// The broker engine, generic over its storage and event-bus ports.
pub struct LogEngine<S, B>
where
    S: LogStore,
    B: EventBus,
{
    config: LogConfig,
    store: Arc<S>,
    bus: Arc<B>,
    limiter: RateLimiter,
    retry: RetryPolicy,
    round_robin: AtomicU64,
}

impl<S, B> LogEngine<S, B>
where
    S: LogStore,
    B: EventBus,
{
    /// Assemble an engine from its config and ports.
    #[must_use]
    pub fn new(config: LogConfig, store: Arc<S>, bus: Arc<B>) -> Self {
        let limiter = RateLimiter::new(config.produce_burst, config.produce_rate_per_sec);
        Self {
            config,
            store,
            bus,
            limiter,
            retry: RetryPolicy::default(),
            round_robin: AtomicU64::new(0),
        }
    }

    /// Create a new topic (command). Publishes a [`LogEvent::TopicCreated`].
    ///
    /// # Errors
    /// - [`CoreError::Invalid`] if the name/partition/retention is invalid.
    /// - [`CoreError::CapacityExceeded`] if `max_topics` is reached.
    /// - [`CoreError::TopicExists`] if a topic with the name already exists.
    pub async fn create_topic(
        &self,
        name: TopicName,
        partitions: u32,
        retention: RetentionPolicy,
    ) -> Result<(), CoreError> {
        let partitions = if partitions == 0 {
            self.config.default_partitions
        } else {
            partitions
        };
        let config = TopicConfig::new(name.clone(), partitions, retention)?;

        let existing = self
            .store_call(|s| {
                let s = Arc::clone(&s);
                async move { s.topics().await }
            })
            .await?;
        if existing.len() >= self.config.max_topics {
            return Err(CoreError::CapacityExceeded(self.config.max_topics));
        }

        let created = self
            .store_call(|s| {
                let s = Arc::clone(&s);
                let config = config.clone();
                async move { s.create_topic(config).await }
            })
            .await?;
        if !created {
            return Err(CoreError::TopicExists(name.as_str().to_owned()));
        }

        let _ = self
            .bus
            .publish(LogEvent::TopicCreated {
                topic: name,
                partitions,
            })
            .await;
        metrics::counter!("connectforge_topics_created_total").increment(1);
        Ok(())
    }

    /// Produce a batch of records to a topic (command).
    ///
    /// The batch is routed to a single partition — by the first record's key
    /// (stable hash, preserving per-key ordering) or round-robin when keyless —
    /// then durably appended and fanned out on the bus.
    ///
    /// # Errors
    /// - [`CoreError::EmptyBatch`] if `records` is empty.
    /// - [`CoreError::RateLimited`] if the produce budget is exhausted.
    /// - [`CoreError::Invalid`] if any record fails validation.
    /// - [`CoreError::TopicNotFound`] if the topic is unknown.
    /// - [`CoreError::Timeout`] / [`CoreError::Port`] on storage failure.
    pub async fn produce(
        &self,
        topic: &TopicName,
        records: Vec<ProduceRecord>,
    ) -> Result<ProduceResult, CoreError> {
        if records.is_empty() {
            return Err(CoreError::EmptyBatch);
        }
        #[allow(clippy::cast_precision_loss)]
        if self.limiter.try_acquire_n(records.len() as f64).is_err() {
            metrics::counter!("connectforge_produce_rate_limited_total").increment(1);
            return Err(CoreError::RateLimited);
        }
        for r in &records {
            r.validate()?;
        }

        let count = self
            .partition_count(topic)
            .await?
            .ok_or_else(|| CoreError::TopicNotFound(topic.as_str().to_owned()))?;
        let partition = self.route(records.first().and_then(|r| r.key.as_deref()), count);

        let timestamp = Utc::now();
        let outcome = self
            .store_call(|s| {
                let s = Arc::clone(&s);
                let topic = topic.clone();
                let records = records.clone();
                async move { s.append(&topic, partition, records, timestamp).await }
            })
            .await?;

        let records = outcome.records;
        let base_offset = records.first().map_or(Offset::ZERO, |r| r.offset);
        let last_offset = records.last().map_or(Offset::ZERO, |r| r.offset);
        let result = ProduceResult {
            base_offset,
            last_offset,
            count: records.len(),
            truncated_to: outcome.truncated_to,
        };

        let _ = self
            .bus
            .publish(LogEvent::RecordsAppended {
                topic: topic.clone(),
                partition,
                base_offset,
                records: Arc::clone(&records),
            })
            .await;
        if let Some(new_start) = outcome.truncated_to {
            let _ = self
                .bus
                .publish(LogEvent::RecordsTruncated {
                    topic: topic.clone(),
                    partition,
                    new_start,
                })
                .await;
        }
        metrics::counter!("connectforge_records_produced_total").increment(result.count as u64);
        Ok(result)
    }

    /// Fetch records from a partition starting at `from` (query).
    ///
    /// # Errors
    /// - [`CoreError::TopicNotFound`] if the topic is unknown.
    /// - [`CoreError::PartitionOutOfRange`] if the partition does not exist.
    /// - [`CoreError::Timeout`] / [`CoreError::Port`] on storage failure.
    pub async fn fetch(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max: usize,
    ) -> Result<FetchResult, CoreError> {
        let count = self
            .partition_count(topic)
            .await?
            .ok_or_else(|| CoreError::TopicNotFound(topic.as_str().to_owned()))?;
        if partition.value() >= count {
            return Err(CoreError::PartitionOutOfRange {
                topic: topic.as_str().to_owned(),
                partition: partition.value(),
                count,
            });
        }
        let max = max.clamp(1, self.config.fetch_max_records);
        let result = self
            .store_call(|s| {
                let s = Arc::clone(&s);
                let topic = topic.clone();
                async move { s.fetch(&topic, partition, from, max).await }
            })
            .await?;
        metrics::counter!("connectforge_records_fetched_total")
            .increment(result.records.len() as u64);
        Ok(result)
    }

    /// List all topics (query).
    ///
    /// # Errors
    /// Returns a storage error if the topic listing cannot be read.
    pub async fn topics(&self) -> Result<Vec<TopicConfig>, CoreError> {
        self.store_call(|s| {
            let s = Arc::clone(&s);
            async move { s.topics().await }
        })
        .await
    }

    /// Aggregate broker statistics (query).
    ///
    /// # Errors
    /// Returns a storage error if statistics cannot be gathered.
    pub async fn stats(&self) -> Result<LogStats, CoreError> {
        self.store_call(|s| {
            let s = Arc::clone(&s);
            async move { s.stats().await }
        })
        .await
    }

    /// Subscribe to the live event stream, optionally filtered by topic.
    ///
    /// # Errors
    /// Returns a port error if the subscription cannot be established.
    pub async fn subscribe(&self, topics: Vec<TopicName>) -> Result<EventStream, CoreError> {
        Ok(self.bus.subscribe(topics).await?)
    }

    async fn partition_count(&self, topic: &TopicName) -> Result<Option<u32>, CoreError> {
        self.store_call(|s| {
            let s = Arc::clone(&s);
            let topic = topic.clone();
            async move { s.partition_count(&topic).await }
        })
        .await
    }

    fn route(&self, key: Option<&str>, count: u32) -> PartitionId {
        let slot = match key {
            Some(k) => fnv1a(k.as_bytes()) % u64::from(count),
            None => self.round_robin.fetch_add(1, Ordering::Relaxed) % u64::from(count),
        };
        PartitionId(slot as u32)
    }

    /// Run a storage operation under a timeout with jittered retries on
    /// transient failures.
    async fn store_call<T, F, Fut>(&self, op: F) -> Result<T, CoreError>
    where
        F: Fn(Arc<S>) -> Fut,
        Fut: std::future::Future<Output = Result<T, crate::error::PortError>>,
    {
        let store = Arc::clone(&self.store);
        let result = with_timeout(
            self.config.storage_timeout,
            retry_if(
                self.retry,
                || op(Arc::clone(&store)),
                super::error::PortError::is_retryable,
            ),
        )
        .await;
        match result {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(CoreError::Port(e)),
            Err(_) => Err(CoreError::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PortError;
    use crate::ports::{AppendOutcome, MockEventBus, MockLogStore};
    use connectforge_types::Record;

    fn topic() -> TopicName {
        TopicName::new("orders").unwrap()
    }

    fn engine(store: MockLogStore, bus: MockEventBus) -> LogEngine<MockLogStore, MockEventBus> {
        LogEngine::new(LogConfig::default(), Arc::new(store), Arc::new(bus))
    }

    #[tokio::test]
    async fn produce_appends_and_publishes() {
        let mut store = MockLogStore::new();
        store.expect_partition_count().returning(|_| Ok(Some(3)));
        store.expect_append().returning(|_, _, recs, ts| {
            let records: Vec<Record> = recs
                .into_iter()
                .enumerate()
                .map(|(i, r)| Record::from_produce(r, Offset(i as u64), ts))
                .collect();
            Ok(AppendOutcome {
                records: Arc::new(records),
                truncated_to: None,
            })
        });
        let mut bus = MockEventBus::new();
        bus.expect_publish().returning(|_| Ok(()));

        let engine = engine(store, bus);
        let rec = ProduceRecord::new(Some("k".into()), b"hello".to_vec()).unwrap();
        let out = engine.produce(&topic(), vec![rec]).await.unwrap();
        assert_eq!(out.count, 1);
        assert_eq!(out.base_offset, Offset(0));
    }

    #[tokio::test]
    async fn produce_empty_batch_is_rejected() {
        let engine = engine(MockLogStore::new(), MockEventBus::new());
        let err = engine.produce(&topic(), Vec::new()).await.unwrap_err();
        assert_eq!(err.code(), "EMPTY_BATCH");
    }

    #[tokio::test]
    async fn produce_unknown_topic_fails() {
        let mut store = MockLogStore::new();
        store.expect_partition_count().returning(|_| Ok(None));
        let engine = engine(store, MockEventBus::new());
        let rec = ProduceRecord::new(None, b"x".to_vec()).unwrap();
        let err = engine.produce(&topic(), vec![rec]).await.unwrap_err();
        assert_eq!(err.code(), "TOPIC_NOT_FOUND");
    }

    #[tokio::test]
    async fn fetch_rejects_out_of_range_partition() {
        let mut store = MockLogStore::new();
        store.expect_partition_count().returning(|_| Ok(Some(2)));
        let engine = engine(store, MockEventBus::new());
        let err = engine
            .fetch(&topic(), PartitionId(5), Offset::ZERO, 10)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "PARTITION_OUT_OF_RANGE");
    }

    #[tokio::test]
    async fn store_call_retries_transient_then_succeeds() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let mut store = MockLogStore::new();
        store.expect_partition_count().returning(|_| Ok(Some(1)));
        let calls = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&calls);
        store.expect_append().returning(move |_, _, recs, ts| {
            if c.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(PortError::Transient("flap".into()));
            }
            let records: Vec<Record> = recs
                .into_iter()
                .map(|r| Record::from_produce(r, Offset(0), ts))
                .collect();
            Ok(AppendOutcome {
                records: Arc::new(records),
                truncated_to: None,
            })
        });
        let mut bus = MockEventBus::new();
        bus.expect_publish().returning(|_| Ok(()));
        let engine = engine(store, bus);
        let rec = ProduceRecord::new(None, b"x".to_vec()).unwrap();
        let out = engine.produce(&topic(), vec![rec]).await.unwrap();
        assert_eq!(out.count, 1);
        assert!(calls.load(Ordering::SeqCst) >= 2);
    }
}
