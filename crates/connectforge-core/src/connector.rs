//! The connector runtime: at-least-once sink delivery with offset
//! checkpointing and a dead-letter queue, plus a source poll→append loop.
//!
//! A [`SinkRuntime`] reads committed records from a [`LogStore`], delivers each
//! outward through a [`Sink`] with bounded retries, routes records that exhaust
//! their retries to a [`DeadLetterSink`], and persists a [`Checkpoint`] so a
//! restart resumes exactly where it left off. A [`SourceRuntime`] polls a
//! [`Source`] and appends the produced records to a topic.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use connectforge_resilience::{retry_if, RetryPolicy};
use connectforge_types::{
    Checkpoint, ConnectorId, ConnectorKind, ConnectorStatus, DeadLetterRecord, DeliveryGuarantee,
    DeliveryReport, Offset, PartitionId, Record, TopicName,
};
use parking_lot::Mutex;

use crate::error::{CoreError, PortError};
use crate::ports::LogStore;

/// A sink adapter: delivers records to an external system.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Sink: Send + Sync + 'static {
    /// Deliver a batch of records. Returning a retryable [`PortError`] triggers
    /// bounded retries; a permanent error dead-letters immediately.
    async fn deliver(&self, records: &[Record]) -> Result<(), PortError>;
}

/// A source adapter: polls an external system for new records.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// Poll for the next batch of records (an empty vec means "no data yet").
    async fn poll(&self) -> Result<Vec<connectforge_types::ProduceRecord>, PortError>;
}

/// Durable storage for connector checkpoints (resumable offsets).
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait CheckpointStore: Send + Sync + 'static {
    /// Load the saved next-offset for a connector's `(topic, partition)`.
    async fn load(
        &self,
        connector: &ConnectorId,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Result<Option<Offset>, PortError>;

    /// Persist a checkpoint durably.
    async fn save(&self, checkpoint: Checkpoint) -> Result<(), PortError>;
}

/// A dead-letter destination for records that fail delivery.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait DeadLetterSink: Send + Sync + 'static {
    /// Store a record that could not be delivered.
    async fn dead_letter(&self, record: DeadLetterRecord) -> Result<(), PortError>;
}

/// Configuration for a sink connector runtime.
#[derive(Debug, Clone, Copy)]
pub struct SinkConfig {
    /// Delivery guarantee.
    pub guarantee: DeliveryGuarantee,
    /// Maximum delivery attempts before dead-lettering (>= 1).
    pub max_attempts: u32,
    /// Maximum records to read per poll round.
    pub batch_size: usize,
    /// Base retry backoff.
    pub base_backoff: Duration,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            max_attempts: 3,
            batch_size: 500,
            base_backoff: Duration::from_millis(20),
        }
    }
}

/// Drives a single sink connector over one `(topic, partition)`.
pub struct SinkRuntime<St, Sk, Cp, Dl>
where
    St: LogStore,
    Sk: Sink,
    Cp: CheckpointStore,
    Dl: DeadLetterSink,
{
    id: ConnectorId,
    topic: TopicName,
    partition: PartitionId,
    config: SinkConfig,
    store: Arc<St>,
    sink: Arc<Sk>,
    checkpoints: Arc<Cp>,
    dlq: Arc<Dl>,
    processed: AtomicU64,
    dead: AtomicU64,
    cursor: Mutex<Option<Offset>>,
}

impl<St, Sk, Cp, Dl> SinkRuntime<St, Sk, Cp, Dl>
where
    St: LogStore,
    Sk: Sink,
    Cp: CheckpointStore,
    Dl: DeadLetterSink,
{
    /// Assemble a sink runtime from its config and ports.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ConnectorId,
        topic: TopicName,
        partition: PartitionId,
        config: SinkConfig,
        store: Arc<St>,
        sink: Arc<Sk>,
        checkpoints: Arc<Cp>,
        dlq: Arc<Dl>,
    ) -> Self {
        Self {
            id,
            topic,
            partition,
            config,
            store,
            sink,
            checkpoints,
            dlq,
            processed: AtomicU64::new(0),
            dead: AtomicU64::new(0),
            cursor: Mutex::new(None),
        }
    }

    /// Total records delivered so far.
    #[must_use]
    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }

    /// Total records dead-lettered so far.
    #[must_use]
    pub fn dead_lettered(&self) -> u64 {
        self.dead.load(Ordering::Relaxed)
    }

    /// The connector's current status snapshot.
    pub async fn status(&self) -> Result<ConnectorStatus, CoreError> {
        Ok(ConnectorStatus {
            id: self.id.clone(),
            kind: ConnectorKind::Sink,
            guarantee: self.config.guarantee,
            running: true,
            processed: self.processed(),
            dead_lettered: self.dead_lettered(),
            checkpoint: self.resolve_checkpoint().await?,
        })
    }

    /// Process one batch: read → deliver (with retry) → dead-letter failures →
    /// checkpoint. Returns a [`DeliveryReport`]. Call repeatedly to drain a
    /// partition; a `polled == 0` report means the connector is caught up.
    ///
    /// # Errors
    /// - [`CoreError::Port`] if reading, checkpointing, or dead-lettering fails.
    pub async fn run_once(&self) -> Result<DeliveryReport, CoreError> {
        let start = self.resolve_checkpoint().await?;
        let batch = self
            .store
            .fetch(&self.topic, self.partition, start, self.config.batch_size)
            .await?;

        if batch.records.is_empty() {
            return Ok(DeliveryReport {
                polled: 0,
                delivered: 0,
                dead_lettered: 0,
                checkpoint: start,
            });
        }

        let polled = batch.records.len() as u64;
        let next = batch.records.last().map_or(start, |r| r.offset.next());

        // At-most-once advances the checkpoint before delivery.
        if self.config.guarantee == DeliveryGuarantee::AtMostOnce {
            self.commit_checkpoint(next).await?;
        }

        let mut delivered = 0u64;
        let mut dead = 0u64;
        for record in &batch.records {
            match self.deliver_one(record).await {
                Ok(()) => {
                    delivered += 1;
                    self.processed.fetch_add(1, Ordering::Relaxed);
                    metrics::counter!(
                        "connectforge_records_delivered_total",
                        "connector" => self.id.as_str().to_owned()
                    )
                    .increment(1);
                }
                Err(reason) => {
                    self.dead_letter(record, reason).await?;
                    dead += 1;
                    self.dead.fetch_add(1, Ordering::Relaxed);
                    metrics::counter!(
                        "connectforge_records_dead_lettered_total",
                        "connector" => self.id.as_str().to_owned()
                    )
                    .increment(1);
                }
            }
        }

        // At-least-once advances only after every record is delivered or
        // dead-lettered, so a crash re-delivers rather than drops.
        if self.config.guarantee == DeliveryGuarantee::AtLeastOnce {
            self.commit_checkpoint(next).await?;
        }

        Ok(DeliveryReport {
            polled,
            delivered,
            dead_lettered: dead,
            checkpoint: next,
        })
    }

    /// Deliver a single record, retrying retryable failures up to the
    /// configured attempt bound. Returns the failure reason on exhaustion.
    async fn deliver_one(&self, record: &Record) -> Result<(), String> {
        let policy = RetryPolicy {
            max_attempts: self.config.max_attempts.max(1),
            base_delay: self.config.base_backoff,
            max_delay: Duration::from_secs(1),
        };
        let slice = std::slice::from_ref(record);
        retry_if(policy, || self.sink.deliver(slice), PortError::is_retryable)
            .await
            .map_err(|e| e.to_string())
    }

    async fn dead_letter(&self, record: &Record, reason: String) -> Result<(), CoreError> {
        let dlq_record = DeadLetterRecord {
            connector: self.id.clone(),
            topic: self.topic.clone(),
            partition: self.partition,
            offset: record.offset,
            payload: record.payload.clone(),
            reason,
            attempts: self.config.max_attempts.max(1),
            failed_at: Utc::now(),
        };
        self.dlq.dead_letter(dlq_record).await?;
        Ok(())
    }

    async fn resolve_checkpoint(&self) -> Result<Offset, CoreError> {
        if let Some(cached) = *self.cursor.lock() {
            return Ok(cached);
        }
        let loaded = self
            .checkpoints
            .load(&self.id, &self.topic, self.partition)
            .await?
            .unwrap_or(Offset::ZERO);
        *self.cursor.lock() = Some(loaded);
        Ok(loaded)
    }

    async fn commit_checkpoint(&self, offset: Offset) -> Result<(), CoreError> {
        self.checkpoints
            .save(Checkpoint {
                connector: self.id.clone(),
                topic: self.topic.clone(),
                partition: self.partition,
                offset,
            })
            .await?;
        *self.cursor.lock() = Some(offset);
        Ok(())
    }
}

/// Drives a single source connector: poll → append to a topic partition.
pub struct SourceRuntime<St, Sr>
where
    St: LogStore,
    Sr: Source,
{
    id: ConnectorId,
    topic: TopicName,
    partition: PartitionId,
    store: Arc<St>,
    source: Arc<Sr>,
    produced: AtomicU64,
}

impl<St, Sr> SourceRuntime<St, Sr>
where
    St: LogStore,
    Sr: Source,
{
    /// Assemble a source runtime.
    #[must_use]
    pub fn new(
        id: ConnectorId,
        topic: TopicName,
        partition: PartitionId,
        store: Arc<St>,
        source: Arc<Sr>,
    ) -> Self {
        Self {
            id,
            topic,
            partition,
            store,
            source,
            produced: AtomicU64::new(0),
        }
    }

    /// Total records produced so far.
    #[must_use]
    pub fn produced(&self) -> u64 {
        self.produced.load(Ordering::Relaxed)
    }

    /// Poll the source once and append whatever it returns. Returns the number
    /// of records produced this round (0 = source is idle).
    ///
    /// # Errors
    /// - [`CoreError::Port`] if the source poll or the append fails.
    pub async fn run_once(&self) -> Result<u64, CoreError> {
        let records = self.source.poll().await?;
        if records.is_empty() {
            return Ok(0);
        }
        let count = records.len() as u64;
        self.store
            .append(&self.topic, self.partition, records, Utc::now())
            .await?;
        self.produced.fetch_add(count, Ordering::Relaxed);
        metrics::counter!(
            "connectforge_records_sourced_total",
            "connector" => self.id.as_str().to_owned()
        )
        .increment(count);
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::{AppendOutcome, MockLogStore};
    use connectforge_types::{FetchResult, ProduceRecord};

    fn record(offset: u64) -> Record {
        Record {
            offset: Offset(offset),
            timestamp: Utc::now(),
            key: None,
            payload: vec![offset as u8],
        }
    }

    fn store_with(records: Vec<Record>) -> Arc<MockLogStore> {
        let mut store = MockLogStore::new();
        let records = Arc::new(records);
        store.expect_fetch().returning(move |_t, _p, from, max| {
            let recs: Vec<Record> = records
                .iter()
                .filter(|r| r.offset.value() >= from.value())
                .take(max)
                .cloned()
                .collect();
            let next = recs.last().map_or(from, |r| r.offset.next());
            Ok(FetchResult {
                records: recs,
                next_offset: next,
                high_watermark: next,
            })
        });
        Arc::new(store)
    }

    fn checkpoints() -> Arc<MockCheckpointStore> {
        let mut cp = MockCheckpointStore::new();
        cp.expect_load().returning(|_, _, _| Ok(None));
        cp.expect_save().returning(|_| Ok(()));
        Arc::new(cp)
    }

    fn id() -> ConnectorId {
        ConnectorId::new("sink-1").unwrap()
    }

    fn topic() -> TopicName {
        TopicName::new("events").unwrap()
    }

    #[tokio::test]
    async fn delivers_all_records_and_checkpoints() {
        let store = store_with(vec![record(0), record(1), record(2)]);
        let mut sink = MockSink::new();
        sink.expect_deliver().returning(|_| Ok(()));
        let mut dlq = MockDeadLetterSink::new();
        dlq.expect_dead_letter().never();

        let runtime = SinkRuntime::new(
            id(),
            topic(),
            PartitionId(0),
            SinkConfig::default(),
            store,
            Arc::new(sink),
            checkpoints(),
            Arc::new(dlq),
        );
        let report = runtime.run_once().await.unwrap();
        assert_eq!(report.polled, 3);
        assert_eq!(report.delivered, 3);
        assert_eq!(report.dead_lettered, 0);
        assert_eq!(report.checkpoint, Offset(3));
    }

    #[tokio::test]
    async fn permanent_failure_dead_letters() {
        let store = store_with(vec![record(0)]);
        let mut sink = MockSink::new();
        sink.expect_deliver()
            .returning(|_| Err(PortError::Permanent("bad record".into())));
        let mut dlq = MockDeadLetterSink::new();
        dlq.expect_dead_letter().times(1).returning(|_| Ok(()));

        let runtime = SinkRuntime::new(
            id(),
            topic(),
            PartitionId(0),
            SinkConfig::default(),
            store,
            Arc::new(sink),
            checkpoints(),
            Arc::new(dlq),
        );
        let report = runtime.run_once().await.unwrap();
        assert_eq!(report.delivered, 0);
        assert_eq!(report.dead_lettered, 1);
        assert_eq!(runtime.dead_lettered(), 1);
    }

    #[tokio::test]
    async fn retries_transient_then_succeeds() {
        use std::sync::atomic::{AtomicU32, Ordering as O};
        let store = store_with(vec![record(0)]);
        let attempts = Arc::new(AtomicU32::new(0));
        let a = Arc::clone(&attempts);
        let mut sink = MockSink::new();
        sink.expect_deliver().returning(move |_| {
            if a.fetch_add(1, O::Relaxed) == 0 {
                Err(PortError::Transient("flaky".into()))
            } else {
                Ok(())
            }
        });
        let mut dlq = MockDeadLetterSink::new();
        dlq.expect_dead_letter().never();

        let cfg = SinkConfig {
            base_backoff: Duration::from_millis(1),
            ..SinkConfig::default()
        };
        let runtime = SinkRuntime::new(
            id(),
            topic(),
            PartitionId(0),
            cfg,
            store,
            Arc::new(sink),
            checkpoints(),
            Arc::new(dlq),
        );
        let report = runtime.run_once().await.unwrap();
        assert_eq!(report.delivered, 1);
        assert!(attempts.load(O::Relaxed) >= 2);
    }

    #[tokio::test]
    async fn empty_partition_reports_zero() {
        let store = store_with(vec![]);
        let mut sink = MockSink::new();
        sink.expect_deliver().never();
        let dlq = {
            let mut d = MockDeadLetterSink::new();
            d.expect_dead_letter().never();
            Arc::new(d)
        };
        let runtime = SinkRuntime::new(
            id(),
            topic(),
            PartitionId(0),
            SinkConfig::default(),
            store,
            Arc::new(sink),
            checkpoints(),
            dlq,
        );
        let report = runtime.run_once().await.unwrap();
        assert_eq!(report.polled, 0);
    }

    #[tokio::test]
    async fn source_polls_and_appends() {
        let mut store = MockLogStore::new();
        store.expect_append().returning(|_t, _p, recs, ts| {
            let records: Vec<Record> = recs
                .into_iter()
                .enumerate()
                .map(|(i, r)| Record {
                    offset: Offset(i as u64),
                    timestamp: ts,
                    key: r.key,
                    payload: r.payload,
                })
                .collect();
            Ok(AppendOutcome {
                records: Arc::new(records),
                truncated_to: None,
            })
        });
        let mut source = MockSource::new();
        source.expect_poll().returning(|| {
            Ok(vec![
                ProduceRecord::new(None, vec![1]).unwrap(),
                ProduceRecord::new(None, vec![2]).unwrap(),
            ])
        });
        let runtime = SourceRuntime::new(
            ConnectorId::new("src-1").unwrap(),
            topic(),
            PartitionId(0),
            Arc::new(store),
            Arc::new(source),
        );
        let n = runtime.run_once().await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(runtime.produced(), 2);
    }
}
