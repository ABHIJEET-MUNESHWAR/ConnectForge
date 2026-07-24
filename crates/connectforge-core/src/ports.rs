//! Ports (hexagonal interfaces) the engine depends on.
//!
//! The engine is written against these traits, never against a concrete
//! adapter, so storage and transport can be swapped (in-memory ↔ segmented
//! file ↔ future networked replicas) and mocked in tests.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use std::sync::Arc;

use connectforge_types::{
    FetchResult, LogStats, Offset, PartitionId, ProduceRecord, Record, TopicConfig, TopicName,
};

use crate::error::PortError;
use crate::event::LogEvent;

/// What a durable append returns: the materialized records (with assigned
/// offsets and timestamps) plus the new head offset if retention pruned data.
#[derive(Debug, Clone)]
pub struct AppendOutcome {
    /// The stored records, shared cheaply for bus fan-out.
    pub records: Arc<Vec<Record>>,
    /// If retention removed segments, the new earliest retained offset.
    pub truncated_to: Option<Offset>,
}

/// The durable storage port: an append-only, partitioned, segmented log.
///
/// The store owns offset assignment and the high-watermark so that offsets
/// survive restarts.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Create a topic. Must be idempotent-safe: return `false` if it existed.
    async fn create_topic(&self, config: TopicConfig) -> Result<bool, PortError>;

    /// List every topic's configuration.
    async fn topics(&self) -> Result<Vec<TopicConfig>, PortError>;

    /// The partition count for a topic, or `None` if the topic is unknown.
    async fn partition_count(&self, topic: &TopicName) -> Result<Option<u32>, PortError>;

    /// Append a batch of records to a partition, assigning sequential offsets
    /// and the supplied `timestamp`, then apply retention.
    async fn append(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        records: Vec<ProduceRecord>,
        timestamp: DateTime<Utc>,
    ) -> Result<AppendOutcome, PortError>;

    /// Read up to `max` records from `from` (inclusive) onward.
    async fn fetch(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max: usize,
    ) -> Result<FetchResult, PortError>;

    /// Aggregate broker statistics.
    async fn stats(&self) -> Result<LogStats, PortError>;
}

/// Blanket forwarding impl so an `Arc<dyn LogStore>` (or any `Arc<T>`) is itself
/// a [`LogStore`]. This lets the composition root pick a concrete store
/// (in-memory or durable file) at runtime behind a single erased type while the
/// engine stays generic and monomorphized.
#[async_trait]
impl<T: LogStore + ?Sized> LogStore for Arc<T> {
    async fn create_topic(&self, config: TopicConfig) -> Result<bool, PortError> {
        (**self).create_topic(config).await
    }

    async fn topics(&self) -> Result<Vec<TopicConfig>, PortError> {
        (**self).topics().await
    }

    async fn partition_count(&self, topic: &TopicName) -> Result<Option<u32>, PortError> {
        (**self).partition_count(topic).await
    }

    async fn append(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        records: Vec<ProduceRecord>,
        timestamp: DateTime<Utc>,
    ) -> Result<AppendOutcome, PortError> {
        (**self).append(topic, partition, records, timestamp).await
    }

    async fn fetch(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max: usize,
    ) -> Result<FetchResult, PortError> {
        (**self).fetch(topic, partition, from, max).await
    }

    async fn stats(&self) -> Result<LogStats, PortError> {
        (**self).stats().await
    }
}

/// A live stream of [`LogEvent`]s.
pub type EventStream = BoxStream<'static, LogEvent>;

/// The event-bus port: publish control/data events and subscribe to them.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EventBus: Send + Sync + 'static {
    /// Publish an event to all current subscribers.
    async fn publish(&self, event: LogEvent) -> Result<(), PortError>;

    /// Subscribe to events, optionally filtered to a set of topics (empty =
    /// all topics).
    async fn subscribe(&self, topics: Vec<TopicName>) -> Result<EventStream, PortError>;
}
