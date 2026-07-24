//! An in-memory [`LogStore`] used as the default backend and in tests.
//!
//! It mirrors the semantics of [`crate::FileLogStore`] — dense per-partition
//! offsets, segment rolling, and head retention — but keeps records in memory
//! for zero-setup deployments, demos, and fast unit tests.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;

use connectforge_core::{AppendOutcome, LogStore, PortError};
use connectforge_types::{
    FetchResult, LogStats, Offset, PartitionId, ProduceRecord, Record, TopicConfig, TopicName,
};

#[derive(Default)]
struct Partition {
    records: Vec<Record>,
    start_offset: u64,
    next_offset: u64,
    segments: u64,
    records_in_active_segment: u64,
}

impl Partition {
    fn append(
        &mut self,
        records: Vec<Record>,
        segment_max: u64,
        max_records: u64,
    ) -> Option<Offset> {
        for rec in records {
            if self.segments == 0 || self.records_in_active_segment >= segment_max {
                self.segments += 1;
                self.records_in_active_segment = 0;
            }
            self.records_in_active_segment += 1;
            self.next_offset += 1;
            self.records.push(rec);
        }
        if max_records == 0 {
            return None;
        }
        let mut pruned = false;
        while (self.next_offset - self.start_offset) > max_records && self.records.len() > 1 {
            self.records.remove(0);
            self.start_offset += 1;
            pruned = true;
        }
        pruned.then_some(Offset(self.start_offset))
    }

    fn fetch(&self, from: Offset, max: usize) -> FetchResult {
        let effective_from = from.value().max(self.start_offset);
        let records: Vec<Record> = self
            .records
            .iter()
            .filter(|r| r.offset.value() >= effective_from)
            .take(max)
            .cloned()
            .collect();
        let next_offset = records
            .last()
            .map_or(Offset(effective_from), |r| r.offset.next());
        FetchResult {
            records,
            next_offset,
            high_watermark: Offset(self.next_offset),
        }
    }
}

struct Topic {
    config: TopicConfig,
    partitions: Vec<Partition>,
}

#[derive(Default)]
struct Inner {
    topics: BTreeMap<String, Topic>,
    produced_total: u64,
    fetched_total: u64,
}

/// A sharded, lock-guarded in-memory log store.
#[derive(Default)]
pub struct MemoryLogStore {
    inner: Mutex<Inner>,
}

impl MemoryLogStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LogStore for MemoryLogStore {
    async fn create_topic(&self, config: TopicConfig) -> Result<bool, PortError> {
        let mut inner = self.inner.lock();
        let name = config.name.as_str().to_owned();
        if inner.topics.contains_key(&name) {
            return Ok(false);
        }
        let partitions = (0..config.partitions)
            .map(|_| Partition::default())
            .collect();
        inner.topics.insert(name, Topic { config, partitions });
        Ok(true)
    }

    async fn topics(&self) -> Result<Vec<TopicConfig>, PortError> {
        Ok(self
            .inner
            .lock()
            .topics
            .values()
            .map(|t| t.config.clone())
            .collect())
    }

    async fn partition_count(&self, topic: &TopicName) -> Result<Option<u32>, PortError> {
        Ok(self
            .inner
            .lock()
            .topics
            .get(topic.as_str())
            .map(|t| t.config.partitions))
    }

    async fn append(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        records: Vec<ProduceRecord>,
        timestamp: DateTime<Utc>,
    ) -> Result<AppendOutcome, PortError> {
        let mut inner = self.inner.lock();
        let count = records.len() as u64;
        let state = inner
            .topics
            .get_mut(topic.as_str())
            .ok_or_else(|| PortError::NotFound(topic.as_str().to_owned()))?;
        let segment_max = state.config.retention.segment_max_records;
        let max_records = state.config.retention.max_records;
        let part = state
            .partitions
            .get_mut(partition.value() as usize)
            .ok_or_else(|| PortError::NotFound(format!("{topic}/{partition}")))?;

        let mut materialized = Vec::with_capacity(records.len());
        let mut next = part.next_offset;
        for r in records {
            materialized.push(Record::from_produce(r, Offset(next), timestamp));
            next += 1;
        }
        let truncated_to = part.append(materialized.clone(), segment_max, max_records);
        inner.produced_total += count;
        Ok(AppendOutcome {
            records: Arc::new(materialized),
            truncated_to,
        })
    }

    async fn fetch(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max: usize,
    ) -> Result<FetchResult, PortError> {
        let mut inner = self.inner.lock();
        let state = inner
            .topics
            .get(topic.as_str())
            .ok_or_else(|| PortError::NotFound(topic.as_str().to_owned()))?;
        let part = state
            .partitions
            .get(partition.value() as usize)
            .ok_or_else(|| PortError::NotFound(format!("{topic}/{partition}")))?;
        let result = part.fetch(from, max);
        inner.fetched_total += result.records.len() as u64;
        Ok(result)
    }

    async fn stats(&self) -> Result<LogStats, PortError> {
        let inner = self.inner.lock();
        let mut records = 0u64;
        let mut segments = 0u64;
        let mut partitions = 0u64;
        for t in inner.topics.values() {
            partitions += t.partitions.len() as u64;
            for p in &t.partitions {
                records += p.next_offset - p.start_offset;
                segments += p.segments;
            }
        }
        Ok(LogStats {
            topics: inner.topics.len() as u64,
            partitions,
            records,
            segments,
            produced_total: inner.produced_total,
            fetched_total: inner.fetched_total,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connectforge_types::RetentionPolicy;

    fn cfg(name: &str) -> TopicConfig {
        TopicConfig::new(TopicName::new(name).unwrap(), 2, RetentionPolicy::default()).unwrap()
    }

    fn pr(p: &[u8]) -> ProduceRecord {
        ProduceRecord::new(None, p.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn append_and_fetch() {
        let store = MemoryLogStore::new();
        assert!(store.create_topic(cfg("t")).await.unwrap());
        assert!(!store.create_topic(cfg("t")).await.unwrap());
        let topic = TopicName::new("t").unwrap();
        store
            .append(&topic, PartitionId(0), vec![pr(b"a"), pr(b"b")], Utc::now())
            .await
            .unwrap();
        let res = store
            .fetch(&topic, PartitionId(0), Offset(0), 10)
            .await
            .unwrap();
        assert_eq!(res.records.len(), 2);
        assert_eq!(res.high_watermark, Offset(2));
        let stats = store.stats().await.unwrap();
        assert_eq!(stats.topics, 1);
        assert_eq!(stats.partitions, 2);
        assert_eq!(stats.records, 2);
    }

    #[tokio::test]
    async fn retention_prunes_head() {
        let store = MemoryLogStore::new();
        let cfg = TopicConfig::new(
            TopicName::new("t").unwrap(),
            1,
            RetentionPolicy {
                segment_max_records: 2,
                max_records: 3,
            },
        )
        .unwrap();
        store.create_topic(cfg).await.unwrap();
        let topic = TopicName::new("t").unwrap();
        for i in 0..6u8 {
            store
                .append(&topic, PartitionId(0), vec![pr(&[i])], Utc::now())
                .await
                .unwrap();
        }
        let res = store
            .fetch(&topic, PartitionId(0), Offset(0), 100)
            .await
            .unwrap();
        assert!(res.records.len() <= 3);
        assert_eq!(res.high_watermark, Offset(6));
    }

    #[tokio::test]
    async fn unknown_topic_errors() {
        let store = MemoryLogStore::new();
        let topic = TopicName::new("missing").unwrap();
        let err = store
            .append(&topic, PartitionId(0), vec![pr(b"x")], Utc::now())
            .await
            .unwrap_err();
        assert!(matches!(err, PortError::NotFound(_)));
    }
}
