//! A durable, segmented, on-disk implementation of [`LogStore`].
//!
//! Layout on disk:
//!
//! ```text
//! <root>/<topic>/topic.json          # serialized TopicConfig (for recovery)
//! <root>/<topic>/<partition>/<base>.log
//! ```
//!
//! Each partition is an ordered list of [`Segment`]s. Appends land on the
//! active (last) segment, which is rolled once it reaches the configured record
//! count; retention prunes whole segments from the head. On startup the store
//! recovers all topics/partitions/segments by scanning the directory tree, so
//! offsets and data survive restarts. All blocking file I/O runs inside
//! `spawn_blocking`, keeping the async executor free.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;

use connectforge_core::{AppendOutcome, LogStore, PortError};
use connectforge_types::{
    FetchResult, LogStats, Offset, PartitionId, ProduceRecord, Record, TopicConfig, TopicName,
};

use crate::segment::Segment;

fn io_err(e: std::io::Error) -> PortError {
    PortError::Transient(e.to_string())
}

/// Mutable state for a single partition.
struct PartitionState {
    dir: PathBuf,
    segments: Vec<Segment>,
    start_offset: u64,
    next_offset: u64,
}

impl PartitionState {
    fn total_records(&self) -> u64 {
        self.next_offset.saturating_sub(self.start_offset)
    }

    fn append_records(&mut self, records: &[Record], segment_max: u64) -> Result<(), PortError> {
        for rec in records {
            let roll = self
                .segments
                .last()
                .is_none_or(|s| s.len() as u64 >= segment_max);
            if roll {
                let seg = Segment::create(&self.dir, self.next_offset).map_err(io_err)?;
                self.segments.push(seg);
            }
            let seg = self
                .segments
                .last_mut()
                .expect("a segment exists after roll check");
            seg.append(rec).map_err(io_err)?;
            self.next_offset += 1;
        }
        Ok(())
    }

    /// Prune whole head segments until the retention bound is satisfied.
    /// Returns the new start offset if anything was pruned.
    fn apply_retention(&mut self, max_records: u64) -> Result<Option<Offset>, PortError> {
        if max_records == 0 {
            return Ok(None);
        }
        let mut pruned = false;
        while self.total_records() > max_records && self.segments.len() > 1 {
            let seg = self.segments.remove(0);
            seg.remove().map_err(io_err)?;
            self.start_offset = self
                .segments
                .first()
                .map_or(self.next_offset, Segment::base);
            pruned = true;
        }
        Ok(pruned.then_some(Offset(self.start_offset)))
    }

    fn fetch(&self, from: Offset, max: usize) -> Result<FetchResult, PortError> {
        let high = Offset(self.next_offset);
        let effective_from = from.value().max(self.start_offset);
        let mut out = Vec::new();
        let mut remaining = max;
        for seg in &self.segments {
            if remaining == 0 {
                break;
            }
            let seg_last = seg.base() + seg.len() as u64;
            if effective_from >= seg_last {
                continue;
            }
            let n = seg
                .read_from(effective_from, remaining, &mut out)
                .map_err(io_err)?;
            remaining -= n;
        }
        let next_offset = out
            .last()
            .map_or(Offset(effective_from), |r| r.offset.next());
        Ok(FetchResult {
            records: out,
            next_offset,
            high_watermark: high,
        })
    }
}

struct TopicState {
    config: TopicConfig,
    partitions: Vec<PartitionState>,
}

struct Inner {
    root: PathBuf,
    topics: BTreeMap<String, TopicState>,
    fetched_total: u64,
    produced_total: u64,
}

impl Inner {
    fn create_topic(&mut self, config: TopicConfig) -> Result<bool, PortError> {
        let name = config.name.as_str().to_owned();
        if self.topics.contains_key(&name) {
            return Ok(false);
        }
        let topic_dir = self.root.join(&name);
        std::fs::create_dir_all(&topic_dir).map_err(io_err)?;
        let meta =
            serde_json::to_vec_pretty(&config).map_err(|e| PortError::Permanent(e.to_string()))?;
        std::fs::write(topic_dir.join("topic.json"), meta).map_err(io_err)?;

        let mut partitions = Vec::with_capacity(config.partitions as usize);
        for p in 0..config.partitions {
            let dir = topic_dir.join(p.to_string());
            std::fs::create_dir_all(&dir).map_err(io_err)?;
            partitions.push(PartitionState {
                dir,
                segments: Vec::new(),
                start_offset: 0,
                next_offset: 0,
            });
        }
        self.topics.insert(name, TopicState { config, partitions });
        Ok(true)
    }

    fn append(
        &mut self,
        topic: &TopicName,
        partition: PartitionId,
        records: Vec<ProduceRecord>,
        timestamp: DateTime<Utc>,
    ) -> Result<AppendOutcome, PortError> {
        let state = self
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
        part.append_records(&materialized, segment_max)?;
        let truncated_to = part.apply_retention(max_records)?;
        self.produced_total += materialized.len() as u64;
        Ok(AppendOutcome {
            records: Arc::new(materialized),
            truncated_to,
        })
    }

    fn fetch(
        &mut self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max: usize,
    ) -> Result<FetchResult, PortError> {
        let state = self
            .topics
            .get(topic.as_str())
            .ok_or_else(|| PortError::NotFound(topic.as_str().to_owned()))?;
        let part = state
            .partitions
            .get(partition.value() as usize)
            .ok_or_else(|| PortError::NotFound(format!("{topic}/{partition}")))?;
        let result = part.fetch(from, max)?;
        self.fetched_total += result.records.len() as u64;
        Ok(result)
    }

    fn stats(&self) -> LogStats {
        let mut records = 0u64;
        let mut segments = 0u64;
        let mut partitions = 0u64;
        for t in self.topics.values() {
            partitions += t.partitions.len() as u64;
            for p in &t.partitions {
                records += p.total_records();
                segments += p.segments.len() as u64;
            }
        }
        LogStats {
            topics: self.topics.len() as u64,
            partitions,
            records,
            segments,
            produced_total: self.produced_total,
            fetched_total: self.fetched_total,
        }
    }

    fn recover(&mut self) -> Result<(), PortError> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(e) => e,
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io_err(e)),
        };
        for entry in entries {
            let entry = entry.map_err(io_err)?;
            if !entry.path().is_dir() {
                continue;
            }
            let meta_path = entry.path().join("topic.json");
            if !meta_path.exists() {
                continue;
            }
            let bytes = std::fs::read(&meta_path).map_err(io_err)?;
            let config: TopicConfig =
                serde_json::from_slice(&bytes).map_err(|e| PortError::Permanent(e.to_string()))?;
            let name = config.name.as_str().to_owned();
            let mut partitions = Vec::with_capacity(config.partitions as usize);
            for p in 0..config.partitions {
                let dir = entry.path().join(p.to_string());
                partitions.push(recover_partition(&dir)?);
            }
            self.topics.insert(name, TopicState { config, partitions });
        }
        Ok(())
    }
}

/// Rebuild a partition's segment list from its directory.
fn recover_partition(dir: &Path) -> Result<PartitionState, PortError> {
    std::fs::create_dir_all(dir).map_err(io_err)?;
    let mut bases = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("log") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(base) = stem.parse::<u64>() {
                    bases.push(base);
                }
            }
        }
    }
    bases.sort_unstable();
    let mut segments = Vec::with_capacity(bases.len());
    for base in &bases {
        segments.push(Segment::open(dir, *base).map_err(io_err)?);
    }
    let start_offset = segments.first().map_or(0, Segment::base);
    let next_offset = segments.last().map_or(0, |s| s.base() + s.len() as u64);
    Ok(PartitionState {
        dir: dir.to_path_buf(),
        segments,
        start_offset,
        next_offset,
    })
}

/// A durable, segmented file-backed log store.
pub struct FileLogStore {
    inner: Arc<Mutex<Inner>>,
}

impl FileLogStore {
    /// Open (or create) a store rooted at `root`, recovering any existing
    /// topics.
    ///
    /// # Errors
    /// Returns a [`PortError`] if the directory tree cannot be read or a
    /// `topic.json` is corrupt.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, PortError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(io_err)?;
        let mut inner = Inner {
            root,
            topics: BTreeMap::new(),
            fetched_total: 0,
            produced_total: 0,
        };
        inner.recover()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    async fn blocking<T, F>(&self, f: F) -> Result<T, PortError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Inner) -> Result<T, PortError> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock();
            f(&mut guard)
        })
        .await
        .map_err(|e| PortError::Transient(e.to_string()))?
    }
}

#[async_trait]
impl LogStore for FileLogStore {
    async fn create_topic(&self, config: TopicConfig) -> Result<bool, PortError> {
        self.blocking(move |inner| inner.create_topic(config)).await
    }

    async fn topics(&self) -> Result<Vec<TopicConfig>, PortError> {
        self.blocking(|inner| Ok(inner.topics.values().map(|t| t.config.clone()).collect()))
            .await
    }

    async fn partition_count(&self, topic: &TopicName) -> Result<Option<u32>, PortError> {
        let topic = topic.clone();
        self.blocking(move |inner| {
            Ok(inner
                .topics
                .get(topic.as_str())
                .map(|t| t.config.partitions))
        })
        .await
    }

    async fn append(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        records: Vec<ProduceRecord>,
        timestamp: DateTime<Utc>,
    ) -> Result<AppendOutcome, PortError> {
        let topic = topic.clone();
        self.blocking(move |inner| inner.append(&topic, partition, records, timestamp))
            .await
    }

    async fn fetch(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max: usize,
    ) -> Result<FetchResult, PortError> {
        let topic = topic.clone();
        self.blocking(move |inner| inner.fetch(&topic, partition, from, max))
            .await
    }

    async fn stats(&self) -> Result<LogStats, PortError> {
        self.blocking(|inner| Ok(inner.stats())).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connectforge_types::RetentionPolicy;
    use tempfile::tempdir;

    fn topic_config(name: &str, partitions: u32, seg_max: u64, max: u64) -> TopicConfig {
        TopicConfig::new(
            TopicName::new(name).unwrap(),
            partitions,
            RetentionPolicy {
                segment_max_records: seg_max,
                max_records: max,
            },
        )
        .unwrap()
    }

    fn produce(payload: &[u8]) -> ProduceRecord {
        ProduceRecord::new(None, payload.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn append_then_fetch_roundtrip() {
        let dir = tempdir().unwrap();
        let store = FileLogStore::open(dir.path()).unwrap();
        let cfg = topic_config("t", 1, 1000, 0);
        assert!(store.create_topic(cfg).await.unwrap());
        let topic = TopicName::new("t").unwrap();

        let batch = vec![produce(b"a"), produce(b"b"), produce(b"c")];
        let out = store
            .append(&topic, PartitionId(0), batch, Utc::now())
            .await
            .unwrap();
        assert_eq!(out.records.len(), 3);
        assert_eq!(out.records[0].offset, Offset(0));

        let res = store
            .fetch(&topic, PartitionId(0), Offset(1), 10)
            .await
            .unwrap();
        assert_eq!(res.records.len(), 2);
        assert_eq!(res.records[0].offset, Offset(1));
        assert_eq!(res.high_watermark, Offset(3));
        assert_eq!(res.next_offset, Offset(3));
    }

    #[tokio::test]
    async fn segments_roll_and_retention_prunes() {
        let dir = tempdir().unwrap();
        let store = FileLogStore::open(dir.path()).unwrap();
        // roll every 2 records, retain at most 4.
        store
            .create_topic(topic_config("t", 1, 2, 4))
            .await
            .unwrap();
        let topic = TopicName::new("t").unwrap();
        for i in 0..10u8 {
            store
                .append(&topic, PartitionId(0), vec![produce(&[i])], Utc::now())
                .await
                .unwrap();
        }
        let stats = store.stats().await.unwrap();
        assert!(stats.records <= 6, "retained {}", stats.records);
        assert_eq!(stats.produced_total, 10);
        // Earliest data was pruned; fetching from 0 clamps forward.
        let res = store
            .fetch(&topic, PartitionId(0), Offset(0), 100)
            .await
            .unwrap();
        assert_eq!(res.high_watermark, Offset(10));
        assert!(res.records.iter().all(|r| r.offset.value() >= 4));
    }

    #[tokio::test]
    async fn recovers_after_reopen() {
        let dir = tempdir().unwrap();
        {
            let store = FileLogStore::open(dir.path()).unwrap();
            store
                .create_topic(topic_config("t", 2, 2, 0))
                .await
                .unwrap();
            let topic = TopicName::new("t").unwrap();
            for i in 0..5u8 {
                store
                    .append(&topic, PartitionId(0), vec![produce(&[i])], Utc::now())
                    .await
                    .unwrap();
            }
        }
        let store = FileLogStore::open(dir.path()).unwrap();
        let topic = TopicName::new("t").unwrap();
        assert_eq!(store.partition_count(&topic).await.unwrap(), Some(2));
        let res = store
            .fetch(&topic, PartitionId(0), Offset(0), 100)
            .await
            .unwrap();
        assert_eq!(res.records.len(), 5);
        assert_eq!(res.high_watermark, Offset(5));
        // A further append continues from the recovered high-watermark.
        let out = store
            .append(&topic, PartitionId(0), vec![produce(b"z")], Utc::now())
            .await
            .unwrap();
        assert_eq!(out.records[0].offset, Offset(5));
    }

    #[tokio::test]
    async fn duplicate_topic_returns_false() {
        let dir = tempdir().unwrap();
        let store = FileLogStore::open(dir.path()).unwrap();
        assert!(store
            .create_topic(topic_config("t", 1, 8, 0))
            .await
            .unwrap());
        assert!(!store
            .create_topic(topic_config("t", 1, 8, 0))
            .await
            .unwrap());
    }
}
