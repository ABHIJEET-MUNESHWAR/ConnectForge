//! Result and statistics types returned by produce/fetch operations.

use serde::{Deserialize, Serialize};

use crate::record::Record;
use crate::units::Offset;

/// The outcome of a successful produce (append) operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProduceResult {
    /// Offset assigned to the first appended record.
    pub base_offset: Offset,
    /// Offset assigned to the last appended record.
    pub last_offset: Offset,
    /// Number of records appended.
    pub count: usize,
    /// If retention pruned the head, the new earliest retained offset.
    pub truncated_to: Option<Offset>,
}

/// The outcome of a fetch (read) operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchResult {
    /// The records read, in ascending offset order.
    pub records: Vec<Record>,
    /// The next offset a consumer should request to continue tailing.
    pub next_offset: Offset,
    /// The partition's current high-watermark (one past the last committed
    /// record).
    pub high_watermark: Offset,
}

impl FetchResult {
    /// An empty result positioned at `high_watermark`.
    #[must_use]
    pub fn empty(high_watermark: Offset) -> Self {
        Self {
            records: Vec::new(),
            next_offset: high_watermark,
            high_watermark,
        }
    }
}

/// Per-partition statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PartitionStats {
    /// Partition index.
    pub partition: u32,
    /// Earliest retained offset (advances as retention prunes segments).
    pub start_offset: u64,
    /// One past the last committed record.
    pub high_watermark: u64,
    /// Number of on-disk/in-memory segments.
    pub segments: u64,
}

/// Engine-wide statistics, aggregated across all topics and partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LogStats {
    /// Number of topics.
    pub topics: u64,
    /// Total number of partitions across all topics.
    pub partitions: u64,
    /// Total committed records currently retained.
    pub records: u64,
    /// Total segments across all partitions.
    pub segments: u64,
    /// Cumulative records ever produced (including pruned ones).
    pub produced_total: u64,
    /// Cumulative records ever served via fetch.
    pub fetched_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_fetch_positions_at_high_watermark() {
        let r = FetchResult::empty(Offset(9));
        assert!(r.records.is_empty());
        assert_eq!(r.next_offset, Offset(9));
        assert_eq!(r.high_watermark, Offset(9));
    }
}
