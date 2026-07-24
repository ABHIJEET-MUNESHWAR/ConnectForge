//! Topic configuration, retention policy, and partition routing.

use serde::{Deserialize, Serialize};

use crate::error::InvalidRecord;
use crate::units::{PartitionId, TopicName};

/// How much data a partition retains before the oldest segments are pruned.
///
/// Retention is expressed in whole records; whenever a partition holds more
/// than `max_records` committed records, entire trailing-from-the-head segments
/// are deleted until the bound is satisfied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Roll a new segment once the active one reaches this many records.
    pub segment_max_records: u64,
    /// Retain at most this many records per partition (0 = unbounded).
    pub max_records: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            segment_max_records: 1024,
            max_records: 1_000_000,
        }
    }
}

impl RetentionPolicy {
    /// Validate the policy.
    ///
    /// # Errors
    /// Returns [`InvalidRecord::ZeroSegmentSize`] if the segment roll size is
    /// zero, which would otherwise cause unbounded segment creation.
    pub fn validate(&self) -> Result<(), InvalidRecord> {
        if self.segment_max_records == 0 {
            return Err(InvalidRecord::ZeroSegmentSize);
        }
        Ok(())
    }
}

/// Immutable configuration for a topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfig {
    /// The topic's validated name.
    pub name: TopicName,
    /// Number of partitions (must be >= 1).
    pub partitions: u32,
    /// Retention/segmentation policy applied to every partition.
    pub retention: RetentionPolicy,
}

impl TopicConfig {
    /// Construct and validate a topic configuration.
    ///
    /// # Errors
    /// Returns [`InvalidRecord::ZeroPartitions`] if `partitions` is zero, or a
    /// retention error if the policy is invalid.
    pub fn new(
        name: TopicName,
        partitions: u32,
        retention: RetentionPolicy,
    ) -> Result<Self, InvalidRecord> {
        if partitions == 0 {
            return Err(InvalidRecord::ZeroPartitions);
        }
        retention.validate()?;
        Ok(Self {
            name,
            partitions,
            retention,
        })
    }

    /// Deterministically map an optional key to a partition.
    ///
    /// Keyed records use a stable FNV-1a hash so that a given key always lands
    /// on the same partition (ordering guarantee). Keyless records are spread
    /// by the caller-supplied `round_robin` counter.
    #[must_use]
    pub fn partition_for(&self, key: Option<&str>, round_robin: u64) -> PartitionId {
        let slot = match key {
            Some(k) => fnv1a(k.as_bytes()) % u64::from(self.partitions),
            None => round_robin % u64::from(self.partitions),
        };
        PartitionId(slot as u32)
    }
}

/// FNV-1a 64-bit hash — small, dependency-free, and stable across runs.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(partitions: u32) -> TopicConfig {
        TopicConfig::new(
            TopicName::new("t").unwrap(),
            partitions,
            RetentionPolicy::default(),
        )
        .unwrap()
    }

    #[test]
    fn rejects_zero_partitions() {
        assert_eq!(
            TopicConfig::new(TopicName::new("t").unwrap(), 0, RetentionPolicy::default()),
            Err(InvalidRecord::ZeroPartitions)
        );
    }

    #[test]
    fn rejects_zero_segment_size() {
        let policy = RetentionPolicy {
            segment_max_records: 0,
            max_records: 10,
        };
        assert_eq!(policy.validate(), Err(InvalidRecord::ZeroSegmentSize));
    }

    #[test]
    fn keyed_routing_is_stable_and_in_range() {
        let cfg = cfg(8);
        let a = cfg.partition_for(Some("user-42"), 0);
        let b = cfg.partition_for(Some("user-42"), 999);
        assert_eq!(a, b, "same key must map to same partition");
        assert!(a.value() < 8);
    }

    #[test]
    fn keyless_routing_uses_round_robin() {
        let cfg = cfg(4);
        assert_eq!(cfg.partition_for(None, 5).value(), 1);
        assert_eq!(cfg.partition_for(None, 6).value(), 2);
    }
}
