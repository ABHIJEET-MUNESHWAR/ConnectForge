//! Strongly-typed newtypes for the commit-log domain.
//!
//! Every identifier and quantity that flows through ConnectForge is wrapped in a
//! newtype so that invalid states are unrepresentable and units can never be
//! transposed at a call site (an [`Offset`] can never be passed where a
//! [`PartitionId`] is expected).

use serde::{Deserialize, Serialize};

use crate::error::InvalidRecord;

/// Maximum length, in bytes, of a topic name.
pub const MAX_TOPIC_NAME_LEN: usize = 255;

/// Maximum size, in bytes, of a single record payload (1 MiB).
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

/// A monotonically increasing position within a single partition's log.
///
/// Offsets start at zero and are dense: every appended record consumes exactly
/// one offset.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Offset(pub u64);

impl Offset {
    /// The very first offset in any partition.
    pub const ZERO: Self = Self(0);

    /// The raw offset value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// The next sequential offset.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Saturating distance from an earlier offset to this one.
    #[must_use]
    pub const fn saturating_sub(self, other: Self) -> u64 {
        self.0.saturating_sub(other.0)
    }
}

impl std::fmt::Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A zero-based partition index within a topic.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct PartitionId(pub u32);

impl PartitionId {
    /// The raw partition index.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The base offset that identifies a log segment.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct SegmentId(pub u64);

impl SegmentId {
    /// The raw base offset of the segment.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for SegmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:020}", self.0)
    }
}

/// A validated topic name.
///
/// Names must be non-empty, at most [`MAX_TOPIC_NAME_LEN`] bytes, and contain
/// only ASCII alphanumerics plus `-`, `_`, and `.` so they can safely be used
/// as on-disk directory names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TopicName(String);

impl TopicName {
    /// Validate and construct a topic name.
    ///
    /// # Errors
    /// Returns [`InvalidRecord`] if the name is empty, too long, or contains
    /// characters outside the allowed set.
    pub fn new(name: impl Into<String>) -> Result<Self, InvalidRecord> {
        let name = name.into();
        if name.is_empty() {
            return Err(InvalidRecord::EmptyTopicName);
        }
        if name.len() > MAX_TOPIC_NAME_LEN {
            return Err(InvalidRecord::TopicNameTooLong(name.len()));
        }
        if !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(InvalidRecord::InvalidTopicChar);
        }
        Ok(Self(name))
    }

    /// The topic name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TopicName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for TopicName {
    type Error = InvalidRecord;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<TopicName> for String {
    fn from(value: TopicName) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_advances_and_measures_distance() {
        let base = Offset::ZERO;
        let next = base.next();
        assert_eq!(next.value(), 1);
        assert_eq!(next.saturating_sub(base), 1);
        assert_eq!(base.saturating_sub(next), 0);
    }

    #[test]
    fn topic_name_accepts_valid_and_rejects_invalid() {
        assert!(TopicName::new("orders.v1_2-A").is_ok());
        assert_eq!(TopicName::new(""), Err(InvalidRecord::EmptyTopicName));
        assert_eq!(
            TopicName::new("bad name"),
            Err(InvalidRecord::InvalidTopicChar)
        );
        let long = "a".repeat(MAX_TOPIC_NAME_LEN + 1);
        assert_eq!(
            TopicName::new(long),
            Err(InvalidRecord::TopicNameTooLong(MAX_TOPIC_NAME_LEN + 1))
        );
    }

    #[test]
    fn topic_name_serde_roundtrip() {
        let t = TopicName::new("events").unwrap();
        let json = serde_json::to_string(&t).unwrap();
        assert_eq!(json, "\"events\"");
        let back: TopicName = serde_json::from_str(&json).unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn segment_id_zero_pads() {
        assert_eq!(SegmentId(42).to_string(), "00000000000000000042");
    }
}
