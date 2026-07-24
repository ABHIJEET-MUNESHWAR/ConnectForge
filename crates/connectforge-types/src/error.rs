//! Domain validation errors for the commit-log types.

use thiserror::Error;

/// A record, topic, or configuration value that violates a domain invariant.
///
/// These are pure validation failures with no I/O component, so they live in
/// the `types` crate and are surfaced by constructors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidRecord {
    /// The payload contained no bytes.
    #[error("record payload must not be empty")]
    EmptyPayload,
    /// The payload exceeded the maximum permitted size.
    #[error("record payload of {0} bytes exceeds the maximum")]
    PayloadTooLarge(usize),
    /// The topic name was empty.
    #[error("topic name must not be empty")]
    EmptyTopicName,
    /// The topic name exceeded the maximum length.
    #[error("topic name of {0} bytes exceeds the maximum")]
    TopicNameTooLong(usize),
    /// The topic name contained a disallowed character.
    #[error("topic name may only contain [A-Za-z0-9._-]")]
    InvalidTopicChar,
    /// A topic was configured with zero partitions.
    #[error("a topic must have at least one partition")]
    ZeroPartitions,
    /// A retention policy specified a zero-record segment roll size.
    #[error("segment roll size must be greater than zero")]
    ZeroSegmentSize,
}

impl InvalidRecord {
    /// A stable, machine-readable error code for API responses and metrics.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::EmptyPayload => "EMPTY_PAYLOAD",
            Self::PayloadTooLarge(_) => "PAYLOAD_TOO_LARGE",
            Self::EmptyTopicName => "EMPTY_TOPIC_NAME",
            Self::TopicNameTooLong(_) => "TOPIC_NAME_TOO_LONG",
            Self::InvalidTopicChar => "INVALID_TOPIC_CHAR",
            Self::ZeroPartitions => "ZERO_PARTITIONS",
            Self::ZeroSegmentSize => "ZERO_SEGMENT_SIZE",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_distinct_and_stable() {
        assert_eq!(InvalidRecord::EmptyPayload.code(), "EMPTY_PAYLOAD");
        assert_eq!(
            InvalidRecord::PayloadTooLarge(1).code(),
            "PAYLOAD_TOO_LARGE"
        );
        assert_eq!(InvalidRecord::ZeroPartitions.code(), "ZERO_PARTITIONS");
    }
}
