//! Error types for the core engine.
//!
//! [`PortError`] is what infra adapters return; [`CoreError`] is what the
//! engine surfaces to callers. Keeping them separate lets the engine classify
//! transient failures for retry without leaking adapter details.

use connectforge_types::InvalidRecord;
use thiserror::Error;

/// An error returned by a port (storage or event-sink adapter).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PortError {
    /// The dependency is currently unavailable; the operation may succeed if
    /// retried after backoff.
    #[error("dependency unavailable: {0}")]
    Unavailable(String),
    /// The requested topic or partition does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// A transient failure that is safe to retry.
    #[error("transient failure: {0}")]
    Transient(String),
    /// A permanent failure that must not be retried.
    #[error("permanent failure: {0}")]
    Permanent(String),
}

impl PortError {
    /// Whether retrying the operation could plausibly succeed.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable(_) | Self::Transient(_))
    }
}

/// An error surfaced by the engine to its API/callers.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CoreError {
    /// A record or configuration value failed domain validation.
    #[error(transparent)]
    Invalid(#[from] InvalidRecord),
    /// The produce rate limit was exceeded; the caller should back off.
    #[error("produce rate limit exceeded")]
    RateLimited,
    /// A produce call carried no records.
    #[error("produce batch must contain at least one record")]
    EmptyBatch,
    /// A topic with the same name already exists.
    #[error("topic '{0}' already exists")]
    TopicExists(String),
    /// The referenced topic does not exist.
    #[error("topic '{0}' not found")]
    TopicNotFound(String),
    /// The referenced partition is out of range for the topic.
    #[error("partition {partition} out of range for topic '{topic}' with {count} partitions")]
    PartitionOutOfRange {
        /// Topic name.
        topic: String,
        /// Requested partition.
        partition: u32,
        /// Number of partitions the topic actually has.
        count: u32,
    },
    /// The maximum number of topics has been reached.
    #[error("topic capacity of {0} reached")]
    CapacityExceeded(usize),
    /// A storage operation timed out even after retries.
    #[error("storage operation timed out")]
    Timeout,
    /// An underlying port failed permanently.
    #[error(transparent)]
    Port(#[from] PortError),
}

impl CoreError {
    /// A stable, machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Invalid(e) => e.code(),
            Self::RateLimited => "RATE_LIMITED",
            Self::EmptyBatch => "EMPTY_BATCH",
            Self::TopicExists(_) => "TOPIC_EXISTS",
            Self::TopicNotFound(_) => "TOPIC_NOT_FOUND",
            Self::PartitionOutOfRange { .. } => "PARTITION_OUT_OF_RANGE",
            Self::CapacityExceeded(_) => "CAPACITY_EXCEEDED",
            Self::Timeout => "TIMEOUT",
            Self::Port(_) => "PORT_ERROR",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_classification() {
        assert!(PortError::Unavailable("x".into()).is_retryable());
        assert!(PortError::Transient("x".into()).is_retryable());
        assert!(!PortError::NotFound("x".into()).is_retryable());
        assert!(!PortError::Permanent("x".into()).is_retryable());
    }

    #[test]
    fn invalid_record_maps_to_core_error() {
        let e: CoreError = InvalidRecord::EmptyPayload.into();
        assert_eq!(e.code(), "EMPTY_PAYLOAD");
    }

    #[test]
    fn port_error_maps_to_core_error() {
        let e: CoreError = PortError::Transient("db".into()).into();
        assert_eq!(e.code(), "PORT_ERROR");
    }
}
