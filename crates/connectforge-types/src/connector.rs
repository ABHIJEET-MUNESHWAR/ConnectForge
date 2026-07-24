//! Connector domain types: connector identity, kinds, delivery guarantees,
//! checkpoints, and dead-letter records.
//!
//! ConnectForge is a connector SDK layered on the commit log. **Source**
//! connectors poll an external system and append records to a topic;
//! **sink** connectors read committed records and deliver them outward, with
//! **at-least-once** delivery, **offset checkpointing** for resumable restarts,
//! and a **dead-letter queue** for records that repeatedly fail.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::InvalidRecord;
use crate::units::{Offset, PartitionId, TopicName};

/// Maximum length, in bytes, of a connector id.
pub const MAX_CONNECTOR_ID_LEN: usize = 128;

/// A validated, stable connector identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ConnectorId(String);

impl ConnectorId {
    /// Create a connector id, validating length and charset
    /// (`[A-Za-z0-9._-]`, non-empty).
    ///
    /// # Errors
    /// Returns [`InvalidRecord`] if the id is empty, too long, or contains an
    /// unsupported character.
    pub fn new(raw: impl Into<String>) -> Result<Self, InvalidRecord> {
        let raw = raw.into();
        if raw.is_empty() {
            return Err(InvalidRecord::EmptyTopicName);
        }
        if raw.len() > MAX_CONNECTOR_ID_LEN {
            return Err(InvalidRecord::TopicNameTooLong(raw.len()));
        }
        if !raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        {
            return Err(InvalidRecord::InvalidTopicChar);
        }
        Ok(Self(raw))
    }

    /// The id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ConnectorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for ConnectorId {
    type Error = InvalidRecord;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ConnectorId> for String {
    fn from(value: ConnectorId) -> Self {
        value.0
    }
}

/// Whether a connector reads from (`Source`) or writes to (`Sink`) an external
/// system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectorKind {
    /// Polls an external system and appends records to a topic.
    Source,
    /// Reads committed records and delivers them outward.
    Sink,
}

impl std::fmt::Display for ConnectorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source => write!(f, "source"),
            Self::Sink => write!(f, "sink"),
        }
    }
}

/// The delivery guarantee a sink connector provides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DeliveryGuarantee {
    /// Records may be redelivered on failure but are never lost: the checkpoint
    /// advances only after a record is delivered or dead-lettered.
    #[default]
    AtLeastOnce,
    /// The checkpoint advances before delivery; a crash may drop in-flight
    /// records but never redelivers them.
    AtMostOnce,
}

impl std::fmt::Display for DeliveryGuarantee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AtLeastOnce => write!(f, "at-least-once"),
            Self::AtMostOnce => write!(f, "at-most-once"),
        }
    }
}

/// A durable position: the **next** offset a connector should read for a
/// `(topic, partition)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Owning connector.
    pub connector: ConnectorId,
    /// Source topic.
    pub topic: TopicName,
    /// Source partition.
    pub partition: PartitionId,
    /// The next offset to consume (records below this are already handled).
    pub offset: Offset,
}

/// A record that could not be delivered and was routed to the dead-letter
/// queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    /// Connector that failed to deliver the record.
    pub connector: ConnectorId,
    /// Origin topic.
    pub topic: TopicName,
    /// Origin partition.
    pub partition: PartitionId,
    /// Origin offset.
    pub offset: Offset,
    /// The record payload.
    pub payload: Vec<u8>,
    /// Why delivery failed.
    pub reason: String,
    /// How many delivery attempts were made before giving up.
    pub attempts: u32,
    /// When the record was dead-lettered.
    pub failed_at: DateTime<Utc>,
}

/// The outcome of processing one batch through a sink runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeliveryReport {
    /// Records read from the log this round.
    pub polled: u64,
    /// Records successfully delivered.
    pub delivered: u64,
    /// Records routed to the dead-letter queue after exhausting retries.
    pub dead_lettered: u64,
    /// The checkpoint (next offset) after this round.
    pub checkpoint: Offset,
}

/// A snapshot of a connector's health for observability/API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorStatus {
    /// Connector id.
    pub id: ConnectorId,
    /// Source or sink.
    pub kind: ConnectorKind,
    /// Delivery guarantee (sinks).
    pub guarantee: DeliveryGuarantee,
    /// Whether the connector is currently running.
    pub running: bool,
    /// Total records delivered (sink) or produced (source).
    pub processed: u64,
    /// Total records dead-lettered.
    pub dead_lettered: u64,
    /// The connector's current checkpoint offset.
    pub checkpoint: Offset,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_id_validates() {
        assert!(ConnectorId::new("file-sink_1.v2").is_ok());
        assert!(ConnectorId::new("").is_err());
        assert!(ConnectorId::new("bad name").is_err());
        assert!(ConnectorId::new("x".repeat(200)).is_err());
    }

    #[test]
    fn displays() {
        assert_eq!(ConnectorKind::Source.to_string(), "source");
        assert_eq!(ConnectorKind::Sink.to_string(), "sink");
        assert_eq!(DeliveryGuarantee::default().to_string(), "at-least-once");
    }

    #[test]
    fn id_roundtrips_through_string() {
        let id = ConnectorId::new("c1").unwrap();
        let s: String = id.clone().into();
        assert_eq!(ConnectorId::try_from(s).unwrap(), id);
    }
}
