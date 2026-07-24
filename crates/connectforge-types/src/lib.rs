//! Domain types for **ConnectForge** — a connector (source/sink) SDK built on a
//! durable, partitioned, segmented commit log.
//!
//! This crate is pure data and validation logic: no async, no I/O, no
//! framework dependencies. It defines the vocabulary the rest of the system
//! speaks — offsets, partitions, topics, records, and the connector types
//! (checkpoints, delivery guarantees, dead-letter records).

pub mod connector;
pub mod error;
pub mod record;
pub mod stats;
pub mod topic;
pub mod units;

pub use connector::{
    Checkpoint, ConnectorId, ConnectorKind, ConnectorStatus, DeadLetterRecord, DeliveryGuarantee,
    DeliveryReport, MAX_CONNECTOR_ID_LEN,
};
pub use error::InvalidRecord;
pub use record::{ProduceRecord, Record};
pub use stats::{FetchResult, LogStats, PartitionStats, ProduceResult};
pub use topic::{RetentionPolicy, TopicConfig};
pub use units::{Offset, PartitionId, SegmentId, TopicName, MAX_PAYLOAD_BYTES, MAX_TOPIC_NAME_LEN};
