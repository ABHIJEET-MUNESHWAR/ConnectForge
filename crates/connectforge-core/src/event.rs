//! Domain events published on the broker's control/data bus.
//!
//! Subscribers receive [`LogEvent`]s as records are appended, topics created,
//! or retention prunes the head of a partition. `RecordsAppended` carries the
//! appended records themselves (behind an [`Arc`] to keep fan-out cheap) so a
//! subscriber can tail live data without polling.

use std::sync::Arc;

use connectforge_types::{Offset, PartitionId, Record, TopicName};

/// An event describing a state change in the log.
#[derive(Debug, Clone)]
pub enum LogEvent {
    /// A new topic was created.
    TopicCreated {
        /// The topic name.
        topic: TopicName,
        /// Number of partitions.
        partitions: u32,
    },
    /// One or more records were committed to a partition.
    RecordsAppended {
        /// The topic.
        topic: TopicName,
        /// The partition the records landed on.
        partition: PartitionId,
        /// Offset of the first appended record.
        base_offset: Offset,
        /// The appended records, shared cheaply across subscribers.
        records: Arc<Vec<Record>>,
    },
    /// Retention pruned the head of a partition.
    RecordsTruncated {
        /// The topic.
        topic: TopicName,
        /// The partition.
        partition: PartitionId,
        /// The new earliest retained offset.
        new_start: Offset,
    },
}

impl LogEvent {
    /// A stable event-kind label, useful for metrics and filtering.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::TopicCreated { .. } => "topic_created",
            Self::RecordsAppended { .. } => "records_appended",
            Self::RecordsTruncated { .. } => "records_truncated",
        }
    }

    /// The topic this event pertains to.
    #[must_use]
    pub const fn topic(&self) -> &TopicName {
        match self {
            Self::TopicCreated { topic, .. }
            | Self::RecordsAppended { topic, .. }
            | Self::RecordsTruncated { topic, .. } => topic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_and_topic_accessors() {
        let t = TopicName::new("t").unwrap();
        let ev = LogEvent::TopicCreated {
            topic: t.clone(),
            partitions: 3,
        };
        assert_eq!(ev.kind(), "topic_created");
        assert_eq!(ev.topic(), &t);

        let ev = LogEvent::RecordsTruncated {
            topic: t.clone(),
            partition: PartitionId(0),
            new_start: Offset(5),
        };
        assert_eq!(ev.kind(), "records_truncated");
    }
}
