//! GraphQL input/output object types that map the domain onto the API.

use async_graphql::{InputObject, SimpleObject};
use chrono::{DateTime, Utc};

use connectforge_core::LogEvent;
use connectforge_types::{
    Checkpoint, ConnectorStatus, DeadLetterRecord, DeliveryReport, FetchResult, LogStats,
    ProduceResult, Record, TopicConfig,
};

/// A record as returned to clients. Payloads are surfaced as UTF-8 strings.
#[derive(Debug, Clone, SimpleObject)]
pub struct RecordObject {
    /// Partition-local offset.
    pub offset: u64,
    /// Broker append timestamp.
    pub timestamp: DateTime<Utc>,
    /// Optional routing/compaction key.
    pub key: Option<String>,
    /// Payload decoded as a UTF-8 string (lossy for non-UTF-8 bytes).
    pub payload: String,
}

impl From<Record> for RecordObject {
    fn from(r: Record) -> Self {
        Self {
            offset: r.offset.value(),
            timestamp: r.timestamp,
            key: r.key,
            payload: String::from_utf8_lossy(&r.payload).into_owned(),
        }
    }
}

/// A topic's configuration.
#[derive(Debug, Clone, SimpleObject)]
pub struct TopicObject {
    /// Topic name.
    pub name: String,
    /// Partition count.
    pub partitions: u32,
    /// Segment roll size (records).
    pub segment_max_records: u64,
    /// Retention bound (records; 0 = unbounded).
    pub max_records: u64,
}

impl From<TopicConfig> for TopicObject {
    fn from(c: TopicConfig) -> Self {
        Self {
            name: c.name.as_str().to_owned(),
            partitions: c.partitions,
            segment_max_records: c.retention.segment_max_records,
            max_records: c.retention.max_records,
        }
    }
}

/// The result of a produce mutation.
#[derive(Debug, Clone, SimpleObject)]
pub struct ProduceResultObject {
    /// Offset of the first appended record.
    pub base_offset: u64,
    /// Offset of the last appended record.
    pub last_offset: u64,
    /// Number of records appended.
    pub count: u32,
    /// New earliest retained offset if retention pruned data.
    pub truncated_to: Option<u64>,
}

impl From<ProduceResult> for ProduceResultObject {
    fn from(r: ProduceResult) -> Self {
        Self {
            base_offset: r.base_offset.value(),
            last_offset: r.last_offset.value(),
            count: r.count as u32,
            truncated_to: r.truncated_to.map(|o| o.value()),
        }
    }
}

/// The result of a fetch query.
#[derive(Debug, Clone, SimpleObject)]
pub struct FetchResultObject {
    /// The records read.
    pub records: Vec<RecordObject>,
    /// Offset to request next to continue tailing.
    pub next_offset: u64,
    /// Current high-watermark.
    pub high_watermark: u64,
}

impl From<FetchResult> for FetchResultObject {
    fn from(r: FetchResult) -> Self {
        Self {
            records: r.records.into_iter().map(RecordObject::from).collect(),
            next_offset: r.next_offset.value(),
            high_watermark: r.high_watermark.value(),
        }
    }
}

/// Broker statistics.
#[derive(Debug, Clone, SimpleObject)]
pub struct StatsObject {
    /// Number of topics.
    pub topics: u64,
    /// Total partitions.
    pub partitions: u64,
    /// Retained records.
    pub records: u64,
    /// Total segments.
    pub segments: u64,
    /// Cumulative records produced.
    pub produced_total: u64,
    /// Cumulative records fetched.
    pub fetched_total: u64,
}

impl From<LogStats> for StatsObject {
    fn from(s: LogStats) -> Self {
        Self {
            topics: s.topics,
            partitions: s.partitions,
            records: s.records,
            segments: s.segments,
            produced_total: s.produced_total,
            fetched_total: s.fetched_total,
        }
    }
}

/// A single record to produce.
#[derive(Debug, Clone, InputObject)]
pub struct ProduceInput {
    /// Optional routing/compaction key.
    pub key: Option<String>,
    /// UTF-8 payload.
    pub payload: String,
}

/// A live event delivered over the subscription.
#[derive(Debug, Clone, SimpleObject)]
pub struct EventObject {
    /// Event kind: `topic_created`, `records_appended`, or `records_truncated`.
    pub kind: String,
    /// The topic the event pertains to.
    pub topic: String,
    /// Partition, when applicable.
    pub partition: Option<u32>,
    /// Base offset for appends / new start for truncation.
    pub offset: Option<u64>,
    /// Number of records for append events.
    pub count: Option<u32>,
}

impl From<LogEvent> for EventObject {
    fn from(e: LogEvent) -> Self {
        let kind = e.kind().to_owned();
        match e {
            LogEvent::TopicCreated { topic, .. } => Self {
                kind,
                topic: topic.as_str().to_owned(),
                partition: None,
                offset: None,
                count: None,
            },
            LogEvent::RecordsAppended {
                topic,
                partition,
                base_offset,
                records,
            } => Self {
                kind,
                topic: topic.as_str().to_owned(),
                partition: Some(partition.value()),
                offset: Some(base_offset.value()),
                count: Some(records.len() as u32),
            },
            LogEvent::RecordsTruncated {
                topic,
                partition,
                new_start,
            } => Self {
                kind,
                topic: topic.as_str().to_owned(),
                partition: Some(partition.value()),
                offset: Some(new_start.value()),
                count: None,
            },
        }
    }
}

/// A committed checkpoint (resume position) for a connector partition.
#[derive(Debug, Clone, SimpleObject)]
pub struct CheckpointObject {
    /// Owning connector id.
    pub connector: String,
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
    /// Last processed offset (exclusive resume position is `offset + 1`).
    pub offset: u64,
}

impl From<Checkpoint> for CheckpointObject {
    fn from(c: Checkpoint) -> Self {
        Self {
            connector: c.connector.to_string(),
            topic: c.topic.as_str().to_owned(),
            partition: c.partition.value(),
            offset: c.offset.value(),
        }
    }
}

/// A record that a connector failed to deliver and routed to the DLQ.
#[derive(Debug, Clone, SimpleObject)]
pub struct DeadLetterObject {
    /// Owning connector id.
    pub connector: String,
    /// Source topic.
    pub topic: String,
    /// Source partition.
    pub partition: u32,
    /// Source offset.
    pub offset: u64,
    /// Payload decoded as UTF-8 (lossy).
    pub payload: String,
    /// Human-readable failure reason.
    pub reason: String,
    /// Delivery attempts made before giving up.
    pub attempts: u32,
    /// When the record was dead-lettered.
    pub failed_at: DateTime<Utc>,
}

impl From<DeadLetterRecord> for DeadLetterObject {
    fn from(r: DeadLetterRecord) -> Self {
        Self {
            connector: r.connector.to_string(),
            topic: r.topic.as_str().to_owned(),
            partition: r.partition.value(),
            offset: r.offset.value(),
            payload: String::from_utf8_lossy(&r.payload).into_owned(),
            reason: r.reason,
            attempts: r.attempts,
            failed_at: r.failed_at,
        }
    }
}

/// The outcome of one connector run.
#[derive(Debug, Clone, SimpleObject)]
pub struct DeliveryReportObject {
    /// Records polled/read this run.
    pub polled: u32,
    /// Records successfully delivered.
    pub delivered: u32,
    /// Records routed to the DLQ.
    pub dead_lettered: u32,
    /// Next offset to consume after this run (the committed checkpoint).
    pub checkpoint: u64,
}

impl From<DeliveryReport> for DeliveryReportObject {
    fn from(r: DeliveryReport) -> Self {
        Self {
            polled: r.polled as u32,
            delivered: r.delivered as u32,
            dead_lettered: r.dead_lettered as u32,
            checkpoint: r.checkpoint.value(),
        }
    }
}

/// The observable status of a registered connector.
#[derive(Debug, Clone, SimpleObject)]
pub struct ConnectorStatusObject {
    /// Connector id.
    pub id: String,
    /// `source` or `sink`.
    pub kind: String,
    /// Delivery guarantee: `at_least_once` or `at_most_once`.
    pub guarantee: String,
    /// Whether the connector is actively running.
    pub running: bool,
    /// Records processed since start.
    pub processed: u64,
    /// Records dead-lettered since start.
    pub dead_lettered: u64,
    /// Current checkpoint offset (next offset to consume).
    pub checkpoint: u64,
}

impl From<ConnectorStatus> for ConnectorStatusObject {
    fn from(s: ConnectorStatus) -> Self {
        Self {
            id: s.id.to_string(),
            kind: match s.kind {
                connectforge_types::ConnectorKind::Source => "source",
                connectforge_types::ConnectorKind::Sink => "sink",
            }
            .to_owned(),
            guarantee: match s.guarantee {
                connectforge_types::DeliveryGuarantee::AtLeastOnce => "at_least_once",
                connectforge_types::DeliveryGuarantee::AtMostOnce => "at_most_once",
            }
            .to_owned(),
            running: s.running,
            processed: s.processed,
            dead_lettered: s.dead_lettered,
            checkpoint: s.checkpoint.value(),
        }
    }
}
