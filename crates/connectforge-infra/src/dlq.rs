//! Dead-letter sinks: destinations for records a connector could not deliver.

use async_trait::async_trait;
use connectforge_core::{DeadLetterSink, PortError};
use connectforge_types::DeadLetterRecord;
use parking_lot::Mutex;

/// In-memory dead-letter queue that retains failed records for inspection.
#[derive(Debug, Default)]
pub struct MemoryDeadLetterSink {
    records: Mutex<Vec<DeadLetterRecord>>,
}

impl MemoryDeadLetterSink {
    /// Create an empty dead-letter queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A snapshot of all dead-lettered records.
    #[must_use]
    pub fn records(&self) -> Vec<DeadLetterRecord> {
        self.records.lock().clone()
    }

    /// Number of dead-lettered records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.lock().len()
    }

    /// Whether the queue is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.lock().is_empty()
    }
}

#[async_trait]
impl DeadLetterSink for MemoryDeadLetterSink {
    async fn dead_letter(&self, record: DeadLetterRecord) -> Result<(), PortError> {
        metrics::counter!("connectforge_dlq_records_total").increment(1);
        self.records.lock().push(record);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use connectforge_types::{ConnectorId, Offset, PartitionId, TopicName};

    fn dlq_record() -> DeadLetterRecord {
        DeadLetterRecord {
            connector: ConnectorId::new("sink-1").unwrap(),
            topic: TopicName::new("events").unwrap(),
            partition: PartitionId(0),
            offset: Offset(7),
            payload: vec![1, 2, 3],
            reason: "permanent failure".into(),
            attempts: 3,
            failed_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn retains_dead_lettered_records() {
        let dlq = MemoryDeadLetterSink::new();
        assert!(dlq.is_empty());
        dlq.dead_letter(dlq_record()).await.unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq.records()[0].offset, Offset(7));
    }
}
