//! A lightweight, thread-safe registry that makes connector activity
//! observable through the GraphQL API.
//!
//! The composition root (node) runs the connector runtimes and pushes
//! status/DLQ updates here; the query side reads consistent snapshots.

use parking_lot::RwLock;

use connectforge_types::{ConnectorStatus, DeadLetterRecord};

/// Shared, observable state for all registered connectors.
#[derive(Debug, Default)]
pub struct ConnectorRegistry {
    statuses: RwLock<Vec<ConnectorStatus>>,
    dead_letters: RwLock<Vec<DeadLetterRecord>>,
}

impl ConnectorRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a connector's status (keyed by id).
    pub fn upsert_status(&self, status: ConnectorStatus) {
        let mut statuses = self.statuses.write();
        if let Some(existing) = statuses.iter_mut().find(|s| s.id == status.id) {
            *existing = status;
        } else {
            statuses.push(status);
        }
    }

    /// Append dead-lettered records observed during a run.
    pub fn extend_dead_letters(&self, records: impl IntoIterator<Item = DeadLetterRecord>) {
        self.dead_letters.write().extend(records);
    }

    /// Replace the dead-letter snapshot (used by a supervisor that owns the DLQ).
    pub fn set_dead_letters(&self, records: Vec<DeadLetterRecord>) {
        *self.dead_letters.write() = records;
    }

    /// A snapshot of all connector statuses.
    #[must_use]
    pub fn statuses(&self) -> Vec<ConnectorStatus> {
        self.statuses.read().clone()
    }

    /// A snapshot of all dead-lettered records.
    #[must_use]
    pub fn dead_letters(&self) -> Vec<DeadLetterRecord> {
        self.dead_letters.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use connectforge_types::{
        ConnectorId, ConnectorKind, DeliveryGuarantee, Offset, PartitionId, TopicName,
    };

    fn status(processed: u64) -> ConnectorStatus {
        ConnectorStatus {
            id: ConnectorId::new("sink-1").unwrap(),
            kind: ConnectorKind::Sink,
            guarantee: DeliveryGuarantee::AtLeastOnce,
            running: true,
            processed,
            dead_lettered: 0,
            checkpoint: Offset(0),
        }
    }

    #[test]
    fn upsert_replaces_by_id() {
        let reg = ConnectorRegistry::new();
        reg.upsert_status(status(1));
        reg.upsert_status(status(5));
        let all = reg.statuses();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].processed, 5);
    }

    #[test]
    fn dead_letters_accumulate() {
        let reg = ConnectorRegistry::new();
        reg.extend_dead_letters([DeadLetterRecord {
            connector: ConnectorId::new("sink-1").unwrap(),
            topic: TopicName::new("events").unwrap(),
            partition: PartitionId(0),
            offset: Offset(3),
            payload: vec![9],
            reason: "boom".into(),
            attempts: 3,
            failed_at: Utc::now(),
        }]);
        assert_eq!(reg.dead_letters().len(), 1);
    }
}
