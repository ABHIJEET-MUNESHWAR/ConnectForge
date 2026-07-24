//! Record types: the unit of data stored in and served from the log.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::InvalidRecord;
use crate::units::{Offset, MAX_PAYLOAD_BYTES};

/// A record as submitted by a producer, before an offset has been assigned.
///
/// The payload is treated as opaque bytes; a UTF-8 API layer sits above this
/// type. An optional key drives partition routing and log compaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProduceRecord {
    /// Optional routing/compaction key.
    pub key: Option<String>,
    /// Opaque record payload.
    pub payload: Vec<u8>,
}

impl ProduceRecord {
    /// Construct and validate a producer record.
    ///
    /// # Errors
    /// Returns [`InvalidRecord`] if the payload is empty or exceeds
    /// [`MAX_PAYLOAD_BYTES`].
    pub fn new(key: Option<String>, payload: Vec<u8>) -> Result<Self, InvalidRecord> {
        if payload.is_empty() {
            return Err(InvalidRecord::EmptyPayload);
        }
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(InvalidRecord::PayloadTooLarge(payload.len()));
        }
        Ok(Self { key, payload })
    }

    /// Validate an already-constructed record (used on the deserialized path).
    ///
    /// # Errors
    /// Returns [`InvalidRecord`] if the payload is empty or too large.
    pub fn validate(&self) -> Result<(), InvalidRecord> {
        if self.payload.is_empty() {
            return Err(InvalidRecord::EmptyPayload);
        }
        if self.payload.len() > MAX_PAYLOAD_BYTES {
            return Err(InvalidRecord::PayloadTooLarge(self.payload.len()));
        }
        Ok(())
    }
}

/// A durably stored record with its assigned offset and broker timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// The dense, partition-local position of this record.
    pub offset: Offset,
    /// The broker-assigned append timestamp.
    pub timestamp: DateTime<Utc>,
    /// Optional routing/compaction key.
    pub key: Option<String>,
    /// Opaque record payload.
    pub payload: Vec<u8>,
}

impl Record {
    /// Materialize a stored record from a producer record plus assigned
    /// metadata.
    #[must_use]
    pub fn from_produce(record: ProduceRecord, offset: Offset, timestamp: DateTime<Utc>) -> Self {
        Self {
            offset,
            timestamp,
            key: record.key,
            payload: record.payload,
        }
    }

    /// The on-wire size of this record's payload in bytes.
    #[must_use]
    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_payload() {
        assert_eq!(
            ProduceRecord::new(None, Vec::new()),
            Err(InvalidRecord::EmptyPayload)
        );
    }

    #[test]
    fn rejects_oversize_payload() {
        let big = vec![0u8; MAX_PAYLOAD_BYTES + 1];
        assert_eq!(
            ProduceRecord::new(None, big),
            Err(InvalidRecord::PayloadTooLarge(MAX_PAYLOAD_BYTES + 1))
        );
    }

    #[test]
    fn from_produce_carries_fields() {
        let pr = ProduceRecord::new(Some("k".into()), b"hi".to_vec()).unwrap();
        let ts = Utc::now();
        let rec = Record::from_produce(pr, Offset(7), ts);
        assert_eq!(rec.offset, Offset(7));
        assert_eq!(rec.timestamp, ts);
        assert_eq!(rec.key.as_deref(), Some("k"));
        assert_eq!(rec.payload_len(), 2);
    }
}
