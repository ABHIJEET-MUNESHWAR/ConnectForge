//! Reference connectors used by demos, tests, and as templates for real
//! source/sink implementations.

use async_trait::async_trait;
use connectforge_core::{PortError, Sink, Source};
use connectforge_types::{ProduceRecord, Record};
use parking_lot::Mutex;

use crate::generator::RecordGenerator;

/// A sink that records every delivered record in memory (for tests/inspection).
#[derive(Debug, Default)]
pub struct CollectingSink {
    delivered: Mutex<Vec<Record>>,
}

impl CollectingSink {
    /// Create an empty collecting sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A snapshot of all delivered records.
    #[must_use]
    pub fn delivered(&self) -> Vec<Record> {
        self.delivered.lock().clone()
    }

    /// Number of records delivered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.delivered.lock().len()
    }

    /// Whether nothing has been delivered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.delivered.lock().is_empty()
    }
}

#[async_trait]
impl Sink for CollectingSink {
    async fn deliver(&self, records: &[Record]) -> Result<(), PortError> {
        self.delivered.lock().extend_from_slice(records);
        Ok(())
    }
}

/// A sink that deterministically fails records whose offset is a multiple of
/// `fail_every`, to exercise retry and dead-lettering. `fail_every == 0`
/// disables failures.
#[derive(Debug)]
pub struct FailingSink {
    fail_every: u64,
    delivered: Mutex<Vec<Record>>,
}

impl FailingSink {
    /// Create a sink that fails every `fail_every`-th offset.
    #[must_use]
    pub fn new(fail_every: u64) -> Self {
        Self {
            fail_every,
            delivered: Mutex::new(Vec::new()),
        }
    }

    /// Number of records successfully delivered.
    #[must_use]
    pub fn delivered_count(&self) -> usize {
        self.delivered.lock().len()
    }
}

#[async_trait]
impl Sink for FailingSink {
    async fn deliver(&self, records: &[Record]) -> Result<(), PortError> {
        for r in records {
            if self.fail_every != 0 && r.offset.value() % self.fail_every == 0 {
                return Err(PortError::Permanent(format!(
                    "poison record at offset {}",
                    r.offset
                )));
            }
        }
        self.delivered.lock().extend_from_slice(records);
        Ok(())
    }
}

/// A source that generates a bounded number of synthetic records in batches,
/// then reports "no data" (empty polls) — useful for demos and load tests.
pub struct GeneratorSource {
    generator: Mutex<RecordGenerator>,
    remaining: Mutex<u64>,
    batch_size: usize,
}

impl GeneratorSource {
    /// Create a source that will emit `total` records in `batch_size` chunks.
    #[must_use]
    pub fn new(total: u64, batch_size: usize, key_space: u64, seed: u64) -> Self {
        Self {
            generator: Mutex::new(RecordGenerator::new(key_space, 32, seed)),
            remaining: Mutex::new(total),
            batch_size: batch_size.max(1),
        }
    }

    /// Records still to be emitted.
    #[must_use]
    pub fn remaining(&self) -> u64 {
        *self.remaining.lock()
    }
}

#[async_trait]
impl Source for GeneratorSource {
    async fn poll(&self) -> Result<Vec<ProduceRecord>, PortError> {
        let take = {
            let mut remaining = self.remaining.lock();
            let take = (*remaining).min(self.batch_size as u64);
            *remaining -= take;
            take
        };
        if take == 0 {
            return Ok(Vec::new());
        }
        let mut generator = self.generator.lock();
        Ok((0..take).map(|_| generator.next_record()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connectforge_types::Offset;

    fn record(offset: u64) -> Record {
        Record {
            offset: Offset(offset),
            timestamp: chrono::Utc::now(),
            key: None,
            payload: vec![offset as u8],
        }
    }

    #[tokio::test]
    async fn collecting_sink_stores_records() {
        let sink = CollectingSink::new();
        assert!(sink.is_empty());
        sink.deliver(&[record(0), record(1)]).await.unwrap();
        assert_eq!(sink.len(), 2);
        assert_eq!(sink.delivered()[1].offset, Offset(1));
    }

    #[tokio::test]
    async fn failing_sink_rejects_poison_offsets() {
        let sink = FailingSink::new(3);
        assert!(sink.deliver(&[record(1)]).await.is_ok());
        assert!(sink.deliver(&[record(3)]).await.is_err());
        assert_eq!(sink.delivered_count(), 1);
    }

    #[tokio::test]
    async fn generator_source_emits_then_drains() {
        let source = GeneratorSource::new(5, 2, 10, 0xABCD);
        let mut total = 0;
        loop {
            let batch = source.poll().await.unwrap();
            if batch.is_empty() {
                break;
            }
            total += batch.len();
        }
        assert_eq!(total, 5);
        assert_eq!(source.remaining(), 0);
    }
}
