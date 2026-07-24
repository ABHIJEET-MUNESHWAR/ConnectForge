use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Returned when the bulkhead is at capacity and admits no more work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("bulkhead is full")]
pub struct BulkheadFull;

/// A concurrency limiter that bounds the number of simultaneous in-flight
/// operations, isolating a slow dependency from exhausting the runtime.
#[derive(Clone)]
pub struct Bulkhead {
    sem: Arc<Semaphore>,
}

/// RAII guard that releases a bulkhead slot on drop.
pub struct BulkheadGuard(#[allow(dead_code)] OwnedSemaphorePermit);

impl Bulkhead {
    /// Create a bulkhead admitting at most `max_concurrent` operations.
    #[must_use]
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(max_concurrent.max(1))),
        }
    }

    /// Try to acquire a slot without waiting.
    ///
    /// # Errors
    /// Returns [`BulkheadFull`] if no slot is currently free.
    pub fn try_acquire(&self) -> Result<BulkheadGuard, BulkheadFull> {
        self.sem
            .clone()
            .try_acquire_owned()
            .map(BulkheadGuard)
            .map_err(|_| BulkheadFull)
    }

    /// Number of currently available slots.
    #[must_use]
    pub fn available(&self) -> usize {
        self.sem.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admits_up_to_capacity_then_rejects() {
        let bh = Bulkhead::new(2);
        let g1 = bh.try_acquire().unwrap();
        let _g2 = bh.try_acquire().unwrap();
        assert_eq!(bh.available(), 0);
        assert_eq!(bh.try_acquire().err(), Some(BulkheadFull));
        drop(g1);
        assert!(bh.try_acquire().is_ok());
    }
}
