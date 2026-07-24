use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

/// A monotonic clock abstraction so time-dependent primitives are testable.
pub trait Clock: Send + Sync {
    /// Elapsed time since some fixed, arbitrary epoch.
    fn now(&self) -> Duration;
}

/// A real clock backed by [`std::time::Instant`].
#[derive(Clone)]
pub struct SystemClock {
    origin: std::time::Instant,
}

impl SystemClock {
    /// Create a system clock anchored at the current instant.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

/// A manually advanced clock for deterministic tests.
#[derive(Clone, Default)]
pub struct ManualClock {
    inner: Arc<Mutex<Duration>>,
}

impl ManualClock {
    /// Create a manual clock starting at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the clock by `delta`.
    pub fn advance(&self, delta: Duration) {
        *self.inner.lock() += delta;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *self.inner.lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_advances() {
        let c = ManualClock::new();
        assert_eq!(c.now(), Duration::ZERO);
        c.advance(Duration::from_millis(50));
        assert_eq!(c.now(), Duration::from_millis(50));
    }

    #[test]
    fn system_clock_is_monotonic() {
        let c = SystemClock::new();
        let a = c.now();
        let b = c.now();
        assert!(b >= a);
    }
}
