use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use thiserror::Error;

use crate::clock::{Clock, SystemClock};

/// Returned when a request is denied because the token bucket is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("rate limit exceeded")]
pub struct RateLimited;

struct Inner {
    tokens: f64,
    last_refill: Duration,
}

/// A token-bucket rate limiter, generic over a [`Clock`] for deterministic
/// tests.
pub struct RateLimiter<C: Clock = SystemClock> {
    capacity: f64,
    refill_per_sec: f64,
    clock: C,
    inner: Arc<Mutex<Inner>>,
}

impl RateLimiter<SystemClock> {
    /// Create a limiter allowing `refill_per_sec` sustained requests with a
    /// burst of `capacity`, backed by the system clock.
    #[must_use]
    pub fn new(capacity: f64, refill_per_sec: f64) -> Self {
        Self::with_clock(capacity, refill_per_sec, SystemClock::new())
    }
}

impl<C: Clock> RateLimiter<C> {
    /// Create a limiter with an injected clock.
    pub fn with_clock(capacity: f64, refill_per_sec: f64, clock: C) -> Self {
        let now = clock.now();
        Self {
            capacity: capacity.max(1.0),
            refill_per_sec: refill_per_sec.max(0.0),
            clock,
            inner: Arc::new(Mutex::new(Inner {
                tokens: capacity.max(1.0),
                last_refill: now,
            })),
        }
    }

    fn refill(&self, inner: &mut Inner) {
        let now = self.clock.now();
        let elapsed = now.saturating_sub(inner.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            inner.tokens = (inner.tokens + elapsed * self.refill_per_sec).min(self.capacity);
            inner.last_refill = now;
        }
    }

    /// Try to acquire a single token.
    ///
    /// # Errors
    /// Returns [`RateLimited`] when no token is available.
    pub fn try_acquire(&self) -> Result<(), RateLimited> {
        self.try_acquire_n(1.0)
    }

    /// Try to acquire `n` tokens at once.
    ///
    /// # Errors
    /// Returns [`RateLimited`] when fewer than `n` tokens are available.
    pub fn try_acquire_n(&self, n: f64) -> Result<(), RateLimited> {
        let mut inner = self.inner.lock();
        self.refill(&mut inner);
        if inner.tokens >= n {
            inner.tokens -= n;
            Ok(())
        } else {
            Err(RateLimited)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;

    #[test]
    fn denies_when_bucket_empty_and_refills_over_time() {
        let clock = ManualClock::new();
        let rl = RateLimiter::with_clock(2.0, 1.0, clock.clone());
        assert!(rl.try_acquire().is_ok());
        assert!(rl.try_acquire().is_ok());
        assert_eq!(rl.try_acquire(), Err(RateLimited));

        clock.advance(Duration::from_secs(1));
        assert!(rl.try_acquire().is_ok());
        assert_eq!(rl.try_acquire(), Err(RateLimited));
    }
}
