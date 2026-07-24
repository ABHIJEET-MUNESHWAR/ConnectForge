use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use thiserror::Error;

use crate::clock::{Clock, SystemClock};

/// Returned when the breaker is open and rejects a call fast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("circuit breaker is open")]
pub struct RejectedByBreaker;

/// Observable breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    /// Calls flow normally.
    Closed,
    /// Calls are rejected until the cooldown elapses.
    Open,
    /// A single trial call is allowed to probe recovery.
    HalfOpen,
}

impl BreakerState {
    /// Stable label for metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

struct Inner {
    state: BreakerState,
    consecutive_failures: u32,
    opened_at: Duration,
}

/// A circuit breaker that opens after `failure_threshold` consecutive failures
/// and probes recovery after `cooldown`.
pub struct CircuitBreaker<C: Clock = SystemClock> {
    failure_threshold: u32,
    cooldown: Duration,
    clock: C,
    inner: Arc<Mutex<Inner>>,
}

impl CircuitBreaker<SystemClock> {
    /// Create a breaker backed by the system clock.
    #[must_use]
    pub fn new(failure_threshold: u32, cooldown: Duration) -> Self {
        Self::with_clock(failure_threshold, cooldown, SystemClock::new())
    }
}

impl<C: Clock> CircuitBreaker<C> {
    /// Create a breaker with an injected clock.
    pub fn with_clock(failure_threshold: u32, cooldown: Duration, clock: C) -> Self {
        Self {
            failure_threshold: failure_threshold.max(1),
            cooldown,
            clock,
            inner: Arc::new(Mutex::new(Inner {
                state: BreakerState::Closed,
                consecutive_failures: 0,
                opened_at: Duration::ZERO,
            })),
        }
    }

    /// Current state (transitioning Open -> HalfOpen when cooldown elapsed).
    pub fn state(&self) -> BreakerState {
        let mut inner = self.inner.lock();
        if inner.state == BreakerState::Open
            && self.clock.now().saturating_sub(inner.opened_at) >= self.cooldown
        {
            inner.state = BreakerState::HalfOpen;
        }
        inner.state
    }

    /// Acquire permission to make a call.
    ///
    /// # Errors
    /// Returns [`RejectedByBreaker`] while the breaker is open.
    pub fn acquire(&self) -> Result<(), RejectedByBreaker> {
        match self.state() {
            BreakerState::Open => Err(RejectedByBreaker),
            BreakerState::Closed | BreakerState::HalfOpen => Ok(()),
        }
    }

    /// Record a successful call, closing the breaker.
    pub fn on_success(&self) {
        let mut inner = self.inner.lock();
        inner.consecutive_failures = 0;
        inner.state = BreakerState::Closed;
    }

    /// Record a failed call, opening the breaker at the threshold.
    pub fn on_failure(&self) {
        let mut inner = self.inner.lock();
        inner.consecutive_failures += 1;
        if inner.consecutive_failures >= self.failure_threshold
            || inner.state == BreakerState::HalfOpen
        {
            inner.state = BreakerState::Open;
            inner.opened_at = self.clock.now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;

    #[test]
    fn opens_after_threshold_and_recovers() {
        let clock = ManualClock::new();
        let cb = CircuitBreaker::with_clock(2, Duration::from_secs(1), clock.clone());
        assert_eq!(cb.state(), BreakerState::Closed);
        cb.on_failure();
        assert!(cb.acquire().is_ok());
        cb.on_failure();
        assert_eq!(cb.state(), BreakerState::Open);
        assert_eq!(cb.acquire(), Err(RejectedByBreaker));

        clock.advance(Duration::from_secs(1));
        assert_eq!(cb.state(), BreakerState::HalfOpen);
        cb.on_success();
        assert_eq!(cb.state(), BreakerState::Closed);
    }

    #[test]
    fn half_open_failure_reopens() {
        let clock = ManualClock::new();
        let cb = CircuitBreaker::with_clock(1, Duration::from_secs(1), clock.clone());
        cb.on_failure();
        assert_eq!(cb.state(), BreakerState::Open);
        clock.advance(Duration::from_secs(1));
        assert_eq!(cb.state(), BreakerState::HalfOpen);
        cb.on_failure();
        assert_eq!(cb.state(), BreakerState::Open);
    }
}
