//! Reusable, framework-agnostic resilience primitives.
//!
//! Every fallible I/O boundary in the system composes these: a [`timeout`]
//! bounds latency, [`RetryPolicy`] recovers transient failures with jittered
//! backoff, [`CircuitBreaker`] sheds load on a failing dependency,
//! [`RateLimiter`] caps inbound rate, and [`Bulkhead`] bounds concurrency.
//!
//! The primitives are generic over a [`Clock`] so tests can drive time
//! deterministically without sleeping.

mod breaker;
mod bulkhead;
mod clock;
mod limiter;
mod retry;
mod timeout;

pub use breaker::{BreakerState, CircuitBreaker, RejectedByBreaker};
pub use bulkhead::{Bulkhead, BulkheadFull, BulkheadGuard};
pub use clock::{Clock, ManualClock, SystemClock};
pub use limiter::{RateLimited, RateLimiter};
pub use retry::{retry_if, RetryPolicy};
pub use timeout::{with_timeout, TimeoutError};
