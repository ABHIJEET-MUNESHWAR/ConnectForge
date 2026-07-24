use std::future::Future;
use std::time::Duration;

/// Configuration for bounded retries with equal-jitter exponential backoff.
///
/// Jitter is derived from the system clock's nanoseconds, so no `rand`
/// dependency is required and behaviour stays deterministic in tests that pin
/// the attempt count.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Maximum number of attempts (must be >= 1).
    pub max_attempts: u32,
    /// Base backoff applied after the first failure.
    pub base_delay: Duration,
    /// Upper bound on any single backoff.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(20),
            max_delay: Duration::from_secs(1),
        }
    }
}

impl RetryPolicy {
    /// Backoff for a given zero-based attempt index using equal jitter.
    #[must_use]
    pub fn backoff(&self, attempt: u32) -> Duration {
        let exp = self.base_delay.saturating_mul(1u32 << attempt.min(16));
        let capped = exp.min(self.max_delay);
        let half = capped / 2;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let jitter = if half.as_nanos() == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(u64::from(nanos) % (half.as_nanos() as u64 + 1))
        };
        half + jitter
    }
}

/// Retry an async operation while `is_retryable` returns true for its error.
///
/// # Errors
/// Returns the last error once attempts are exhausted or the error is deemed
/// non-retryable.
pub async fn retry_if<F, Fut, T, E, R>(
    policy: RetryPolicy,
    mut op: F,
    is_retryable: R,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    R: Fn(&E) -> bool,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                attempt += 1;
                if attempt >= policy.max_attempts || !is_retryable(&e) {
                    return Err(e);
                }
                tokio::time::sleep(policy.backoff(attempt - 1)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test(start_paused = true)]
    async fn succeeds_after_transient_failures() {
        let calls = AtomicU32::new(0);
        let policy = RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(4),
        };
        let r: Result<u32, &str> = retry_if(
            policy,
            || async {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err("transient")
                } else {
                    Ok(n)
                }
            },
            |_| true,
        )
        .await;
        assert_eq!(r, Ok(2));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn stops_on_non_retryable() {
        let calls = AtomicU32::new(0);
        let r: Result<(), &str> = retry_if(
            RetryPolicy::default(),
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("fatal")
            },
            |_| false,
        )
        .await;
        assert_eq!(r, Err("fatal"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
