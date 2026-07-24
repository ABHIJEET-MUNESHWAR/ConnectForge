use std::future::Future;
use std::time::Duration;

use thiserror::Error;

/// Returned when an operation exceeds its deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("operation timed out after {0:?}")]
pub struct TimeoutError(pub Duration);

/// Run `fut`, failing with [`TimeoutError`] if it does not complete in `budget`.
///
/// # Errors
/// Returns `Err(TimeoutError)` when the future does not resolve within `budget`.
pub async fn with_timeout<F, T>(budget: Duration, fut: F) -> Result<T, TimeoutError>
where
    F: Future<Output = T>,
{
    match tokio::time::timeout(budget, fut).await {
        Ok(v) => Ok(v),
        Err(_) => Err(TimeoutError(budget)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn completes_within_budget() {
        let r = with_timeout(Duration::from_secs(1), async { 42 }).await;
        assert_eq!(r, Ok(42));
    }

    #[tokio::test(start_paused = true)]
    async fn times_out() {
        let r = with_timeout(Duration::from_millis(10), async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            1
        })
        .await;
        assert_eq!(r, Err(TimeoutError(Duration::from_millis(10))));
    }
}
