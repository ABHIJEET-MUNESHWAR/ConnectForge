//! Engine configuration.

use std::time::Duration;

use connectforge_types::RetentionPolicy;

/// Tunable limits and policies for a [`crate::LogEngine`].
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Maximum number of topics the broker will host.
    pub max_topics: usize,
    /// Default partition count when a topic is created without one.
    pub default_partitions: u32,
    /// Default retention/segmentation policy for new topics.
    pub default_retention: RetentionPolicy,
    /// Sustained produce rate (records/sec) enforced by the token bucket.
    pub produce_rate_per_sec: f64,
    /// Burst capacity of the produce token bucket (records).
    pub produce_burst: f64,
    /// Maximum records returned by a single fetch.
    pub fetch_max_records: usize,
    /// Per-attempt timeout applied to each storage operation.
    pub storage_timeout: Duration,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            max_topics: 1024,
            default_partitions: 3,
            default_retention: RetentionPolicy::default(),
            produce_rate_per_sec: 1_000_000.0,
            produce_burst: 100_000.0,
            fetch_max_records: 1024,
            storage_timeout: Duration::from_secs(5),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let c = LogConfig::default();
        assert!(c.max_topics > 0);
        assert!(c.default_partitions >= 1);
        assert!(c.produce_burst >= 1.0);
        assert!(c.fetch_max_records > 0);
    }
}
