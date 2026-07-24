//! A deterministic workload generator for demos, load tests, and benchmarks.
//!
//! Given a fixed seed it produces a reproducible stream of [`ProduceRecord`]s
//! with a bounded key space, so runs are comparable across machines.

use connectforge_types::ProduceRecord;

/// A reproducible producer-record generator backed by a small xorshift PRNG.
pub struct RecordGenerator {
    key_space: u64,
    payload_bytes: usize,
    state: u64,
    counter: u64,
}

impl RecordGenerator {
    /// Create a generator over `key_space` distinct keys with `payload_bytes`
    /// payloads, seeded by `seed`.
    #[must_use]
    pub fn new(key_space: u64, payload_bytes: usize, seed: u64) -> Self {
        Self {
            key_space: key_space.max(1),
            payload_bytes: payload_bytes.max(1),
            state: seed | 1,
            counter: 0,
        }
    }

    fn next_u64(&mut self) -> u64 {
        // xorshift64
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Produce the next record in the sequence.
    pub fn next_record(&mut self) -> ProduceRecord {
        let key = self.next_u64() % self.key_space;
        self.counter += 1;
        let payload = format!("record-{}-{}", key, self.counter);
        let mut bytes = payload.into_bytes();
        bytes.resize(self.payload_bytes.max(bytes.len()), b'.');
        ProduceRecord::new(Some(format!("key-{key}")), bytes)
            .expect("generated record is always valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_deterministic_for_a_seed() {
        let mut a = RecordGenerator::new(100, 8, 42);
        let mut b = RecordGenerator::new(100, 8, 42);
        for _ in 0..50 {
            assert_eq!(a.next_record(), b.next_record());
        }
    }

    #[test]
    fn keys_stay_within_space() {
        let mut g = RecordGenerator::new(4, 4, 7);
        for _ in 0..100 {
            let r = g.next_record();
            let key = r.key.unwrap();
            let n: u64 = key.strip_prefix("key-").unwrap().parse().unwrap();
            assert!(n < 4);
        }
    }
}
