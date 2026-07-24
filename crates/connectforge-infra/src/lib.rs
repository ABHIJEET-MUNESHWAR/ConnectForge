//! Infrastructure adapters for **ConnectForge**.
//!
//! Implements the `connectforge-core` ports: an in-memory store
//! ([`MemoryLogStore`]) for demos/tests, a durable segmented file store
//! ([`FileLogStore`]) that survives restarts, a broadcast event bus
//! ([`BroadcastEventBus`]), a deterministic workload [`RecordGenerator`],
//! checkpoint stores, dead-letter sinks, and reference connectors.

pub mod bus;
pub mod checkpoint;
pub mod connectors;
pub mod dlq;
pub mod filestore;
pub mod generator;
pub mod memstore;
pub mod segment;

pub use bus::BroadcastEventBus;
pub use checkpoint::{FileCheckpointStore, MemoryCheckpointStore, SharedCheckpointStore};
pub use connectors::{CollectingSink, FailingSink, GeneratorSource};
pub use dlq::MemoryDeadLetterSink;
pub use filestore::FileLogStore;
pub use generator::RecordGenerator;
pub use memstore::MemoryLogStore;
pub use segment::Segment;
