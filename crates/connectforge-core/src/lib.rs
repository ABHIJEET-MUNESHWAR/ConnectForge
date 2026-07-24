//! Core domain logic for **ConnectForge**, a connector SDK on a commit log.
//!
//! This crate is pure application logic. It depends only on `connectforge-types`
//! and `connectforge-resilience`, and defines the ports (traits) that infra
//! adapters implement (hexagonal architecture). Nothing here touches a network
//! or a filesystem directly — the [`LogEngine`] and connector runtimes are
//! generic over their [`LogStore`], [`Sink`], [`Source`], [`CheckpointStore`],
//! and [`DeadLetterSink`] ports.

pub mod config;
pub mod connector;
pub mod engine;
pub mod error;
pub mod event;
pub mod ports;

pub use config::LogConfig;
pub use connector::{
    CheckpointStore, DeadLetterSink, Sink, SinkConfig, SinkRuntime, Source, SourceRuntime,
};
pub use engine::LogEngine;
pub use error::{CoreError, PortError};
pub use event::LogEvent;
pub use ports::{AppendOutcome, EventBus, EventStream, LogStore};
