//! GraphQL API surface for **ConnectForge**.
//!
//! Exposes queries (`topics`, `topic`, `fetch`, `stats`, `connectors`,
//! `deadLetters`), mutations (`createTopic`, `produce`), and a subscription
//! (`events`) — well beyond five root operations, which is why GraphQL is
//! preferred over REST here.

pub mod model;
pub mod registry;
pub mod schema;

/// The concrete event-bus type wired into the running server.
pub type BusHandle = connectforge_infra::BroadcastEventBus;

pub use registry::ConnectorRegistry;
pub use schema::{
    build_schema, AppEngine, AppRegistry, DynStore, LogSchema, MutationRoot, QueryRoot,
    SubscriptionRoot,
};
