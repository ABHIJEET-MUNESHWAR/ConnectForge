//! The composed GraphQL schema: queries, mutations, and a subscription.
//!
//! Root operations (topics, topic, fetch, stats, connectors, deadLetters,
//! createTopic, produce, events) comfortably exceed five, which is why GraphQL
//! is used over REST.

use std::sync::Arc;

use async_graphql::{Context, Error, Object, Schema, Subscription};
use futures::stream::{Stream, StreamExt};

use connectforge_core::{LogEngine, LogStore};
use connectforge_types::{Offset, PartitionId, ProduceRecord, RetentionPolicy, TopicName};

use crate::model::{
    ConnectorStatusObject, DeadLetterObject, EventObject, FetchResultObject, ProduceInput,
    ProduceResultObject, StatsObject, TopicObject,
};
use crate::registry::ConnectorRegistry;

/// The type-erased store used by the running server (chosen at startup:
/// in-memory or durable file).
pub type DynStore = Arc<dyn LogStore>;

/// The concrete engine wired into the GraphQL context.
pub type AppEngine = Arc<LogEngine<DynStore, crate::BusHandle>>;

/// The shared connector registry wired into the GraphQL context.
pub type AppRegistry = Arc<ConnectorRegistry>;

/// The composed schema type.
pub type LogSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

fn to_err<E: std::fmt::Display>(e: E) -> Error {
    Error::new(e.to_string())
}

/// Read-only queries (the CQRS query side).
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// List all topics.
    async fn topics(&self, ctx: &Context<'_>) -> Result<Vec<TopicObject>, Error> {
        let engine = ctx.data::<AppEngine>()?;
        Ok(engine
            .topics()
            .await
            .map_err(to_err)?
            .into_iter()
            .map(TopicObject::from)
            .collect())
    }

    /// Fetch a single topic's configuration by name.
    async fn topic(&self, ctx: &Context<'_>, name: String) -> Result<Option<TopicObject>, Error> {
        let engine = ctx.data::<AppEngine>()?;
        let want = TopicName::new(name).map_err(to_err)?;
        Ok(engine
            .topics()
            .await
            .map_err(to_err)?
            .into_iter()
            .find(|c| c.name == want)
            .map(TopicObject::from))
    }

    /// Read records from a partition starting at `from`.
    async fn fetch(
        &self,
        ctx: &Context<'_>,
        topic: String,
        partition: u32,
        from: u64,
        #[graphql(default = 100)] max: u32,
    ) -> Result<FetchResultObject, Error> {
        let engine = ctx.data::<AppEngine>()?;
        let topic = TopicName::new(topic).map_err(to_err)?;
        let result = engine
            .fetch(&topic, PartitionId(partition), Offset(from), max as usize)
            .await
            .map_err(to_err)?;
        Ok(FetchResultObject::from(result))
    }

    /// Broker-wide statistics.
    async fn stats(&self, ctx: &Context<'_>) -> Result<StatsObject, Error> {
        let engine = ctx.data::<AppEngine>()?;
        Ok(StatsObject::from(engine.stats().await.map_err(to_err)?))
    }

    /// List all registered connectors and their live status.
    async fn connectors(&self, ctx: &Context<'_>) -> Result<Vec<ConnectorStatusObject>, Error> {
        let registry = ctx.data::<AppRegistry>()?;
        Ok(registry
            .statuses()
            .into_iter()
            .map(ConnectorStatusObject::from)
            .collect())
    }

    /// Inspect records that connectors failed to deliver (the dead-letter queue).
    async fn dead_letters(&self, ctx: &Context<'_>) -> Result<Vec<DeadLetterObject>, Error> {
        let registry = ctx.data::<AppRegistry>()?;
        Ok(registry
            .dead_letters()
            .into_iter()
            .map(DeadLetterObject::from)
            .collect())
    }
}

/// Mutations (the CQRS command side).
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Create a topic with the given partition count and retention.
    async fn create_topic(
        &self,
        ctx: &Context<'_>,
        name: String,
        #[graphql(default = 1)] partitions: u32,
        #[graphql(default = 1024)] segment_max_records: u64,
        #[graphql(default = 1_000_000)] max_records: u64,
    ) -> Result<TopicObject, Error> {
        let engine = ctx.data::<AppEngine>()?;
        let topic = TopicName::new(name).map_err(to_err)?;
        let retention = RetentionPolicy {
            segment_max_records,
            max_records,
        };
        engine
            .create_topic(topic.clone(), partitions, retention)
            .await
            .map_err(to_err)?;
        Ok(TopicObject {
            name: topic.as_str().to_owned(),
            partitions,
            segment_max_records,
            max_records,
        })
    }

    /// Produce a batch of records to a topic.
    async fn produce(
        &self,
        ctx: &Context<'_>,
        topic: String,
        records: Vec<ProduceInput>,
    ) -> Result<ProduceResultObject, Error> {
        let engine = ctx.data::<AppEngine>()?;
        let topic = TopicName::new(topic).map_err(to_err)?;
        let mut batch = Vec::with_capacity(records.len());
        for r in records {
            batch.push(ProduceRecord::new(r.key, r.payload.into_bytes()).map_err(to_err)?);
        }
        let result = engine.produce(&topic, batch).await.map_err(to_err)?;
        Ok(ProduceResultObject::from(result))
    }
}

/// Subscriptions.
pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    /// Stream live broker events, optionally filtered to a set of topics.
    async fn events(
        &self,
        ctx: &Context<'_>,
        #[graphql(default)] topics: Vec<String>,
    ) -> Result<impl Stream<Item = EventObject>, Error> {
        let engine = ctx.data::<AppEngine>()?;
        let mut names = Vec::with_capacity(topics.len());
        for t in topics {
            names.push(TopicName::new(t).map_err(to_err)?);
        }
        let stream = engine.subscribe(names).await.map_err(to_err)?;
        Ok(stream.map(EventObject::from))
    }
}

/// Build the schema with depth/complexity guards and the engine in context.
#[must_use]
pub fn build_schema(engine: AppEngine, registry: AppRegistry) -> LogSchema {
    Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .limit_depth(12)
        .limit_complexity(512)
        .data(engine)
        .data(registry)
        .finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use connectforge_core::LogConfig;
    use connectforge_infra::{BroadcastEventBus, MemoryLogStore};

    fn schema() -> LogSchema {
        let store: DynStore = Arc::new(MemoryLogStore::new());
        let bus = Arc::new(BroadcastEventBus::new(256));
        let engine = Arc::new(LogEngine::new(LogConfig::default(), Arc::new(store), bus));
        let registry = Arc::new(ConnectorRegistry::new());
        build_schema(engine, registry)
    }

    #[tokio::test]
    async fn create_produce_fetch_flow() {
        let schema = schema();
        let create =
            r#"mutation { createTopic(name: "orders", partitions: 1) { name partitions } }"#;
        let res = schema.execute(create).await;
        assert!(res.errors.is_empty(), "{:?}", res.errors);

        let produce = r#"mutation {
            produce(topic: "orders", records: [{payload: "hello"}, {payload: "world"}]) {
                baseOffset lastOffset count
            }
        }"#;
        let res = schema.execute(produce).await;
        assert!(res.errors.is_empty(), "{:?}", res.errors);
        assert!(res.data.to_string().contains("count"));

        let fetch = r#"{ fetch(topic: "orders", partition: 0, from: 0) { records { offset payload } highWatermark } }"#;
        let res = schema.execute(fetch).await;
        assert!(res.errors.is_empty(), "{:?}", res.errors);
        let data = res.data.to_string();
        assert!(data.contains("hello"));
        assert!(data.contains("world"));
    }

    #[tokio::test]
    async fn stats_and_topics_queries() {
        let schema = schema();
        schema
            .execute(r#"mutation { createTopic(name: "t", partitions: 2) { name } }"#)
            .await;
        let res = schema
            .execute("{ topics { name partitions } stats { topics partitions } }")
            .await;
        assert!(res.errors.is_empty(), "{:?}", res.errors);
        assert!(res.data.to_string().contains("partitions"));
    }

    #[tokio::test]
    async fn produce_to_unknown_topic_errors() {
        let schema = schema();
        let res = schema
            .execute(r#"mutation { produce(topic: "nope", records: [{payload: "x"}]) { count } }"#)
            .await;
        assert!(!res.errors.is_empty());
    }

    #[tokio::test]
    async fn connectors_and_dead_letters_queries() {
        let schema = schema();
        let res = schema
            .execute(
                "{ connectors { id kind running processed } deadLetters { connector reason } }",
            )
            .await;
        assert!(res.errors.is_empty(), "{:?}", res.errors);
        let data = res.data.to_string();
        assert!(data.contains("connectors"));
        assert!(data.contains("deadLetters"));
    }
}
