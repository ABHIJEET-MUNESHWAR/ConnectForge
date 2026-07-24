//! A broadcast-based [`EventBus`] adapter.
//!
//! Backed by a [`tokio::sync::broadcast`] channel, it fans every published
//! [`LogEvent`] out to all live subscribers. Lagged subscribers (slow
//! consumers) skip missed events rather than blocking producers, and each skip
//! is counted so backpressure is observable.

use async_trait::async_trait;
use futures::stream::StreamExt;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

use connectforge_core::{EventBus, EventStream, LogEvent, PortError};
use connectforge_types::TopicName;

/// An in-process, broadcast fan-out event bus.
pub struct BroadcastEventBus {
    sender: broadcast::Sender<LogEvent>,
}

impl BroadcastEventBus {
    /// Create a bus with the given per-subscriber buffer capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self { sender }
    }

    /// Current number of live subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for BroadcastEventBus {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[async_trait]
impl EventBus for BroadcastEventBus {
    async fn publish(&self, event: LogEvent) -> Result<(), PortError> {
        // A send error only means there are no subscribers — not a failure.
        let _ = self.sender.send(event);
        Ok(())
    }

    async fn subscribe(&self, topics: Vec<TopicName>) -> Result<EventStream, PortError> {
        let rx = self.sender.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(move |item| {
            let topics = topics.clone();
            async move {
                match item {
                    Ok(event) => {
                        if topics.is_empty() || topics.iter().any(|t| t == event.topic()) {
                            Some(event)
                        } else {
                            None
                        }
                    }
                    Err(_lagged) => {
                        metrics::counter!("connectforge_subscriber_lagged_total").increment(1);
                        None
                    }
                }
            }
        });
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connectforge_types::PartitionId;
    use std::sync::Arc;

    fn topic_created(name: &str) -> LogEvent {
        LogEvent::TopicCreated {
            topic: TopicName::new(name).unwrap(),
            partitions: 1,
        }
    }

    #[tokio::test]
    async fn delivers_to_subscriber() {
        let bus = BroadcastEventBus::new(16);
        let mut sub = bus.subscribe(Vec::new()).await.unwrap();
        bus.publish(topic_created("t")).await.unwrap();
        let ev = sub.next().await.unwrap();
        assert_eq!(ev.kind(), "topic_created");
    }

    #[tokio::test]
    async fn topic_filter_excludes_other_topics() {
        let bus = BroadcastEventBus::new(16);
        let mut sub = bus
            .subscribe(vec![TopicName::new("wanted").unwrap()])
            .await
            .unwrap();
        bus.publish(topic_created("other")).await.unwrap();
        bus.publish(LogEvent::RecordsTruncated {
            topic: TopicName::new("wanted").unwrap(),
            partition: PartitionId(0),
            new_start: connectforge_types::Offset(1),
        })
        .await
        .unwrap();
        let ev = sub.next().await.unwrap();
        assert_eq!(ev.topic().as_str(), "wanted");
    }

    #[tokio::test]
    async fn publish_without_subscribers_is_ok() {
        let bus = Arc::new(BroadcastEventBus::new(4));
        assert_eq!(bus.subscriber_count(), 0);
        bus.publish(topic_created("t")).await.unwrap();
    }
}
