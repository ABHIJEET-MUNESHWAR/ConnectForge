//! Checkpoint stores: durable offset persistence so a sink connector resumes
//! exactly where it left off after a restart.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use connectforge_core::{CheckpointStore, PortError};
use connectforge_types::{Checkpoint, ConnectorId, Offset, PartitionId, TopicName};
use dashmap::DashMap;

/// In-memory checkpoint store for demos and tests (not durable).
#[derive(Debug, Default)]
pub struct MemoryCheckpointStore {
    map: DashMap<String, u64>,
}

impl MemoryCheckpointStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

fn key(connector: &ConnectorId, topic: &TopicName, partition: PartitionId) -> String {
    format!("{connector}__{}__{}", topic.as_str(), partition.value())
}

#[async_trait]
impl CheckpointStore for MemoryCheckpointStore {
    async fn load(
        &self,
        connector: &ConnectorId,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Result<Option<Offset>, PortError> {
        Ok(self
            .map
            .get(&key(connector, topic, partition))
            .map(|v| Offset(*v)))
    }

    async fn save(&self, checkpoint: Checkpoint) -> Result<(), PortError> {
        self.map.insert(
            key(
                &checkpoint.connector,
                &checkpoint.topic,
                checkpoint.partition,
            ),
            checkpoint.offset.value(),
        );
        Ok(())
    }
}

/// Durable checkpoint store backed by one small JSON file per
/// `(connector, topic, partition)`.
pub struct FileCheckpointStore {
    root: PathBuf,
}

impl FileCheckpointStore {
    /// Open (creating if needed) a checkpoint store rooted at `root`.
    ///
    /// # Errors
    /// Returns a [`PortError`] if the root directory cannot be created.
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self, PortError> {
        let root = root.into();
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|e| PortError::Transient(format!("create checkpoint dir: {e}")))?;
        Ok(Self { root })
    }

    fn path(&self, connector: &ConnectorId, topic: &TopicName, partition: PartitionId) -> PathBuf {
        self.root
            .join(format!("{}.json", key(connector, topic, partition)))
    }
}

#[async_trait]
impl CheckpointStore for FileCheckpointStore {
    async fn load(
        &self,
        connector: &ConnectorId,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Result<Option<Offset>, PortError> {
        let path = self.path(connector, topic, partition);
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let offset: u64 = serde_json::from_slice(&bytes)
                    .map_err(|e| PortError::Permanent(format!("decode checkpoint: {e}")))?;
                Ok(Some(Offset(offset)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PortError::Transient(format!("read checkpoint: {e}"))),
        }
    }

    async fn save(&self, checkpoint: Checkpoint) -> Result<(), PortError> {
        let path = self.path(
            &checkpoint.connector,
            &checkpoint.topic,
            checkpoint.partition,
        );
        let bytes = serde_json::to_vec(&checkpoint.offset.value())
            .map_err(|e| PortError::Permanent(format!("encode checkpoint: {e}")))?;
        // Write to a temp file then rename for atomic, crash-safe persistence.
        let tmp = path.with_extension("json.tmp");
        tokio::fs::write(&tmp, &bytes)
            .await
            .map_err(|e| PortError::Transient(format!("write checkpoint: {e}")))?;
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|e| PortError::Transient(format!("commit checkpoint: {e}")))?;
        Ok(())
    }
}

/// Shared-pointer alias used by the composition root.
pub type SharedCheckpointStore = Arc<dyn CheckpointStore>;

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (ConnectorId, TopicName) {
        (
            ConnectorId::new("sink-1").unwrap(),
            TopicName::new("events").unwrap(),
        )
    }

    #[tokio::test]
    async fn memory_store_roundtrips() {
        let store = MemoryCheckpointStore::new();
        let (c, t) = ids();
        assert!(store.load(&c, &t, PartitionId(0)).await.unwrap().is_none());
        store
            .save(Checkpoint {
                connector: c.clone(),
                topic: t.clone(),
                partition: PartitionId(0),
                offset: Offset(42),
            })
            .await
            .unwrap();
        assert_eq!(
            store.load(&c, &t, PartitionId(0)).await.unwrap(),
            Some(Offset(42))
        );
    }

    #[tokio::test]
    async fn file_store_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (c, t) = ids();
        {
            let store = FileCheckpointStore::open(dir.path()).await.unwrap();
            store
                .save(Checkpoint {
                    connector: c.clone(),
                    topic: t.clone(),
                    partition: PartitionId(3),
                    offset: Offset(100),
                })
                .await
                .unwrap();
        }
        let reopened = FileCheckpointStore::open(dir.path()).await.unwrap();
        assert_eq!(
            reopened.load(&c, &t, PartitionId(3)).await.unwrap(),
            Some(Offset(100))
        );
        assert!(reopened
            .load(&c, &t, PartitionId(9))
            .await
            .unwrap()
            .is_none());
    }
}
