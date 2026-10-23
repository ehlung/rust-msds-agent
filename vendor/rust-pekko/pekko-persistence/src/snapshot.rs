//! Snapshot store trait and implementations

use crate::{SnapshotResult, SnapshotMetadata};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for snapshot storage backends
#[async_trait]
pub trait SnapshotStore: Send + Sync {
    /// Save a snapshot
    async fn save_snapshot(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_data: Vec<u8>,
    ) -> SnapshotResult<()>;

    /// Load the latest snapshot for a persistence ID
    async fn load_snapshot(
        &self,
        persistence_id: &str,
        max_sequence_nr: Option<u64>,
        max_timestamp: Option<DateTime<Utc>>,
    ) -> SnapshotResult<Option<(SnapshotMetadata, Vec<u8>)>>;

    /// Delete a specific snapshot
    async fn delete_snapshot(&self, metadata: &SnapshotMetadata) -> SnapshotResult<()>;

    /// Delete snapshots matching criteria
    async fn delete_snapshots(
        &self,
        persistence_id: &str,
        max_sequence_nr: Option<u64>,
        max_timestamp: Option<DateTime<Utc>>,
    ) -> SnapshotResult<()>;

    /// List all snapshots for a persistence ID
    async fn list_snapshots(
        &self,
        persistence_id: &str,
    ) -> SnapshotResult<Vec<SnapshotMetadata>>;
}

/// Configuration for snapshot stores
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    pub keyspace: String,
    pub table_name: String,
    pub max_snapshots_per_persistence_id: Option<u64>,
    pub retention_period_days: Option<u64>,
    pub compression: CompressionType,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            keyspace: "pekko".to_string(),
            table_name: "snapshots".to_string(),
            max_snapshots_per_persistence_id: Some(10),
            retention_period_days: Some(30),
            compression: CompressionType::None,
        }
    }
}

/// Compression types for snapshot data
#[derive(Debug, Clone, Copy)]
pub enum CompressionType {
    None,
    Gzip,
    Lz4,
    Snappy,
}

/// In-memory snapshot store for testing
pub struct InMemorySnapshotStore {
    snapshots: Arc<parking_lot::RwLock<HashMap<String, Vec<(SnapshotMetadata, Vec<u8>)>>>>,
}

impl InMemorySnapshotStore {
    pub fn new() -> Self {
        Self {
            snapshots: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemorySnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SnapshotStore for InMemorySnapshotStore {
    async fn save_snapshot(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_data: Vec<u8>,
    ) -> SnapshotResult<()> {
        let mut storage = self.snapshots.write();
        let entry = storage
            .entry(metadata.persistence_id.clone())
            .or_insert_with(Vec::new);

        // Remove existing snapshot with same sequence number if it exists
        entry.retain(|(meta, _)| meta.sequence_nr != metadata.sequence_nr);

        // Add new snapshot
        entry.push((metadata.clone(), snapshot_data));

        // Sort by sequence number (newest first)
        entry.sort_by(|a, b| b.0.sequence_nr.cmp(&a.0.sequence_nr));

        Ok(())
    }

    async fn load_snapshot(
        &self,
        persistence_id: &str,
        max_sequence_nr: Option<u64>,
        max_timestamp: Option<DateTime<Utc>>,
    ) -> SnapshotResult<Option<(SnapshotMetadata, Vec<u8>)>> {
        let storage = self.snapshots.read();

        if let Some(snapshots) = storage.get(persistence_id) {
            for (metadata, data) in snapshots {
                // Check sequence number constraint
                if let Some(max_seq) = max_sequence_nr {
                    if metadata.sequence_nr > max_seq {
                        continue;
                    }
                }

                // Check timestamp constraint
                if let Some(max_time) = max_timestamp {
                    if metadata.timestamp > max_time {
                        continue;
                    }
                }

                // Return the first matching snapshot (newest due to sorting)
                return Ok(Some((metadata.clone(), data.clone())));
            }
        }

        Ok(None)
    }

    async fn delete_snapshot(&self, metadata: &SnapshotMetadata) -> SnapshotResult<()> {
        let mut storage = self.snapshots.write();

        if let Some(snapshots) = storage.get_mut(&metadata.persistence_id) {
            snapshots.retain(|(meta, _)| {
                meta.sequence_nr != metadata.sequence_nr
                    || meta.timestamp != metadata.timestamp
            });
        }

        Ok(())
    }

    async fn delete_snapshots(
        &self,
        persistence_id: &str,
        max_sequence_nr: Option<u64>,
        max_timestamp: Option<DateTime<Utc>>,
    ) -> SnapshotResult<()> {
        let mut storage = self.snapshots.write();

        if let Some(snapshots) = storage.get_mut(persistence_id) {
            snapshots.retain(|(metadata, _)| {
                // Keep snapshot if it doesn't match deletion criteria
                if let Some(max_seq) = max_sequence_nr {
                    if metadata.sequence_nr <= max_seq {
                        return false; // Delete this snapshot
                    }
                }

                if let Some(max_time) = max_timestamp {
                    if metadata.timestamp <= max_time {
                        return false; // Delete this snapshot
                    }
                }

                true // Keep this snapshot
            });
        }

        Ok(())
    }

    async fn list_snapshots(
        &self,
        persistence_id: &str,
    ) -> SnapshotResult<Vec<SnapshotMetadata>> {
        let storage = self.snapshots.read();

        if let Some(snapshots) = storage.get(persistence_id) {
            Ok(snapshots.iter().map(|(metadata, _)| metadata.clone()).collect())
        } else {
            Ok(Vec::new())
        }
    }
}

/// Snapshot selection criteria
#[derive(Debug, Clone)]
pub struct SnapshotSelectionCriteria {
    pub max_sequence_nr: Option<u64>,
    pub max_timestamp: Option<DateTime<Utc>>,
    pub min_sequence_nr: Option<u64>,
    pub min_timestamp: Option<DateTime<Utc>>,
}

impl SnapshotSelectionCriteria {
    pub fn latest() -> Self {
        Self {
            max_sequence_nr: None,
            max_timestamp: None,
            min_sequence_nr: None,
            min_timestamp: None,
        }
    }

    pub fn max_sequence_nr(mut self, max: u64) -> Self {
        self.max_sequence_nr = Some(max);
        self
    }

    pub fn max_timestamp(mut self, max: DateTime<Utc>) -> Self {
        self.max_timestamp = Some(max);
        self
    }

    pub fn min_sequence_nr(mut self, min: u64) -> Self {
        self.min_sequence_nr = Some(min);
        self
    }

    pub fn min_timestamp(mut self, min: DateTime<Utc>) -> Self {
        self.min_timestamp = Some(min);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[tokio::test]
    async fn test_in_memory_snapshot_store() {
        let store = InMemorySnapshotStore::new();
        let persistence_id = "test-actor";

        // Save a snapshot
        let metadata = SnapshotMetadata {
            persistence_id: persistence_id.to_string(),
            sequence_nr: 100,
            timestamp: Utc::now(),
        };
        let snapshot_data = b"test snapshot data".to_vec();

        store
            .save_snapshot(&metadata, snapshot_data.clone())
            .await
            .unwrap();

        // Load the snapshot
        let loaded = store
            .load_snapshot(persistence_id, None, None)
            .await
            .unwrap();

        assert!(loaded.is_some());
        let (loaded_metadata, loaded_data) = loaded.unwrap();
        assert_eq!(loaded_metadata.persistence_id, persistence_id);
        assert_eq!(loaded_metadata.sequence_nr, 100);
        assert_eq!(loaded_data, snapshot_data);
    }

    #[tokio::test]
    async fn test_snapshot_sequence_filtering() {
        let store = InMemorySnapshotStore::new();
        let persistence_id = "test-actor";

        // Save multiple snapshots
        for seq_nr in [50, 100, 150] {
            let metadata = SnapshotMetadata {
                persistence_id: persistence_id.to_string(),
                sequence_nr: seq_nr,
                timestamp: Utc::now(),
            };
            let snapshot_data = format!("snapshot-{}", seq_nr).into_bytes();

            store
                .save_snapshot(&metadata, snapshot_data)
                .await
                .unwrap();
        }

        // Load with max sequence number filter
        let loaded = store
            .load_snapshot(persistence_id, Some(120), None)
            .await
            .unwrap();

        assert!(loaded.is_some());
        let (metadata, _) = loaded.unwrap();
        assert_eq!(metadata.sequence_nr, 100); // Should get the highest <= 120
    }

    #[tokio::test]
    async fn test_snapshot_deletion() {
        let store = InMemorySnapshotStore::new();
        let persistence_id = "test-actor";

        // Save snapshots
        for seq_nr in [50, 100, 150] {
            let metadata = SnapshotMetadata {
                persistence_id: persistence_id.to_string(),
                sequence_nr: seq_nr,
                timestamp: Utc::now(),
            };
            let snapshot_data = format!("snapshot-{}", seq_nr).into_bytes();

            store
                .save_snapshot(&metadata, snapshot_data)
                .await
                .unwrap();
        }

        // Delete snapshots up to sequence 100
        store
            .delete_snapshots(persistence_id, Some(100), None)
            .await
            .unwrap();

        // Should only have snapshot 150 left
        let snapshots = store.list_snapshots(persistence_id).await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].sequence_nr, 150);
    }

    #[test]
    fn test_snapshot_selection_criteria() {
        let criteria = SnapshotSelectionCriteria::latest()
            .max_sequence_nr(100)
            .min_sequence_nr(50);

        assert_eq!(criteria.max_sequence_nr, Some(100));
        assert_eq!(criteria.min_sequence_nr, Some(50));
        assert!(criteria.max_timestamp.is_none());
    }
}