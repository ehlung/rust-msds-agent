//! CoreDB implementations for journal and snapshot store
//!
//! This module provides persistence backends using CoreDB, a Cassandra-compatible
//! embedded database written in Rust.

use crate::{
    Journal, JournalConfig, JournalError, JournalResult, PersistentRepr,
    SnapshotConfig, SnapshotError, SnapshotMetadata, SnapshotResult, SnapshotStore,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use coredb::{CoreDB, DatabaseConfig, QueryResult, CassandraValue};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug};
use uuid::Uuid;

/// CoreDB journal implementation
pub struct CoreDBJournal {
    db: Arc<RwLock<CoreDB>>,
    config: JournalConfig,
}

impl CoreDBJournal {
    /// Create a new CoreDB journal
    pub async fn new(
        db_path: &str,
        config: JournalConfig,
    ) -> Result<Self, CoreDBError> {
        let db_config = DatabaseConfig {
            data_directory: PathBuf::from(db_path).join("data"),
            commitlog_directory: PathBuf::from(db_path).join("commitlog"),
            memtable_flush_threshold_mb: 16,
            compaction_throughput_mb_per_sec: 16,
            concurrent_reads: 32,
            concurrent_writes: 32,
            block_cache_size_mb: 64,
            block_cache_max_entries: 1000,
        };

        let db = CoreDB::new(db_config)
            .await
            .map_err(|e| CoreDBError::Connection(e.to_string()))?;

        let db = Arc::new(RwLock::new(db));

        // Initialize schema
        Self::initialize_schema(&db, &config).await?;
        Ok(Self { db, config })
    }

    /// Create from existing CoreDB instance (shared with other components)
    pub fn from_existing(db: Arc<RwLock<CoreDB>>, config: JournalConfig) -> Self {
        Self { db, config }
    }

    async fn initialize_schema(
        db: &Arc<RwLock<CoreDB>>,
        config: &JournalConfig,
    ) -> Result<(), CoreDBError> {
        let db_guard = db.read().await;

        // Create keyspace
        let create_keyspace = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': {}}}",
            config.keyspace, config.replication_factor
        );
        let _ = db_guard.execute_cql(&create_keyspace).await; // Ignore if exists

        // Create messages table
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (
                persistence_id TEXT,
                sequence_nr BIGINT,
                timestamp BIGINT,
                writer_uuid TEXT,
                manifest TEXT,
                payload TEXT,
                tags TEXT,
                PRIMARY KEY (persistence_id, sequence_nr)
            )",
            config.keyspace, config.table_name
        );
        let _ = db_guard.execute_cql(&create_table).await; // Ignore if exists
        Ok(())
    }

    fn escape_cql_string(s: &str) -> String {
        s.replace('\'', "''")
    }
}

#[async_trait]
impl Journal for CoreDBJournal {
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> JournalResult<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let db = self.db.read().await;

        // Validate sequence numbers
        for msg in &messages {
            let current_highest = self.highest_sequence_nr(&msg.persistence_id).await?;
            let expected = if current_highest == 0 { 1 } else { current_highest + 1 };
            
            if msg.sequence_nr < expected {
                return Err(JournalError::SequenceNumberOutOfRange(msg.sequence_nr));
            }
        }

        // Write messages
        for msg in &messages {
            let payload_base64 = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &msg.payload
            );
            let tags_json = serde_json::to_string(&msg.tags)
                .map_err(|e| JournalError::Write(e.to_string()))?;

            let query = format!(
                "INSERT INTO {}.{} (persistence_id, sequence_nr, timestamp, writer_uuid, manifest, payload, tags) \
                 VALUES ('{}', {}, {}, '{}', '{}', '{}', '{}')",
                self.config.keyspace,
                self.config.table_name,
                Self::escape_cql_string(&msg.persistence_id),
                msg.sequence_nr,
                msg.timestamp.timestamp_millis(),
                msg.writer_uuid,
                Self::escape_cql_string(&msg.manifest),
                Self::escape_cql_string(&payload_base64),
                Self::escape_cql_string(&tags_json)
            );

            db.execute_cql(&query)
                .await
                .map_err(|e| JournalError::Write(e.to_string()))?;
        }

        // Flush to persist
        db.flush_all()
            .await
            .map_err(|e| JournalError::Write(e.to_string()))?;

        debug!("Wrote {} messages to journal", messages.len());
        Ok(())
    }

    async fn replay_messages(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
        max_messages: Option<u64>,
    ) -> JournalResult<Vec<PersistentRepr>> {
        let db = self.db.read().await;

        let query = format!(
            "SELECT persistence_id, sequence_nr, timestamp, writer_uuid, manifest, payload, tags \
             FROM {}.{} WHERE persistence_id = '{}' AND sequence_nr >= {} AND sequence_nr <= {}",
            self.config.keyspace,
            self.config.table_name,
            Self::escape_cql_string(persistence_id),
            from_sequence_nr,
            to_sequence_nr
        );

        let result = db.execute_cql(&query)
            .await
            .map_err(|e| JournalError::Read(e.to_string()))?;

        let mut messages = Vec::new();

        if let QueryResult::Rows(rows) = result {
            for row in rows {
                if let Some(msg) = self.row_to_persistent_repr(&row) {
                    messages.push(msg);
                }
            }
        }

        // Sort by sequence number
        messages.sort_by_key(|m| m.sequence_nr);

        // Apply limit
        if let Some(max) = max_messages {
            messages.truncate(max as usize);
        }

        debug!("Replayed {} messages for {}", messages.len(), persistence_id);
        Ok(messages)
    }

    async fn highest_sequence_nr(&self, persistence_id: &str) -> JournalResult<u64> {
        let db = self.db.read().await;

        let query = format!(
            "SELECT sequence_nr FROM {}.{} WHERE persistence_id = '{}'",
            self.config.keyspace,
            self.config.table_name,
            Self::escape_cql_string(persistence_id)
        );

        let result = db.execute_cql(&query)
            .await
            .map_err(|e| JournalError::Read(e.to_string()))?;

        let mut highest: u64 = 0;

        if let QueryResult::Rows(rows) = result {
            for row in rows {
                if let Some(val) = row.columns.get("sequence_nr") {
                    let seq = match val {
                        CassandraValue::BigInt(n) => *n as u64,
                        CassandraValue::Int(n) => *n as u64,
                        _ => continue,
                    };
                    if seq > highest {
                        highest = seq;
                    }
                }
            }
        }

        Ok(highest)
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> JournalResult<()> {
        let db = self.db.read().await;

        // CoreDB doesn't support range DELETE, so we need to delete individually
        // First, get all sequence numbers to delete
        let query = format!(
            "SELECT sequence_nr FROM {}.{} WHERE persistence_id = '{}' AND sequence_nr <= {}",
            self.config.keyspace,
            self.config.table_name,
            Self::escape_cql_string(persistence_id),
            to_sequence_nr
        );

        let result = db.execute_cql(&query)
            .await
            .map_err(|e| JournalError::Write(e.to_string()))?;

        if let QueryResult::Rows(rows) = result {
            for row in rows {
                if let Some(CassandraValue::BigInt(seq)) = row.columns.get("sequence_nr") {
                    let delete_query = format!(
                        "DELETE FROM {}.{} WHERE persistence_id = '{}' AND sequence_nr = {}",
                        self.config.keyspace,
                        self.config.table_name,
                        Self::escape_cql_string(persistence_id),
                        seq
                    );
                    let _ = db.execute_cql(&delete_query).await;
                }
            }
        }

        db.flush_all()
            .await
            .map_err(|e| JournalError::Write(e.to_string()))?;

        debug!("Deleted messages up to {} for {}", to_sequence_nr, persistence_id);
        Ok(())
    }

    async fn persistence_ids(&self) -> JournalResult<Vec<String>> {
        let db = self.db.read().await;

        let query = format!(
            "SELECT DISTINCT persistence_id FROM {}.{}",
            self.config.keyspace, self.config.table_name
        );

        let result = db.execute_cql(&query)
            .await
            .map_err(|e| JournalError::Read(e.to_string()))?;

        let mut ids = Vec::new();

        if let QueryResult::Rows(rows) = result {
            for row in rows {
                if let Some(CassandraValue::Text(id)) = row.columns.get("persistence_id") {
                    if !ids.contains(id) {
                        ids.push(id.clone());
                    }
                }
            }
        }

        Ok(ids)
    }
}

impl CoreDBJournal {
    fn row_to_persistent_repr(&self, row: &coredb::query::Row) -> Option<PersistentRepr> {
        let columns = &row.columns;

        let persistence_id = match columns.get("persistence_id")? {
            CassandraValue::Text(s) => s.clone(),
            _ => return None,
        };

        let sequence_nr = match columns.get("sequence_nr")? {
            CassandraValue::BigInt(n) => *n as u64,
            CassandraValue::Int(n) => *n as u64,
            _ => return None,
        };

        let timestamp = match columns.get("timestamp") {
            Some(CassandraValue::BigInt(ts)) => {
                DateTime::from_timestamp_millis(*ts).unwrap_or_else(Utc::now)
            }
            _ => Utc::now(),
        };

        let writer_uuid = match columns.get("writer_uuid") {
            Some(CassandraValue::Text(s)) => {
                Uuid::parse_str(s).unwrap_or_else(|_| Uuid::new_v4())
            }
            _ => Uuid::new_v4(),
        };

        let manifest = match columns.get("manifest") {
            Some(CassandraValue::Text(s)) => s.clone(),
            _ => String::new(),
        };

        let payload = match columns.get("payload") {
            Some(CassandraValue::Text(s)) => {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                    .unwrap_or_default()
            }
            _ => Vec::new(),
        };

        let tags: Vec<String> = match columns.get("tags") {
            Some(CassandraValue::Text(s)) => {
                serde_json::from_str(s).unwrap_or_default()
            }
            _ => Vec::new(),
        };

        Some(PersistentRepr {
            persistence_id,
            sequence_nr,
            manifest,
            payload,
            timestamp,
            writer_uuid,
            tags,
        })
    }
}

// ============================================================================
// Snapshot Store Implementation
// ============================================================================

/// CoreDB snapshot store implementation
pub struct CoreDBSnapshotStore {
    db: Arc<RwLock<CoreDB>>,
    config: SnapshotConfig,
}

impl CoreDBSnapshotStore {
    /// Create a new CoreDB snapshot store
    pub async fn new(
        db_path: &str,
        config: SnapshotConfig,
    ) -> Result<Self, CoreDBError> {
        let db_config = DatabaseConfig {
            data_directory: PathBuf::from(db_path).join("data"),
            commitlog_directory: PathBuf::from(db_path).join("commitlog"),
            memtable_flush_threshold_mb: 16,
            compaction_throughput_mb_per_sec: 16,
            concurrent_reads: 32,
            concurrent_writes: 32,
            block_cache_size_mb: 64,
            block_cache_max_entries: 1000,
        };

        let db = CoreDB::new(db_config)
            .await
            .map_err(|e| CoreDBError::Connection(e.to_string()))?;

        let db = Arc::new(RwLock::new(db));

        Self::initialize_schema(&db, &config).await?;
        Ok(Self { db, config })
    }

    /// Create from existing CoreDB instance
    pub fn from_existing(db: Arc<RwLock<CoreDB>>, config: SnapshotConfig) -> Self {
        Self { db, config }
    }

    async fn initialize_schema(
        db: &Arc<RwLock<CoreDB>>,
        config: &SnapshotConfig,
    ) -> Result<(), CoreDBError> {
        let db_guard = db.read().await;

        // Create keyspace
        let create_keyspace = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}",
            config.keyspace
        );
        let _ = db_guard.execute_cql(&create_keyspace).await;

        // Create snapshots table
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (
                persistence_id TEXT,
                sequence_nr BIGINT,
                timestamp BIGINT,
                snapshot_data TEXT,
                PRIMARY KEY (persistence_id, sequence_nr)
            )",
            config.keyspace, config.table_name
        );
        let _ = db_guard.execute_cql(&create_table).await;
        Ok(())
    }

    fn escape_cql_string(s: &str) -> String {
        s.replace('\'', "''")
    }
}

#[async_trait]
impl SnapshotStore for CoreDBSnapshotStore {
    async fn save_snapshot(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_data: Vec<u8>,
    ) -> SnapshotResult<()> {
        let db = self.db.read().await;

        let data_base64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &snapshot_data
        );

        let query = format!(
            "INSERT INTO {}.{} (persistence_id, sequence_nr, timestamp, snapshot_data) \
             VALUES ('{}', {}, {}, '{}')",
            self.config.keyspace,
            self.config.table_name,
            Self::escape_cql_string(&metadata.persistence_id),
            metadata.sequence_nr,
            metadata.timestamp.timestamp_millis(),
            Self::escape_cql_string(&data_base64)
        );

        db.execute_cql(&query)
            .await
            .map_err(|e| SnapshotError::Save(e.to_string()))?;

        db.flush_all()
            .await
            .map_err(|e| SnapshotError::Save(e.to_string()))?;

        debug!("Saved snapshot for {} at seq {}", metadata.persistence_id, metadata.sequence_nr);
        Ok(())
    }

    async fn load_snapshot(
        &self,
        persistence_id: &str,
        max_sequence_nr: Option<u64>,
        max_timestamp: Option<DateTime<Utc>>,
    ) -> SnapshotResult<Option<(SnapshotMetadata, Vec<u8>)>> {
        let db = self.db.read().await;

        let mut query = format!(
            "SELECT persistence_id, sequence_nr, timestamp, snapshot_data \
             FROM {}.{} WHERE persistence_id = '{}'",
            self.config.keyspace,
            self.config.table_name,
            Self::escape_cql_string(persistence_id)
        );

        if let Some(max_seq) = max_sequence_nr {
            query.push_str(&format!(" AND sequence_nr <= {}", max_seq));
        }

        let result = db.execute_cql(&query)
            .await
            .map_err(|e| SnapshotError::Load(e.to_string()))?;

        let mut snapshots = Vec::new();

        if let QueryResult::Rows(rows) = result {
            for row in rows {
                if let Some((meta, data)) = self.row_to_snapshot(&row) {
                    // Filter by timestamp if specified
                    if let Some(max_ts) = max_timestamp {
                        if meta.timestamp > max_ts {
                            continue;
                        }
                    }
                    snapshots.push((meta, data));
                }
            }
        }

        // Sort by sequence_nr descending and return the latest
        snapshots.sort_by(|a, b| b.0.sequence_nr.cmp(&a.0.sequence_nr));
        Ok(snapshots.into_iter().next())
    }

    async fn delete_snapshot(&self, metadata: &SnapshotMetadata) -> SnapshotResult<()> {
        let db = self.db.read().await;

        let query = format!(
            "DELETE FROM {}.{} WHERE persistence_id = '{}' AND sequence_nr = {}",
            self.config.keyspace,
            self.config.table_name,
            Self::escape_cql_string(&metadata.persistence_id),
            metadata.sequence_nr
        );

        db.execute_cql(&query)
            .await
            .map_err(|e| SnapshotError::Delete(e.to_string()))?;

        db.flush_all()
            .await
            .map_err(|e| SnapshotError::Delete(e.to_string()))?;

        Ok(())
    }

    async fn delete_snapshots(
        &self,
        persistence_id: &str,
        max_sequence_nr: Option<u64>,
        max_timestamp: Option<DateTime<Utc>>,
    ) -> SnapshotResult<()> {
        let db = self.db.read().await;

        // First, find snapshots to delete
        let mut query = format!(
            "SELECT sequence_nr, timestamp FROM {}.{} WHERE persistence_id = '{}'",
            self.config.keyspace,
            self.config.table_name,
            Self::escape_cql_string(persistence_id)
        );

        if let Some(max_seq) = max_sequence_nr {
            query.push_str(&format!(" AND sequence_nr <= {}", max_seq));
        }

        let result = db.execute_cql(&query)
            .await
            .map_err(|e| SnapshotError::Delete(e.to_string()))?;

        if let QueryResult::Rows(rows) = result {
            for row in rows {
                let seq = match row.columns.get("sequence_nr") {
                    Some(CassandraValue::BigInt(n)) => *n,
                    Some(CassandraValue::Int(n)) => *n as i64,
                    _ => continue,
                };

                // Check timestamp filter
                if let Some(max_ts) = max_timestamp {
                    if let Some(CassandraValue::BigInt(ts)) = row.columns.get("timestamp") {
                        if let Some(row_ts) = DateTime::from_timestamp_millis(*ts) {
                            if row_ts > max_ts {
                                continue;
                            }
                        }
                    }
                }

                let delete_query = format!(
                    "DELETE FROM {}.{} WHERE persistence_id = '{}' AND sequence_nr = {}",
                    self.config.keyspace,
                    self.config.table_name,
                    Self::escape_cql_string(persistence_id),
                    seq
                );
                let _ = db.execute_cql(&delete_query).await;
            }
        }

        db.flush_all()
            .await
            .map_err(|e| SnapshotError::Delete(e.to_string()))?;

        Ok(())
    }

    async fn list_snapshots(
        &self,
        persistence_id: &str,
    ) -> SnapshotResult<Vec<SnapshotMetadata>> {
        let db = self.db.read().await;

        let query = format!(
            "SELECT persistence_id, sequence_nr, timestamp FROM {}.{} WHERE persistence_id = '{}'",
            self.config.keyspace,
            self.config.table_name,
            Self::escape_cql_string(persistence_id)
        );

        let result = db.execute_cql(&query)
            .await
            .map_err(|e| SnapshotError::Load(e.to_string()))?;

        let mut snapshots = Vec::new();

        if let QueryResult::Rows(rows) = result {
            for row in rows {
                if let Some(meta) = self.row_to_metadata(&row) {
                    snapshots.push(meta);
                }
            }
        }

        // Sort by sequence_nr ascending
        snapshots.sort_by_key(|m| m.sequence_nr);
        Ok(snapshots)
    }
}

impl CoreDBSnapshotStore {
    fn row_to_snapshot(&self, row: &coredb::query::Row) -> Option<(SnapshotMetadata, Vec<u8>)> {
        let meta = self.row_to_metadata(row)?;
        let columns = &row.columns;

        let data = match columns.get("snapshot_data") {
            Some(CassandraValue::Text(s)) => {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                    .unwrap_or_default()
            }
            _ => Vec::new(),
        };

        Some((meta, data))
    }

    fn row_to_metadata(&self, row: &coredb::query::Row) -> Option<SnapshotMetadata> {
        let columns = &row.columns;

        let persistence_id = match columns.get("persistence_id")? {
            CassandraValue::Text(s) => s.clone(),
            _ => return None,
        };

        let sequence_nr = match columns.get("sequence_nr")? {
            CassandraValue::BigInt(n) => *n as u64,
            CassandraValue::Int(n) => *n as u64,
            _ => return None,
        };

        let timestamp = match columns.get("timestamp") {
            Some(CassandraValue::BigInt(ts)) => {
                DateTime::from_timestamp_millis(*ts).unwrap_or_else(Utc::now)
            }
            _ => Utc::now(),
        };

        Some(SnapshotMetadata {
            persistence_id,
            sequence_nr,
            timestamp,
        })
    }
}

// ============================================================================
// Error Types
// ============================================================================

/// Errors specific to CoreDB operations
#[derive(Debug, thiserror::Error)]
pub enum CoreDBError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Schema initialization error: {0}")]
    Schema(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<CoreDBError> for JournalError {
    fn from(e: CoreDBError) -> Self {
        match e {
            CoreDBError::Connection(msg) => JournalError::Connection(msg),
            CoreDBError::Query(msg) => JournalError::Query(msg),
            CoreDBError::Schema(msg) => JournalError::Write(msg),
            CoreDBError::Serialization(msg) => JournalError::Write(msg),
        }
    }
}

impl From<CoreDBError> for SnapshotError {
    fn from(e: CoreDBError) -> Self {
        match e {
            CoreDBError::Connection(msg) => SnapshotError::Connection(msg),
            CoreDBError::Query(msg) => SnapshotError::Load(msg),
            CoreDBError::Schema(msg) => SnapshotError::Save(msg),
            CoreDBError::Serialization(msg) => SnapshotError::Save(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_coredb_journal_basic() {
        let tmp = tempdir().unwrap();
        let config = JournalConfig::default();

        let journal = CoreDBJournal::new(tmp.path().to_str().unwrap(), config)
            .await
            .unwrap();

        // Test write and replay
        let msg = PersistentRepr::new(
            "test-actor".to_string(),
            1,
            "TestEvent".to_string(),
            b"test payload".to_vec(),
        );

        journal.write_messages(vec![msg]).await.unwrap();

        let replayed = journal
            .replay_messages("test-actor", 1, 100, None)
            .await
            .unwrap();

        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].sequence_nr, 1);
    }

    #[tokio::test]
    async fn test_coredb_snapshot_basic() {
        let tmp = tempdir().unwrap();
        let config = SnapshotConfig::default();

        let store = CoreDBSnapshotStore::new(tmp.path().to_str().unwrap(), config)
            .await
            .unwrap();

        let metadata = SnapshotMetadata {
            persistence_id: "test-actor".to_string(),
            sequence_nr: 10,
            timestamp: Utc::now(),
        };

        store
            .save_snapshot(&metadata, b"snapshot data".to_vec())
            .await
            .unwrap();

        let loaded = store
            .load_snapshot("test-actor", None, None)
            .await
            .unwrap();

        assert!(loaded.is_some());
        let (meta, data) = loaded.unwrap();
        assert_eq!(meta.sequence_nr, 10);
        assert_eq!(data, b"snapshot data");
    }
}
