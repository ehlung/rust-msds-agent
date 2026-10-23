//! PostgreSQL backend for event sourcing persistence
//!
//! This module provides Journal and SnapshotStore implementations
//! using PostgreSQL (compatible with Neon, Supabase, etc.)

#[cfg(feature = "postgres")]
use crate::{
    Journal, JournalError, JournalResult, PersistentRepr, SnapshotError, SnapshotMetadata,
    SnapshotResult, SnapshotStore,
};

#[cfg(feature = "postgres")]
use async_trait::async_trait;

#[cfg(feature = "postgres")]
use chrono::{DateTime, Utc};

#[cfg(feature = "postgres")]
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

#[cfg(feature = "postgres")]
use uuid::Uuid;

#[cfg(feature = "postgres")]
use std::sync::Arc;

/// PostgreSQL Journal configuration
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct PostgresJournalConfig {
    /// Database connection URL (e.g., postgres://user:pass@host/db)
    pub database_url: String,
    /// Table name for storing events
    pub table_name: String,
    /// Maximum number of connections in the pool
    pub max_connections: u32,
    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,
}

#[cfg(feature = "postgres")]
impl Default for PostgresJournalConfig {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            table_name: "pekko_journal".to_string(),
            max_connections: 5,
            connect_timeout_secs: 30,
        }
    }
}

#[cfg(feature = "postgres")]
impl PostgresJournalConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            ..Default::default()
        }
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    pub fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }
}

/// PostgreSQL-based Journal implementation
#[cfg(feature = "postgres")]
pub struct PostgresJournal {
    pool: PgPool,
    config: PostgresJournalConfig,
}

#[cfg(feature = "postgres")]
impl PostgresJournal {
    /// Create a new PostgreSQL journal
    pub async fn new(config: PostgresJournalConfig) -> Result<Self, JournalError> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
            .connect(&config.database_url)
            .await
            .map_err(|e| JournalError::Connection(e.to_string()))?;

        let journal = Self { pool, config };
        journal.initialize().await?;
        Ok(journal)
    }

    /// Initialize the journal table
    async fn initialize(&self) -> Result<(), JournalError> {
        // Create table (separate statement for Neon Pooler compatibility)
        let create_table_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                persistence_id VARCHAR(255) NOT NULL,
                sequence_nr BIGINT NOT NULL,
                manifest VARCHAR(255) NOT NULL,
                payload BYTEA NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                writer_uuid UUID NOT NULL,
                tags TEXT[] DEFAULT '{{}}',
                PRIMARY KEY (persistence_id, sequence_nr)
            )
            "#,
            self.config.table_name,
        );

        sqlx::query(&create_table_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| JournalError::Write(format!("Failed to create table: {}", e)))?;

        // Create persistence_id index
        let create_idx1_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_persistence_id ON {} (persistence_id)",
            self.config.table_name.replace(".", "_"),
            self.config.table_name,
        );

        sqlx::query(&create_idx1_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| JournalError::Write(format!("Failed to create index: {}", e)))?;

        // Create timestamp index
        let create_idx2_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_timestamp ON {} (timestamp)",
            self.config.table_name.replace(".", "_"),
            self.config.table_name,
        );

        sqlx::query(&create_idx2_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| JournalError::Write(format!("Failed to create index: {}", e)))?;

        Ok(())
    }

    /// Get the connection pool (for advanced usage)
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl Journal for PostgresJournal {
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> JournalResult<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await
            .map_err(|e| JournalError::Write(e.to_string()))?;

        for msg in messages {
            let sql = format!(
                r#"
                INSERT INTO {} (persistence_id, sequence_nr, manifest, payload, timestamp, writer_uuid, tags)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
                self.config.table_name
            );

            let tags: Vec<String> = msg.tags.clone();

            sqlx::query(&sql)
                .bind(&msg.persistence_id)
                .bind(msg.sequence_nr as i64)
                .bind(&msg.manifest)
                .bind(&msg.payload)
                .bind(msg.timestamp)
                .bind(msg.writer_uuid)
                .bind(&tags)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    if e.to_string().contains("duplicate key") || e.to_string().contains("unique constraint") {
                        JournalError::SequenceNumberOutOfRange(msg.sequence_nr)
                    } else {
                        JournalError::Write(e.to_string())
                    }
                })?;
        }

        tx.commit().await
            .map_err(|e| JournalError::Write(e.to_string()))?;

        Ok(())
    }

    async fn replay_messages(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
        max_messages: Option<u64>,
    ) -> JournalResult<Vec<PersistentRepr>> {
        let sql = format!(
            r#"
            SELECT persistence_id, sequence_nr, manifest, payload, timestamp, writer_uuid, tags
            FROM {}
            WHERE persistence_id = $1 
              AND sequence_nr >= $2 
              AND sequence_nr <= $3
            ORDER BY sequence_nr ASC
            LIMIT $4
            "#,
            self.config.table_name
        );

        let limit = max_messages.unwrap_or(i64::MAX as u64) as i64;
        // Cap to i64::MAX to avoid overflow (u64::MAX as i64 = -1)
        let to_seq = std::cmp::min(to_sequence_nr, i64::MAX as u64) as i64;

        let rows = sqlx::query(&sql)
            .bind(persistence_id)
            .bind(from_sequence_nr as i64)
            .bind(to_seq)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JournalError::Read(e.to_string()))?;

        let messages = rows
            .into_iter()
            .map(|row| {
                let seq_nr: i64 = row.get("sequence_nr");
                let tags: Vec<String> = row.get("tags");
                
                PersistentRepr {
                    persistence_id: row.get("persistence_id"),
                    sequence_nr: seq_nr as u64,
                    manifest: row.get("manifest"),
                    payload: row.get("payload"),
                    timestamp: row.get("timestamp"),
                    writer_uuid: row.get("writer_uuid"),
                    tags,
                }
            })
            .collect();

        Ok(messages)
    }

    async fn highest_sequence_nr(&self, persistence_id: &str) -> JournalResult<u64> {
        let sql = format!(
            r#"
            SELECT COALESCE(MAX(sequence_nr), 0) as max_seq
            FROM {}
            WHERE persistence_id = $1
            "#,
            self.config.table_name
        );

        let row = sqlx::query(&sql)
            .bind(persistence_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| JournalError::Read(e.to_string()))?;

        let max_seq: i64 = row.get("max_seq");
        Ok(max_seq as u64)
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> JournalResult<()> {
        let sql = format!(
            r#"
            DELETE FROM {}
            WHERE persistence_id = $1 AND sequence_nr <= $2
            "#,
            self.config.table_name
        );

        sqlx::query(&sql)
            .bind(persistence_id)
            .bind(to_sequence_nr as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| JournalError::Write(e.to_string()))?;

        Ok(())
    }

    async fn persistence_ids(&self) -> JournalResult<Vec<String>> {
        let sql = format!(
            r#"
            SELECT DISTINCT persistence_id FROM {}
            "#,
            self.config.table_name
        );

        let rows = sqlx::query(&sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JournalError::Read(e.to_string()))?;

        let ids = rows
            .into_iter()
            .map(|row| row.get("persistence_id"))
            .collect();

        Ok(ids)
    }
}

// ============================================================
// PostgreSQL Snapshot Store
// ============================================================

/// PostgreSQL Snapshot Store configuration
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct PostgresSnapshotConfig {
    /// Database connection URL
    pub database_url: String,
    /// Table name for storing snapshots
    pub table_name: String,
    /// Maximum number of connections in the pool
    pub max_connections: u32,
}

#[cfg(feature = "postgres")]
impl Default for PostgresSnapshotConfig {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            table_name: "pekko_snapshots".to_string(),
            max_connections: 5,
        }
    }
}

#[cfg(feature = "postgres")]
impl PostgresSnapshotConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            ..Default::default()
        }
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }
}

/// PostgreSQL-based Snapshot Store implementation
#[cfg(feature = "postgres")]
pub struct PostgresSnapshotStore {
    pool: PgPool,
    config: PostgresSnapshotConfig,
}

#[cfg(feature = "postgres")]
impl PostgresSnapshotStore {
    /// Create a new PostgreSQL snapshot store
    pub async fn new(config: PostgresSnapshotConfig) -> Result<Self, SnapshotError> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await
            .map_err(|e| SnapshotError::Connection(e.to_string()))?;

        let store = Self { pool, config };
        store.initialize().await?;
        Ok(store)
    }

    /// Create from an existing pool (share with journal)
    pub async fn from_pool(pool: PgPool, config: PostgresSnapshotConfig) -> Result<Self, SnapshotError> {
        let store = Self { pool, config };
        store.initialize().await?;
        Ok(store)
    }

    /// Initialize the snapshot table
    async fn initialize(&self) -> Result<(), SnapshotError> {
        // Create table (separate statement for Neon Pooler compatibility)
        let create_table_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                persistence_id VARCHAR(255) NOT NULL,
                sequence_nr BIGINT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                snapshot_data BYTEA NOT NULL,
                PRIMARY KEY (persistence_id, sequence_nr)
            )
            "#,
            self.config.table_name,
        );

        sqlx::query(&create_table_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| SnapshotError::Save(format!("Failed to create table: {}", e)))?;

        // Create index (separate statement)
        let create_idx_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_persistence_id ON {} (persistence_id)",
            self.config.table_name.replace(".", "_"),
            self.config.table_name,
        );

        sqlx::query(&create_idx_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| SnapshotError::Save(format!("Failed to create index: {}", e)))?;

        Ok(())
    }

    /// Get the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl SnapshotStore for PostgresSnapshotStore {
    async fn save_snapshot(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_data: Vec<u8>,
    ) -> SnapshotResult<()> {
        let sql = format!(
            r#"
            INSERT INTO {} (persistence_id, sequence_nr, timestamp, snapshot_data)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (persistence_id, sequence_nr) 
            DO UPDATE SET snapshot_data = $4, timestamp = $3
            "#,
            self.config.table_name
        );

        sqlx::query(&sql)
            .bind(&metadata.persistence_id)
            .bind(metadata.sequence_nr as i64)
            .bind(metadata.timestamp)
            .bind(&snapshot_data)
            .execute(&self.pool)
            .await
            .map_err(|e| SnapshotError::Save(e.to_string()))?;

        Ok(())
    }

    async fn load_snapshot(
        &self,
        persistence_id: &str,
        max_sequence_nr: Option<u64>,
        max_timestamp: Option<DateTime<Utc>>,
    ) -> SnapshotResult<Option<(SnapshotMetadata, Vec<u8>)>> {
        let mut sql = format!(
            r#"
            SELECT persistence_id, sequence_nr, timestamp, snapshot_data
            FROM {}
            WHERE persistence_id = $1
            "#,
            self.config.table_name
        );

        let mut conditions = Vec::new();
        if max_sequence_nr.is_some() {
            conditions.push("sequence_nr <= $2".to_string());
        }
        if max_timestamp.is_some() {
            let idx = if max_sequence_nr.is_some() { 3 } else { 2 };
            conditions.push(format!("timestamp <= ${}", idx));
        }

        if !conditions.is_empty() {
            sql.push_str(" AND ");
            sql.push_str(&conditions.join(" AND "));
        }

        sql.push_str(" ORDER BY sequence_nr DESC LIMIT 1");

        let mut query = sqlx::query(&sql).bind(persistence_id);

        if let Some(max_seq) = max_sequence_nr {
            query = query.bind(max_seq as i64);
        }
        if let Some(max_ts) = max_timestamp {
            query = query.bind(max_ts);
        }

        let row = query
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SnapshotError::Load(e.to_string()))?;

        match row {
            Some(row) => {
                let seq_nr: i64 = row.get("sequence_nr");
                let metadata = SnapshotMetadata {
                    persistence_id: row.get("persistence_id"),
                    sequence_nr: seq_nr as u64,
                    timestamp: row.get("timestamp"),
                };
                let data: Vec<u8> = row.get("snapshot_data");
                Ok(Some((metadata, data)))
            }
            None => Ok(None),
        }
    }

    async fn delete_snapshot(&self, metadata: &SnapshotMetadata) -> SnapshotResult<()> {
        let sql = format!(
            r#"
            DELETE FROM {}
            WHERE persistence_id = $1 AND sequence_nr = $2
            "#,
            self.config.table_name
        );

        sqlx::query(&sql)
            .bind(&metadata.persistence_id)
            .bind(metadata.sequence_nr as i64)
            .execute(&self.pool)
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
        let mut sql = format!(
            "DELETE FROM {} WHERE persistence_id = $1",
            self.config.table_name
        );

        if let Some(max_seq) = max_sequence_nr {
            sql.push_str(&format!(" AND sequence_nr <= {}", max_seq));
        }
        if let Some(max_ts) = max_timestamp {
            sql.push_str(&format!(" AND timestamp <= '{}'", max_ts.to_rfc3339()));
        }

        sqlx::query(&sql)
            .bind(persistence_id)
            .execute(&self.pool)
            .await
            .map_err(|e| SnapshotError::Delete(e.to_string()))?;

        Ok(())
    }

    async fn list_snapshots(
        &self,
        persistence_id: &str,
    ) -> SnapshotResult<Vec<SnapshotMetadata>> {
        let sql = format!(
            r#"
            SELECT persistence_id, sequence_nr, timestamp
            FROM {}
            WHERE persistence_id = $1
            ORDER BY sequence_nr DESC
            "#,
            self.config.table_name
        );

        let rows = sqlx::query(&sql)
            .bind(persistence_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SnapshotError::Load(e.to_string()))?;

        let snapshots = rows
            .into_iter()
            .map(|row| {
                let seq_nr: i64 = row.get("sequence_nr");
                SnapshotMetadata {
                    persistence_id: row.get("persistence_id"),
                    sequence_nr: seq_nr as u64,
                    timestamp: row.get("timestamp"),
                }
            })
            .collect();

        Ok(snapshots)
    }
}

// ============================================================
// Helper: Create both from single connection string
// ============================================================

/// Create Journal and SnapshotStore that share the same connection pool
#[cfg(feature = "postgres")]
pub async fn create_postgres_persistence(
    database_url: &str,
) -> Result<(Arc<PostgresJournal>, Arc<PostgresSnapshotStore>), String> {
    let journal_config = PostgresJournalConfig::new(database_url);
    let journal = PostgresJournal::new(journal_config)
        .await
        .map_err(|e| e.to_string())?;

    let snapshot_config = PostgresSnapshotConfig::new(database_url);
    let snapshot_store = PostgresSnapshotStore::from_pool(journal.pool().clone(), snapshot_config)
        .await
        .map_err(|e| e.to_string())?;

    Ok((Arc::new(journal), Arc::new(snapshot_store)))
}

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use super::*;

    // Note: These tests require a real PostgreSQL database
    // Set DATABASE_URL environment variable to run them
    
    #[tokio::test]
    #[ignore] // Run with: cargo test --features postgres -- --ignored
    async fn test_postgres_journal_basic() {
        let database_url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set for postgres tests");

        let config = PostgresJournalConfig::new(&database_url)
            .with_table_name("test_journal");

        let journal = PostgresJournal::new(config).await.unwrap();

        // Write a message
        let msg = PersistentRepr::new(
            "test-actor-pg".to_string(),
            1,
            "TestEvent".to_string(),
            b"test data".to_vec(),
        );

        journal.write_messages(vec![msg]).await.unwrap();

        // Read it back
        let messages = journal
            .replay_messages("test-actor-pg", 1, u64::MAX, None)
            .await
            .unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].persistence_id, "test-actor-pg");
        assert_eq!(messages[0].sequence_nr, 1);

        // Cleanup
        journal.delete_messages_to("test-actor-pg", 100).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_postgres_snapshot_basic() {
        let database_url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set for postgres tests");

        let config = PostgresSnapshotConfig::new(&database_url)
            .with_table_name("test_snapshots");

        let store = PostgresSnapshotStore::new(config).await.unwrap();

        let metadata = SnapshotMetadata {
            persistence_id: "test-actor-snap".to_string(),
            sequence_nr: 100,
            timestamp: Utc::now(),
        };

        // Save snapshot
        store.save_snapshot(&metadata, b"snapshot data".to_vec()).await.unwrap();

        // Load it back
        let loaded = store.load_snapshot("test-actor-snap", None, None).await.unwrap();
        assert!(loaded.is_some());

        let (meta, data) = loaded.unwrap();
        assert_eq!(meta.sequence_nr, 100);
        assert_eq!(data, b"snapshot data");

        // Cleanup
        store.delete_snapshot(&metadata).await.unwrap();
    }
}
