//! Journal trait and implementations for event storage

use crate::{JournalError, JournalResult, PersistentRepr};
use async_trait::async_trait;
use std::sync::Arc;

/// Trait for journal backends that store events
#[async_trait]
pub trait Journal: Send + Sync {
    /// Write a batch of messages to the journal
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> JournalResult<()>;

    /// Replay messages for a given persistence ID within a sequence number range
    async fn replay_messages(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
        max_messages: Option<u64>,
    ) -> JournalResult<Vec<PersistentRepr>>;

    /// Get the highest sequence number for a given persistence ID
    async fn highest_sequence_nr(&self, persistence_id: &str) -> JournalResult<u64>;

    /// Delete messages up to a given sequence number
    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> JournalResult<()>;

    /// Get all persistence IDs in the journal
    async fn persistence_ids(&self) -> JournalResult<Vec<String>>;
}

/// Write result for individual messages
#[derive(Debug, Clone)]
pub struct WriteResult {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub success: bool,
    pub error: Option<String>,
}

/// Journal query for reading messages
#[derive(Debug, Clone)]
pub struct JournalQuery {
    pub persistence_id: String,
    pub from_sequence_nr: u64,
    pub to_sequence_nr: u64,
    pub max_messages: Option<u64>,
}

impl JournalQuery {
    pub fn new(persistence_id: String) -> Self {
        Self {
            persistence_id,
            from_sequence_nr: 1,
            to_sequence_nr: u64::MAX,
            max_messages: None,
        }
    }

    pub fn from_sequence_nr(mut self, from: u64) -> Self {
        self.from_sequence_nr = from;
        self
    }

    pub fn to_sequence_nr(mut self, to: u64) -> Self {
        self.to_sequence_nr = to;
        self
    }

    pub fn max_messages(mut self, max: u64) -> Self {
        self.max_messages = Some(max);
        self
    }
}

/// Configuration for journal backends
#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub keyspace: String,
    pub table_name: String,
    pub replication_factor: u32,
    pub consistency_level: ConsistencyLevel,
    pub max_message_batch_size: usize,
    pub request_timeout_ms: u64,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            keyspace: "pekko".to_string(),
            table_name: "messages".to_string(),
            replication_factor: 3,
            consistency_level: ConsistencyLevel::Quorum,
            max_message_batch_size: 100,
            request_timeout_ms: 5000,
        }
    }
}

/// Consistency levels for Cassandra operations
#[derive(Debug, Clone, Copy)]
pub enum ConsistencyLevel {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    Serial,
    LocalSerial,
    LocalOne,
}

/// In-memory journal implementation for testing
pub struct InMemoryJournal {
    messages: Arc<parking_lot::RwLock<std::collections::HashMap<String, Vec<PersistentRepr>>>>,
}

impl InMemoryJournal {
    pub fn new() -> Self {
        Self {
            messages: Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new())),
        }
    }
}

impl Default for InMemoryJournal {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Journal for InMemoryJournal {
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> JournalResult<()> {
        let mut storage = self.messages.write();
        
        for msg in messages {
            let entry = storage.entry(msg.persistence_id.clone()).or_insert_with(Vec::new);
            
            // Ensure sequence numbers are consecutive
            if let Some(last_msg) = entry.last() {
                if msg.sequence_nr != last_msg.sequence_nr + 1 {
                    return Err(JournalError::SequenceNumberOutOfRange(msg.sequence_nr));
                }
            } else if msg.sequence_nr != 1 {
                return Err(JournalError::SequenceNumberOutOfRange(msg.sequence_nr));
            }
            
            entry.push(msg);
        }
        
        Ok(())
    }

    async fn replay_messages(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
        max_messages: Option<u64>,
    ) -> JournalResult<Vec<PersistentRepr>> {
        let storage = self.messages.read();
        
        if let Some(messages) = storage.get(persistence_id) {
            let mut result: Vec<PersistentRepr> = messages
                .iter()
                .filter(|msg| {
                    msg.sequence_nr >= from_sequence_nr && msg.sequence_nr <= to_sequence_nr
                })
                .cloned()
                .collect();
            
            if let Some(max) = max_messages {
                result.truncate(max as usize);
            }
            
            Ok(result)
        } else {
            Ok(Vec::new())
        }
    }

    async fn highest_sequence_nr(&self, persistence_id: &str) -> JournalResult<u64> {
        let storage = self.messages.read();
        
        if let Some(messages) = storage.get(persistence_id) {
            Ok(messages.last().map(|msg| msg.sequence_nr).unwrap_or(0))
        } else {
            Ok(0)
        }
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> JournalResult<()> {
        let mut storage = self.messages.write();
        
        if let Some(messages) = storage.get_mut(persistence_id) {
            messages.retain(|msg| msg.sequence_nr > to_sequence_nr);
        }
        
        Ok(())
    }

    async fn persistence_ids(&self) -> JournalResult<Vec<String>> {
        let storage = self.messages.read();
        Ok(storage.keys().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_in_memory_journal_write_and_read() {
        let journal = InMemoryJournal::new();
        let persistence_id = "test-actor";

        // Write some messages
        let messages = vec![
            PersistentRepr {
                persistence_id: persistence_id.to_string(),
                sequence_nr: 1,
                manifest: "TestEvent".to_string(),
                payload: b"event1".to_vec(),
                timestamp: Utc::now(),
                writer_uuid: Uuid::new_v4(),
                tags: vec![],
            },
            PersistentRepr {
                persistence_id: persistence_id.to_string(),
                sequence_nr: 2,
                manifest: "TestEvent".to_string(),
                payload: b"event2".to_vec(),
                timestamp: Utc::now(),
                writer_uuid: Uuid::new_v4(),
                tags: vec![],
            },
        ];

        journal.write_messages(messages.clone()).await.unwrap();

        // Read messages back
        let read_messages = journal
            .replay_messages(persistence_id, 1, u64::MAX, None)
            .await
            .unwrap();

        assert_eq!(read_messages.len(), 2);
        assert_eq!(read_messages[0].sequence_nr, 1);
        assert_eq!(read_messages[1].sequence_nr, 2);

        // Check highest sequence number
        let highest = journal.highest_sequence_nr(persistence_id).await.unwrap();
        assert_eq!(highest, 2);
    }

    #[tokio::test]
    async fn test_journal_sequence_number_validation() {
        let journal = InMemoryJournal::new();
        let persistence_id = "test-actor";

        // Try to write message with sequence number 2 first (should fail)
        let message = PersistentRepr {
            persistence_id: persistence_id.to_string(),
            sequence_nr: 2,
            manifest: "TestEvent".to_string(),
            payload: b"event".to_vec(),
            timestamp: Utc::now(),
            writer_uuid: Uuid::new_v4(),
            tags: vec![],
        };

        let result = journal.write_messages(vec![message]).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), JournalError::SequenceNumberOutOfRange(2)));
    }

    #[tokio::test]
    async fn test_journal_query_builder() {
        let query = JournalQuery::new("test-actor".to_string())
            .from_sequence_nr(5)
            .to_sequence_nr(10)
            .max_messages(3);

        assert_eq!(query.persistence_id, "test-actor");
        assert_eq!(query.from_sequence_nr, 5);
        assert_eq!(query.to_sequence_nr, 10);
        assert_eq!(query.max_messages, Some(3));
    }
}