//! Durable State — Pure state persistence without events
//!
//! Implements Scala Pekko's DurableStateBehavior pattern:
//! Instead of event sourcing (storing events and replaying),
//! this persists the full state directly — simpler for CRUD-like use cases.
//!
//! - **DurableStateStore** — Trait for state storage backends
//! - **InMemoryDurableStateStore** — In-memory implementation
//! - **DurableStateHandler** — Command handler returning state changes
//! - **DurableStateManager** — Orchestrates state persistence and recovery

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

// ============= DurableStateStore =============

/// Stored state entry with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableStateEntry<S: Clone> {
    /// The persistence ID.
    pub persistence_id: String,
    /// The revision (version number, monotonically increasing).
    pub revision: u64,
    /// The persisted state.
    pub state: S,
    /// Optional tag for querying.
    pub tag: Option<String>,
    /// Timestamp when saved.
    pub timestamp: DateTime<Utc>,
}

/// Trait for durable state storage backends.
#[async_trait]
pub trait DurableStateStore<S: Clone + Send + Sync>: Send + Sync {
    /// Get the current state for a persistence ID.
    async fn get(&self, persistence_id: &str) -> Result<Option<DurableStateEntry<S>>, DurableStateError>;

    /// Upsert state. Revision must be current_revision + 1 (optimistic locking).
    async fn upsert(
        &self,
        persistence_id: &str,
        revision: u64,
        state: S,
        tag: Option<String>,
    ) -> Result<(), DurableStateError>;

    /// Delete state for a persistence ID.
    async fn delete(&self, persistence_id: &str) -> Result<(), DurableStateError>;

    /// Get the current revision for a persistence ID.
    async fn current_revision(&self, persistence_id: &str) -> Result<u64, DurableStateError>;

    /// List all persistence IDs in the store.
    async fn persistence_ids(&self) -> Result<Vec<String>, DurableStateError>;

    /// Get states by tag.
    async fn get_by_tag(&self, tag: &str) -> Result<Vec<DurableStateEntry<S>>, DurableStateError>;
}

// ============= Error =============

/// Errors for durable state operations.
#[derive(Debug, Clone)]
pub enum DurableStateError {
    /// Revision conflict — another writer updated first.
    RevisionConflict { expected: u64, actual: u64 },
    /// State not found.
    NotFound(String),
    /// Storage backend error.
    StorageError(String),
    /// Serialization error.
    SerializationError(String),
}

impl std::fmt::Display for DurableStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RevisionConflict { expected, actual } => {
                write!(f, "Revision conflict: expected {}, actual {}", expected, actual)
            }
            Self::NotFound(id) => write!(f, "State not found: {}", id),
            Self::StorageError(msg) => write!(f, "Storage error: {}", msg),
            Self::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
        }
    }
}

// ============= InMemoryDurableStateStore =============

/// In-memory implementation of DurableStateStore for testing.
pub struct InMemoryDurableStateStore<S: Clone + Send + Sync> {
    states: RwLock<HashMap<String, DurableStateEntry<S>>>,
    operation_count: AtomicU64,
}

impl<S: Clone + Send + Sync + Debug> InMemoryDurableStateStore<S> {
    pub fn new() -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
            operation_count: AtomicU64::new(0),
        }
    }

    /// Get total number of operations performed.
    pub fn operation_count(&self) -> u64 {
        self.operation_count.load(Ordering::Relaxed)
    }

    /// Get number of stored states.
    pub async fn state_count(&self) -> usize {
        self.states.read().await.len()
    }
}

#[async_trait]
impl<S: Clone + Send + Sync + Debug + 'static> DurableStateStore<S> for InMemoryDurableStateStore<S> {
    async fn get(&self, persistence_id: &str) -> Result<Option<DurableStateEntry<S>>, DurableStateError> {
        self.operation_count.fetch_add(1, Ordering::Relaxed);
        let states = self.states.read().await;
        Ok(states.get(persistence_id).cloned())
    }

    async fn upsert(
        &self,
        persistence_id: &str,
        revision: u64,
        state: S,
        tag: Option<String>,
    ) -> Result<(), DurableStateError> {
        self.operation_count.fetch_add(1, Ordering::Relaxed);
        let mut states = self.states.write().await;

        // Optimistic locking: check revision
        let current_rev = states
            .get(persistence_id)
            .map(|e| e.revision)
            .unwrap_or(0);

        if revision != current_rev + 1 {
            return Err(DurableStateError::RevisionConflict {
                expected: current_rev + 1,
                actual: revision,
            });
        }

        let entry = DurableStateEntry {
            persistence_id: persistence_id.to_string(),
            revision,
            state,
            tag,
            timestamp: Utc::now(),
        };

        states.insert(persistence_id.to_string(), entry);
        Ok(())
    }

    async fn delete(&self, persistence_id: &str) -> Result<(), DurableStateError> {
        self.operation_count.fetch_add(1, Ordering::Relaxed);
        let mut states = self.states.write().await;
        states.remove(persistence_id);
        Ok(())
    }

    async fn current_revision(&self, persistence_id: &str) -> Result<u64, DurableStateError> {
        let states = self.states.read().await;
        Ok(states.get(persistence_id).map(|e| e.revision).unwrap_or(0))
    }

    async fn persistence_ids(&self) -> Result<Vec<String>, DurableStateError> {
        let states = self.states.read().await;
        Ok(states.keys().cloned().collect())
    }

    async fn get_by_tag(&self, tag: &str) -> Result<Vec<DurableStateEntry<S>>, DurableStateError> {
        let states = self.states.read().await;
        Ok(states
            .values()
            .filter(|e| e.tag.as_deref() == Some(tag))
            .cloned()
            .collect())
    }
}

// ============= DurableStateChange =============

/// Result of handling a command — what state change to persist.
#[derive(Debug, Clone)]
pub enum DurableStateChange<S> {
    /// Persist new state.
    Persist(S),
    /// Persist new state with a tag.
    PersistWithTag(S, String),
    /// Delete the state.
    Delete,
    /// No change (command rejected or read-only).
    None,
}

// ============= DurableStateManager =============

/// Manages durable state lifecycle: load, handle commands, persist.
pub struct DurableStateManager<S: Clone + Send + Sync + 'static> {
    persistence_id: String,
    store: Arc<dyn DurableStateStore<S>>,
    current_state: RwLock<Option<S>>,
    current_revision: AtomicU64,
    commands_processed: AtomicU64,
    states_persisted: AtomicU64,
}

impl<S: Clone + Send + Sync + Debug + 'static> DurableStateManager<S> {
    /// Create a new manager.
    pub fn new(persistence_id: impl Into<String>, store: Arc<dyn DurableStateStore<S>>) -> Self {
        Self {
            persistence_id: persistence_id.into(),
            store,
            current_state: RwLock::new(None),
            current_revision: AtomicU64::new(0),
            commands_processed: AtomicU64::new(0),
            states_persisted: AtomicU64::new(0),
        }
    }

    /// Load state from the store (recovery).
    pub async fn load(&self) -> Result<Option<S>, DurableStateError> {
        match self.store.get(&self.persistence_id).await? {
            Some(entry) => {
                let state = entry.state.clone();
                *self.current_state.write().await = Some(entry.state);
                self.current_revision.store(entry.revision, Ordering::SeqCst);
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// Persist a new state.
    pub async fn persist(&self, state: S, tag: Option<String>) -> Result<u64, DurableStateError> {
        let new_revision = self.current_revision.load(Ordering::SeqCst) + 1;
        self.store
            .upsert(&self.persistence_id, new_revision, state.clone(), tag)
            .await?;

        *self.current_state.write().await = Some(state);
        self.current_revision.store(new_revision, Ordering::SeqCst);
        self.states_persisted.fetch_add(1, Ordering::Relaxed);
        Ok(new_revision)
    }

    /// Delete the persisted state.
    pub async fn delete_state(&self) -> Result<(), DurableStateError> {
        self.store.delete(&self.persistence_id).await?;
        *self.current_state.write().await = None;
        self.current_revision.store(0, Ordering::SeqCst);
        Ok(())
    }

    /// Get current state.
    pub async fn state(&self) -> Option<S> {
        self.current_state.read().await.clone()
    }

    /// Get current revision.
    pub fn revision(&self) -> u64 {
        self.current_revision.load(Ordering::SeqCst)
    }

    /// Get persistence ID.
    pub fn persistence_id(&self) -> &str {
        &self.persistence_id
    }

    /// Get stats.
    pub fn stats(&self) -> DurableStateManagerStats {
        DurableStateManagerStats {
            persistence_id: self.persistence_id.clone(),
            current_revision: self.revision(),
            commands_processed: self.commands_processed.load(Ordering::Relaxed),
            states_persisted: self.states_persisted.load(Ordering::Relaxed),
        }
    }
}

/// Statistics for a DurableStateManager.
#[derive(Debug, Clone)]
pub struct DurableStateManagerStats {
    pub persistence_id: String,
    pub current_revision: u64,
    pub commands_processed: u64,
    pub states_persisted: u64,
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    // ---- InMemoryDurableStateStore ----

    #[tokio::test]
    async fn test_store_get_empty() {
        let store = InMemoryDurableStateStore::<String>::new();
        let result = store.get("actor-1").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_store_upsert_and_get() {
        let store = InMemoryDurableStateStore::<String>::new();
        store
            .upsert("actor-1", 1, "state-v1".to_string(), None)
            .await
            .unwrap();

        let entry = store.get("actor-1").await.unwrap().unwrap();
        assert_eq!(entry.state, "state-v1");
        assert_eq!(entry.revision, 1);
    }

    #[tokio::test]
    async fn test_store_upsert_incremental() {
        let store = InMemoryDurableStateStore::<i32>::new();
        store.upsert("a", 1, 100, None).await.unwrap();
        store.upsert("a", 2, 200, None).await.unwrap();
        store.upsert("a", 3, 300, None).await.unwrap();

        let entry = store.get("a").await.unwrap().unwrap();
        assert_eq!(entry.state, 300);
        assert_eq!(entry.revision, 3);
    }

    #[tokio::test]
    async fn test_store_revision_conflict() {
        let store = InMemoryDurableStateStore::<i32>::new();
        store.upsert("a", 1, 100, None).await.unwrap();

        // Try to upsert with wrong revision
        let result = store.upsert("a", 1, 200, None).await;
        assert!(matches!(
            result,
            Err(DurableStateError::RevisionConflict { expected: 2, actual: 1 })
        ));
    }

    #[tokio::test]
    async fn test_store_delete() {
        let store = InMemoryDurableStateStore::<String>::new();
        store.upsert("a", 1, "data".to_string(), None).await.unwrap();
        store.delete("a").await.unwrap();

        assert!(store.get("a").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_store_current_revision() {
        let store = InMemoryDurableStateStore::<i32>::new();
        assert_eq!(store.current_revision("a").await.unwrap(), 0);

        store.upsert("a", 1, 10, None).await.unwrap();
        assert_eq!(store.current_revision("a").await.unwrap(), 1);

        store.upsert("a", 2, 20, None).await.unwrap();
        assert_eq!(store.current_revision("a").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_store_persistence_ids() {
        let store = InMemoryDurableStateStore::<i32>::new();
        store.upsert("a", 1, 1, None).await.unwrap();
        store.upsert("b", 1, 2, None).await.unwrap();
        store.upsert("c", 1, 3, None).await.unwrap();

        let mut ids = store.persistence_ids().await.unwrap();
        ids.sort();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn test_store_get_by_tag() {
        let store = InMemoryDurableStateStore::<String>::new();
        store
            .upsert("a", 1, "data-a".to_string(), Some("important".to_string()))
            .await
            .unwrap();
        store
            .upsert("b", 1, "data-b".to_string(), Some("normal".to_string()))
            .await
            .unwrap();
        store
            .upsert("c", 1, "data-c".to_string(), Some("important".to_string()))
            .await
            .unwrap();

        let important = store.get_by_tag("important").await.unwrap();
        assert_eq!(important.len(), 2);
    }

    #[tokio::test]
    async fn test_store_operation_count() {
        let store = InMemoryDurableStateStore::<i32>::new();
        store.get("a").await.unwrap();
        store.upsert("a", 1, 10, None).await.unwrap();
        store.get("a").await.unwrap();
        assert_eq!(store.operation_count(), 3);
    }

    #[tokio::test]
    async fn test_store_state_count() {
        let store = InMemoryDurableStateStore::<i32>::new();
        store.upsert("a", 1, 1, None).await.unwrap();
        store.upsert("b", 1, 2, None).await.unwrap();
        assert_eq!(store.state_count().await, 2);
    }

    // ---- DurableStateManager ----

    #[tokio::test]
    async fn test_manager_load_empty() {
        let store = Arc::new(InMemoryDurableStateStore::<i32>::new());
        let mgr = DurableStateManager::new("test", store);
        let state = mgr.load().await.unwrap();
        assert!(state.is_none());
        assert_eq!(mgr.revision(), 0);
    }

    #[tokio::test]
    async fn test_manager_persist_and_load() {
        let store = Arc::new(InMemoryDurableStateStore::<String>::new());
        let mgr = DurableStateManager::new("test", store.clone());

        let rev = mgr.persist("v1".to_string(), None).await.unwrap();
        assert_eq!(rev, 1);
        assert_eq!(mgr.state().await, Some("v1".to_string()));

        // Load in a new manager (simulating restart)
        let mgr2 = DurableStateManager::new("test", store);
        let loaded = mgr2.load().await.unwrap();
        assert_eq!(loaded, Some("v1".to_string()));
        assert_eq!(mgr2.revision(), 1);
    }

    #[tokio::test]
    async fn test_manager_multiple_persists() {
        let store = Arc::new(InMemoryDurableStateStore::<i32>::new());
        let mgr = DurableStateManager::new("counter", store);

        mgr.persist(1, None).await.unwrap();
        mgr.persist(2, None).await.unwrap();
        mgr.persist(3, None).await.unwrap();

        assert_eq!(mgr.state().await, Some(3));
        assert_eq!(mgr.revision(), 3);
    }

    #[tokio::test]
    async fn test_manager_delete_state() {
        let store = Arc::new(InMemoryDurableStateStore::<String>::new());
        let mgr = DurableStateManager::new("test", store);

        mgr.persist("data".to_string(), None).await.unwrap();
        mgr.delete_state().await.unwrap();

        assert!(mgr.state().await.is_none());
        assert_eq!(mgr.revision(), 0);
    }

    #[tokio::test]
    async fn test_manager_persist_with_tag() {
        let store = Arc::new(InMemoryDurableStateStore::<String>::new());
        let mgr = DurableStateManager::new("tagged", store.clone());

        mgr.persist("data".to_string(), Some("important".to_string()))
            .await
            .unwrap();

        let tagged = store.get_by_tag("important").await.unwrap();
        assert_eq!(tagged.len(), 1);
        assert_eq!(tagged[0].state, "data");
    }

    #[tokio::test]
    async fn test_manager_stats() {
        let store = Arc::new(InMemoryDurableStateStore::<i32>::new());
        let mgr = DurableStateManager::new("stats-test", store);

        mgr.persist(1, None).await.unwrap();
        mgr.persist(2, None).await.unwrap();

        let stats = mgr.stats();
        assert_eq!(stats.persistence_id, "stats-test");
        assert_eq!(stats.current_revision, 2);
        assert_eq!(stats.states_persisted, 2);
    }

    // ---- DurableStateChange ----

    #[test]
    fn test_state_change_variants() {
        let persist = DurableStateChange::Persist(42);
        assert!(matches!(persist, DurableStateChange::Persist(42)));

        let tagged = DurableStateChange::PersistWithTag("data", "tag".to_string());
        assert!(matches!(tagged, DurableStateChange::PersistWithTag(_, _)));

        let delete: DurableStateChange<i32> = DurableStateChange::Delete;
        assert!(matches!(delete, DurableStateChange::Delete));

        let none: DurableStateChange<i32> = DurableStateChange::None;
        assert!(matches!(none, DurableStateChange::None));
    }

    // ---- Error Display ----

    #[test]
    fn test_error_display() {
        let e = DurableStateError::RevisionConflict { expected: 5, actual: 3 };
        assert!(e.to_string().contains("expected 5"));

        let e = DurableStateError::NotFound("actor-1".into());
        assert!(e.to_string().contains("actor-1"));

        let e = DurableStateError::StorageError("connection lost".into());
        assert!(e.to_string().contains("connection lost"));
    }
}
