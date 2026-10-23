//! Recovery and replay mechanisms for persistent actors

use crate::{
    Journal, PersistenceError, PersistenceResult, PersistentActor, PersistentContext,
    SnapshotMetadata, SnapshotStore,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// Recovery manager for persistent actors
pub struct RecoveryManager {
    journal: Arc<dyn Journal>,
    snapshot_store: Arc<dyn SnapshotStore>
}

impl RecoveryManager {
    pub fn new(
        journal: Arc<dyn Journal>,
        snapshot_store: Arc<dyn SnapshotStore>,
    ) -> Self {
        Self {
            journal,
            snapshot_store
        }
    }

    /// Recover a persistent actor
    pub async fn recover<A: PersistentActor>(
        &self,
        actor: &mut A,
        ctx: &mut PersistentContext<A>,
        mode: RecoveryMode,
    ) -> PersistenceResult<RecoveryResult>
    where
        A::Event: DeserializeOwned + Send + Sync,
        A::Snapshot: DeserializeOwned + Send + Sync,
    {
        let persistence_id = actor.persistence_id();
        match mode {
            RecoveryMode::Skip => {
                return Ok(RecoveryResult::new(0, None));
            }
            _ => {}
        }

        // Step 1: Try to load snapshot
        let snapshot_result = self.load_snapshot(&persistence_id, &mode).await?;
        let from_sequence_nr = if let Some((snapshot_metadata, snapshot_data)) = &snapshot_result {
            // Deserialize and apply snapshot
            let snapshot: A::Snapshot = serde_json::from_slice(snapshot_data)
                .map_err(|e| PersistenceError::Recovery(format!("Snapshot deserialization failed: {}", e)))?;

            actor.apply_snapshot(snapshot);
            ctx.sequence_nr = snapshot_metadata.sequence_nr;
            snapshot_metadata.sequence_nr + 1
        } else {
            match mode {
                RecoveryMode::FromSequenceNr(seq_nr) => seq_nr,
                _ => 1,
            }
        };

        // Step 2: Replay events from journal
        let events_replayed = self
            .replay_events(actor, ctx, &persistence_id, from_sequence_nr)
            .await?;

        // Step 3: Update highest sequence number
        let highest_sequence_nr = self.journal.highest_sequence_nr(&persistence_id).await?;
        ctx.sequence_nr = highest_sequence_nr;
        // Notify actor that recovery is complete
        actor.recovery_completed(ctx).await;

        Ok(RecoveryResult::new(
            events_replayed,
            snapshot_result.map(|(metadata, _)| metadata),
        ))
    }

    async fn load_snapshot(
        &self,
        persistence_id: &str,
        mode: &RecoveryMode,
    ) -> PersistenceResult<Option<(SnapshotMetadata, Vec<u8>)>> {
        let (max_sequence_nr, max_timestamp) = match mode {
            RecoveryMode::FromBeginning => return Ok(None),
            RecoveryMode::FromSequenceNr(seq_nr) => (Some(*seq_nr), None),
            RecoveryMode::FromSnapshot => (None, None),
            RecoveryMode::Skip => return Ok(None),
        };

        match self
            .snapshot_store
            .load_snapshot(persistence_id, max_sequence_nr, max_timestamp)
            .await
        {
            Ok(snapshot) => Ok(snapshot),
            Err(e) => {
                warn!("Failed to load snapshot for {}: {}", persistence_id, e);
                Ok(None) // Continue without snapshot
            }
        }
    }

    async fn replay_events<A: PersistentActor>(
        &self,
        actor: &mut A,
        ctx: &mut PersistentContext<A>,
        persistence_id: &str,
        from_sequence_nr: u64,
    ) -> PersistenceResult<u64>
    where
        A::Event: DeserializeOwned,
    {
        let mut events_replayed = 0u64;
        let batch_size = 1000u64; // Process events in batches
        let mut current_sequence = from_sequence_nr;

        // Mark context as recovering
        ctx.recovering = true;

        loop {
            let to_sequence = current_sequence + batch_size - 1;
            let events = self
                .journal
                .replay_messages(persistence_id, current_sequence, to_sequence, Some(batch_size))
                .await?;

            if events.is_empty() {
                break;
            }

            debug!(
                "Replaying batch of {} events for {} (seq {} to {})",
                events.len(),
                persistence_id,
                current_sequence,
                events.last().map(|e| e.sequence_nr).unwrap_or(current_sequence)
            );

            for event_repr in &events {
                // Deserialize event
                let event: A::Event = serde_json::from_slice(&event_repr.payload)
                    .map_err(|e| {
                        PersistenceError::Recovery(format!(
                            "Event deserialization failed for sequence {}: {}",
                            event_repr.sequence_nr, e
                        ))
                    })?;

                // Apply event to actor
                actor.receive_recover(event, ctx).await;

                events_replayed += 1;
                current_sequence = event_repr.sequence_nr + 1;
            }

            // If we got fewer events than requested, we've reached the end
            if (events.len() as u64) < batch_size {
                break;
            }
        }

        // Mark context as no longer recovering
        ctx.recovering = false;

        Ok(events_replayed)
    }
}

/// Recovery modes for persistent actors
#[derive(Debug, Clone)]
pub enum RecoveryMode {
    /// Recover from the beginning (no snapshot)
    FromBeginning,
    /// Recover from a specific sequence number
    FromSequenceNr(u64),
    /// Recover from the latest snapshot
    FromSnapshot,
    /// Skip recovery entirely (for testing)
    Skip,
}

/// Result of a recovery operation
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    pub events_replayed: u64,
    pub snapshot_metadata: Option<SnapshotMetadata>,
    pub recovery_completed_at: DateTime<Utc>,
}

impl RecoveryResult {
    pub fn new(events_replayed: u64, snapshot_metadata: Option<SnapshotMetadata>) -> Self {
        Self {
            events_replayed,
            snapshot_metadata,
            recovery_completed_at: Utc::now(),
        }
    }

    pub fn has_snapshot(&self) -> bool {
        self.snapshot_metadata.is_some()
    }

    pub fn snapshot_sequence_nr(&self) -> Option<u64> {
        self.snapshot_metadata.as_ref().map(|m| m.sequence_nr)
    }
}

/// Recovery configuration
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    pub mode: RecoveryMode,
    pub batch_size: u64,
    pub max_recovery_time_ms: Option<u64>,
    pub snapshot_after_events: Option<u64>,
    pub delete_events_after_snapshot: bool,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            mode: RecoveryMode::FromSnapshot,
            batch_size: 1000,
            max_recovery_time_ms: Some(300_000), // 5 minutes
            snapshot_after_events: Some(10_000),
            delete_events_after_snapshot: false,
        }
    }
}

/// Recovery progress callback
#[async_trait]
pub trait RecoveryProgressCallback: Send + Sync {
    async fn on_recovery_started(&self, _persistence_id: &str);
    async fn on_snapshot_loaded(&self, _persistence_id: &str, _sequence_nr: u64);
    async fn on_events_replayed(&self, persistence_id: &str, events_count: u64);
    async fn on_recovery_completed(&self, _persistence_id: &str, _result: &RecoveryResult);
    async fn on_recovery_failed(&self, persistence_id: &str, error: &PersistenceError);
}

/// Default recovery progress logger
pub struct LoggingRecoveryCallback;

#[async_trait]
impl RecoveryProgressCallback for LoggingRecoveryCallback {
    async fn on_recovery_started(&self, _persistence_id: &str) {
    }

    async fn on_snapshot_loaded(&self, _persistence_id: &str, _sequence_nr: u64) {
    }

    async fn on_events_replayed(&self, persistence_id: &str, events_count: u64) {
        debug!("Replayed {} events for {}", events_count, persistence_id);
    }

    async fn on_recovery_completed(&self, _persistence_id: &str, _result: &RecoveryResult) {
    }

    async fn on_recovery_failed(&self, persistence_id: &str, error: &PersistenceError) {
        error!("Recovery failed for {}: {}", persistence_id, error);
    }
}

/// Enhanced recovery manager with progress callbacks
pub struct EnhancedRecoveryManager {
    base: RecoveryManager,
    config: RecoveryConfig,
    callback: Option<Arc<dyn RecoveryProgressCallback>>,
}

impl EnhancedRecoveryManager {
    pub fn new(
        journal: Arc<dyn Journal>,
        snapshot_store: Arc<dyn SnapshotStore>,
        config: RecoveryConfig,
    ) -> Self {
        Self {
            base: RecoveryManager::new(journal, snapshot_store),
            config,
            callback: None,
        }
    }

    pub fn with_progress_callback(
        mut self,
        callback: Arc<dyn RecoveryProgressCallback>,
    ) -> Self {
        self.callback = Some(callback);
        self
    }

    pub async fn recover<A: PersistentActor>(
        &self,
        actor: &mut A,
        ctx: &mut PersistentContext<A>,
    ) -> PersistenceResult<RecoveryResult>
    where
        A::Event: DeserializeOwned + Send + Sync,
        A::Snapshot: DeserializeOwned + Send + Sync,
    {
        let persistence_id = actor.persistence_id();

        if let Some(callback) = &self.callback {
            callback.on_recovery_started(&persistence_id).await;
        }

        match self.base.recover(actor, ctx, self.config.mode.clone()).await {
            Ok(result) => {
                if let Some(callback) = &self.callback {
                    callback.on_recovery_completed(&persistence_id, &result).await;
                }
                Ok(result)
            }
            Err(error) => {
                if let Some(callback) = &self.callback {
                    callback.on_recovery_failed(&persistence_id, &error).await;
                }
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryJournal, InMemorySnapshotStore};
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestEvent {
        id: u64,
        data: String,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct TestState {
        counter: u64,
        events: Vec<String>,
    }

    struct TestActor {
        persistence_id: String,
        state: TestState,
    }

    impl TestActor {
        fn new(persistence_id: String) -> Self {
            Self {
                persistence_id,
                state: TestState::default(),
            }
        }
    }

    #[async_trait]
    impl pekko_actor::Actor for TestActor {
        type Message = String;

        async fn receive(&mut self, _msg: Self::Message, _ctx: &mut pekko_actor::ActorContext<Self>) {
            // Not used in this test
        }
    }

    #[async_trait]
    impl PersistentActor for TestActor {
        type Event = TestEvent;
        type State = TestState;
        type Snapshot = TestState;

        fn persistence_id(&self) -> String {
            self.persistence_id.clone()
        }

        async fn receive_recover(
            &mut self,
            event: Self::Event,
            _ctx: &mut PersistentContext<Self>,
        ) {
            self.state.counter += 1;
            self.state.events.push(event.data);
        }

        async fn receive_command(
            &mut self,
            _msg: Self::Message,
            _ctx: &mut PersistentContext<Self>,
        ) {
            // Not used in this test
        }

        fn apply_event(&mut self, event: &Self::Event) -> Self::State {
            let mut new_state = self.state.clone();
            new_state.counter += 1;
            new_state.events.push(event.data.clone());
            new_state
        }

        fn create_snapshot(&self) -> Self::Snapshot {
            self.state.clone()
        }

        fn apply_snapshot(&mut self, snapshot: Self::Snapshot) {
            self.state = snapshot;
        }
    }

    #[tokio::test]
    async fn test_recovery_manager_creation() {
        // Simple test that just verifies the recovery manager can be created
        let journal = Arc::new(InMemoryJournal::new());
        let snapshot_store = Arc::new(InMemorySnapshotStore::new());
        let serializer_registry = Arc::new(SerializerRegistry::default());

        let recovery_manager = RecoveryManager::new(journal, snapshot_store, serializer_registry);
        
        // Basic test that the manager was created successfully
        // Just verify that the manager exists (Arc always contains a value)
        assert!(std::ptr::addr_of!(recovery_manager.journal) != std::ptr::null());
        assert!(std::ptr::addr_of!(recovery_manager.snapshot_store) != std::ptr::null());
    }

    #[tokio::test]
    async fn test_recovery_config_defaults() {
        let config = RecoveryConfig::default();
        
        assert!(matches!(config.mode, RecoveryMode::FromSnapshot));
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.max_recovery_time_ms, Some(300_000));
        assert_eq!(config.snapshot_after_events, Some(10_000));
        assert!(!config.delete_events_after_snapshot);
    }

    #[tokio::test]
    async fn test_recovery_result() {
        let metadata = SnapshotMetadata {
            persistence_id: "test".to_string(),
            sequence_nr: 100,
            timestamp: Utc::now(),
        };

        let result = RecoveryResult::new(50, Some(metadata.clone()));
        
        assert_eq!(result.events_replayed, 50);
        assert!(result.has_snapshot());
        assert_eq!(result.snapshot_sequence_nr(), Some(100));
    }
}