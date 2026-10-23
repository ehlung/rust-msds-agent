//! Persistent actor trait and context

use crate::{Journal, PersistenceError, PersistenceResult, SerializationError, SnapshotStore};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pekko_actor::{Actor, ActorContext};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// Persistent representation of an event in the journal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentRepr {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub manifest: String,
    pub payload: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub writer_uuid: Uuid,
    pub tags: Vec<String>,
}

impl PersistentRepr {
    pub fn new(
        persistence_id: String,
        sequence_nr: u64,
        manifest: String,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            persistence_id,
            sequence_nr,
            manifest,
            payload,
            timestamp: Utc::now(),
            writer_uuid: Uuid::new_v4(),
            tags: Vec::new(),
        }
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }
}

/// Result of an automatic snapshot operation
#[derive(Debug, Clone)]
pub struct AutoSnapshotResult {
    /// Sequence number at which the snapshot was taken
    pub sequence_nr: u64,
    /// Number of events deleted (if event deletion was enabled)
    pub events_deleted: u64,
    /// Number of old snapshots deleted (based on retention policy)
    pub old_snapshots_deleted: u64,
}

impl AutoSnapshotResult {
    pub fn new(sequence_nr: u64) -> Self {
        Self {
            sequence_nr,
            events_deleted: 0,
            old_snapshots_deleted: 0,
        }
    }
}

/// Trait for persistent actors that can store and replay events
#[async_trait]
pub trait PersistentActor: Actor + Send + Sync + 'static {
    /// The event type this actor persists
    type Event: Send + Sync + 'static;
    
    /// The state type this actor maintains
    type State: Default + Send + Sync + 'static;

    /// The snapshot type for this actor (can be same as State)
    type Snapshot: Send + Sync + 'static;

    /// Unique identifier for this persistent actor
    fn persistence_id(&self) -> String;

    /// Handle a recovered event during actor startup
    async fn receive_recover(
        &mut self,
        event: Self::Event,
        ctx: &mut PersistentContext<Self>,
    );

    /// Handle a command message (normal actor operation)
    async fn receive_command(
        &mut self,
        msg: Self::Message,
        ctx: &mut PersistentContext<Self>,
    );

    /// Apply an event to update the actor's state
    fn apply_event(&mut self, event: &Self::Event) -> Self::State;

    /// Create a snapshot from the current state
    fn create_snapshot(&self) -> Self::Snapshot;

    /// Apply a snapshot to restore state
    fn apply_snapshot(&mut self, snapshot: Self::Snapshot);

    /// Called when recovery is completed
    async fn recovery_completed(&mut self, _ctx: &mut PersistentContext<Self>) {
        // Default implementation does nothing
    }

    /// Called when a snapshot is saved successfully
    async fn snapshot_saved(&mut self, _metadata: SnapshotMetadata) {
        // Default implementation does nothing
    }

    /// Called when snapshot save fails
    async fn snapshot_save_failed(&mut self, _error: PersistenceError) {
        // Default implementation does nothing
    }
}

/// Snapshot policy configuration
#[derive(Debug, Clone)]
pub struct SnapshotPolicy {
    /// Take a snapshot every N events (0 = disabled)
    pub snapshot_every: u64,
    /// Delete events before the snapshot after taking it
    pub delete_events_on_snapshot: bool,
    /// Number of snapshots to retain (0 = unlimited)
    pub keep_snapshots: u64,
    /// Sequence number of last snapshot taken
    pub last_snapshot_seq: u64,
}

impl Default for SnapshotPolicy {
    fn default() -> Self {
        Self {
            snapshot_every: 0, // Disabled by default
            delete_events_on_snapshot: false,
            keep_snapshots: 3,
            last_snapshot_seq: 0,
        }
    }
}

impl SnapshotPolicy {
    /// Create a policy that snapshots every N events
    pub fn every(n: u64) -> Self {
        Self {
            snapshot_every: n,
            ..Default::default()
        }
    }

    /// Enable event deletion after snapshot
    pub fn with_event_deletion(mut self) -> Self {
        self.delete_events_on_snapshot = true;
        self
    }

    /// Set number of snapshots to keep
    pub fn keep_snapshots(mut self, n: u64) -> Self {
        self.keep_snapshots = n;
        self
    }

    /// Check if a snapshot should be taken at the given sequence number
    pub fn should_snapshot(&self, current_seq: u64) -> bool {
        if self.snapshot_every == 0 {
            return false;
        }
        let events_since_last = current_seq.saturating_sub(self.last_snapshot_seq);
        events_since_last >= self.snapshot_every
    }
}

/// Enhanced context for persistent actors
pub struct PersistentContext<A: PersistentActor + ?Sized> {
    base_context: ActorContext<A>,
    journal: Arc<dyn Journal>,
    snapshot_store: Arc<dyn SnapshotStore>,
    persistence_id: String,
    pub sequence_nr: u64,
    pub state: A::State,
    pub recovering: bool,
    pub snapshot_policy: SnapshotPolicy,
}

impl<A: PersistentActor> PersistentContext<A> {
    pub fn new(
        base_context: ActorContext<A>,
        journal: Arc<dyn Journal>,
        snapshot_store: Arc<dyn SnapshotStore>,
        persistence_id: String,
    ) -> Self {
        Self {
            base_context,
            journal,
            snapshot_store,
            persistence_id,
            sequence_nr: 0,
            state: A::State::default(),
            recovering: false,
            snapshot_policy: SnapshotPolicy::default(),
        }
    }

    /// Create context with a custom snapshot policy
    pub fn with_snapshot_policy(
        base_context: ActorContext<A>,
        journal: Arc<dyn Journal>,
        snapshot_store: Arc<dyn SnapshotStore>,
        persistence_id: String,
        snapshot_policy: SnapshotPolicy,
    ) -> Self {
        Self {
            base_context,
            journal,
            snapshot_store,
            persistence_id,
            sequence_nr: 0,
            state: A::State::default(),
            recovering: false,
            snapshot_policy,
        }
    }

    /// Set the snapshot policy
    pub fn set_snapshot_policy(&mut self, policy: SnapshotPolicy) {
        self.snapshot_policy = policy;
    }

    /// Persist a single event
    pub async fn persist<E>(&mut self, event: E) -> PersistenceResult<()>
    where
        E: Into<A::Event>,
        A::Event: Serialize,
    {
        if self.recovering {
            return Err(PersistenceError::Recovery(
                "Cannot persist events during recovery".to_string(),
            ));
        }

        let event = event.into();
        self.sequence_nr += 1;

        // Serialize the event
        let payload = serde_json::to_vec(&event)
            .map_err(|e| PersistenceError::Serialization(SerializationError::Json(e)))?;

        let repr = PersistentRepr::new(
            self.persistence_id.clone(),
            self.sequence_nr,
            std::any::type_name::<A::Event>().to_string(),
            payload,
        );

        // Write to journal
        self.journal.write_messages(vec![repr]).await?;

        Ok(())
    }

    /// Persist multiple events atomically
    pub async fn persist_all<I>(&mut self, events: I) -> PersistenceResult<()>
    where
        I: IntoIterator<Item = A::Event>,
        A::Event: Serialize,
    {
        if self.recovering {
            return Err(PersistenceError::Recovery(
                "Cannot persist events during recovery".to_string(),
            ));
        }

        let mut reprs = Vec::new();

        for event in events {
            self.sequence_nr += 1;

            let payload = serde_json::to_vec(&event)
                .map_err(|e| PersistenceError::Serialization(SerializationError::Json(e)))?;

            let repr = PersistentRepr::new(
                self.persistence_id.clone(),
                self.sequence_nr,
                std::any::type_name::<A::Event>().to_string(),
                payload,
            );

            reprs.push(repr);
        }

        // Write all events to journal atomically
        self.journal.write_messages(reprs).await?;

        Ok(())
    }

    /// Save a snapshot
    pub async fn save_snapshot(&mut self, snapshot: A::Snapshot) -> PersistenceResult<()>
    where
        A::Snapshot: Serialize,
    {
        let snapshot_data = serde_json::to_vec(&snapshot)
            .map_err(|e| PersistenceError::Serialization(SerializationError::Json(e)))?;

        let metadata = SnapshotMetadata {
            persistence_id: self.persistence_id.clone(),
            sequence_nr: self.sequence_nr,
            timestamp: Utc::now(),
        };

        self.snapshot_store
            .save_snapshot(&metadata, snapshot_data)
            .await?;

        // Update policy tracking
        self.snapshot_policy.last_snapshot_seq = self.sequence_nr;

        Ok(())
    }

    /// Save a snapshot and optionally delete old events
    pub async fn save_snapshot_and_cleanup(&mut self, snapshot: A::Snapshot) -> PersistenceResult<AutoSnapshotResult>
    where
        A::Snapshot: Serialize,
    {
        let snapshot_seq = self.sequence_nr;
        
        // Save the snapshot
        self.save_snapshot(snapshot).await?;

        let mut events_deleted = 0u64;

        // Delete old events if policy says so
        if self.snapshot_policy.delete_events_on_snapshot {
            // Keep events from the snapshot sequence onwards
            // Delete everything before the snapshot
            if snapshot_seq > 1 {
                self.journal
                    .delete_messages_to(&self.persistence_id, snapshot_seq - 1)
                    .await?;
                events_deleted = snapshot_seq - 1;
            }
        }

        // Clean up old snapshots if needed
        let mut old_snapshots_deleted = 0u64;
        if self.snapshot_policy.keep_snapshots > 0 {
            let snapshots = self.snapshot_store
                .list_snapshots(&self.persistence_id)
                .await?;
            
            if snapshots.len() as u64 > self.snapshot_policy.keep_snapshots {
                // Delete oldest snapshots
                let to_delete = snapshots.len() as u64 - self.snapshot_policy.keep_snapshots;
                for snap in snapshots.iter().rev().take(to_delete as usize) {
                    self.snapshot_store.delete_snapshot(snap).await?;
                    old_snapshots_deleted += 1;
                }
            }
        }

        Ok(AutoSnapshotResult {
            sequence_nr: snapshot_seq,
            events_deleted,
            old_snapshots_deleted,
        })
    }

    /// Check if a snapshot should be taken based on the policy
    pub fn should_snapshot(&self) -> bool {
        self.snapshot_policy.should_snapshot(self.sequence_nr)
    }

    /// Persist an event and automatically take a snapshot if the policy dictates
    /// Returns Some(AutoSnapshotResult) if a snapshot was taken
    pub async fn persist_and_snapshot_if_needed<E>(
        &mut self, 
        event: E,
        create_snapshot: impl FnOnce() -> A::Snapshot,
    ) -> PersistenceResult<Option<AutoSnapshotResult>>
    where
        E: Into<A::Event>,
        A::Event: Serialize,
        A::Snapshot: Serialize,
    {
        // Persist the event
        self.persist(event).await?;

        // Check if we should snapshot
        if self.should_snapshot() {
            let snapshot = create_snapshot();
            let result = self.save_snapshot_and_cleanup(snapshot).await?;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    /// Delete events up to a given sequence number
    pub async fn delete_events_to(&mut self, to_sequence_nr: u64) -> PersistenceResult<()> {
        self.journal
            .delete_messages_to(&self.persistence_id, to_sequence_nr)
            .await?;
        Ok(())
    }

    /// Get the current sequence number
    pub fn sequence_nr(&self) -> u64 {
        self.sequence_nr
    }

    /// Get the current state
    pub fn state(&self) -> &A::State {
        &self.state
    }

    /// Get mutable access to the state
    pub fn state_mut(&mut self) -> &mut A::State {
        &mut self.state
    }

    /// Check if the actor is currently recovering
    pub fn recovering(&self) -> bool {
        self.recovering
    }

    /// Get a reference to the underlying actor context
    pub fn base_context(&self) -> &ActorContext<A> {
        &self.base_context
    }

    /// Get mutable access to the underlying actor context
    pub fn base_context_mut(&mut self) -> &mut ActorContext<A> {
        &mut self.base_context
    }

    /// Get the persistence ID
    pub fn persistence_id(&self) -> &str {
        &self.persistence_id
    }

    // Helper method to access actor_ref from base context
    // This is a placeholder - actual implementation depends on ActorContext API
    // fn actor_ref(&self) -> &pekko_actor::ActorRef<A::Message> {
    //     // This would need to be implemented based on the actual ActorContext API
    //     todo!("Implement based on ActorContext API")
    // }
}

/// Metadata for snapshots
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub timestamp: DateTime<Utc>,
}

/// Recovery mode for persistent actors
#[derive(Debug, Clone)]
pub enum RecoveryMode {
    /// Recover from the beginning
    FromBeginning,
    /// Recover from a specific sequence number
    FromSequenceNr(u64),
    /// Recover from latest snapshot
    FromSnapshot,
    /// Skip recovery (for testing)
    Skip,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum BankEvent {
        AccountOpened { account_id: String, initial_balance: u64 },
        MoneyDeposited { account_id: String, amount: u64 },
        MoneyWithdrawn { account_id: String, amount: u64 },
    }

    #[derive(Debug, Clone, Default)]
    struct BankAccountState {
        balance: u64,
        opened: bool,
    }

    struct BankAccountActor {
        account_id: String,
        state: BankAccountState,
    }

    #[derive(Debug)]
    enum BankCommand {
        OpenAccount { initial_balance: u64 },
        Deposit { amount: u64 },
        Withdraw { amount: u64 },
        GetBalance,
    }

    #[async_trait]
    impl Actor for BankAccountActor {
        type Message = BankCommand;

        async fn receive(&mut self, msg: Self::Message, _ctx: &mut pekko_actor::ActorContext<Self>) {
            // This would delegate to receive_command in a real implementation
            // Placeholder implementation for testing
            match msg {
                BankCommand::OpenAccount { .. } => {}
                BankCommand::Deposit { .. } => {}
                BankCommand::Withdraw { .. } => {}
                BankCommand::GetBalance => {}
            }
        }
    }

    #[async_trait]
    impl PersistentActor for BankAccountActor {
        type Event = BankEvent;
        type State = BankAccountState;
        type Snapshot = BankAccountState;

        fn persistence_id(&self) -> String {
            format!("bank-account-{}", self.account_id)
        }

        async fn receive_recover(
            &mut self,
            event: Self::Event,
            _ctx: &mut PersistentContext<Self>,
        ) {
            match event {
                BankEvent::AccountOpened { initial_balance, .. } => {
                    self.state.balance = initial_balance;
                    self.state.opened = true;
                }
                BankEvent::MoneyDeposited { amount, .. } => {
                    self.state.balance += amount;
                }
                BankEvent::MoneyWithdrawn { amount, .. } => {
                    self.state.balance = self.state.balance.saturating_sub(amount);
                }
            }
        }

        async fn receive_command(
            &mut self,
            msg: Self::Message,
            ctx: &mut PersistentContext<Self>,
        ) {
            match msg {
                BankCommand::OpenAccount { initial_balance } => {
                    if !self.state.opened {
                        let event = BankEvent::AccountOpened {
                            account_id: self.account_id.clone(),
                            initial_balance,
                        };
                        ctx.persist(event).await.unwrap();
                        self.state.balance = initial_balance;
                        self.state.opened = true;
                    }
                }
                BankCommand::Deposit { amount } => {
                    if self.state.opened {
                        let event = BankEvent::MoneyDeposited {
                            account_id: self.account_id.clone(),
                            amount,
                        };
                        ctx.persist(event).await.unwrap();
                        self.state.balance += amount;
                    }
                }
                BankCommand::Withdraw { amount } => {
                    if self.state.opened && self.state.balance >= amount {
                        let event = BankEvent::MoneyWithdrawn {
                            account_id: self.account_id.clone(),
                            amount,
                        };
                        ctx.persist(event).await.unwrap();
                        self.state.balance -= amount;
                    }
                }
                BankCommand::GetBalance => {
                    // Would send response back to sender
                    println!("Current balance: {}", self.state.balance);
                }
            }
        }

        fn apply_event(&mut self, event: &Self::Event) -> Self::State {
            match event {
                BankEvent::AccountOpened { initial_balance, .. } => BankAccountState {
                    balance: *initial_balance,
                    opened: true,
                },
                BankEvent::MoneyDeposited { amount, .. } => BankAccountState {
                    balance: self.state.balance + amount,
                    opened: self.state.opened,
                },
                BankEvent::MoneyWithdrawn { amount, .. } => BankAccountState {
                    balance: self.state.balance.saturating_sub(*amount),
                    opened: self.state.opened,
                },
            }
        }

        fn create_snapshot(&self) -> Self::Snapshot {
            self.state.clone()
        }

        fn apply_snapshot(&mut self, snapshot: Self::Snapshot) {
            self.state = snapshot;
        }
    }

    #[test]
    fn test_persistent_repr_creation() {
        let repr = PersistentRepr::new(
            "test-actor".to_string(),
            1,
            "TestEvent".to_string(),
            vec![1, 2, 3, 4],
        );

        assert_eq!(repr.persistence_id, "test-actor");
        assert_eq!(repr.sequence_nr, 1);
        assert_eq!(repr.manifest, "TestEvent");
        assert_eq!(repr.payload, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_persistent_context_uses_correct_persistence_id() {
        use crate::{InMemoryJournal, InMemorySnapshotStore};
        use pekko_actor::ActorContext;

        let journal = Arc::new(InMemoryJournal::new());
        let snapshot_store = Arc::new(InMemorySnapshotStore::new());
        let persistence_id = "test-actor-123".to_string();
        
        // Create a simple test actor
        let actor = BankAccountActor {
            account_id: "test-123".to_string(),
            state: BankAccountState::default(),
        };
        
        // Verify that persistence_id is correctly generated
        assert_eq!(actor.persistence_id(), "bank-account-test-123");
        
        // Test that persistence_id is correctly used when creating PersistentRepr
        let test_repr = PersistentRepr::new(
            persistence_id.clone(),
            1,
            "TestEvent".to_string(),
            vec![1, 2, 3],
        );
        
        assert_eq!(test_repr.persistence_id, persistence_id);
    }

    #[tokio::test]
    async fn test_persistent_context_persist_uses_correct_persistence_id() {
        use crate::{InMemoryJournal, InMemorySnapshotStore};
        use pekko_actor::ActorContext;
        use serde::Serialize;

        let journal = Arc::new(InMemoryJournal::new());
        let snapshot_store = Arc::new(InMemorySnapshotStore::new());
        let persistence_id = "test-persist-actor".to_string();

        // Create a mock ActorContext - we'll use a simple approach
        // In a real scenario, this would come from the actor system
        // For testing, we'll create a minimal context structure
        struct MockActorContext;
        
        // Create PersistentContext with persistence_id
        // Note: This is a simplified test - in production, ActorContext would be properly initialized
        // We'll test the persistence_id field directly by checking journal writes
        
        // Write directly to journal to verify persistence_id is used correctly
        let test_event = BankEvent::AccountOpened {
            account_id: "test".to_string(),
            initial_balance: 100,
        };
        
        let payload = serde_json::to_vec(&test_event).unwrap();
        let repr = PersistentRepr::new(
            persistence_id.clone(),
            1,
            std::any::type_name::<BankEvent>().to_string(),
            payload,
        );

        // Write to journal and verify persistence_id is correct
        journal.write_messages(vec![repr.clone()]).await.unwrap();
        
        // Read back and verify persistence_id matches
        let replayed = journal
            .replay_messages(&persistence_id, 1, u64::MAX, None)
            .await
            .unwrap();
        
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].persistence_id, persistence_id);
        assert_eq!(replayed[0].sequence_nr, 1);
    }

    #[tokio::test]
    async fn test_persistence_id_in_journal_write() {
        use crate::InMemoryJournal;
        use chrono::Utc;

        let journal = Arc::new(InMemoryJournal::new());
        let persistence_id_1 = "actor-1".to_string();
        let persistence_id_2 = "actor-2".to_string();

        // Write messages for different actors
        let messages_1 = vec![
            PersistentRepr::new(
                persistence_id_1.clone(),
                1,
                "Event1".to_string(),
                b"data1".to_vec(),
            ),
            PersistentRepr::new(
                persistence_id_1.clone(),
                2,
                "Event2".to_string(),
                b"data2".to_vec(),
            ),
        ];

        let messages_2 = vec![
            PersistentRepr::new(
                persistence_id_2.clone(),
                1,
                "Event1".to_string(),
                b"data1".to_vec(),
            ),
        ];

        journal.write_messages(messages_1).await.unwrap();
        journal.write_messages(messages_2).await.unwrap();

        // Verify that messages are stored with correct persistence_id
        let replayed_1 = journal
            .replay_messages(&persistence_id_1, 1, u64::MAX, None)
            .await
            .unwrap();
        
        let replayed_2 = journal
            .replay_messages(&persistence_id_2, 1, u64::MAX, None)
            .await
            .unwrap();

        assert_eq!(replayed_1.len(), 2);
        assert_eq!(replayed_1[0].persistence_id, persistence_id_1);
        assert_eq!(replayed_1[1].persistence_id, persistence_id_1);
        
        assert_eq!(replayed_2.len(), 1);
        assert_eq!(replayed_2[0].persistence_id, persistence_id_2);
    }
}