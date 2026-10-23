//! Projections — CQRS read-side event processing
//!
//! Projections consume events from the journal and build read-side views.
//! Modeled after Scala Pekko Projections:
//!
//! - **ProjectionHandler**: process events and update read models
//! - **ProjectionOffset**: track processing progress
//! - **OffsetStore**: persist projection offsets for resumability
//! - **Projection**: orchestrates reading events and applying handlers
//! - **AtLeastOnce / ExactlyOnce**: delivery semantics

use crate::errors::{PersistenceError, PersistenceResult};
use crate::query::{EventEnvelope, Offset, ReadJournal};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

// ============= Projection Handler =============

/// A handler that processes events from a projection.
#[async_trait]
pub trait ProjectionHandler: Send + Sync {
    /// Process a single event. The handler should update its read model
    /// and return Ok(()) on success, or Err on failure.
    async fn process(&self, envelope: &EventEnvelope) -> PersistenceResult<()>;
}

/// A stateful projection handler that processes events and maintains state.
#[async_trait]
pub trait StatefulProjectionHandler: Send + Sync {
    /// State type for this handler
    type State: Send + Sync + Clone;

    /// Process an event and return the new state.
    async fn process(
        &self,
        state: &Self::State,
        envelope: &EventEnvelope,
    ) -> PersistenceResult<Self::State>;

    /// Initial state before any events are processed.
    fn initial_state(&self) -> Self::State;
}

// ============= Offset Store =============

/// Stores projection offsets for resumability.
#[async_trait]
pub trait OffsetStore: Send + Sync {
    /// Read the latest stored offset for a projection.
    async fn read_offset(&self, projection_id: &str) -> PersistenceResult<Option<ProjectionOffset>>;

    /// Save a projection offset.
    async fn save_offset(&self, projection_id: &str, offset: ProjectionOffset) -> PersistenceResult<()>;

    /// Clear stored offset for a projection.
    async fn clear_offset(&self, projection_id: &str) -> PersistenceResult<()>;
}

/// A stored offset with metadata.
#[derive(Debug, Clone)]
pub struct ProjectionOffset {
    /// The sequence number offset
    pub sequence_nr: u64,
    /// The persistence ID this offset belongs to (for per-actor projections)
    pub persistence_id: Option<String>,
    /// When this offset was stored
    pub timestamp: DateTime<Utc>,
}

impl ProjectionOffset {
    pub fn new(sequence_nr: u64) -> Self {
        Self {
            sequence_nr,
            persistence_id: None,
            timestamp: Utc::now(),
        }
    }

    pub fn with_persistence_id(mut self, id: String) -> Self {
        self.persistence_id = Some(id);
        self
    }
}

/// In-memory offset store for testing.
pub struct InMemoryOffsetStore {
    offsets: Arc<RwLock<HashMap<String, ProjectionOffset>>>,
}

impl InMemoryOffsetStore {
    pub fn new() -> Self {
        Self {
            offsets: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryOffsetStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl OffsetStore for InMemoryOffsetStore {
    async fn read_offset(&self, projection_id: &str) -> PersistenceResult<Option<ProjectionOffset>> {
        let offsets = self.offsets.read().await;
        Ok(offsets.get(projection_id).cloned())
    }

    async fn save_offset(&self, projection_id: &str, offset: ProjectionOffset) -> PersistenceResult<()> {
        let mut offsets = self.offsets.write().await;
        offsets.insert(projection_id.to_string(), offset);
        Ok(())
    }

    async fn clear_offset(&self, projection_id: &str) -> PersistenceResult<()> {
        let mut offsets = self.offsets.write().await;
        offsets.remove(projection_id);
        Ok(())
    }
}

// ============= Projection Status =============

/// Status of a running projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionStatus {
    /// Projection has not started yet
    NotStarted,
    /// Projection is running and processing events
    Running,
    /// Projection is paused
    Paused,
    /// Projection stopped normally
    Stopped,
    /// Projection failed with an error
    Failed(String),
}

// ============= Projection Config =============

/// Configuration for a projection.
#[derive(Debug, Clone)]
pub struct ProjectionConfig {
    /// Unique identifier for this projection
    pub projection_id: String,
    /// The persistence ID to follow (or None for all)
    pub source_persistence_id: Option<String>,
    /// Tag to filter events (or None for all)
    pub source_tag: Option<String>,
    /// How often to poll for new events (milliseconds)
    pub poll_interval_ms: u64,
    /// How often to save offset (every N events)
    pub save_offset_every: u64,
    /// Maximum events to process in one batch
    pub batch_size: u64,
    /// Restart on failure
    pub restart_on_failure: bool,
    /// Delay before restart
    pub restart_delay_ms: u64,
}

impl ProjectionConfig {
    pub fn new(projection_id: &str) -> Self {
        Self {
            projection_id: projection_id.to_string(),
            source_persistence_id: None,
            source_tag: None,
            poll_interval_ms: 1000,
            save_offset_every: 1,
            batch_size: 100,
            restart_on_failure: true,
            restart_delay_ms: 5000,
        }
    }

    pub fn for_persistence_id(mut self, id: &str) -> Self {
        self.source_persistence_id = Some(id.to_string());
        self
    }

    pub fn for_tag(mut self, tag: &str) -> Self {
        self.source_tag = Some(tag.to_string());
        self
    }

    pub fn poll_interval(mut self, ms: u64) -> Self {
        self.poll_interval_ms = ms;
        self
    }

    pub fn save_offset_every(mut self, n: u64) -> Self {
        self.save_offset_every = n;
        self
    }

    pub fn batch_size(mut self, size: u64) -> Self {
        self.batch_size = size;
        self
    }
}

// ============= Projection =============

/// A Projection that reads events from a ReadJournal, processes them
/// through a handler, and tracks progress via an OffsetStore.
pub struct Projection {
    config: ProjectionConfig,
    read_journal: Arc<dyn ReadJournal>,
    handler: Arc<dyn ProjectionHandler>,
    offset_store: Arc<dyn OffsetStore>,
    status: Arc<RwLock<ProjectionStatus>>,
    stats: Arc<RwLock<ProjectionStats>>,
}

/// Statistics for a running projection.
#[derive(Debug, Clone, Default)]
pub struct ProjectionStats {
    pub events_processed: u64,
    pub events_failed: u64,
    pub last_processed_offset: u64,
    pub last_processed_at: Option<DateTime<Utc>>,
    pub total_processing_time_ms: u64,
}

impl Projection {
    pub fn new(
        config: ProjectionConfig,
        read_journal: Arc<dyn ReadJournal>,
        handler: Arc<dyn ProjectionHandler>,
        offset_store: Arc<dyn OffsetStore>,
    ) -> Self {
        Self {
            config,
            read_journal,
            handler,
            offset_store,
            status: Arc::new(RwLock::new(ProjectionStatus::NotStarted)),
            stats: Arc::new(RwLock::new(ProjectionStats::default())),
        }
    }

    /// Run the projection once (process available events).
    /// Returns the number of events processed.
    pub async fn run_once(&self) -> PersistenceResult<u64> {
        // Read current offset
        let stored_offset = self.offset_store
            .read_offset(&self.config.projection_id)
            .await?;

        let from_seq = stored_offset
            .as_ref()
            .map(|o| o.sequence_nr + 1)
            .unwrap_or(1);

        // Fetch events
        let events = if let Some(ref pid) = self.config.source_persistence_id {
            self.read_journal
                .events_by_persistence_id(pid, from_seq, u64::MAX)
                .await
                .map_err(PersistenceError::Journal)?
        } else if let Some(ref tag) = self.config.source_tag {
            self.read_journal
                .events_by_tag(tag, Offset::Sequence(from_seq))
                .await
                .map_err(PersistenceError::Journal)?
        } else {
            // No filter specified — error
            return Err(PersistenceError::Configuration(
                "Projection must specify source_persistence_id or source_tag".to_string(),
            ));
        };

        // Limit to batch_size
        let batch: Vec<_> = events.into_iter().take(self.config.batch_size as usize).collect();
        let mut processed = 0u64;

        for envelope in &batch {
            match self.handler.process(envelope).await {
                Ok(()) => {
                    processed += 1;

                    // Save offset periodically
                    if processed % self.config.save_offset_every == 0 {
                        let offset = ProjectionOffset::new(envelope.sequence_nr)
                            .with_persistence_id(envelope.persistence_id.clone());
                        self.offset_store
                            .save_offset(&self.config.projection_id, offset)
                            .await?;
                    }

                    // Update stats
                    let mut stats = self.stats.write().await;
                    stats.events_processed += 1;
                    stats.last_processed_offset = envelope.sequence_nr;
                    stats.last_processed_at = Some(Utc::now());
                }
                Err(e) => {
                    warn!("Projection {} failed on event {}/{}: {}",
                        self.config.projection_id,
                        envelope.persistence_id,
                        envelope.sequence_nr,
                        e
                    );
                    let mut stats = self.stats.write().await;
                    stats.events_failed += 1;
                    return Err(e);
                }
            }
        }

        // Final offset save
        if processed > 0 {
            if let Some(last) = batch.last() {
                let offset = ProjectionOffset::new(last.sequence_nr)
                    .with_persistence_id(last.persistence_id.clone());
                self.offset_store
                    .save_offset(&self.config.projection_id, offset)
                    .await?;
            }
        }

        Ok(processed)
    }

    /// Start the projection as a background loop.
    /// Returns a handle that can be used to stop it.
    pub async fn start(self: Arc<Self>) -> ProjectionHandle {
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);

        let projection = self.clone();
        let task = tokio::spawn(async move {
            {
                let mut status = projection.status.write().await;
                *status = ProjectionStatus::Running;
            }
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                    }
                    result = projection.run_once() => {
                        match result {
                            Ok(count) => {
                                if count > 0 {
                                    debug!("Projection {} processed {} events",
                                        projection.config.projection_id, count);
                                }
                            }
                            Err(e) => {
                                error!("Projection {} error: {}",
                                    projection.config.projection_id, e);
                                if !projection.config.restart_on_failure {
                                    let mut status = projection.status.write().await;
                                    *status = ProjectionStatus::Failed(e.to_string());
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(
                                    projection.config.restart_delay_ms
                                )).await;
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(
                    projection.config.poll_interval_ms
                )).await;
            }

            let mut status = projection.status.write().await;
            if *status == ProjectionStatus::Running {
                *status = ProjectionStatus::Stopped;
            }
        });

        ProjectionHandle { stop_tx, task }
    }

    /// Get current status.
    pub async fn status(&self) -> ProjectionStatus {
        self.status.read().await.clone()
    }

    /// Get current stats.
    pub async fn stats(&self) -> ProjectionStats {
        self.stats.read().await.clone()
    }

    /// Get projection ID
    pub fn projection_id(&self) -> &str {
        &self.config.projection_id
    }
}

/// Handle to a running projection for lifecycle management.
pub struct ProjectionHandle {
    stop_tx: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl ProjectionHandle {
    /// Signal the projection to stop.
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    /// Wait for the projection to finish.
    pub async fn join(self) {
        let _ = self.task.await;
    }

    /// Stop and wait for the projection to finish.
    pub async fn stop_and_join(self) {
        self.stop();
        self.join().await;
    }
}

// ============= Convenience: Function-based Handler =============

/// A handler created from a closure/function for quick prototyping.
pub struct FnProjectionHandler<F>
where
    F: Fn(&EventEnvelope) -> PersistenceResult<()> + Send + Sync,
{
    handler_fn: F,
}

impl<F> FnProjectionHandler<F>
where
    F: Fn(&EventEnvelope) -> PersistenceResult<()> + Send + Sync,
{
    pub fn new(handler_fn: F) -> Self {
        Self { handler_fn }
    }
}

#[async_trait]
impl<F> ProjectionHandler for FnProjectionHandler<F>
where
    F: Fn(&EventEnvelope) -> PersistenceResult<()> + Send + Sync,
{
    async fn process(&self, envelope: &EventEnvelope) -> PersistenceResult<()> {
        (self.handler_fn)(envelope)
    }
}

// ============= Collecting Handler (for testing) =============

/// A handler that collects all processed events (useful for testing).
pub struct CollectingHandler {
    events: Arc<RwLock<Vec<EventEnvelope>>>,
}

impl CollectingHandler {
    pub fn new() -> Self {
        Self {
            events: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn collected(&self) -> Vec<EventEnvelope> {
        self.events.read().await.clone()
    }

    pub async fn count(&self) -> usize {
        self.events.read().await.len()
    }
}

impl Default for CollectingHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ProjectionHandler for CollectingHandler {
    async fn process(&self, envelope: &EventEnvelope) -> PersistenceResult<()> {
        let mut events = self.events.write().await;
        events.push(envelope.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::PersistentRepr;
    use crate::journal::InMemoryJournal;
    use crate::query::JournalReadJournal;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_event(pid: &str, seq: u64, tags: Vec<String>) -> PersistentRepr {
        PersistentRepr {
            persistence_id: pid.to_string(),
            sequence_nr: seq,
            manifest: "TestEvent".to_string(),
            payload: format!("data-{}", seq).into_bytes(),
            timestamp: Utc::now(),
            writer_uuid: Uuid::new_v4(),
            tags,
        }
    }

    async fn setup() -> (Arc<InMemoryJournal>, Arc<JournalReadJournal>) {
        let journal = Arc::new(InMemoryJournal::new());

        journal.write_messages(vec![
            make_event("orders", 1, vec!["order".to_string()]),
            make_event("orders", 2, vec!["order".to_string()]),
            make_event("orders", 3, vec!["order".to_string()]),
        ]).await.unwrap();

        let read = Arc::new(JournalReadJournal::new(journal.clone()));
        (journal, read)
    }

    // ===== InMemoryOffsetStore Tests =====

    #[tokio::test]
    async fn test_offset_store_basic() {
        let store = InMemoryOffsetStore::new();

        // Initially no offset
        assert!(store.read_offset("proj-1").await.unwrap().is_none());

        // Save offset
        store.save_offset("proj-1", ProjectionOffset::new(42)).await.unwrap();

        let offset = store.read_offset("proj-1").await.unwrap().unwrap();
        assert_eq!(offset.sequence_nr, 42);

        // Clear offset
        store.clear_offset("proj-1").await.unwrap();
        assert!(store.read_offset("proj-1").await.unwrap().is_none());
    }

    // ===== CollectingHandler Tests =====

    #[tokio::test]
    async fn test_collecting_handler() {
        let handler = CollectingHandler::new();
        assert_eq!(handler.count().await, 0);

        let envelope = EventEnvelope {
            offset: Offset::Sequence(1),
            persistence_id: "test".to_string(),
            sequence_nr: 1,
            manifest: "Test".to_string(),
            payload: vec![],
            timestamp: Utc::now(),
            tags: vec![],
        };

        handler.process(&envelope).await.unwrap();
        assert_eq!(handler.count().await, 1);
    }

    // ===== Projection run_once Tests =====

    #[tokio::test]
    async fn test_projection_run_once() {
        let (_journal, read) = setup().await;
        let handler = Arc::new(CollectingHandler::new());
        let offset_store = Arc::new(InMemoryOffsetStore::new());

        let config = ProjectionConfig::new("test-projection")
            .for_persistence_id("orders");

        let projection = Projection::new(config, read, handler.clone(), offset_store.clone());

        // First run: processes all 3 events
        let count = projection.run_once().await.unwrap();
        assert_eq!(count, 3);
        assert_eq!(handler.count().await, 3);

        // Check offset was saved
        let offset = offset_store.read_offset("test-projection").await.unwrap().unwrap();
        assert_eq!(offset.sequence_nr, 3);

        // Second run: no new events
        let count = projection.run_once().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_projection_resumes_from_offset() {
        let (journal, read) = setup().await;
        let handler = Arc::new(CollectingHandler::new());
        let offset_store = Arc::new(InMemoryOffsetStore::new());

        // Pre-set offset to 2 (already processed events 1-2)
        offset_store.save_offset("test-proj",
            ProjectionOffset::new(2).with_persistence_id("orders".to_string())
        ).await.unwrap();

        let config = ProjectionConfig::new("test-proj")
            .for_persistence_id("orders");

        let projection = Projection::new(config, read, handler.clone(), offset_store.clone());

        // Should only process event 3
        let count = projection.run_once().await.unwrap();
        assert_eq!(count, 1);

        let events = handler.collected().await;
        assert_eq!(events[0].sequence_nr, 3);
    }

    #[tokio::test]
    async fn test_projection_by_tag() {
        let journal = Arc::new(InMemoryJournal::new());

        journal.write_messages(vec![
            make_event("actor-a", 1, vec!["important".to_string()]),
            make_event("actor-a", 2, vec![]),
            make_event("actor-a", 3, vec!["important".to_string()]),
        ]).await.unwrap();
        journal.write_messages(vec![
            make_event("actor-b", 1, vec!["important".to_string()]),
        ]).await.unwrap();

        let read = Arc::new(JournalReadJournal::new(journal));
        let handler = Arc::new(CollectingHandler::new());
        let offset_store = Arc::new(InMemoryOffsetStore::new());

        let config = ProjectionConfig::new("tag-proj")
            .for_tag("important");

        let projection = Projection::new(config, read, handler.clone(), offset_store.clone());

        let count = projection.run_once().await.unwrap();
        assert_eq!(count, 3); // actor-a#1, actor-a#3, actor-b#1
    }

    #[tokio::test]
    async fn test_projection_batch_size() {
        let (_journal, read) = setup().await;
        let handler = Arc::new(CollectingHandler::new());
        let offset_store = Arc::new(InMemoryOffsetStore::new());

        let config = ProjectionConfig::new("batch-proj")
            .for_persistence_id("orders")
            .batch_size(2); // Only process 2 at a time

        let projection = Projection::new(config, read, handler.clone(), offset_store.clone());

        let count = projection.run_once().await.unwrap();
        assert_eq!(count, 2); // Only 2 of 3

        // Next run gets the remaining 1
        let count = projection.run_once().await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_projection_stats() {
        let (_journal, read) = setup().await;
        let handler = Arc::new(CollectingHandler::new());
        let offset_store = Arc::new(InMemoryOffsetStore::new());

        let config = ProjectionConfig::new("stats-proj")
            .for_persistence_id("orders");

        let projection = Projection::new(config, read, handler.clone(), offset_store.clone());
        projection.run_once().await.unwrap();

        let stats = projection.stats().await;
        assert_eq!(stats.events_processed, 3);
        assert_eq!(stats.events_failed, 0);
        assert_eq!(stats.last_processed_offset, 3);
        assert!(stats.last_processed_at.is_some());
    }

    #[tokio::test]
    async fn test_projection_status() {
        let (_journal, read) = setup().await;
        let handler = Arc::new(CollectingHandler::new());
        let offset_store = Arc::new(InMemoryOffsetStore::new());

        let config = ProjectionConfig::new("status-proj")
            .for_persistence_id("orders");

        let projection = Projection::new(config, read, handler, offset_store);
        assert_eq!(projection.status().await, ProjectionStatus::NotStarted);
    }

    #[tokio::test]
    async fn test_projection_no_source_error() {
        let (_journal, read) = setup().await;
        let handler = Arc::new(CollectingHandler::new());
        let offset_store = Arc::new(InMemoryOffsetStore::new());

        let config = ProjectionConfig::new("no-source");
        // No source_persistence_id or source_tag

        let projection = Projection::new(config, read, handler, offset_store);
        let result = projection.run_once().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_fn_handler() {
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter_clone = counter.clone();

        let handler = FnProjectionHandler::new(move |_envelope| {
            counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        });

        let envelope = EventEnvelope {
            offset: Offset::Sequence(1),
            persistence_id: "test".to_string(),
            sequence_nr: 1,
            manifest: "Test".to_string(),
            payload: vec![],
            timestamp: Utc::now(),
            tags: vec![],
        };

        handler.process(&envelope).await.unwrap();
        handler.process(&envelope).await.unwrap();
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_projection_offset_with_persistence_id() {
        let offset = ProjectionOffset::new(10)
            .with_persistence_id("my-actor".to_string());

        assert_eq!(offset.sequence_nr, 10);
        assert_eq!(offset.persistence_id, Some("my-actor".to_string()));
    }
}
