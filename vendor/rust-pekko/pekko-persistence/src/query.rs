//! Persistence Query Plugin — Read-side access to event streams
//!
//! Modeled after Scala Pekko's Persistence Query, this module provides
//! a read-side API for querying the event journal without requiring
//! an actor. Supports:
//!
//! - `events_by_persistence_id` — stream events for a specific actor
//! - `current_events_by_persistence_id` — one-shot query (no live updates)
//! - `events_by_tag` — stream events matching a tag across all actors
//! - `current_persistence_ids` — list all known persistence IDs
//! - `persistence_ids` — live stream of persistence IDs (including new ones)

use crate::errors::{JournalResult};
use crate::journal::Journal;
use crate::actor::PersistentRepr;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, warn};

// ============= Query Offset =============

/// Represents a position in the event stream for resumable queries
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Offset {
    /// No offset — start from the beginning
    NoOffset,
    /// Sequence-based offset for a specific persistence ID
    Sequence(u64),
    /// Timestamp-based offset across all persistence IDs
    TimeBasedOffset(DateTime<Utc>),
}

impl Offset {
    pub fn sequence(seq_nr: u64) -> Self {
        Offset::Sequence(seq_nr)
    }

    pub fn time_based(timestamp: DateTime<Utc>) -> Self {
        Offset::TimeBasedOffset(timestamp)
    }
}

impl Default for Offset {
    fn default() -> Self {
        Offset::NoOffset
    }
}

// ============= Event Envelope =============

/// An event read from the query side, enriched with metadata
#[derive(Debug, Clone)]
pub struct EventEnvelope {
    /// The offset of this event in the stream
    pub offset: Offset,
    /// The persistence ID of the actor that produced this event
    pub persistence_id: String,
    /// The sequence number within the actor's event stream
    pub sequence_nr: u64,
    /// The event manifest (type name)
    pub manifest: String,
    /// The serialized event payload
    pub payload: Vec<u8>,
    /// Timestamp when the event was persisted
    pub timestamp: DateTime<Utc>,
    /// Tags associated with this event
    pub tags: Vec<String>,
}

impl EventEnvelope {
    /// Create from a PersistentRepr
    pub fn from_persistent_repr(repr: &PersistentRepr) -> Self {
        Self {
            offset: Offset::Sequence(repr.sequence_nr),
            persistence_id: repr.persistence_id.clone(),
            sequence_nr: repr.sequence_nr,
            manifest: repr.manifest.clone(),
            payload: repr.payload.clone(),
            timestamp: repr.timestamp,
            tags: repr.tags.clone(),
        }
    }
}

// ============= Query Trait =============

/// Read-side query interface for the event journal.
///
/// This trait provides the Persistence Query API, modeled after
/// Scala Pekko's `ReadJournal` / `PersistenceQuery`.
#[async_trait]
pub trait ReadJournal: Send + Sync {
    /// Query events for a specific persistence ID, starting from the given offset.
    /// Returns a batch of events. Call repeatedly with updated offset for streaming.
    async fn events_by_persistence_id(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
    ) -> JournalResult<Vec<EventEnvelope>>;

    /// Query current events (one-shot, no live tail).
    async fn current_events_by_persistence_id(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
    ) -> JournalResult<Vec<EventEnvelope>>;

    /// Query events by tag across all persistence IDs.
    async fn events_by_tag(
        &self,
        tag: &str,
        offset: Offset,
    ) -> JournalResult<Vec<EventEnvelope>>;

    /// Query current events by tag (one-shot).
    async fn current_events_by_tag(
        &self,
        tag: &str,
        offset: Offset,
    ) -> JournalResult<Vec<EventEnvelope>>;

    /// Get all known persistence IDs (one-shot).
    async fn current_persistence_ids(&self) -> JournalResult<Vec<String>>;

    /// Get the highest sequence number for a persistence ID.
    async fn latest_sequence_nr(&self, persistence_id: &str) -> JournalResult<u64>;
}

// ============= Journal-backed ReadJournal =============

/// A ReadJournal implementation backed by any Journal trait impl.
pub struct JournalReadJournal {
    journal: Arc<dyn Journal>,
}

impl JournalReadJournal {
    pub fn new(journal: Arc<dyn Journal>) -> Self {
        Self { journal }
    }
}

#[async_trait]
impl ReadJournal for JournalReadJournal {
    async fn events_by_persistence_id(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
    ) -> JournalResult<Vec<EventEnvelope>> {
        let reprs = self.journal.replay_messages(
            persistence_id,
            from_sequence_nr,
            to_sequence_nr,
            None,
        ).await?;

        Ok(reprs.iter().map(EventEnvelope::from_persistent_repr).collect())
    }

    async fn current_events_by_persistence_id(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
    ) -> JournalResult<Vec<EventEnvelope>> {
        // For current (one-shot), same as events_by_persistence_id
        self.events_by_persistence_id(persistence_id, from_sequence_nr, to_sequence_nr).await
    }

    async fn events_by_tag(
        &self,
        tag: &str,
        offset: Offset,
    ) -> JournalResult<Vec<EventEnvelope>> {
        let ids = self.journal.persistence_ids().await?;
        let mut result = Vec::new();

        let from_seq = match &offset {
            Offset::Sequence(s) => *s,
            _ => 1,
        };

        for id in ids {
            let events = self.journal.replay_messages(&id, from_seq, u64::MAX, None).await?;
            for event in &events {
                if event.tags.contains(&tag.to_string()) {
                    result.push(EventEnvelope::from_persistent_repr(event));
                }
            }
        }

        // Sort by timestamp for cross-actor ordering
        result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(result)
    }

    async fn current_events_by_tag(
        &self,
        tag: &str,
        offset: Offset,
    ) -> JournalResult<Vec<EventEnvelope>> {
        self.events_by_tag(tag, offset).await
    }

    async fn current_persistence_ids(&self) -> JournalResult<Vec<String>> {
        self.journal.persistence_ids().await
    }

    async fn latest_sequence_nr(&self, persistence_id: &str) -> JournalResult<u64> {
        self.journal.highest_sequence_nr(persistence_id).await
    }
}

// ============= Live Event Source =============

/// A streaming event source that polls the journal for new events.
/// Useful for building live tailing / subscriptions.
pub struct LiveEventSource {
    journal: Arc<dyn Journal>,
    persistence_id: String,
    current_offset: u64,
    poll_interval_ms: u64,
}

impl LiveEventSource {
    pub fn new(
        journal: Arc<dyn Journal>,
        persistence_id: String,
        from_offset: u64,
        poll_interval_ms: u64,
    ) -> Self {
        Self {
            journal,
            persistence_id,
            current_offset: from_offset,
            poll_interval_ms,
        }
    }

    /// Poll for new events since last offset. Returns empty vec if no new events.
    pub async fn poll(&mut self) -> JournalResult<Vec<EventEnvelope>> {
        let events = self.journal.replay_messages(
            &self.persistence_id,
            self.current_offset + 1,
            u64::MAX,
            None,
        ).await?;

        if let Some(last) = events.last() {
            self.current_offset = last.sequence_nr;
        }

        Ok(events.iter().map(EventEnvelope::from_persistent_repr).collect())
    }

    /// Start a background polling loop, sending events to a channel.
    pub fn start_streaming(mut self) -> mpsc::Receiver<EventEnvelope> {
        let (tx, rx) = mpsc::channel(256);

        tokio::spawn(async move {
            loop {
                match self.poll().await {
                    Ok(events) => {
                        for event in events {
                            if tx.send(event).await.is_err() {
                                debug!("LiveEventSource: receiver dropped, stopping");
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("LiveEventSource poll error: {}", e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(self.poll_interval_ms)).await;
            }
        });

        rx
    }

    /// Current offset position
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }
}

// ============= Tag-based Live Source =============

/// Live event source that streams events matching a specific tag.
pub struct TagEventSource {
    journal: Arc<dyn Journal>,
    tag: String,
    last_seen_timestamps: std::collections::HashMap<String, DateTime<Utc>>,
}

impl TagEventSource {
    pub fn new(journal: Arc<dyn Journal>, tag: String) -> Self {
        Self {
            journal,
            tag,
            last_seen_timestamps: std::collections::HashMap::new(),
        }
    }

    /// Poll for new tagged events
    pub async fn poll(&mut self) -> JournalResult<Vec<EventEnvelope>> {
        let ids = self.journal.persistence_ids().await?;
        let mut result = Vec::new();

        for id in ids {
            let events = self.journal.replay_messages(&id, 1, u64::MAX, None).await?;
            for event in &events {
                if !event.tags.contains(&self.tag) {
                    continue;
                }
                // Skip events we've already seen
                if let Some(last_ts) = self.last_seen_timestamps.get(&id) {
                    if event.timestamp <= *last_ts {
                        continue;
                    }
                }
                result.push(EventEnvelope::from_persistent_repr(event));
            }

            // Update last seen timestamp for this persistence ID
            if let Some(last) = events.last() {
                self.last_seen_timestamps.insert(id, last.timestamp);
            }
        }

        result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::InMemoryJournal;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_event(pid: &str, seq: u64, tags: Vec<String>) -> PersistentRepr {
        PersistentRepr {
            persistence_id: pid.to_string(),
            sequence_nr: seq,
            manifest: "TestEvent".to_string(),
            payload: format!("event-{}", seq).into_bytes(),
            timestamp: Utc::now(),
            writer_uuid: Uuid::new_v4(),
            tags,
        }
    }

    async fn setup_journal() -> Arc<InMemoryJournal> {
        let journal = Arc::new(InMemoryJournal::new());

        // Actor A: 3 events, some tagged
        journal.write_messages(vec![
            make_event("actor-a", 1, vec!["order".to_string()]),
            make_event("actor-a", 2, vec!["order".to_string(), "payment".to_string()]),
            make_event("actor-a", 3, vec![]),
        ]).await.unwrap();

        // Actor B: 2 events
        journal.write_messages(vec![
            make_event("actor-b", 1, vec!["payment".to_string()]),
            make_event("actor-b", 2, vec!["shipping".to_string()]),
        ]).await.unwrap();

        journal
    }

    #[tokio::test]
    async fn test_events_by_persistence_id() {
        let journal = setup_journal().await;
        let read = JournalReadJournal::new(journal);

        let events = read.events_by_persistence_id("actor-a", 1, u64::MAX).await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].sequence_nr, 1);
        assert_eq!(events[2].sequence_nr, 3);
    }

    #[tokio::test]
    async fn test_events_by_persistence_id_range() {
        let journal = setup_journal().await;
        let read = JournalReadJournal::new(journal);

        let events = read.events_by_persistence_id("actor-a", 2, 3).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence_nr, 2);
    }

    #[tokio::test]
    async fn test_events_by_tag() {
        let journal = setup_journal().await;
        let read = JournalReadJournal::new(journal);

        let payment_events = read.events_by_tag("payment", Offset::NoOffset).await.unwrap();
        assert_eq!(payment_events.len(), 2); // actor-a#2, actor-b#1

        let order_events = read.events_by_tag("order", Offset::NoOffset).await.unwrap();
        assert_eq!(order_events.len(), 2); // actor-a#1, actor-a#2

        let shipping_events = read.events_by_tag("shipping", Offset::NoOffset).await.unwrap();
        assert_eq!(shipping_events.len(), 1);
    }

    #[tokio::test]
    async fn test_current_persistence_ids() {
        let journal = setup_journal().await;
        let read = JournalReadJournal::new(journal);

        let mut ids = read.current_persistence_ids().await.unwrap();
        ids.sort();
        assert_eq!(ids, vec!["actor-a", "actor-b"]);
    }

    #[tokio::test]
    async fn test_latest_sequence_nr() {
        let journal = setup_journal().await;
        let read = JournalReadJournal::new(journal);

        assert_eq!(read.latest_sequence_nr("actor-a").await.unwrap(), 3);
        assert_eq!(read.latest_sequence_nr("actor-b").await.unwrap(), 2);
        assert_eq!(read.latest_sequence_nr("nonexistent").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_event_envelope_from_repr() {
        let repr = make_event("test", 42, vec!["tag1".to_string()]);
        let envelope = EventEnvelope::from_persistent_repr(&repr);

        assert_eq!(envelope.persistence_id, "test");
        assert_eq!(envelope.sequence_nr, 42);
        assert_eq!(envelope.offset, Offset::Sequence(42));
        assert_eq!(envelope.tags, vec!["tag1".to_string()]);
    }

    #[tokio::test]
    async fn test_live_event_source_poll() {
        let journal = setup_journal().await;
        let mut source = LiveEventSource::new(
            journal.clone(),
            "actor-a".to_string(),
            0, // start from beginning
            100,
        );

        // First poll: get all 3 events
        let events = source.poll().await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(source.current_offset(), 3);

        // Second poll: no new events
        let events = source.poll().await.unwrap();
        assert_eq!(events.len(), 0);

        // Write a new event
        journal.write_messages(vec![
            make_event("actor-a", 4, vec![]),
        ]).await.unwrap();

        // Third poll: get the new event
        let events = source.poll().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence_nr, 4);
        assert_eq!(source.current_offset(), 4);
    }

    #[tokio::test]
    async fn test_offset_ordering() {
        assert!(Offset::NoOffset < Offset::Sequence(1));
        assert!(Offset::Sequence(1) < Offset::Sequence(2));
    }

    #[tokio::test]
    async fn test_events_by_tag_empty() {
        let journal = setup_journal().await;
        let read = JournalReadJournal::new(journal);

        let events = read.events_by_tag("nonexistent-tag", Offset::NoOffset).await.unwrap();
        assert_eq!(events.len(), 0);
    }
}
