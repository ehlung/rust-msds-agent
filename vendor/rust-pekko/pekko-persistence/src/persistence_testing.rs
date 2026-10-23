// persistence_testing.rs - Persistence Testing DSL & Utilities
// Mock journal, snapshot store, and testing assertions for persistent actors

use std::collections::{HashMap};
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};

// ============================================================================
// Core Types
// ============================================================================

pub type PersistenceId = String;
pub type SequenceNr = u64;

#[derive(Debug, Clone)]
pub struct TestEvent {
    pub persistence_id: String,
    pub sequence_nr: SequenceNr,
    pub payload: String,
    pub tags: Vec<String>,
    pub timestamp: Instant,
}

#[derive(Debug, Clone)]
pub struct TestSnapshot {
    pub persistence_id: String,
    pub sequence_nr: SequenceNr,
    pub payload: String,
    pub timestamp: Instant,
}

// ============================================================================
// Mock Journal
// ============================================================================

#[derive(Clone)]
pub struct MockJournal {
    events: Arc<Mutex<HashMap<String, Vec<TestEvent>>>>,
    write_latency: Duration,
    fail_on_write: Arc<Mutex<bool>>,
    write_count: Arc<Mutex<u64>>,
    read_count: Arc<Mutex<u64>>,
}

impl MockJournal {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(HashMap::new())),
            write_latency: Duration::ZERO,
            fail_on_write: Arc::new(Mutex::new(false)),
            write_count: Arc::new(Mutex::new(0)),
            read_count: Arc::new(Mutex::new(0)),
        }
    }

    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.write_latency = latency;
        self
    }

    pub fn set_fail_on_write(&self, fail: bool) {
        *self.fail_on_write.lock().unwrap() = fail;
    }

    pub fn write_event(&self, persistence_id: &str, seq_nr: SequenceNr, payload: &str, tags: Vec<String>) -> Result<(), MockJournalError> {
        if *self.fail_on_write.lock().unwrap() {
            return Err(MockJournalError::WriteFailed("Simulated failure".into()));
        }

        if !self.write_latency.is_zero() {
            std::thread::sleep(self.write_latency);
        }

        let event = TestEvent {
            persistence_id: persistence_id.to_string(),
            sequence_nr: seq_nr,
            payload: payload.to_string(),
            tags,
            timestamp: Instant::now(),
        };

        let mut events = self.events.lock().unwrap();
        events.entry(persistence_id.to_string())
            .or_insert_with(Vec::new)
            .push(event);

        *self.write_count.lock().unwrap() += 1;
        Ok(())
    }

    pub fn read_events(&self, persistence_id: &str) -> Vec<TestEvent> {
        *self.read_count.lock().unwrap() += 1;
        let events = self.events.lock().unwrap();
        events.get(persistence_id).cloned().unwrap_or_default()
    }

    pub fn read_events_from(&self, persistence_id: &str, from_seq: SequenceNr) -> Vec<TestEvent> {
        self.read_events(persistence_id)
            .into_iter()
            .filter(|e| e.sequence_nr >= from_seq)
            .collect()
    }

    pub fn read_events_by_tag(&self, tag: &str) -> Vec<TestEvent> {
        let events = self.events.lock().unwrap();
        events.values()
            .flatten()
            .filter(|e| e.tags.contains(&tag.to_string()))
            .cloned()
            .collect()
    }

    pub fn delete_events(&self, persistence_id: &str, to_seq: SequenceNr) -> usize {
        let mut events = self.events.lock().unwrap();
        if let Some(actor_events) = events.get_mut(persistence_id) {
            let before = actor_events.len();
            actor_events.retain(|e| e.sequence_nr > to_seq);
            before - actor_events.len()
        } else {
            0
        }
    }

    pub fn highest_sequence_nr(&self, persistence_id: &str) -> SequenceNr {
        let events = self.events.lock().unwrap();
        events.get(persistence_id)
            .and_then(|evts| evts.last().map(|e| e.sequence_nr))
            .unwrap_or(0)
    }

    pub fn event_count(&self, persistence_id: &str) -> usize {
        let events = self.events.lock().unwrap();
        events.get(persistence_id).map(|e| e.len()).unwrap_or(0)
    }

    pub fn total_event_count(&self) -> usize {
        let events = self.events.lock().unwrap();
        events.values().map(|e| e.len()).sum()
    }

    pub fn write_count(&self) -> u64 {
        *self.write_count.lock().unwrap()
    }

    pub fn read_count(&self) -> u64 {
        *self.read_count.lock().unwrap()
    }

    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
        *self.write_count.lock().unwrap() = 0;
        *self.read_count.lock().unwrap() = 0;
    }
}

impl Default for MockJournal {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MockJournalError {
    WriteFailed(String),
    ReadFailed(String),
}

impl std::fmt::Display for MockJournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MockJournalError::WriteFailed(msg) => write!(f, "Write failed: {}", msg),
            MockJournalError::ReadFailed(msg) => write!(f, "Read failed: {}", msg),
        }
    }
}

impl std::error::Error for MockJournalError {}

// ============================================================================
// Mock Snapshot Store
// ============================================================================

#[derive(Clone)]
pub struct MockSnapshotStore {
    snapshots: Arc<Mutex<HashMap<String, Vec<TestSnapshot>>>>,
    save_count: Arc<Mutex<u64>>,
    load_count: Arc<Mutex<u64>>,
    fail_on_save: Arc<Mutex<bool>>,
}

impl MockSnapshotStore {
    pub fn new() -> Self {
        Self {
            snapshots: Arc::new(Mutex::new(HashMap::new())),
            save_count: Arc::new(Mutex::new(0)),
            load_count: Arc::new(Mutex::new(0)),
            fail_on_save: Arc::new(Mutex::new(false)),
        }
    }

    pub fn set_fail_on_save(&self, fail: bool) {
        *self.fail_on_save.lock().unwrap() = fail;
    }

    pub fn save_snapshot(&self, persistence_id: &str, seq_nr: SequenceNr, payload: &str) -> Result<(), MockJournalError> {
        if *self.fail_on_save.lock().unwrap() {
            return Err(MockJournalError::WriteFailed("Simulated snapshot failure".into()));
        }

        let snapshot = TestSnapshot {
            persistence_id: persistence_id.to_string(),
            sequence_nr: seq_nr,
            payload: payload.to_string(),
            timestamp: Instant::now(),
        };

        let mut snapshots = self.snapshots.lock().unwrap();
        snapshots.entry(persistence_id.to_string())
            .or_insert_with(Vec::new)
            .push(snapshot);

        *self.save_count.lock().unwrap() += 1;
        Ok(())
    }

    pub fn load_latest_snapshot(&self, persistence_id: &str) -> Option<TestSnapshot> {
        *self.load_count.lock().unwrap() += 1;
        let snapshots = self.snapshots.lock().unwrap();
        snapshots.get(persistence_id)
            .and_then(|snaps| snaps.last().cloned())
    }

    pub fn load_snapshot_at(&self, persistence_id: &str, seq_nr: SequenceNr) -> Option<TestSnapshot> {
        let snapshots = self.snapshots.lock().unwrap();
        snapshots.get(persistence_id)
            .and_then(|snaps| snaps.iter().find(|s| s.sequence_nr == seq_nr).cloned())
    }

    pub fn delete_snapshots(&self, persistence_id: &str, to_seq: SequenceNr) -> usize {
        let mut snapshots = self.snapshots.lock().unwrap();
        if let Some(snaps) = snapshots.get_mut(persistence_id) {
            let before = snaps.len();
            snaps.retain(|s| s.sequence_nr > to_seq);
            before - snaps.len()
        } else {
            0
        }
    }

    pub fn snapshot_count(&self, persistence_id: &str) -> usize {
        let snapshots = self.snapshots.lock().unwrap();
        snapshots.get(persistence_id).map(|s| s.len()).unwrap_or(0)
    }

    pub fn save_count(&self) -> u64 {
        *self.save_count.lock().unwrap()
    }

    pub fn load_count(&self) -> u64 {
        *self.load_count.lock().unwrap()
    }

    pub fn clear(&self) {
        self.snapshots.lock().unwrap().clear();
    }
}

impl Default for MockSnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Persistence Test Probe
// ============================================================================

pub struct PersistenceTestProbe {
    journal: MockJournal,
    snapshot_store: MockSnapshotStore,
    persistence_id: String,
    current_seq_nr: SequenceNr,
}

impl PersistenceTestProbe {
    pub fn new(persistence_id: &str) -> Self {
        Self {
            journal: MockJournal::new(),
            snapshot_store: MockSnapshotStore::new(),
            persistence_id: persistence_id.to_string(),
            current_seq_nr: 0,
        }
    }

    pub fn with_journal(mut self, journal: MockJournal) -> Self {
        self.journal = journal;
        self
    }

    pub fn with_snapshot_store(mut self, store: MockSnapshotStore) -> Self {
        self.snapshot_store = store;
        self
    }

    /// Persist an event and auto-increment sequence number
    pub fn persist(&mut self, payload: &str) -> Result<SequenceNr, MockJournalError> {
        self.persist_tagged(payload, vec![])
    }

    pub fn persist_tagged(&mut self, payload: &str, tags: Vec<String>) -> Result<SequenceNr, MockJournalError> {
        self.current_seq_nr += 1;
        self.journal.write_event(&self.persistence_id, self.current_seq_nr, payload, tags)?;
        Ok(self.current_seq_nr)
    }

    /// Save a snapshot at current sequence number
    pub fn save_snapshot(&self, payload: &str) -> Result<(), MockJournalError> {
        self.snapshot_store.save_snapshot(&self.persistence_id, self.current_seq_nr, payload)
    }

    /// Simulate recovery: load snapshot + replay events
    pub fn recover(&mut self) -> RecoveryResult {
        let snapshot = self.snapshot_store.load_latest_snapshot(&self.persistence_id);
        let start_seq = snapshot.as_ref().map(|s| s.sequence_nr + 1).unwrap_or(1);
        let events = self.journal.read_events_from(&self.persistence_id, start_seq);

        self.current_seq_nr = events.last()
            .map(|e| e.sequence_nr)
            .or(snapshot.as_ref().map(|s| s.sequence_nr))
            .unwrap_or(0);

        RecoveryResult {
            snapshot,
            events_replayed: events.len(),
            highest_seq_nr: self.current_seq_nr,
        }
    }

    pub fn events(&self) -> Vec<TestEvent> {
        self.journal.read_events(&self.persistence_id)
    }

    pub fn event_payloads(&self) -> Vec<String> {
        self.events().into_iter().map(|e| e.payload).collect()
    }

    pub fn latest_snapshot(&self) -> Option<TestSnapshot> {
        self.snapshot_store.load_latest_snapshot(&self.persistence_id)
    }

    pub fn current_seq_nr(&self) -> SequenceNr {
        self.current_seq_nr
    }

    pub fn journal(&self) -> &MockJournal {
        &self.journal
    }

    pub fn snapshot_store(&self) -> &MockSnapshotStore {
        &self.snapshot_store
    }

    pub fn persistence_id(&self) -> &str {
        &self.persistence_id
    }
}

#[derive(Debug)]
pub struct RecoveryResult {
    pub snapshot: Option<TestSnapshot>,
    pub events_replayed: usize,
    pub highest_seq_nr: SequenceNr,
}

// ============================================================================
// Assertion DSL
// ============================================================================

pub struct PersistenceAssertions<'a> {
    probe: &'a PersistenceTestProbe,
}

impl<'a> PersistenceAssertions<'a> {
    pub fn new(probe: &'a PersistenceTestProbe) -> Self {
        Self { probe }
    }

    pub fn has_event_count(self, expected: usize) -> Self {
        let actual = self.probe.journal.event_count(&self.probe.persistence_id);
        assert_eq!(actual, expected, "Expected {} events, got {}", expected, actual);
        self
    }

    pub fn has_snapshot_count(self, expected: usize) -> Self {
        let actual = self.probe.snapshot_store.snapshot_count(&self.probe.persistence_id);
        assert_eq!(actual, expected, "Expected {} snapshots, got {}", expected, actual);
        self
    }

    pub fn latest_event_is(self, payload: &str) -> Self {
        let events = self.probe.events();
        let last = events.last().expect("No events found");
        assert_eq!(last.payload, payload, "Expected latest event '{}', got '{}'", payload, last.payload);
        self
    }

    pub fn event_at(self, index: usize, payload: &str) -> Self {
        let events = self.probe.events();
        let event = events.get(index).unwrap_or_else(|| panic!("No event at index {}", index));
        assert_eq!(event.payload, payload, "Event at {} expected '{}', got '{}'", index, payload, event.payload);
        self
    }

    pub fn has_tagged_event(self, tag: &str) -> Self {
        let events = self.probe.journal.read_events_by_tag(tag);
        assert!(!events.is_empty(), "No events found with tag '{}'", tag);
        self
    }

    pub fn sequence_nr_is(self, expected: SequenceNr) -> Self {
        assert_eq!(self.probe.current_seq_nr, expected, "Expected seq_nr {}, got {}", expected, self.probe.current_seq_nr);
        self
    }

    pub fn latest_snapshot_payload_is(self, payload: &str) -> Self {
        let snap = self.probe.latest_snapshot().expect("No snapshot found");
        assert_eq!(snap.payload, payload, "Expected snapshot payload '{}', got '{}'", payload, snap.payload);
        self
    }

    pub fn has_no_events(self) -> Self {
        self.has_event_count(0)
    }

    pub fn has_no_snapshots(self) -> Self {
        self.has_snapshot_count(0)
    }

    pub fn events_contain(self, payload: &str) -> Self {
        let payloads = self.probe.event_payloads();
        assert!(payloads.iter().any(|p| p == payload), "Events do not contain '{}'", payload);
        self
    }
}

/// Convenience function: create assertions for a probe
pub fn assert_persistence(probe: &PersistenceTestProbe) -> PersistenceAssertions<'_> {
    PersistenceAssertions::new(probe)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_journal_write_read() {
        let journal = MockJournal::new();
        journal.write_event("actor-1", 1, "Created", vec![]).unwrap();
        journal.write_event("actor-1", 2, "Updated", vec![]).unwrap();

        let events = journal.read_events("actor-1");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].payload, "Created");
        assert_eq!(events[1].payload, "Updated");
    }

    #[test]
    fn test_mock_journal_fail_on_write() {
        let journal = MockJournal::new();
        journal.set_fail_on_write(true);

        let result = journal.write_event("actor-1", 1, "Event", vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_mock_journal_read_from_seq() {
        let journal = MockJournal::new();
        journal.write_event("a", 1, "E1", vec![]).unwrap();
        journal.write_event("a", 2, "E2", vec![]).unwrap();
        journal.write_event("a", 3, "E3", vec![]).unwrap();

        let events = journal.read_events_from("a", 2);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].payload, "E2");
    }

    #[test]
    fn test_mock_journal_read_by_tag() {
        let journal = MockJournal::new();
        journal.write_event("a", 1, "E1", vec!["order".into()]).unwrap();
        journal.write_event("a", 2, "E2", vec!["payment".into()]).unwrap();
        journal.write_event("b", 1, "E3", vec!["order".into()]).unwrap();

        let events = journal.read_events_by_tag("order");
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_mock_journal_delete_events() {
        let journal = MockJournal::new();
        journal.write_event("a", 1, "E1", vec![]).unwrap();
        journal.write_event("a", 2, "E2", vec![]).unwrap();
        journal.write_event("a", 3, "E3", vec![]).unwrap();

        let deleted = journal.delete_events("a", 2);
        assert_eq!(deleted, 2);
        assert_eq!(journal.event_count("a"), 1);
    }

    #[test]
    fn test_mock_journal_highest_seq_nr() {
        let journal = MockJournal::new();
        assert_eq!(journal.highest_sequence_nr("a"), 0);

        journal.write_event("a", 1, "E1", vec![]).unwrap();
        journal.write_event("a", 5, "E5", vec![]).unwrap();
        assert_eq!(journal.highest_sequence_nr("a"), 5);
    }

    #[test]
    fn test_mock_journal_counters() {
        let journal = MockJournal::new();
        journal.write_event("a", 1, "E1", vec![]).unwrap();
        journal.read_events("a");
        journal.read_events("a");

        assert_eq!(journal.write_count(), 1);
        assert_eq!(journal.read_count(), 2);
    }

    #[test]
    fn test_mock_snapshot_store() {
        let store = MockSnapshotStore::new();
        store.save_snapshot("a", 10, "state-10").unwrap();
        store.save_snapshot("a", 20, "state-20").unwrap();

        let latest = store.load_latest_snapshot("a").unwrap();
        assert_eq!(latest.sequence_nr, 20);
        assert_eq!(latest.payload, "state-20");
    }

    #[test]
    fn test_mock_snapshot_store_fail() {
        let store = MockSnapshotStore::new();
        store.set_fail_on_save(true);

        let result = store.save_snapshot("a", 10, "state");
        assert!(result.is_err());
    }

    #[test]
    fn test_mock_snapshot_load_at() {
        let store = MockSnapshotStore::new();
        store.save_snapshot("a", 10, "s10").unwrap();
        store.save_snapshot("a", 20, "s20").unwrap();

        let snap = store.load_snapshot_at("a", 10).unwrap();
        assert_eq!(snap.payload, "s10");
    }

    #[test]
    fn test_mock_snapshot_delete() {
        let store = MockSnapshotStore::new();
        store.save_snapshot("a", 10, "s10").unwrap();
        store.save_snapshot("a", 20, "s20").unwrap();
        store.save_snapshot("a", 30, "s30").unwrap();

        let deleted = store.delete_snapshots("a", 20);
        assert_eq!(deleted, 2);
        assert_eq!(store.snapshot_count("a"), 1);
    }

    #[test]
    fn test_persistence_test_probe_basic() {
        let mut probe = PersistenceTestProbe::new("test-actor");
        probe.persist("Created").unwrap();
        probe.persist("Updated").unwrap();

        assert_eq!(probe.current_seq_nr(), 2);
        assert_eq!(probe.event_payloads(), vec!["Created", "Updated"]);
    }

    #[test]
    fn test_persistence_test_probe_snapshot() {
        let mut probe = PersistenceTestProbe::new("test-actor");
        probe.persist("E1").unwrap();
        probe.persist("E2").unwrap();
        probe.save_snapshot("state-after-E2").unwrap();

        let snap = probe.latest_snapshot().unwrap();
        assert_eq!(snap.sequence_nr, 2);
        assert_eq!(snap.payload, "state-after-E2");
    }

    #[test]
    fn test_persistence_test_probe_recovery() {
        let journal = MockJournal::new();
        let store = MockSnapshotStore::new();

        // Setup: persist events and snapshot
        journal.write_event("actor", 1, "E1", vec![]).unwrap();
        journal.write_event("actor", 2, "E2", vec![]).unwrap();
        store.save_snapshot("actor", 2, "snapshot-at-2").unwrap();
        journal.write_event("actor", 3, "E3", vec![]).unwrap();

        // Recovery: load snapshot + replay events since snapshot
        let mut probe = PersistenceTestProbe::new("actor")
            .with_journal(journal)
            .with_snapshot_store(store);

        let result = probe.recover();
        assert!(result.snapshot.is_some());
        assert_eq!(result.events_replayed, 1); // only E3 replayed
        assert_eq!(result.highest_seq_nr, 3);
    }

    #[test]
    fn test_persistence_test_probe_tagged() {
        let mut probe = PersistenceTestProbe::new("test-actor");
        probe.persist_tagged("OrderCreated", vec!["order".into()]).unwrap();
        probe.persist_tagged("PaymentReceived", vec!["payment".into()]).unwrap();

        let order_events = probe.journal().read_events_by_tag("order");
        assert_eq!(order_events.len(), 1);
    }

    #[test]
    fn test_assertion_dsl() {
        let mut probe = PersistenceTestProbe::new("test-actor");
        probe.persist("Created").unwrap();
        probe.persist("Updated").unwrap();
        probe.save_snapshot("state").unwrap();

        assert_persistence(&probe)
            .has_event_count(2)
            .has_snapshot_count(1)
            .latest_event_is("Updated")
            .event_at(0, "Created")
            .sequence_nr_is(2)
            .latest_snapshot_payload_is("state")
            .events_contain("Created");
    }

    #[test]
    fn test_assertion_dsl_empty() {
        let probe = PersistenceTestProbe::new("empty-actor");
        assert_persistence(&probe)
            .has_no_events()
            .has_no_snapshots()
            .sequence_nr_is(0);
    }

    #[test]
    fn test_shared_journal_across_clones() {
        let journal = MockJournal::new();
        let j2 = journal.clone();

        journal.write_event("a", 1, "E1", vec![]).unwrap();
        let events = j2.read_events("a");
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_journal_clear() {
        let journal = MockJournal::new();
        journal.write_event("a", 1, "E1", vec![]).unwrap();
        journal.clear();

        assert_eq!(journal.total_event_count(), 0);
        assert_eq!(journal.write_count(), 0);
    }
}
