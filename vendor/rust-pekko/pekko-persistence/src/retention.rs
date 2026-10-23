//! Retention Policy — Automated cleanup of old events and snapshots
//!
//! Provides configurable retention strategies for managing journal and snapshot growth:
//!
//! - **SnapshotCountRetention** — Keep N most recent snapshots
//! - **EventSequenceRetention** — Delete events before snapshot sequence
//! - **TimeBasedRetention** — Delete events/snapshots older than duration
//! - **CompositeRetention** — Combine multiple strategies
//! - **RetentionManager** — Orchestrates cleanup operations

use std::fmt;
use std::time::Duration;
use chrono::{DateTime, Utc};

// ============= RetentionCriteria =============

/// Criteria for what to retain or delete.
#[derive(Debug, Clone)]
pub struct RetentionCriteria {
    /// Delete events with sequence_nr <= this value.
    pub delete_events_to: Option<u64>,
    /// Delete snapshots with sequence_nr <= this value.
    pub delete_snapshots_to: Option<u64>,
    /// Delete events older than this timestamp.
    pub delete_events_before: Option<DateTime<Utc>>,
    /// Delete snapshots older than this timestamp.
    pub delete_snapshots_before: Option<DateTime<Utc>>,
}

impl RetentionCriteria {
    pub fn none() -> Self {
        Self {
            delete_events_to: None,
            delete_snapshots_to: None,
            delete_events_before: None,
            delete_snapshots_before: None,
        }
    }

    /// Merge two criteria, taking the more aggressive (higher/later) threshold.
    pub fn merge(self, other: RetentionCriteria) -> Self {
        Self {
            delete_events_to: match (self.delete_events_to, other.delete_events_to) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            },
            delete_snapshots_to: match (self.delete_snapshots_to, other.delete_snapshots_to) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            },
            delete_events_before: match (self.delete_events_before, other.delete_events_before) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            },
            delete_snapshots_before: match (
                self.delete_snapshots_before,
                other.delete_snapshots_before,
            ) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            },
        }
    }

    /// Check if any cleanup is needed.
    pub fn has_work(&self) -> bool {
        self.delete_events_to.is_some()
            || self.delete_snapshots_to.is_some()
            || self.delete_events_before.is_some()
            || self.delete_snapshots_before.is_some()
    }
}

// ============= RetentionStrategy =============

/// A retention strategy that evaluates what to clean up.
pub trait RetentionStrategy: Send + Sync {
    /// Evaluate retention criteria given current state.
    fn evaluate(&self, state: &RetentionState) -> RetentionCriteria;

    /// Name of this strategy.
    fn name(&self) -> &str;
}

/// Current state information for retention evaluation.
#[derive(Debug, Clone)]
pub struct RetentionState {
    /// Persistence ID.
    pub persistence_id: String,
    /// Current (highest) sequence number.
    pub current_sequence_nr: u64,
    /// Latest snapshot sequence number, if any.
    pub latest_snapshot_seq: Option<u64>,
    /// All snapshot sequence numbers (newest first).
    pub snapshot_sequence_nrs: Vec<u64>,
    /// Timestamp of oldest event.
    pub oldest_event_time: Option<DateTime<Utc>>,
    /// Timestamp of newest event.
    pub newest_event_time: Option<DateTime<Utc>>,
    /// Total event count.
    pub event_count: u64,
    /// Total snapshot count.
    pub snapshot_count: u64,
}

// ============= SnapshotCountRetention =============

/// Keep only the N most recent snapshots, delete older ones.
pub struct SnapshotCountRetention {
    keep_n: usize,
}

impl SnapshotCountRetention {
    pub fn new(keep_n: usize) -> Self {
        Self { keep_n: keep_n.max(1) }
    }
}

impl RetentionStrategy for SnapshotCountRetention {
    fn evaluate(&self, state: &RetentionState) -> RetentionCriteria {
        if state.snapshot_sequence_nrs.len() <= self.keep_n {
            return RetentionCriteria::none();
        }

        // snapshot_sequence_nrs is newest first
        let delete_before_seq = state.snapshot_sequence_nrs[self.keep_n - 1];

        RetentionCriteria {
            delete_events_to: None,
            delete_snapshots_to: Some(delete_before_seq.saturating_sub(1)),
            delete_events_before: None,
            delete_snapshots_before: None,
        }
    }

    fn name(&self) -> &str {
        "SnapshotCountRetention"
    }
}

// ============= EventSequenceRetention =============

/// Delete events that have been "covered" by a snapshot.
/// Keeps events after the latest snapshot sequence number.
pub struct EventSequenceRetention {
    /// Keep N extra events before the snapshot as safety margin.
    safety_margin: u64,
}

impl EventSequenceRetention {
    pub fn new() -> Self {
        Self { safety_margin: 0 }
    }

    pub fn with_safety_margin(margin: u64) -> Self {
        Self { safety_margin: margin }
    }
}

impl RetentionStrategy for EventSequenceRetention {
    fn evaluate(&self, state: &RetentionState) -> RetentionCriteria {
        match state.latest_snapshot_seq {
            Some(snap_seq) => {
                let delete_to = snap_seq.saturating_sub(self.safety_margin);
                if delete_to == 0 {
                    return RetentionCriteria::none();
                }
                RetentionCriteria {
                    delete_events_to: Some(delete_to),
                    delete_snapshots_to: None,
                    delete_events_before: None,
                    delete_snapshots_before: None,
                }
            }
            None => RetentionCriteria::none(),
        }
    }

    fn name(&self) -> &str {
        "EventSequenceRetention"
    }
}

// ============= TimeBasedRetention =============

/// Delete events and snapshots older than a specified duration.
pub struct TimeBasedRetention {
    max_age: Duration,
    delete_events: bool,
    delete_snapshots: bool,
}

impl TimeBasedRetention {
    pub fn new(max_age: Duration) -> Self {
        Self {
            max_age,
            delete_events: true,
            delete_snapshots: true,
        }
    }

    pub fn events_only(max_age: Duration) -> Self {
        Self {
            max_age,
            delete_events: true,
            delete_snapshots: false,
        }
    }

    pub fn snapshots_only(max_age: Duration) -> Self {
        Self {
            max_age,
            delete_events: false,
            delete_snapshots: true,
        }
    }

    fn cutoff_time(&self) -> DateTime<Utc> {
        Utc::now() - chrono::Duration::from_std(self.max_age).unwrap_or(chrono::Duration::zero())
    }
}

impl RetentionStrategy for TimeBasedRetention {
    fn evaluate(&self, _state: &RetentionState) -> RetentionCriteria {
        let cutoff = self.cutoff_time();
        RetentionCriteria {
            delete_events_to: None,
            delete_snapshots_to: None,
            delete_events_before: if self.delete_events { Some(cutoff) } else { None },
            delete_snapshots_before: if self.delete_snapshots { Some(cutoff) } else { None },
        }
    }

    fn name(&self) -> &str {
        "TimeBasedRetention"
    }
}

// ============= EventCountRetention =============

/// Keep only the N most recent events. Delete older ones.
pub struct EventCountRetention {
    max_events: u64,
}

impl EventCountRetention {
    pub fn new(max_events: u64) -> Self {
        Self { max_events }
    }
}

impl RetentionStrategy for EventCountRetention {
    fn evaluate(&self, state: &RetentionState) -> RetentionCriteria {
        if state.event_count <= self.max_events {
            return RetentionCriteria::none();
        }
        
        // Delete the oldest `excess` events by sequence number
        let delete_to = state.current_sequence_nr.saturating_sub(self.max_events);

        RetentionCriteria {
            delete_events_to: Some(delete_to),
            delete_snapshots_to: None,
            delete_events_before: None,
            delete_snapshots_before: None,
        }
    }

    fn name(&self) -> &str {
        "EventCountRetention"
    }
}

// ============= CompositeRetention =============

/// Combines multiple retention strategies. Uses the most aggressive criteria.
pub struct CompositeRetention {
    strategies: Vec<Box<dyn RetentionStrategy>>,
}

impl CompositeRetention {
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
        }
    }

    pub fn add<S: RetentionStrategy + 'static>(mut self, strategy: S) -> Self {
        self.strategies.push(Box::new(strategy));
        self
    }

    pub fn strategy_count(&self) -> usize {
        self.strategies.len()
    }
}

impl RetentionStrategy for CompositeRetention {
    fn evaluate(&self, state: &RetentionState) -> RetentionCriteria {
        let mut result = RetentionCriteria::none();
        for strategy in &self.strategies {
            let criteria = strategy.evaluate(state);
            result = result.merge(criteria);
        }
        result
    }

    fn name(&self) -> &str {
        "CompositeRetention"
    }
}

// ============= RetentionResult =============

/// Result of a retention cleanup operation.
#[derive(Debug, Clone)]
pub struct RetentionResult {
    pub persistence_id: String,
    pub events_deleted: u64,
    pub snapshots_deleted: u64,
    pub criteria_applied: String,
    pub duration: Duration,
}

impl fmt::Display for RetentionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] Retention: {} events deleted, {} snapshots deleted ({:?})",
            self.persistence_id, self.events_deleted, self.snapshots_deleted, self.duration
        )
    }
}

// ============= RetentionStats =============

/// Accumulated retention statistics.
#[derive(Debug, Clone)]
pub struct RetentionStats {
    pub total_runs: u64,
    pub total_events_deleted: u64,
    pub total_snapshots_deleted: u64,
    pub last_run_result: Option<RetentionResult>,
}

impl RetentionStats {
    pub fn new() -> Self {
        Self {
            total_runs: 0,
            total_events_deleted: 0,
            total_snapshots_deleted: 0,
            last_run_result: None,
        }
    }

    pub fn record(&mut self, result: &RetentionResult) {
        self.total_runs += 1;
        self.total_events_deleted += result.events_deleted;
        self.total_snapshots_deleted += result.snapshots_deleted;
        self.last_run_result = Some(result.clone());
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state(seq: u64, snap_seq: Option<u64>, snap_seqs: Vec<u64>, events: u64) -> RetentionState {
        RetentionState {
            persistence_id: "test-actor".to_string(),
            current_sequence_nr: seq,
            latest_snapshot_seq: snap_seq,
            snapshot_sequence_nrs: snap_seqs,
            oldest_event_time: Some(Utc::now() - chrono::Duration::hours(48)),
            newest_event_time: Some(Utc::now()),
            event_count: events,
            snapshot_count: 0,
        }
    }

    // ---- RetentionCriteria ----

    #[test]
    fn test_criteria_none() {
        let c = RetentionCriteria::none();
        assert!(!c.has_work());
    }

    #[test]
    fn test_criteria_merge() {
        let a = RetentionCriteria {
            delete_events_to: Some(10),
            delete_snapshots_to: None,
            delete_events_before: None,
            delete_snapshots_before: None,
        };
        let b = RetentionCriteria {
            delete_events_to: Some(20),
            delete_snapshots_to: Some(5),
            delete_events_before: None,
            delete_snapshots_before: None,
        };
        let merged = a.merge(b);
        assert_eq!(merged.delete_events_to, Some(20)); // max
        assert_eq!(merged.delete_snapshots_to, Some(5));
        assert!(merged.has_work());
    }

    // ---- SnapshotCountRetention ----

    #[test]
    fn test_snapshot_count_no_cleanup_needed() {
        let strategy = SnapshotCountRetention::new(3);
        let state = make_state(100, Some(100), vec![100, 80], 100);
        let criteria = strategy.evaluate(&state);
        assert!(!criteria.has_work()); // only 2 snapshots, keep 3
    }

    #[test]
    fn test_snapshot_count_cleanup() {
        let strategy = SnapshotCountRetention::new(2);
        let state = make_state(100, Some(100), vec![100, 80, 60, 40], 100);
        let criteria = strategy.evaluate(&state);
        assert!(criteria.has_work());
        // Keep snapshots at 100 and 80, delete below 80
        assert_eq!(criteria.delete_snapshots_to, Some(79));
    }

    // ---- EventSequenceRetention ----

    #[test]
    fn test_event_seq_no_snapshot() {
        let strategy = EventSequenceRetention::new();
        let state = make_state(100, None, vec![], 100);
        let criteria = strategy.evaluate(&state);
        assert!(!criteria.has_work()); // no snapshot = nothing to delete
    }

    #[test]
    fn test_event_seq_with_snapshot() {
        let strategy = EventSequenceRetention::new();
        let state = make_state(100, Some(80), vec![80], 100);
        let criteria = strategy.evaluate(&state);
        assert_eq!(criteria.delete_events_to, Some(80)); // delete events <= 80
    }

    #[test]
    fn test_event_seq_with_safety_margin() {
        let strategy = EventSequenceRetention::with_safety_margin(10);
        let state = make_state(100, Some(80), vec![80], 100);
        let criteria = strategy.evaluate(&state);
        assert_eq!(criteria.delete_events_to, Some(70)); // 80 - 10
    }

    // ---- TimeBasedRetention ----

    #[test]
    fn test_time_based_both() {
        let strategy = TimeBasedRetention::new(Duration::from_secs(3600));
        let state = make_state(100, Some(80), vec![80], 100);
        let criteria = strategy.evaluate(&state);
        assert!(criteria.delete_events_before.is_some());
        assert!(criteria.delete_snapshots_before.is_some());
    }

    #[test]
    fn test_time_based_events_only() {
        let strategy = TimeBasedRetention::events_only(Duration::from_secs(3600));
        let state = make_state(100, Some(80), vec![80], 100);
        let criteria = strategy.evaluate(&state);
        assert!(criteria.delete_events_before.is_some());
        assert!(criteria.delete_snapshots_before.is_none());
    }

    #[test]
    fn test_time_based_snapshots_only() {
        let strategy = TimeBasedRetention::snapshots_only(Duration::from_secs(3600));
        let state = make_state(100, Some(80), vec![80], 100);
        let criteria = strategy.evaluate(&state);
        assert!(criteria.delete_events_before.is_none());
        assert!(criteria.delete_snapshots_before.is_some());
    }

    // ---- EventCountRetention ----

    #[test]
    fn test_event_count_under_limit() {
        let strategy = EventCountRetention::new(1000);
        let state = make_state(100, None, vec![], 100);
        let criteria = strategy.evaluate(&state);
        assert!(!criteria.has_work());
    }

    #[test]
    fn test_event_count_over_limit() {
        let strategy = EventCountRetention::new(50);
        let state = make_state(100, None, vec![], 100);
        let criteria = strategy.evaluate(&state);
        assert_eq!(criteria.delete_events_to, Some(50)); // keep last 50
    }

    // ---- CompositeRetention ----

    #[test]
    fn test_composite_empty() {
        let strategy = CompositeRetention::new();
        let state = make_state(100, None, vec![], 100);
        let criteria = strategy.evaluate(&state);
        assert!(!criteria.has_work());
    }

    #[test]
    fn test_composite_merges() {
        let strategy = CompositeRetention::new()
            .add(EventSequenceRetention::new())
            .add(EventCountRetention::new(30));

        let state = make_state(100, Some(60), vec![60], 100);
        let criteria = strategy.evaluate(&state);

        // EventSequence: delete_events_to=60, EventCount: delete_events_to=70
        // Merged: max(60, 70) = 70
        assert_eq!(criteria.delete_events_to, Some(70));
    }

    #[test]
    fn test_composite_strategy_count() {
        let strategy = CompositeRetention::new()
            .add(EventSequenceRetention::new())
            .add(SnapshotCountRetention::new(3));
        assert_eq!(strategy.strategy_count(), 2);
    }

    // ---- RetentionResult ----

    #[test]
    fn test_retention_result_display() {
        let result = RetentionResult {
            persistence_id: "actor-1".to_string(),
            events_deleted: 50,
            snapshots_deleted: 3,
            criteria_applied: "CompositeRetention".to_string(),
            duration: Duration::from_millis(15),
        };
        let s = result.to_string();
        assert!(s.contains("actor-1"));
        assert!(s.contains("50 events"));
        assert!(s.contains("3 snapshots"));
    }

    // ---- RetentionStats ----

    #[test]
    fn test_retention_stats() {
        let mut stats = RetentionStats::new();
        assert_eq!(stats.total_runs, 0);

        stats.record(&RetentionResult {
            persistence_id: "a".into(),
            events_deleted: 10,
            snapshots_deleted: 2,
            criteria_applied: "test".into(),
            duration: Duration::from_millis(5),
        });
        stats.record(&RetentionResult {
            persistence_id: "a".into(),
            events_deleted: 5,
            snapshots_deleted: 1,
            criteria_applied: "test".into(),
            duration: Duration::from_millis(3),
        });

        assert_eq!(stats.total_runs, 2);
        assert_eq!(stats.total_events_deleted, 15);
        assert_eq!(stats.total_snapshots_deleted, 3);
        assert!(stats.last_run_result.is_some());
    }

    // ---- Strategy Names ----

    #[test]
    fn test_strategy_names() {
        assert_eq!(SnapshotCountRetention::new(3).name(), "SnapshotCountRetention");
        assert_eq!(EventSequenceRetention::new().name(), "EventSequenceRetention");
        assert_eq!(TimeBasedRetention::new(Duration::from_secs(1)).name(), "TimeBasedRetention");
        assert_eq!(EventCountRetention::new(100).name(), "EventCountRetention");
        assert_eq!(CompositeRetention::new().name(), "CompositeRetention");
    }
}
