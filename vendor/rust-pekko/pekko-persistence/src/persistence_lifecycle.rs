// persistence_lifecycle.rs - Persistence Lifecycle Hooks
// Pre/Post persist callbacks, snapshot interceptors, recovery hooks

use std::collections::HashMap;
use std::time::{Duration};

// ============================================================================
// Core Traits & Types
// ============================================================================

pub type PersistenceId = String;
pub type SequenceNr = u64;

/// Hook that runs before an event is persisted
pub trait PrePersistHook: Send + Sync {
    fn name(&self) -> &str;
    fn on_pre_persist(&self, persistence_id: &str, seq_nr: SequenceNr, event_type: &str) -> HookResult;
}

/// Hook that runs after an event is persisted
pub trait PostPersistHook: Send + Sync {
    fn name(&self) -> &str;
    fn on_post_persist(&self, persistence_id: &str, seq_nr: SequenceNr, event_type: &str, duration: Duration);
}

/// Hook that runs before/after snapshot operations
pub trait SnapshotHook: Send + Sync {
    fn name(&self) -> &str;
    fn on_pre_snapshot(&self, persistence_id: &str, seq_nr: SequenceNr) -> HookResult;
    fn on_post_snapshot(&self, persistence_id: &str, seq_nr: SequenceNr, success: bool);
}

/// Hook result determines whether to proceed or abort
#[derive(Debug, Clone, PartialEq)]
pub enum HookResult {
    Continue,
    Abort { reason: String },
    Transform { metadata: HashMap<String, String> },
}

impl HookResult {
    pub fn is_continue(&self) -> bool {
        matches!(self, HookResult::Continue | HookResult::Transform { .. })
    }

    pub fn is_abort(&self) -> bool {
        matches!(self, HookResult::Abort { .. })
    }
}

// ============================================================================
// Lifecycle Events
// ============================================================================

#[derive(Debug, Clone)]
pub enum LifecycleEvent {
    RecoveryStarted { persistence_id: String },
    RecoveryCompleted { persistence_id: String, highest_seq_nr: SequenceNr, duration: Duration },
    RecoveryFailed { persistence_id: String, error: String },
    EventPersisted { persistence_id: String, seq_nr: SequenceNr, event_type: String },
    EventPersistFailed { persistence_id: String, seq_nr: SequenceNr, error: String },
    SnapshotSaved { persistence_id: String, seq_nr: SequenceNr },
    SnapshotSaveFailed { persistence_id: String, seq_nr: SequenceNr, error: String },
    SnapshotLoaded { persistence_id: String, seq_nr: SequenceNr },
    SnapshotDeleted { persistence_id: String, seq_nr: SequenceNr },
}

impl LifecycleEvent {
    pub fn persistence_id(&self) -> &str {
        match self {
            LifecycleEvent::RecoveryStarted { persistence_id } => persistence_id,
            LifecycleEvent::RecoveryCompleted { persistence_id, .. } => persistence_id,
            LifecycleEvent::RecoveryFailed { persistence_id, .. } => persistence_id,
            LifecycleEvent::EventPersisted { persistence_id, .. } => persistence_id,
            LifecycleEvent::EventPersistFailed { persistence_id, .. } => persistence_id,
            LifecycleEvent::SnapshotSaved { persistence_id, .. } => persistence_id,
            LifecycleEvent::SnapshotSaveFailed { persistence_id, .. } => persistence_id,
            LifecycleEvent::SnapshotLoaded { persistence_id, .. } => persistence_id,
            LifecycleEvent::SnapshotDeleted { persistence_id, .. } => persistence_id,
        }
    }

    pub fn is_error(&self) -> bool {
        matches!(self,
            LifecycleEvent::RecoveryFailed { .. } |
            LifecycleEvent::EventPersistFailed { .. } |
            LifecycleEvent::SnapshotSaveFailed { .. }
        )
    }
}

// ============================================================================
// Lifecycle Manager
// ============================================================================

pub struct PersistenceLifecycleManager {
    pre_persist_hooks: Vec<Box<dyn PrePersistHook>>,
    post_persist_hooks: Vec<Box<dyn PostPersistHook>>,
    snapshot_hooks: Vec<Box<dyn SnapshotHook>>,
    event_log: Vec<LifecycleEvent>,
    max_log_size: usize,
    stats: LifecycleStats,
}

#[derive(Debug, Clone, Default)]
pub struct LifecycleStats {
    pub events_persisted: u64,
    pub events_failed: u64,
    pub snapshots_saved: u64,
    pub snapshots_failed: u64,
    pub hooks_aborted: u64,
    pub recoveries_completed: u64,
    pub recoveries_failed: u64,
}

impl PersistenceLifecycleManager {
    pub fn new() -> Self {
        Self {
            pre_persist_hooks: Vec::new(),
            post_persist_hooks: Vec::new(),
            snapshot_hooks: Vec::new(),
            event_log: Vec::new(),
            max_log_size: 1000,
            stats: LifecycleStats::default(),
        }
    }

    pub fn with_max_log_size(mut self, size: usize) -> Self {
        self.max_log_size = size;
        self
    }

    pub fn add_pre_persist_hook(&mut self, hook: Box<dyn PrePersistHook>) {
        self.pre_persist_hooks.push(hook);
    }

    pub fn add_post_persist_hook(&mut self, hook: Box<dyn PostPersistHook>) {
        self.post_persist_hooks.push(hook);
    }

    pub fn add_snapshot_hook(&mut self, hook: Box<dyn SnapshotHook>) {
        self.snapshot_hooks.push(hook);
    }

    /// Run all pre-persist hooks. Returns Abort if any hook aborts.
    pub fn run_pre_persist(&mut self, persistence_id: &str, seq_nr: SequenceNr, event_type: &str) -> HookResult {
        let mut metadata = HashMap::new();

        for hook in &self.pre_persist_hooks {
            match hook.on_pre_persist(persistence_id, seq_nr, event_type) {
                HookResult::Continue => {},
                HookResult::Transform { metadata: m } => {
                    metadata.extend(m);
                },
                HookResult::Abort { reason } => {
                    self.stats.hooks_aborted += 1;
                    return HookResult::Abort { reason };
                }
            }
        }

        if metadata.is_empty() {
            HookResult::Continue
        } else {
            HookResult::Transform { metadata }
        }
    }

    /// Run all post-persist hooks
    pub fn run_post_persist(&mut self, persistence_id: &str, seq_nr: SequenceNr, event_type: &str, duration: Duration) {
        self.stats.events_persisted += 1;
        self.log_event(LifecycleEvent::EventPersisted {
            persistence_id: persistence_id.to_string(),
            seq_nr,
            event_type: event_type.to_string(),
        });

        for hook in &self.post_persist_hooks {
            hook.on_post_persist(persistence_id, seq_nr, event_type, duration);
        }
    }

    /// Run pre-snapshot hooks
    pub fn run_pre_snapshot(&mut self, persistence_id: &str, seq_nr: SequenceNr) -> HookResult {
        for hook in &self.snapshot_hooks {
            match hook.on_pre_snapshot(persistence_id, seq_nr) {
                HookResult::Continue => {},
                result @ HookResult::Abort { .. } => {
                    self.stats.hooks_aborted += 1;
                    return result;
                },
                result @ HookResult::Transform { .. } => return result,
            }
        }
        HookResult::Continue
    }

    /// Run post-snapshot hooks
    pub fn run_post_snapshot(&mut self, persistence_id: &str, seq_nr: SequenceNr, success: bool) {
        if success {
            self.stats.snapshots_saved += 1;
            self.log_event(LifecycleEvent::SnapshotSaved {
                persistence_id: persistence_id.to_string(),
                seq_nr,
            });
        } else {
            self.stats.snapshots_failed += 1;
            self.log_event(LifecycleEvent::SnapshotSaveFailed {
                persistence_id: persistence_id.to_string(),
                seq_nr,
                error: "Snapshot save failed".to_string(),
            });
        }

        for hook in &self.snapshot_hooks {
            hook.on_post_snapshot(persistence_id, seq_nr, success);
        }
    }

    /// Record recovery events
    pub fn record_recovery_started(&mut self, persistence_id: &str) {
        self.log_event(LifecycleEvent::RecoveryStarted {
            persistence_id: persistence_id.to_string(),
        });
    }

    pub fn record_recovery_completed(&mut self, persistence_id: &str, highest_seq_nr: SequenceNr, duration: Duration) {
        self.stats.recoveries_completed += 1;
        self.log_event(LifecycleEvent::RecoveryCompleted {
            persistence_id: persistence_id.to_string(),
            highest_seq_nr,
            duration,
        });
    }

    pub fn record_recovery_failed(&mut self, persistence_id: &str, error: &str) {
        self.stats.recoveries_failed += 1;
        self.log_event(LifecycleEvent::RecoveryFailed {
            persistence_id: persistence_id.to_string(),
            error: error.to_string(),
        });
    }

    pub fn record_persist_failed(&mut self, persistence_id: &str, seq_nr: SequenceNr, error: &str) {
        self.stats.events_failed += 1;
        self.log_event(LifecycleEvent::EventPersistFailed {
            persistence_id: persistence_id.to_string(),
            seq_nr,
            error: error.to_string(),
        });
    }

    fn log_event(&mut self, event: LifecycleEvent) {
        if self.event_log.len() >= self.max_log_size {
            self.event_log.remove(0);
        }
        self.event_log.push(event);
    }

    pub fn event_log(&self) -> &[LifecycleEvent] {
        &self.event_log
    }

    pub fn stats(&self) -> &LifecycleStats {
        &self.stats
    }

    pub fn hook_count(&self) -> usize {
        self.pre_persist_hooks.len() + self.post_persist_hooks.len() + self.snapshot_hooks.len()
    }

    pub fn errors(&self) -> Vec<&LifecycleEvent> {
        self.event_log.iter().filter(|e| e.is_error()).collect()
    }

    pub fn events_for(&self, persistence_id: &str) -> Vec<&LifecycleEvent> {
        self.event_log.iter()
            .filter(|e| e.persistence_id() == persistence_id)
            .collect()
    }
}

impl Default for PersistenceLifecycleManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Built-in Hook Implementations
// ============================================================================

/// Hook that validates event types against an allow list
pub struct EventTypeValidator {
    allowed_types: Vec<String>,
}

impl EventTypeValidator {
    pub fn new(allowed_types: Vec<String>) -> Self {
        Self { allowed_types }
    }
}

impl PrePersistHook for EventTypeValidator {
    fn name(&self) -> &str { "EventTypeValidator" }

    fn on_pre_persist(&self, _persistence_id: &str, _seq_nr: SequenceNr, event_type: &str) -> HookResult {
        if self.allowed_types.iter().any(|t| t == event_type) {
            HookResult::Continue
        } else {
            HookResult::Abort {
                reason: format!("Event type '{}' not in allowed list", event_type),
            }
        }
    }
}

/// Hook that adds metadata tags to events
pub struct MetadataTagHook {
    tags: HashMap<String, String>,
}

impl MetadataTagHook {
    pub fn new(tags: HashMap<String, String>) -> Self {
        Self { tags }
    }
}

impl PrePersistHook for MetadataTagHook {
    fn name(&self) -> &str { "MetadataTagHook" }

    fn on_pre_persist(&self, _persistence_id: &str, _seq_nr: SequenceNr, _event_type: &str) -> HookResult {
        HookResult::Transform { metadata: self.tags.clone() }
    }
}

/// Hook that logs persist durations
pub struct PersistTimingHook {
    slow_threshold: Duration,
    slow_count: std::sync::atomic::AtomicU64,
}

impl PersistTimingHook {
    pub fn new(slow_threshold: Duration) -> Self {
        Self {
            slow_threshold,
            slow_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn slow_count(&self) -> u64 {
        self.slow_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl PostPersistHook for PersistTimingHook {
    fn name(&self) -> &str { "PersistTimingHook" }

    fn on_post_persist(&self, _persistence_id: &str, _seq_nr: SequenceNr, _event_type: &str, duration: Duration) {
        if duration >= self.slow_threshold {
            self.slow_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// Snapshot frequency limiter - prevents too frequent snapshots
pub struct SnapshotRateLimiter {
    min_interval_seq: u64,
    last_snapshot_seq: std::sync::atomic::AtomicU64,
}

impl SnapshotRateLimiter {
    pub fn new(min_interval_seq: u64) -> Self {
        Self {
            min_interval_seq,
            last_snapshot_seq: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl SnapshotHook for SnapshotRateLimiter {
    fn name(&self) -> &str { "SnapshotRateLimiter" }

    fn on_pre_snapshot(&self, _persistence_id: &str, seq_nr: SequenceNr) -> HookResult {
        let last = self.last_snapshot_seq.load(std::sync::atomic::Ordering::Relaxed);
        if last > 0 && seq_nr - last < self.min_interval_seq {
            HookResult::Abort {
                reason: format!("Snapshot too soon: {} events since last (min {})", seq_nr - last, self.min_interval_seq),
            }
        } else {
            HookResult::Continue
        }
    }

    fn on_post_snapshot(&self, _persistence_id: &str, seq_nr: SequenceNr, success: bool) {
        if success {
            self.last_snapshot_seq.store(seq_nr, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    struct AllowHook;
    impl PrePersistHook for AllowHook {
        fn name(&self) -> &str { "AllowHook" }
        fn on_pre_persist(&self, _: &str, _: SequenceNr, _: &str) -> HookResult { HookResult::Continue }
    }

    struct AbortHook { reason: String }
    impl PrePersistHook for AbortHook {
        fn name(&self) -> &str { "AbortHook" }
        fn on_pre_persist(&self, _: &str, _: SequenceNr, _: &str) -> HookResult {
            HookResult::Abort { reason: self.reason.clone() }
        }
    }

    struct CountingPostHook { count: std::sync::atomic::AtomicU64 }
    impl CountingPostHook {
        fn new() -> Self { Self { count: std::sync::atomic::AtomicU64::new(0) } }
        fn count(&self) -> u64 { self.count.load(std::sync::atomic::Ordering::Relaxed) }
    }
    impl PostPersistHook for CountingPostHook {
        fn name(&self) -> &str { "CountingPostHook" }
        fn on_post_persist(&self, _: &str, _: SequenceNr, _: &str, _: Duration) {
            self.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[test]
    fn test_lifecycle_manager_creation() {
        let mgr = PersistenceLifecycleManager::new();
        assert_eq!(mgr.hook_count(), 0);
        assert_eq!(mgr.event_log().len(), 0);
    }

    #[test]
    fn test_pre_persist_continue() {
        let mut mgr = PersistenceLifecycleManager::new();
        mgr.add_pre_persist_hook(Box::new(AllowHook));

        let result = mgr.run_pre_persist("actor-1", 1, "Created");
        assert!(result.is_continue());
    }

    #[test]
    fn test_pre_persist_abort() {
        let mut mgr = PersistenceLifecycleManager::new();
        mgr.add_pre_persist_hook(Box::new(AbortHook { reason: "blocked".into() }));

        let result = mgr.run_pre_persist("actor-1", 1, "Created");
        assert!(result.is_abort());
        assert_eq!(mgr.stats().hooks_aborted, 1);
    }

    #[test]
    fn test_post_persist_tracking() {
        let mut mgr = PersistenceLifecycleManager::new();
        mgr.run_post_persist("actor-1", 1, "Created", Duration::from_millis(5));
        mgr.run_post_persist("actor-1", 2, "Updated", Duration::from_millis(3));

        assert_eq!(mgr.stats().events_persisted, 2);
        assert_eq!(mgr.event_log().len(), 2);
    }

    #[test]
    fn test_snapshot_hooks_success() {
        let mut mgr = PersistenceLifecycleManager::new();
        let result = mgr.run_pre_snapshot("actor-1", 10);
        assert!(result.is_continue());

        mgr.run_post_snapshot("actor-1", 10, true);
        assert_eq!(mgr.stats().snapshots_saved, 1);
    }

    #[test]
    fn test_snapshot_hooks_failure() {
        let mut mgr = PersistenceLifecycleManager::new();
        mgr.run_post_snapshot("actor-1", 10, false);
        assert_eq!(mgr.stats().snapshots_failed, 1);
    }

    #[test]
    fn test_recovery_tracking() {
        let mut mgr = PersistenceLifecycleManager::new();
        mgr.record_recovery_started("actor-1");
        mgr.record_recovery_completed("actor-1", 100, Duration::from_millis(50));

        assert_eq!(mgr.stats().recoveries_completed, 1);
        assert_eq!(mgr.event_log().len(), 2);
    }

    #[test]
    fn test_recovery_failed_tracking() {
        let mut mgr = PersistenceLifecycleManager::new();
        mgr.record_recovery_started("actor-1");
        mgr.record_recovery_failed("actor-1", "corrupted journal");

        assert_eq!(mgr.stats().recoveries_failed, 1);
    }

    #[test]
    fn test_persist_failed_tracking() {
        let mut mgr = PersistenceLifecycleManager::new();
        mgr.record_persist_failed("actor-1", 5, "write error");

        assert_eq!(mgr.stats().events_failed, 1);
        assert_eq!(mgr.errors().len(), 1);
    }

    #[test]
    fn test_event_log_max_size() {
        let mut mgr = PersistenceLifecycleManager::new().with_max_log_size(3);

        mgr.run_post_persist("a", 1, "E1", Duration::from_millis(1));
        mgr.run_post_persist("a", 2, "E2", Duration::from_millis(1));
        mgr.run_post_persist("a", 3, "E3", Duration::from_millis(1));
        mgr.run_post_persist("a", 4, "E4", Duration::from_millis(1));

        assert_eq!(mgr.event_log().len(), 3);
    }

    #[test]
    fn test_events_for_persistence_id() {
        let mut mgr = PersistenceLifecycleManager::new();
        mgr.run_post_persist("actor-1", 1, "E1", Duration::from_millis(1));
        mgr.run_post_persist("actor-2", 1, "E1", Duration::from_millis(1));
        mgr.run_post_persist("actor-1", 2, "E2", Duration::from_millis(1));

        let events = mgr.events_for("actor-1");
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_event_type_validator() {
        let validator = EventTypeValidator::new(vec!["Created".into(), "Updated".into()]);

        let result = validator.on_pre_persist("a", 1, "Created");
        assert!(result.is_continue());

        let result = validator.on_pre_persist("a", 2, "Deleted");
        assert!(result.is_abort());
    }

    #[test]
    fn test_metadata_tag_hook() {
        let mut tags = HashMap::new();
        tags.insert("source".into(), "api".into());
        let hook = MetadataTagHook::new(tags);

        let result = hook.on_pre_persist("a", 1, "Created");
        match result {
            HookResult::Transform { metadata } => {
                assert_eq!(metadata.get("source"), Some(&"api".to_string()));
            },
            _ => panic!("Expected Transform"),
        }
    }

    #[test]
    fn test_persist_timing_hook() {
        let hook = PersistTimingHook::new(Duration::from_millis(10));
        hook.on_post_persist("a", 1, "E", Duration::from_millis(5));
        assert_eq!(hook.slow_count(), 0);

        hook.on_post_persist("a", 2, "E", Duration::from_millis(15));
        assert_eq!(hook.slow_count(), 1);
    }

    #[test]
    fn test_snapshot_rate_limiter() {
        let limiter = SnapshotRateLimiter::new(10);

        // First snapshot always allowed
        let result = limiter.on_pre_snapshot("a", 10);
        assert!(result.is_continue());
        limiter.on_post_snapshot("a", 10, true);

        // Too soon
        let result = limiter.on_pre_snapshot("a", 15);
        assert!(result.is_abort());

        // After enough events
        let result = limiter.on_pre_snapshot("a", 25);
        assert!(result.is_continue());
    }

    #[test]
    fn test_lifecycle_event_types() {
        let event = LifecycleEvent::RecoveryStarted { persistence_id: "test".into() };
        assert_eq!(event.persistence_id(), "test");
        assert!(!event.is_error());

        let event = LifecycleEvent::RecoveryFailed { persistence_id: "test".into(), error: "err".into() };
        assert!(event.is_error());
    }

    #[test]
    fn test_hook_result_variants() {
        assert!(HookResult::Continue.is_continue());
        assert!(!HookResult::Continue.is_abort());

        let abort = HookResult::Abort { reason: "test".into() };
        assert!(abort.is_abort());
        assert!(!abort.is_continue());

        let transform = HookResult::Transform { metadata: HashMap::new() };
        assert!(transform.is_continue()); // Transform is still "continue"
        assert!(!transform.is_abort());
    }

    #[test]
    fn test_multiple_pre_hooks_chain() {
        let mut mgr = PersistenceLifecycleManager::new();

        let mut tags = HashMap::new();
        tags.insert("env".into(), "prod".into());
        mgr.add_pre_persist_hook(Box::new(MetadataTagHook::new(tags)));
        mgr.add_pre_persist_hook(Box::new(AllowHook));

        let result = mgr.run_pre_persist("a", 1, "Created");
        match result {
            HookResult::Transform { metadata } => {
                assert!(metadata.contains_key("env"));
            },
            _ => panic!("Expected Transform with merged metadata"),
        }
    }
}
