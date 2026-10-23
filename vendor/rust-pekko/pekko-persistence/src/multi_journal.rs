// multi_journal.rs - Multi-journal support with routing, replication, and failover
// Extends the single-journal model with multi-backend coordination

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// Journal Routing
// ============================================================================

/// Strategy for routing events to journals
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalRoutingStrategy {
    /// All events go to a single primary journal
    Single(String),
    /// Route by persistence ID prefix (e.g., "order-*" → journal-a)
    PrefixBased(Vec<(String, String)>),
    /// Route by tag (events with tag X → journal-a)
    TagBased(HashMap<String, String>),
    /// Round-robin across journals
    RoundRobin(Vec<String>),
    /// Hash-based partitioning by persistence ID
    HashPartitioned(Vec<String>),
    /// Custom routing via type name
    Custom(String),
}

/// Configuration for a single journal backend in a multi-journal setup
#[derive(Debug, Clone)]
pub struct JournalBackend {
    /// Unique identifier for this backend
    pub id: String,
    /// Backend type (e.g., "inmemory", "cassandra", "postgres")
    pub backend_type: String,
    /// Priority (lower = higher priority for failover)
    pub priority: u32,
    /// Whether this backend is currently available
    pub available: bool,
    /// Whether writes should be replicated to this backend
    pub replica: bool,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Retry delay
    pub retry_delay: Duration,
}

impl JournalBackend {
    pub fn new(id: &str, backend_type: &str) -> Self {
        Self {
            id: id.to_string(),
            backend_type: backend_type.to_string(),
            priority: 0,
            available: true,
            replica: false,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
        }
    }

    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    pub fn as_replica(mut self) -> Self {
        self.replica = true;
        self
    }

    pub fn with_retries(mut self, max: u32, delay: Duration) -> Self {
        self.max_retries = max;
        self.retry_delay = delay;
        self
    }
}

// ============================================================================
// MultiJournal Router
// ============================================================================

/// Routes events to appropriate journal backends based on strategy
pub struct MultiJournalRouter {
    backends: Vec<JournalBackend>,
    strategy: JournalRoutingStrategy,
    round_robin_idx: usize,
    stats: MultiJournalStats,
}

impl MultiJournalRouter {
    pub fn new(strategy: JournalRoutingStrategy) -> Self {
        Self {
            backends: Vec::new(),
            strategy,
            round_robin_idx: 0,
            stats: MultiJournalStats::default(),
        }
    }

    /// Add a journal backend
    pub fn add_backend(&mut self, backend: JournalBackend) {
        self.backends.push(backend);
        // Sort by priority
        self.backends.sort_by_key(|b| b.priority);
    }

    /// Route a persistence ID to a journal backend ID
    pub fn route(&mut self, persistence_id: &str, tags: &[String]) -> Option<String> {
        self.stats.total_routes += 1;

        let result = match &self.strategy {
            JournalRoutingStrategy::Single(id) => Some(id.clone()),
            JournalRoutingStrategy::PrefixBased(rules) => {
                rules
                    .iter()
                    .find(|(prefix, _)| persistence_id.starts_with(prefix))
                    .map(|(_, journal_id)| journal_id.clone())
            }
            JournalRoutingStrategy::TagBased(tag_map) => {
                tags.iter()
                    .find_map(|tag| tag_map.get(tag))
                    .cloned()
            }
            JournalRoutingStrategy::RoundRobin(journals) => {
                if journals.is_empty() {
                    None
                } else {
                    let idx = self.round_robin_idx % journals.len();
                    self.round_robin_idx = self.round_robin_idx.wrapping_add(1);
                    Some(journals[idx].clone())
                }
            }
            JournalRoutingStrategy::HashPartitioned(journals) => {
                if journals.is_empty() {
                    None
                } else {
                    let hash = Self::simple_hash(persistence_id);
                    let idx = (hash as usize) % journals.len();
                    Some(journals[idx].clone())
                }
            }
            JournalRoutingStrategy::Custom(name) => {
                // For custom routing, return the custom name as-is
                Some(name.clone())
            }
        };

        if result.is_some() {
            self.stats.successful_routes += 1;
        } else {
            self.stats.failed_routes += 1;
        }

        result
    }

    /// Get replica backends for replication
    pub fn replicas(&self) -> Vec<&JournalBackend> {
        self.backends.iter().filter(|b| b.replica && b.available).collect()
    }

    /// Get the primary backend (highest priority available)
    pub fn primary(&self) -> Option<&JournalBackend> {
        self.backends.iter().find(|b| !b.replica && b.available)
    }

    /// Mark a backend as unavailable (for failover)
    pub fn mark_unavailable(&mut self, backend_id: &str) {
        if let Some(b) = self.backends.iter_mut().find(|b| b.id == backend_id) {
            b.available = false;
            self.stats.failover_count += 1;
        }
    }

    /// Mark a backend as available (recovered)
    pub fn mark_available(&mut self, backend_id: &str) {
        if let Some(b) = self.backends.iter_mut().find(|b| b.id == backend_id) {
            b.available = true;
            self.stats.recovery_count += 1;
        }
    }

    /// Get failover backend when primary fails
    pub fn failover_target(&self, failed_id: &str) -> Option<&JournalBackend> {
        self.backends
            .iter()
            .filter(|b| b.id != failed_id && b.available && !b.replica)
            .min_by_key(|b| b.priority)
    }

    pub fn get_backend(&self, id: &str) -> Option<&JournalBackend> {
        self.backends.iter().find(|b| b.id == id)
    }

    pub fn available_backends(&self) -> Vec<&JournalBackend> {
        self.backends.iter().filter(|b| b.available).collect()
    }

    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }

    pub fn stats(&self) -> &MultiJournalStats {
        &self.stats
    }

    fn simple_hash(s: &str) -> u32 {
        s.bytes().fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32))
    }
}

impl fmt::Debug for MultiJournalRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiJournalRouter")
            .field("backends", &self.backends.len())
            .field("strategy", &self.strategy)
            .finish()
    }
}

// ============================================================================
// Replication Manager
// ============================================================================

/// Manages replication of events across multiple journal backends
pub struct ReplicationManager {
    /// Replication mode
    mode: ReplicationMode,
    /// Track replication lag per backend
    lag: HashMap<String, ReplicationLag>,
    /// Stats
    stats: ReplicationStats,
}

/// Replication mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMode {
    /// Write to primary, async replicate to replicas
    Async,
    /// Write to primary + at least N replicas before ack
    SemiSync { min_replicas: u32 },
    /// Write to all replicas before ack
    Sync,
    /// No replication
    None,
}

/// Tracks replication lag for a backend
#[derive(Debug, Clone)]
pub struct ReplicationLag {
    pub backend_id: String,
    pub last_replicated_seq: u64,
    pub primary_seq: u64,
    pub last_replication_time: Option<Instant>,
    pub lag_events: u64,
}

impl ReplicationLag {
    pub fn new(backend_id: &str) -> Self {
        Self {
            backend_id: backend_id.to_string(),
            last_replicated_seq: 0,
            primary_seq: 0,
            last_replication_time: None,
            lag_events: 0,
        }
    }

    pub fn update(&mut self, replicated_seq: u64, primary_seq: u64) {
        self.last_replicated_seq = replicated_seq;
        self.primary_seq = primary_seq;
        self.lag_events = primary_seq.saturating_sub(replicated_seq);
        self.last_replication_time = Some(Instant::now());
    }

    pub fn is_caught_up(&self) -> bool {
        self.lag_events == 0
    }
}

/// Replication statistics
#[derive(Debug, Clone, Default)]
pub struct ReplicationStats {
    pub total_replicated: u64,
    pub total_failed: u64,
    pub total_retries: u64,
    pub max_lag_observed: u64,
}

impl ReplicationManager {
    pub fn new(mode: ReplicationMode) -> Self {
        Self {
            mode,
            lag: HashMap::new(),
            stats: ReplicationStats::default(),
        }
    }

    /// Record a successful replication
    pub fn record_replication(&mut self, backend_id: &str, replicated_seq: u64, primary_seq: u64) {
        let lag = self
            .lag
            .entry(backend_id.to_string())
            .or_insert_with(|| ReplicationLag::new(backend_id));
        lag.update(replicated_seq, primary_seq);
        self.stats.total_replicated += 1;
        if lag.lag_events > self.stats.max_lag_observed {
            self.stats.max_lag_observed = lag.lag_events;
        }
    }

    /// Record a failed replication
    pub fn record_failure(&mut self, _backend_id: &str) {
        self.stats.total_failed += 1;
    }

    /// Record a retry
    pub fn record_retry(&mut self) {
        self.stats.total_retries += 1;
    }

    /// Check if replication requirements are met
    pub fn is_satisfied(&self, ack_count: u32) -> bool {
        match self.mode {
            ReplicationMode::None => true,
            ReplicationMode::Async => true, // async always returns immediately
            ReplicationMode::SemiSync { min_replicas } => ack_count >= min_replicas,
            ReplicationMode::Sync => {
                // All replicas must be caught up
                self.lag.values().all(|l| l.is_caught_up())
            }
        }
    }

    /// Get lag for a specific backend
    pub fn get_lag(&self, backend_id: &str) -> Option<&ReplicationLag> {
        self.lag.get(backend_id)
    }

    /// Get total lag across all replicas
    pub fn total_lag(&self) -> u64 {
        self.lag.values().map(|l| l.lag_events).sum()
    }

    /// Get all backend IDs with lag
    pub fn lagging_backends(&self) -> Vec<&str> {
        self.lag
            .values()
            .filter(|l| !l.is_caught_up())
            .map(|l| l.backend_id.as_str())
            .collect()
    }

    pub fn mode(&self) -> ReplicationMode {
        self.mode
    }

    pub fn stats(&self) -> &ReplicationStats {
        &self.stats
    }
}

// ============================================================================
// Statistics
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct MultiJournalStats {
    pub total_routes: u64,
    pub successful_routes: u64,
    pub failed_routes: u64,
    pub failover_count: u64,
    pub recovery_count: u64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- JournalBackend tests --

    #[test]
    fn test_backend_creation() {
        let b = JournalBackend::new("primary", "cassandra");
        assert_eq!(b.id, "primary");
        assert_eq!(b.backend_type, "cassandra");
        assert!(b.available);
        assert!(!b.replica);
    }

    #[test]
    fn test_backend_as_replica() {
        let b = JournalBackend::new("replica1", "postgres").as_replica();
        assert!(b.replica);
    }

    #[test]
    fn test_backend_priority() {
        let b = JournalBackend::new("b", "mem").with_priority(5);
        assert_eq!(b.priority, 5);
    }

    #[test]
    fn test_backend_retries() {
        let b = JournalBackend::new("b", "mem").with_retries(5, Duration::from_secs(1));
        assert_eq!(b.max_retries, 5);
    }

    // -- MultiJournalRouter tests --

    #[test]
    fn test_single_routing() {
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::Single("main".to_string()));
        assert_eq!(router.route("any-id", &[]), Some("main".to_string()));
    }

    #[test]
    fn test_prefix_routing() {
        let rules = vec![
            ("order-".to_string(), "journal-a".to_string()),
            ("user-".to_string(), "journal-b".to_string()),
        ];
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::PrefixBased(rules));
        assert_eq!(router.route("order-123", &[]), Some("journal-a".to_string()));
        assert_eq!(router.route("user-456", &[]), Some("journal-b".to_string()));
        assert_eq!(router.route("product-789", &[]), None);
    }

    #[test]
    fn test_tag_routing() {
        let mut tag_map = HashMap::new();
        tag_map.insert("financial".to_string(), "secure-journal".to_string());
        tag_map.insert("audit".to_string(), "audit-journal".to_string());
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::TagBased(tag_map));
        let tags = vec!["financial".to_string()];
        assert_eq!(router.route("any", &tags), Some("secure-journal".to_string()));
    }

    #[test]
    fn test_round_robin_routing() {
        let journals = vec!["j1".to_string(), "j2".to_string(), "j3".to_string()];
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::RoundRobin(journals));
        assert_eq!(router.route("a", &[]), Some("j1".to_string()));
        assert_eq!(router.route("b", &[]), Some("j2".to_string()));
        assert_eq!(router.route("c", &[]), Some("j3".to_string()));
        assert_eq!(router.route("d", &[]), Some("j1".to_string()));
    }

    #[test]
    fn test_hash_partitioned_routing() {
        let journals = vec!["j1".to_string(), "j2".to_string()];
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::HashPartitioned(journals));
        let r1 = router.route("order-1", &[]);
        let r2 = router.route("order-1", &[]); // same ID → same result
        assert!(r1.is_some());
        assert_eq!(r1, r2); // deterministic
    }

    #[test]
    fn test_add_backend() {
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::Single("main".to_string()));
        router.add_backend(JournalBackend::new("primary", "cassandra").with_priority(0));
        router.add_backend(JournalBackend::new("secondary", "postgres").with_priority(1));
        assert_eq!(router.backend_count(), 2);
    }

    #[test]
    fn test_primary_backend() {
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::Single("main".to_string()));
        router.add_backend(JournalBackend::new("p", "cass").with_priority(0));
        router.add_backend(JournalBackend::new("r", "pg").with_priority(1).as_replica());
        let primary = router.primary().unwrap();
        assert_eq!(primary.id, "p");
    }

    #[test]
    fn test_replicas() {
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::Single("main".to_string()));
        router.add_backend(JournalBackend::new("p", "cass"));
        router.add_backend(JournalBackend::new("r1", "pg").as_replica());
        router.add_backend(JournalBackend::new("r2", "pg").as_replica());
        assert_eq!(router.replicas().len(), 2);
    }

    #[test]
    fn test_failover() {
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::Single("main".to_string()));
        router.add_backend(JournalBackend::new("p", "cass").with_priority(0));
        router.add_backend(JournalBackend::new("s", "pg").with_priority(1));
        router.mark_unavailable("p");
        let target = router.failover_target("p").unwrap();
        assert_eq!(target.id, "s");
    }

    #[test]
    fn test_mark_available() {
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::Single("m".to_string()));
        router.add_backend(JournalBackend::new("p", "cass"));
        router.mark_unavailable("p");
        assert!(!router.get_backend("p").unwrap().available);
        router.mark_available("p");
        assert!(router.get_backend("p").unwrap().available);
    }

    #[test]
    fn test_available_backends() {
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::Single("m".to_string()));
        router.add_backend(JournalBackend::new("a", "mem"));
        router.add_backend(JournalBackend::new("b", "mem"));
        router.mark_unavailable("b");
        assert_eq!(router.available_backends().len(), 1);
    }

    #[test]
    fn test_routing_stats() {
        let mut router = MultiJournalRouter::new(JournalRoutingStrategy::Single("m".to_string()));
        router.route("x", &[]);
        router.route("y", &[]);
        assert_eq!(router.stats().total_routes, 2);
        assert_eq!(router.stats().successful_routes, 2);
    }

    // -- ReplicationManager tests --

    #[test]
    fn test_replication_async() {
        let mgr = ReplicationManager::new(ReplicationMode::Async);
        assert!(mgr.is_satisfied(0));
        assert_eq!(mgr.mode(), ReplicationMode::Async);
    }

    #[test]
    fn test_replication_semi_sync() {
        let mgr = ReplicationManager::new(ReplicationMode::SemiSync { min_replicas: 2 });
        assert!(!mgr.is_satisfied(1));
        assert!(mgr.is_satisfied(2));
    }

    #[test]
    fn test_replication_sync() {
        let mut mgr = ReplicationManager::new(ReplicationMode::Sync);
        mgr.record_replication("r1", 10, 10);
        assert!(mgr.is_satisfied(1));
    }

    #[test]
    fn test_replication_lag() {
        let mut mgr = ReplicationManager::new(ReplicationMode::Async);
        mgr.record_replication("r1", 5, 10);
        let lag = mgr.get_lag("r1").unwrap();
        assert_eq!(lag.lag_events, 5);
        assert!(!lag.is_caught_up());
    }

    #[test]
    fn test_replication_caught_up() {
        let mut mgr = ReplicationManager::new(ReplicationMode::Async);
        mgr.record_replication("r1", 10, 10);
        assert!(mgr.get_lag("r1").unwrap().is_caught_up());
    }

    #[test]
    fn test_total_lag() {
        let mut mgr = ReplicationManager::new(ReplicationMode::Async);
        mgr.record_replication("r1", 5, 10);
        mgr.record_replication("r2", 7, 10);
        assert_eq!(mgr.total_lag(), 8); // 5 + 3
    }

    #[test]
    fn test_lagging_backends() {
        let mut mgr = ReplicationManager::new(ReplicationMode::Async);
        mgr.record_replication("r1", 5, 10);
        mgr.record_replication("r2", 10, 10);
        let lagging = mgr.lagging_backends();
        assert_eq!(lagging.len(), 1);
        assert_eq!(lagging[0], "r1");
    }

    #[test]
    fn test_replication_failure() {
        let mut mgr = ReplicationManager::new(ReplicationMode::Async);
        mgr.record_failure("r1");
        assert_eq!(mgr.stats().total_failed, 1);
    }

    #[test]
    fn test_replication_retry() {
        let mut mgr = ReplicationManager::new(ReplicationMode::Async);
        mgr.record_retry();
        assert_eq!(mgr.stats().total_retries, 1);
    }

    #[test]
    fn test_replication_stats() {
        let mut mgr = ReplicationManager::new(ReplicationMode::Async);
        mgr.record_replication("r1", 5, 10);
        assert_eq!(mgr.stats().total_replicated, 1);
        assert_eq!(mgr.stats().max_lag_observed, 5);
    }

    #[test]
    fn test_replication_none() {
        let mgr = ReplicationManager::new(ReplicationMode::None);
        assert!(mgr.is_satisfied(0));
    }
}
