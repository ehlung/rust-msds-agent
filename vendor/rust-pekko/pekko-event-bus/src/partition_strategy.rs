// partition_strategy.rs - Partition Assignment Strategies
// ConsistentHash, RoundRobin, Sticky strategies for event routing

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

// ============================================================================
// Partition Key
// ============================================================================

/// A key used for partition assignment decisions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PartitionKey {
    /// String-based key (e.g., entity ID, user ID)
    String(String),
    /// Numeric key
    Numeric(u64),
    /// Composite key from multiple parts
    Composite(Vec<String>),
    /// No key — use round-robin or random
    None,
}

impl PartitionKey {
    pub fn from_str(s: &str) -> Self {
        PartitionKey::String(s.to_string())
    }

    pub fn from_u64(n: u64) -> Self {
        PartitionKey::Numeric(n)
    }

    pub fn composite(parts: Vec<&str>) -> Self {
        PartitionKey::Composite(parts.into_iter().map(|s| s.to_string()).collect())
    }

    /// Compute a consistent hash value for this key
    pub fn hash_value(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

// ============================================================================
// Partition Strategy Trait
// ============================================================================

/// Strategy for assigning events to partitions
pub trait PartitionStrategy: Send + Sync {
    /// Select a partition for the given key
    fn select_partition(&self, key: &PartitionKey, num_partitions: u32) -> u32;

    /// Name of this strategy (for metrics/logging)
    fn name(&self) -> &str;
}

// ============================================================================
// Consistent Hash Strategy
// ============================================================================

/// Consistent hashing with virtual nodes for even distribution
pub struct ConsistentHashStrategy {
    virtual_nodes: u32,
    ring: Vec<(u64, u32)>, // (hash, partition_id)
}

impl ConsistentHashStrategy {
    pub fn new(num_partitions: u32, virtual_nodes_per_partition: u32) -> Self {
        let mut ring = Vec::with_capacity((num_partitions * virtual_nodes_per_partition) as usize);

        for partition in 0..num_partitions {
            for vnode in 0..virtual_nodes_per_partition {
                let key = format!("partition-{}-vnode-{}", partition, vnode);
                let mut hasher = DefaultHasher::new();
                key.hash(&mut hasher);
                let hash = hasher.finish();
                ring.push((hash, partition));
            }
        }

        ring.sort_by_key(|(hash, _)| *hash);

        Self {
            virtual_nodes: virtual_nodes_per_partition,
            ring,
        }
    }

    /// Find the partition responsible for a given hash value
    fn find_partition(&self, hash: u64) -> u32 {
        if self.ring.is_empty() {
            return 0;
        }

        // Binary search for the first node with hash >= target
        match self.ring.binary_search_by_key(&hash, |(h, _)| *h) {
            Ok(idx) => self.ring[idx].1,
            Err(idx) => {
                if idx >= self.ring.len() {
                    // Wrap around to first node
                    self.ring[0].1
                } else {
                    self.ring[idx].1
                }
            }
        }
    }

    pub fn virtual_nodes_per_partition(&self) -> u32 {
        self.virtual_nodes
    }

    pub fn ring_size(&self) -> usize {
        self.ring.len()
    }
}

impl PartitionStrategy for ConsistentHashStrategy {
    fn select_partition(&self, key: &PartitionKey, _num_partitions: u32) -> u32 {
        match key {
            PartitionKey::None => {
                // For None keys, use current time as pseudo-random
                let hash = {
                    let mut hasher = DefaultHasher::new();
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                        .hash(&mut hasher);
                    hasher.finish()
                };
                self.find_partition(hash)
            }
            _ => self.find_partition(key.hash_value()),
        }
    }

    fn name(&self) -> &str {
        "consistent-hash"
    }
}

// ============================================================================
// Round Robin Strategy
// ============================================================================

/// Simple round-robin partition assignment (ignores key)
pub struct RoundRobinStrategy {
    counter: AtomicU32,
}

impl RoundRobinStrategy {
    pub fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
        }
    }

    pub fn current_counter(&self) -> u32 {
        self.counter.load(Ordering::Relaxed)
    }
}

impl Default for RoundRobinStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionStrategy for RoundRobinStrategy {
    fn select_partition(&self, _key: &PartitionKey, num_partitions: u32) -> u32 {
        if num_partitions == 0 {
            return 0;
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed);
        idx % num_partitions
    }

    fn name(&self) -> &str {
        "round-robin"
    }
}

// ============================================================================
// Sticky Strategy
// ============================================================================

/// Sticky partition assignment — routes same key to same partition
/// for a configurable duration, then may rebalance.
/// Useful for temporal locality and batch-friendly workloads.
pub struct StickyStrategy {
    duration: Duration,
    assignments: std::sync::Mutex<StickyAssignments>,
}

struct StickyAssignments {
    map: HashMap<u64, StickyEntry>,
    fallback_counter: u32,
}

struct StickyEntry {
    partition: u32,
    assigned_at: Instant,
}

impl StickyStrategy {
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            assignments: std::sync::Mutex::new(StickyAssignments {
                map: HashMap::new(),
                fallback_counter: 0,
            }),
        }
    }

    pub fn sticky_duration(&self) -> Duration {
        self.duration
    }

    /// Number of currently active sticky assignments
    pub fn active_assignments(&self) -> usize {
        self.assignments.lock().unwrap().map.len()
    }

    /// Evict expired entries
    pub fn cleanup_expired(&self) {
        let mut assignments = self.assignments.lock().unwrap();
        let now = Instant::now();
        assignments.map.retain(|_, entry| {
            now.duration_since(entry.assigned_at) < self.duration
        });
    }
}

impl PartitionStrategy for StickyStrategy {
    fn select_partition(&self, key: &PartitionKey, num_partitions: u32) -> u32 {
        if num_partitions == 0 {
            return 0;
        }

        let key_hash = key.hash_value();
        let mut assignments = self.assignments.lock().unwrap();

        // Check existing assignment
        if let Some(entry) = assignments.map.get(&key_hash) {
            let elapsed = Instant::now().duration_since(entry.assigned_at);
            if elapsed < self.duration && entry.partition < num_partitions {
                return entry.partition;
            }
        }

        // Assign new partition
        let partition = match key {
            PartitionKey::None => {
                let p = assignments.fallback_counter % num_partitions;
                assignments.fallback_counter = assignments.fallback_counter.wrapping_add(1);
                p
            }
            _ => (key_hash % num_partitions as u64) as u32,
        };

        assignments.map.insert(key_hash, StickyEntry {
            partition,
            assigned_at: Instant::now(),
        });

        partition
    }

    fn name(&self) -> &str {
        "sticky"
    }
}

// ============================================================================
// Key Extractor
// ============================================================================

/// Extracts a partition key from an event payload
pub trait KeyExtractor<E>: Send + Sync {
    fn extract_key(&self, event: &E) -> PartitionKey;
}

/// Default extractor that always returns None (round-robin behavior)
pub struct NoKeyExtractor;

impl<E> KeyExtractor<E> for NoKeyExtractor {
    fn extract_key(&self, _event: &E) -> PartitionKey {
        PartitionKey::None
    }
}

/// Extractor using a closure
pub struct FnKeyExtractor<E, F>
where
    F: Fn(&E) -> PartitionKey + Send + Sync,
{
    func: F,
    _marker: std::marker::PhantomData<E>,
}

impl<E, F> FnKeyExtractor<E, F>
where
    F: Fn(&E) -> PartitionKey + Send + Sync,
{
    pub fn new(func: F) -> Self {
        Self {
            func,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E, F> KeyExtractor<E> for FnKeyExtractor<E, F>
where
    E: Send + Sync,
    F: Fn(&E) -> PartitionKey + Send + Sync,
{
    fn extract_key(&self, event: &E) -> PartitionKey {
        (self.func)(event)
    }
}

// ============================================================================
// Strategy Factory
// ============================================================================

use crate::bus_config::PartitionStrategyType;

/// Create a PartitionStrategy from configuration
pub fn create_strategy(
    strategy_type: &PartitionStrategyType,
    num_partitions: u32,
) -> Box<dyn PartitionStrategy> {
    match strategy_type {
        PartitionStrategyType::ConsistentHash => {
            Box::new(ConsistentHashStrategy::new(num_partitions, 150))
        }
        PartitionStrategyType::RoundRobin => {
            Box::new(RoundRobinStrategy::new())
        }
        PartitionStrategyType::Sticky { duration } => {
            Box::new(StickyStrategy::new(*duration))
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- PartitionKey tests ---

    #[test]
    fn test_partition_key_string() {
        let k1 = PartitionKey::from_str("user-123");
        let k2 = PartitionKey::from_str("user-123");
        assert_eq!(k1, k2);
        assert_eq!(k1.hash_value(), k2.hash_value());
    }

    #[test]
    fn test_partition_key_numeric() {
        let k = PartitionKey::from_u64(42);
        assert!(k.hash_value() > 0 || k.hash_value() == 0); // just ensure it works
    }

    #[test]
    fn test_partition_key_composite() {
        let k1 = PartitionKey::composite(vec!["tenant-1", "user-42"]);
        let k2 = PartitionKey::composite(vec!["tenant-1", "user-42"]);
        assert_eq!(k1.hash_value(), k2.hash_value());

        let k3 = PartitionKey::composite(vec!["tenant-2", "user-42"]);
        assert_ne!(k1.hash_value(), k3.hash_value());
    }

    #[test]
    fn test_partition_key_none() {
        let k = PartitionKey::None;
        // None key should still produce a hash
        let _ = k.hash_value();
    }

    #[test]
    fn test_different_key_types_different_hashes() {
        let s = PartitionKey::from_str("42");
        let n = PartitionKey::from_u64(42);
        // Different types should (likely) produce different hashes
        // This is not guaranteed but extremely probable
        assert_ne!(s.hash_value(), n.hash_value());
    }

    // --- ConsistentHash tests ---

    #[test]
    fn test_consistent_hash_deterministic() {
        let strategy = ConsistentHashStrategy::new(6, 150);
        let key = PartitionKey::from_str("order-12345");

        let p1 = strategy.select_partition(&key, 6);
        let p2 = strategy.select_partition(&key, 6);
        assert_eq!(p1, p2); // Same key → same partition
    }

    #[test]
    fn test_consistent_hash_distribution() {
        let num_partitions = 6;
        let strategy = ConsistentHashStrategy::new(num_partitions, 150);
        let mut counts = vec![0u32; num_partitions as usize];

        for i in 0..6000 {
            let key = PartitionKey::from_str(&format!("key-{}", i));
            let p = strategy.select_partition(&key, num_partitions);
            assert!(p < num_partitions);
            counts[p as usize] += 1;
        }

        // Each partition should have at least some events (not perfectly even, but reasonable)
        for count in &counts {
            assert!(*count > 100, "Partition got too few events: {}", count);
        }
    }

    #[test]
    fn test_consistent_hash_ring_size() {
        let strategy = ConsistentHashStrategy::new(6, 150);
        assert_eq!(strategy.ring_size(), 900); // 6 * 150
        assert_eq!(strategy.virtual_nodes_per_partition(), 150);
    }

    #[test]
    fn test_consistent_hash_same_key_same_partition() {
        let strategy = ConsistentHashStrategy::new(12, 100);
        let keys = vec!["user-1", "user-2", "user-3", "order-abc", "payment-xyz"];

        for key_str in &keys {
            let key = PartitionKey::from_str(key_str);
            let p1 = strategy.select_partition(&key, 12);
            let p2 = strategy.select_partition(&key, 12);
            let p3 = strategy.select_partition(&key, 12);
            assert_eq!(p1, p2);
            assert_eq!(p2, p3);
        }
    }

    // --- RoundRobin tests ---

    #[test]
    fn test_round_robin_sequential() {
        let strategy = RoundRobinStrategy::new();
        let key = PartitionKey::None;

        assert_eq!(strategy.select_partition(&key, 3), 0);
        assert_eq!(strategy.select_partition(&key, 3), 1);
        assert_eq!(strategy.select_partition(&key, 3), 2);
        assert_eq!(strategy.select_partition(&key, 3), 0); // wraps
    }

    #[test]
    fn test_round_robin_ignores_key() {
        let strategy = RoundRobinStrategy::new();
        let k1 = PartitionKey::from_str("a");
        let k2 = PartitionKey::from_str("b");

        let p1 = strategy.select_partition(&k1, 4);
        let p2 = strategy.select_partition(&k2, 4);
        assert_eq!(p1, 0);
        assert_eq!(p2, 1);
    }

    #[test]
    fn test_round_robin_even_distribution() {
        let strategy = RoundRobinStrategy::new();
        let key = PartitionKey::None;
        let num_partitions = 5;
        let mut counts = vec![0u32; num_partitions as usize];

        for _ in 0..5000 {
            let p = strategy.select_partition(&key, num_partitions);
            counts[p as usize] += 1;
        }

        // Perfect distribution: 1000 each
        for count in &counts {
            assert_eq!(*count, 1000);
        }
    }

    #[test]
    fn test_round_robin_counter() {
        let strategy = RoundRobinStrategy::new();
        assert_eq!(strategy.current_counter(), 0);
        strategy.select_partition(&PartitionKey::None, 3);
        assert_eq!(strategy.current_counter(), 1);
    }

    // --- Sticky tests ---

    #[test]
    fn test_sticky_same_key_same_partition() {
        let strategy = StickyStrategy::new(Duration::from_secs(60));
        let key = PartitionKey::from_str("user-42");

        let p1 = strategy.select_partition(&key, 6);
        let p2 = strategy.select_partition(&key, 6);
        let p3 = strategy.select_partition(&key, 6);
        assert_eq!(p1, p2);
        assert_eq!(p2, p3);
    }

    #[test]
    fn test_sticky_tracks_assignments() {
        let strategy = StickyStrategy::new(Duration::from_secs(60));
        assert_eq!(strategy.active_assignments(), 0);

        strategy.select_partition(&PartitionKey::from_str("a"), 6);
        assert_eq!(strategy.active_assignments(), 1);

        strategy.select_partition(&PartitionKey::from_str("b"), 6);
        assert_eq!(strategy.active_assignments(), 2);

        // Same key doesn't add new assignment
        strategy.select_partition(&PartitionKey::from_str("a"), 6);
        assert_eq!(strategy.active_assignments(), 2);
    }

    #[test]
    fn test_sticky_duration() {
        let strategy = StickyStrategy::new(Duration::from_millis(50));
        assert_eq!(strategy.sticky_duration(), Duration::from_millis(50));
    }

    #[test]
    fn test_sticky_cleanup_expired() {
        let strategy = StickyStrategy::new(Duration::from_millis(1));
        strategy.select_partition(&PartitionKey::from_str("a"), 6);
        strategy.select_partition(&PartitionKey::from_str("b"), 6);
        assert_eq!(strategy.active_assignments(), 2);

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(5));
        strategy.cleanup_expired();
        assert_eq!(strategy.active_assignments(), 0);
    }

    #[test]
    fn test_sticky_partition_in_range() {
        let strategy = StickyStrategy::new(Duration::from_secs(60));
        for i in 0..100 {
            let key = PartitionKey::from_str(&format!("key-{}", i));
            let p = strategy.select_partition(&key, 8);
            assert!(p < 8, "Partition {} out of range", p);
        }
    }

    // --- Strategy name tests ---

    #[test]
    fn test_strategy_names() {
        let ch = ConsistentHashStrategy::new(6, 100);
        assert_eq!(ch.name(), "consistent-hash");

        let rr = RoundRobinStrategy::new();
        assert_eq!(rr.name(), "round-robin");

        let sticky = StickyStrategy::new(Duration::from_secs(30));
        assert_eq!(sticky.name(), "sticky");
    }

    // --- KeyExtractor tests ---

    #[test]
    fn test_no_key_extractor() {
        let extractor = NoKeyExtractor;
        let event = "some event";
        assert_eq!(extractor.extract_key(&event), PartitionKey::None);
    }

    #[test]
    fn test_fn_key_extractor() {
        #[derive(Debug)]
        struct OrderEvent {
            order_id: String,
        }

        let extractor = FnKeyExtractor::new(|e: &OrderEvent| {
            PartitionKey::from_str(&e.order_id)
        });

        let event = OrderEvent { order_id: "ORD-123".to_string() };
        assert_eq!(
            extractor.extract_key(&event),
            PartitionKey::from_str("ORD-123")
        );
    }

    // --- Factory tests ---

    #[test]
    fn test_create_strategy_consistent_hash() {
        let strategy = create_strategy(&PartitionStrategyType::ConsistentHash, 6);
        assert_eq!(strategy.name(), "consistent-hash");
    }

    #[test]
    fn test_create_strategy_round_robin() {
        let strategy = create_strategy(&PartitionStrategyType::RoundRobin, 6);
        assert_eq!(strategy.name(), "round-robin");
    }

    #[test]
    fn test_create_strategy_sticky() {
        let strategy = create_strategy(
            &PartitionStrategyType::Sticky { duration: Duration::from_secs(30) },
            6,
        );
        assert_eq!(strategy.name(), "sticky");
    }

    // --- Edge case tests ---

    #[test]
    fn test_single_partition() {
        let ch = ConsistentHashStrategy::new(1, 100);
        let rr = RoundRobinStrategy::new();
        let sticky = StickyStrategy::new(Duration::from_secs(60));
        let key = PartitionKey::from_str("anything");

        assert_eq!(ch.select_partition(&key, 1), 0);
        assert_eq!(rr.select_partition(&key, 1), 0);
        assert_eq!(sticky.select_partition(&key, 1), 0);
    }

    #[test]
    fn test_zero_partitions_no_panic() {
        let rr = RoundRobinStrategy::new();
        let sticky = StickyStrategy::new(Duration::from_secs(60));
        let key = PartitionKey::from_str("test");

        // Should not panic
        let _ = rr.select_partition(&key, 0);
        let _ = sticky.select_partition(&key, 0);
    }
}
