// at_least_once_delivery.rs - At-Least-Once Delivery Semantics for Persistent Actors
// Provides reliable message delivery with redelivery, confirmation, and deduplication

use std::collections::{HashMap, HashSet, BTreeMap};
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, Ordering};

// ============================================================================
// Core Types
// ============================================================================

pub type DeliveryId = u64;
pub type DestinationId = String;

#[derive(Debug, Clone)]
pub struct DeliveryConfig {
    pub redeliver_interval: Duration,
    pub max_redeliveries: u32,
    pub warn_after_redeliveries: u32,
    pub max_unconfirmed: usize,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            redeliver_interval: Duration::from_secs(5),
            max_redeliveries: 10,
            warn_after_redeliveries: 5,
            max_unconfirmed: 10_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeliveryAttempt<M: Clone> {
    pub delivery_id: DeliveryId,
    pub destination: DestinationId,
    pub message: M,
    pub attempt: u32,
    pub first_sent: Instant,
    pub last_sent: Instant,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryStatus {
    Pending,
    Redelivering { attempt: u32 },
    Confirmed,
    Failed { reason: String },
    MaxRedeliveriesReached,
}

#[derive(Debug, Clone)]
pub struct DeliveryEvent {
    pub delivery_id: DeliveryId,
    pub event_type: DeliveryEventType,
    pub timestamp: Instant,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryEventType {
    Sent { destination: DestinationId },
    Redelivered { attempt: u32 },
    Confirmed,
    Failed { reason: String },
    MaxRedeliveriesReached,
}

// ============================================================================
// AtLeastOnceDelivery Manager
// ============================================================================

pub struct AtLeastOnceDelivery<M: Clone> {
    config: DeliveryConfig,
    next_delivery_id: AtomicU64,
    unconfirmed: HashMap<DeliveryId, DeliveryAttempt<M>>,
    confirmed: HashSet<DeliveryId>,
    events: Vec<DeliveryEvent>,
    stats: DeliveryStats,
}

#[derive(Debug, Clone, Default)]
pub struct DeliveryStats {
    pub total_sent: u64,
    pub total_confirmed: u64,
    pub total_failed: u64,
    pub total_redeliveries: u64,
    pub total_max_reached: u64,
}

impl<M: Clone + std::fmt::Debug> AtLeastOnceDelivery<M> {
    pub fn new(config: DeliveryConfig) -> Self {
        Self {
            config,
            next_delivery_id: AtomicU64::new(1),
            unconfirmed: HashMap::new(),
            confirmed: HashSet::new(),
            events: Vec::new(),
            stats: DeliveryStats::default(),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(DeliveryConfig::default())
    }

    /// Deliver a message to a destination, returning the delivery ID
    pub fn deliver(&mut self, destination: DestinationId, message: M) -> Result<DeliveryId, DeliveryError> {
        if self.unconfirmed.len() >= self.config.max_unconfirmed {
            return Err(DeliveryError::MaxUnconfirmedReached {
                limit: self.config.max_unconfirmed,
            });
        }

        let delivery_id = self.next_delivery_id.fetch_add(1, Ordering::SeqCst);
        let now = Instant::now();

        let attempt = DeliveryAttempt {
            delivery_id,
            destination: destination.clone(),
            message,
            attempt: 1,
            first_sent: now,
            last_sent: now,
        };

        self.unconfirmed.insert(delivery_id, attempt);
        self.stats.total_sent += 1;

        self.events.push(DeliveryEvent {
            delivery_id,
            event_type: DeliveryEventType::Sent { destination },
            timestamp: now,
        });

        Ok(delivery_id)
    }

    /// Confirm delivery of a message
    pub fn confirm_delivery(&mut self, delivery_id: DeliveryId) -> Result<(), DeliveryError> {
        if self.confirmed.contains(&delivery_id) {
            return Ok(()); // Idempotent confirmation
        }

        if self.unconfirmed.remove(&delivery_id).is_some() {
            self.confirmed.insert(delivery_id);
            self.stats.total_confirmed += 1;

            self.events.push(DeliveryEvent {
                delivery_id,
                event_type: DeliveryEventType::Confirmed,
                timestamp: Instant::now(),
            });

            Ok(())
        } else {
            Err(DeliveryError::UnknownDeliveryId(delivery_id))
        }
    }

    /// Get messages that need redelivery
    pub fn get_redeliveries(&mut self) -> Vec<(DeliveryId, DestinationId, M)> {
        let now = Instant::now();
        let interval = self.config.redeliver_interval;
        let max = self.config.max_redeliveries;
        let mut redeliveries = Vec::new();
        let mut failed_ids = Vec::new();

        for (id, attempt) in self.unconfirmed.iter_mut() {
            if now.duration_since(attempt.last_sent) >= interval {
                if attempt.attempt >= max {
                    failed_ids.push(*id);
                } else {
                    attempt.attempt += 1;
                    attempt.last_sent = now;
                    redeliveries.push((*id, attempt.destination.clone(), attempt.message.clone()));
                    self.stats.total_redeliveries += 1;
                }
            }
        }

        for id in failed_ids {
            if let Some(_attempt) = self.unconfirmed.remove(&id) {
                self.stats.total_max_reached += 1;
                self.events.push(DeliveryEvent {
                    delivery_id: id,
                    event_type: DeliveryEventType::MaxRedeliveriesReached,
                    timestamp: now,
                });
            }
        }

        redeliveries
    }

    /// Get the status of a specific delivery
    pub fn delivery_status(&self, delivery_id: DeliveryId) -> DeliveryStatus {
        if self.confirmed.contains(&delivery_id) {
            return DeliveryStatus::Confirmed;
        }
        if let Some(attempt) = self.unconfirmed.get(&delivery_id) {
            if attempt.attempt >= self.config.max_redeliveries {
                DeliveryStatus::MaxRedeliveriesReached
            } else if attempt.attempt > 1 {
                DeliveryStatus::Redelivering { attempt: attempt.attempt }
            } else {
                DeliveryStatus::Pending
            }
        } else {
            DeliveryStatus::Failed {
                reason: "Not found".to_string(),
            }
        }
    }

    pub fn unconfirmed_count(&self) -> usize {
        self.unconfirmed.len()
    }

    pub fn confirmed_count(&self) -> usize {
        self.confirmed.len()
    }

    pub fn stats(&self) -> &DeliveryStats {
        &self.stats
    }

    pub fn events(&self) -> &[DeliveryEvent] {
        &self.events
    }

    pub fn config(&self) -> &DeliveryConfig {
        &self.config
    }

    /// Get all unconfirmed delivery IDs
    pub fn unconfirmed_ids(&self) -> Vec<DeliveryId> {
        self.unconfirmed.keys().copied().collect()
    }

    /// Clear confirmed delivery tracking (to free memory)
    pub fn clear_confirmed(&mut self) -> usize {
        let count = self.confirmed.len();
        self.confirmed.clear();
        count
    }
}

// ============================================================================
// Deduplication
// ============================================================================

pub struct MessageDeduplicator {
    seen: BTreeMap<DeliveryId, Instant>,
    max_entries: usize,
    ttl: Duration,
}

impl MessageDeduplicator {
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            seen: BTreeMap::new(),
            max_entries,
            ttl,
        }
    }

    /// Check if a message is a duplicate. Returns true if already seen.
    pub fn is_duplicate(&mut self, delivery_id: DeliveryId) -> bool {
        self.cleanup_expired();

        if self.seen.contains_key(&delivery_id) {
            true
        } else {
            if self.seen.len() >= self.max_entries {
                // Remove oldest
                if let Some(&oldest_key) = self.seen.keys().next() {
                    self.seen.remove(&oldest_key);
                }
            }
            self.seen.insert(delivery_id, Instant::now());
            false
        }
    }

    /// Process a message, returning true if it should be processed (not a duplicate)
    pub fn process(&mut self, delivery_id: DeliveryId) -> bool {
        !self.is_duplicate(delivery_id)
    }

    fn cleanup_expired(&mut self) {
        let now = Instant::now();
        let ttl = self.ttl;
        self.seen.retain(|_, timestamp| now.duration_since(*timestamp) < ttl);
    }

    pub fn seen_count(&self) -> usize {
        self.seen.len()
    }

    pub fn clear(&mut self) {
        self.seen.clear();
    }
}

// ============================================================================
// Delivery Snapshot (for persistence)
// ============================================================================

#[derive(Debug, Clone)]
pub struct DeliverySnapshot<M: Clone> {
    pub next_delivery_id: u64,
    pub unconfirmed: Vec<UnconfirmedDelivery<M>>,
    pub stats: DeliveryStats,
}

#[derive(Debug, Clone)]
pub struct UnconfirmedDelivery<M: Clone> {
    pub delivery_id: DeliveryId,
    pub destination: DestinationId,
    pub message: M,
    pub attempt: u32,
}

impl<M: Clone + std::fmt::Debug> AtLeastOnceDelivery<M> {
    /// Create a snapshot for persistence
    pub fn snapshot(&self) -> DeliverySnapshot<M> {
        let unconfirmed = self.unconfirmed.values().map(|a| {
            UnconfirmedDelivery {
                delivery_id: a.delivery_id,
                destination: a.destination.clone(),
                message: a.message.clone(),
                attempt: a.attempt,
            }
        }).collect();

        DeliverySnapshot {
            next_delivery_id: self.next_delivery_id.load(Ordering::SeqCst),
            unconfirmed,
            stats: self.stats.clone(),
        }
    }

    /// Restore from a snapshot
    pub fn restore(&mut self, snapshot: DeliverySnapshot<M>) {
        self.next_delivery_id.store(snapshot.next_delivery_id, Ordering::SeqCst);
        self.stats = snapshot.stats;
        self.unconfirmed.clear();
        let now = Instant::now();

        for ud in snapshot.unconfirmed {
            self.unconfirmed.insert(ud.delivery_id, DeliveryAttempt {
                delivery_id: ud.delivery_id,
                destination: ud.destination,
                message: ud.message,
                attempt: ud.attempt,
                first_sent: now,
                last_sent: now,
            });
        }
    }
}

// ============================================================================
// Delivery Builder
// ============================================================================

pub struct DeliveryBuilder {
    config: DeliveryConfig,
}

impl DeliveryBuilder {
    pub fn new() -> Self {
        Self {
            config: DeliveryConfig::default(),
        }
    }

    pub fn redeliver_interval(mut self, interval: Duration) -> Self {
        self.config.redeliver_interval = interval;
        self
    }

    pub fn max_redeliveries(mut self, max: u32) -> Self {
        self.config.max_redeliveries = max;
        self
    }

    pub fn warn_after(mut self, count: u32) -> Self {
        self.config.warn_after_redeliveries = count;
        self
    }

    pub fn max_unconfirmed(mut self, max: usize) -> Self {
        self.config.max_unconfirmed = max;
        self
    }

    pub fn build<M: Clone + std::fmt::Debug>(self) -> AtLeastOnceDelivery<M> {
        AtLeastOnceDelivery::new(self.config)
    }
}

impl Default for DeliveryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryError {
    MaxUnconfirmedReached { limit: usize },
    UnknownDeliveryId(DeliveryId),
    DeliveryFailed { delivery_id: DeliveryId, reason: String },
}

impl std::fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeliveryError::MaxUnconfirmedReached { limit } =>
                write!(f, "Max unconfirmed deliveries reached: {}", limit),
            DeliveryError::UnknownDeliveryId(id) =>
                write!(f, "Unknown delivery ID: {}", id),
            DeliveryError::DeliveryFailed { delivery_id, reason } =>
                write!(f, "Delivery {} failed: {}", delivery_id, reason),
        }
    }
}

impl std::error::Error for DeliveryError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deliver_message() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        let id = delivery.deliver("dest-1".into(), "hello".into()).unwrap();
        assert_eq!(id, 1);
        assert_eq!(delivery.unconfirmed_count(), 1);
        assert_eq!(delivery.stats().total_sent, 1);
    }

    #[test]
    fn test_confirm_delivery() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        let id = delivery.deliver("dest-1".into(), "hello".into()).unwrap();
        delivery.confirm_delivery(id).unwrap();
        assert_eq!(delivery.unconfirmed_count(), 0);
        assert_eq!(delivery.confirmed_count(), 1);
        assert_eq!(delivery.stats().total_confirmed, 1);
    }

    #[test]
    fn test_idempotent_confirmation() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        let id = delivery.deliver("dest-1".into(), "hello".into()).unwrap();
        delivery.confirm_delivery(id).unwrap();
        // Second confirmation should succeed silently
        delivery.confirm_delivery(id).unwrap();
        assert_eq!(delivery.stats().total_confirmed, 1);
    }

    #[test]
    fn test_unknown_delivery_confirmation() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        let result = delivery.confirm_delivery(999);
        assert_eq!(result, Err(DeliveryError::UnknownDeliveryId(999)));
    }

    #[test]
    fn test_delivery_status_pending() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        let id = delivery.deliver("dest-1".into(), "msg".into()).unwrap();
        assert_eq!(delivery.delivery_status(id), DeliveryStatus::Pending);
    }

    #[test]
    fn test_delivery_status_confirmed() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        let id = delivery.deliver("dest-1".into(), "msg".into()).unwrap();
        delivery.confirm_delivery(id).unwrap();
        assert_eq!(delivery.delivery_status(id), DeliveryStatus::Confirmed);
    }

    #[test]
    fn test_multiple_deliveries() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        let id1 = delivery.deliver("dest-1".into(), "msg1".into()).unwrap();
        let id2 = delivery.deliver("dest-2".into(), "msg2".into()).unwrap();
        let id3 = delivery.deliver("dest-1".into(), "msg3".into()).unwrap();

        assert_eq!(delivery.unconfirmed_count(), 3);
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);

        delivery.confirm_delivery(id2).unwrap();
        assert_eq!(delivery.unconfirmed_count(), 2);
    }

    #[test]
    fn test_max_unconfirmed_limit() {
        let config = DeliveryConfig {
            max_unconfirmed: 2,
            ..Default::default()
        };
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::new(config);

        delivery.deliver("d1".into(), "m1".into()).unwrap();
        delivery.deliver("d2".into(), "m2".into()).unwrap();

        let result = delivery.deliver("d3".into(), "m3".into());
        assert_eq!(result, Err(DeliveryError::MaxUnconfirmedReached { limit: 2 }));
    }

    #[test]
    fn test_redelivery_with_short_interval() {
        let config = DeliveryConfig {
            redeliver_interval: Duration::from_millis(1),
            max_redeliveries: 5,
            ..Default::default()
        };
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::new(config);
        delivery.deliver("dest-1".into(), "hello".into()).unwrap();

        std::thread::sleep(Duration::from_millis(5));

        let redeliveries = delivery.get_redeliveries();
        assert_eq!(redeliveries.len(), 1);
        assert_eq!(redeliveries[0].1, "dest-1");
        assert_eq!(delivery.stats().total_redeliveries, 1);
    }

    #[test]
    fn test_max_redeliveries_reached() {
        let config = DeliveryConfig {
            redeliver_interval: Duration::from_millis(1),
            max_redeliveries: 2,
            ..Default::default()
        };
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::new(config);
        let id = delivery.deliver("dest-1".into(), "hello".into()).unwrap();

        std::thread::sleep(Duration::from_millis(5));
        let _ = delivery.get_redeliveries(); // attempt 2
        std::thread::sleep(Duration::from_millis(5));
        let redeliveries = delivery.get_redeliveries(); // should remove (max reached)

        assert_eq!(redeliveries.len(), 0);
        assert_eq!(delivery.unconfirmed_count(), 0);
        assert_eq!(delivery.stats().total_max_reached, 1);

        let status = delivery.delivery_status(id);
        assert_eq!(status, DeliveryStatus::Failed { reason: "Not found".to_string() });
    }

    #[test]
    fn test_delivery_events_tracking() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        let id = delivery.deliver("dest-1".into(), "hello".into()).unwrap();
        delivery.confirm_delivery(id).unwrap();

        assert_eq!(delivery.events().len(), 2);
        assert_eq!(delivery.events()[0].event_type, DeliveryEventType::Sent { destination: "dest-1".into() });
        assert_eq!(delivery.events()[1].event_type, DeliveryEventType::Confirmed);
    }

    #[test]
    fn test_unconfirmed_ids() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        let id1 = delivery.deliver("d1".into(), "m1".into()).unwrap();
        let id2 = delivery.deliver("d2".into(), "m2".into()).unwrap();
        delivery.confirm_delivery(id1).unwrap();

        let ids = delivery.unconfirmed_ids();
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&id2));
    }

    #[test]
    fn test_clear_confirmed() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        let id1 = delivery.deliver("d1".into(), "m1".into()).unwrap();
        let id2 = delivery.deliver("d2".into(), "m2".into()).unwrap();
        delivery.confirm_delivery(id1).unwrap();
        delivery.confirm_delivery(id2).unwrap();

        let cleared = delivery.clear_confirmed();
        assert_eq!(cleared, 2);
        assert_eq!(delivery.confirmed_count(), 0);
    }

    // --- Deduplicator Tests ---

    #[test]
    fn test_deduplicator_first_message() {
        let mut dedup = MessageDeduplicator::new(100, Duration::from_secs(60));
        assert!(!dedup.is_duplicate(1));
        assert_eq!(dedup.seen_count(), 1);
    }

    #[test]
    fn test_deduplicator_duplicate_detection() {
        let mut dedup = MessageDeduplicator::new(100, Duration::from_secs(60));
        assert!(!dedup.is_duplicate(1));
        assert!(dedup.is_duplicate(1)); // duplicate
    }

    #[test]
    fn test_deduplicator_process_method() {
        let mut dedup = MessageDeduplicator::new(100, Duration::from_secs(60));
        assert!(dedup.process(1));  // should process (new)
        assert!(!dedup.process(1)); // should not process (duplicate)
        assert!(dedup.process(2));  // should process (new)
    }

    #[test]
    fn test_deduplicator_max_entries() {
        let mut dedup = MessageDeduplicator::new(3, Duration::from_secs(60));
        dedup.process(1);
        dedup.process(2);
        dedup.process(3);
        dedup.process(4); // should evict oldest (1)
        assert_eq!(dedup.seen_count(), 3);
        assert!(dedup.process(1)); // 1 was evicted, so not a duplicate
    }

    #[test]
    fn test_deduplicator_clear() {
        let mut dedup = MessageDeduplicator::new(100, Duration::from_secs(60));
        dedup.process(1);
        dedup.process(2);
        dedup.clear();
        assert_eq!(dedup.seen_count(), 0);
        assert!(dedup.process(1)); // not a duplicate after clear
    }

    // --- Snapshot Tests ---

    #[test]
    fn test_delivery_snapshot() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        delivery.deliver("d1".into(), "m1".into()).unwrap();
        delivery.deliver("d2".into(), "m2".into()).unwrap();

        let snap = delivery.snapshot();
        assert_eq!(snap.unconfirmed.len(), 2);
        assert_eq!(snap.next_delivery_id, 3);
    }

    #[test]
    fn test_delivery_restore() {
        let mut delivery: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        delivery.deliver("d1".into(), "m1".into()).unwrap();
        let id2 = delivery.deliver("d2".into(), "m2".into()).unwrap();
        delivery.confirm_delivery(id2).unwrap();

        let snap = delivery.snapshot();

        let mut restored: AtLeastOnceDelivery<String> = AtLeastOnceDelivery::with_defaults();
        restored.restore(snap);

        assert_eq!(restored.unconfirmed_count(), 1);
        assert_eq!(restored.stats().total_confirmed, 1);
    }

    // --- Builder Tests ---

    #[test]
    fn test_delivery_builder() {
        let delivery: AtLeastOnceDelivery<String> = DeliveryBuilder::new()
            .redeliver_interval(Duration::from_secs(10))
            .max_redeliveries(20)
            .warn_after(8)
            .max_unconfirmed(5000)
            .build();

        assert_eq!(delivery.config().redeliver_interval, Duration::from_secs(10));
        assert_eq!(delivery.config().max_redeliveries, 20);
        assert_eq!(delivery.config().warn_after_redeliveries, 8);
        assert_eq!(delivery.config().max_unconfirmed, 5000);
    }

    #[test]
    fn test_delivery_builder_default() {
        let delivery: AtLeastOnceDelivery<String> = DeliveryBuilder::default().build();
        assert_eq!(delivery.config().redeliver_interval, Duration::from_secs(5));
        assert_eq!(delivery.config().max_redeliveries, 10);
    }
}
