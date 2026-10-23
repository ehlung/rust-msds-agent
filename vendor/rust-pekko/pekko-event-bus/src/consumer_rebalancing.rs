// consumer_rebalancing.rs - Advanced Consumer Rebalancing (Phase 3)
// Cooperative Sticky Assignor, Rack-Aware Assignment, Generation Tracking

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::consumer_group::{ConsumerId, PartitionAssignment, PartitionAssignor};

// ============================================================================
// Rebalance Protocol Mode
// ============================================================================

/// Kafka KIP-415 style rebalance modes
#[derive(Debug, Clone, PartialEq)]
pub enum RebalanceProtocol {
    /// Eager: revoke all partitions then reassign (stop-the-world)
    Eager,
    /// Cooperative: only revoke partitions that need to move (incremental)
    Cooperative,
}

impl std::fmt::Display for RebalanceProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RebalanceProtocol::Eager => write!(f, "Eager"),
            RebalanceProtocol::Cooperative => write!(f, "Cooperative"),
        }
    }
}

// ============================================================================
// Member Epoch / Generation Tracking
// ============================================================================

/// Tracks generation and epoch for filtering stale requests
#[derive(Debug, Clone, PartialEq)]
pub struct MemberEpoch {
    pub member_id: ConsumerId,
    pub generation: u64,
    pub epoch: u64,
    pub joined_at: Instant,
    pub last_updated: Instant,
}

impl MemberEpoch {
    pub fn new(member_id: &str, generation: u64) -> Self {
        let now = Instant::now();
        Self {
            member_id: member_id.to_string(),
            generation,
            epoch: 0,
            joined_at: now,
            last_updated: now,
        }
    }

    pub fn advance_epoch(&mut self) {
        self.epoch += 1;
        self.last_updated = Instant::now();
    }

    pub fn update_generation(&mut self, generation: u64) {
        self.generation = generation;
        self.epoch = 0;
        self.last_updated = Instant::now();
    }

    /// Check if a request is stale (from an older generation/epoch)
    pub fn is_stale(&self, generation: u64, epoch: u64) -> bool {
        generation < self.generation || (generation == self.generation && epoch < self.epoch)
    }
}

// ============================================================================
// Member Metadata (Rack Awareness, Capabilities)
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub struct ConsumerMetadata {
    pub consumer_id: ConsumerId,
    pub rack_id: Option<String>,
    pub supported_assignors: Vec<String>,
    pub client_host: Option<String>,
    pub custom: HashMap<String, String>,
}

impl ConsumerMetadata {
    pub fn new(consumer_id: &str) -> Self {
        Self {
            consumer_id: consumer_id.to_string(),
            rack_id: None,
            supported_assignors: vec!["cooperative-sticky".to_string()],
            client_host: None,
            custom: HashMap::new(),
        }
    }

    pub fn with_rack(mut self, rack: &str) -> Self {
        self.rack_id = Some(rack.to_string());
        self
    }

    pub fn with_host(mut self, host: &str) -> Self {
        self.client_host = Some(host.to_string());
        self
    }

    pub fn with_custom(mut self, key: &str, value: &str) -> Self {
        self.custom.insert(key.to_string(), value.to_string());
        self
    }
}

// ============================================================================
// Rebalance Plan (Diff-based)
// ============================================================================

/// Describes the migration plan — which partitions move where
#[derive(Debug, Clone, PartialEq)]
pub struct RebalancePlan {
    pub revocations: HashMap<ConsumerId, Vec<PartitionAssignment>>,
    pub new_assignments: HashMap<ConsumerId, Vec<PartitionAssignment>>,
    pub unchanged: HashMap<ConsumerId, Vec<PartitionAssignment>>,
    pub movement_count: usize,
}

impl RebalancePlan {
    pub fn empty() -> Self {
        Self {
            revocations: HashMap::new(),
            new_assignments: HashMap::new(),
            unchanged: HashMap::new(),
            movement_count: 0,
        }
    }

    /// Compute from old and new assignment maps
    pub fn compute(
        old: &HashMap<ConsumerId, Vec<PartitionAssignment>>,
        new: &HashMap<ConsumerId, Vec<PartitionAssignment>>,
    ) -> Self {
        let mut revocations: HashMap<ConsumerId, Vec<PartitionAssignment>> = HashMap::new();
        let mut new_assignments: HashMap<ConsumerId, Vec<PartitionAssignment>> = HashMap::new();
        let mut unchanged: HashMap<ConsumerId, Vec<PartitionAssignment>> = HashMap::new();
        let mut movement_count = 0;

        // Build reverse map: partition → consumer for old
        let mut old_owner: HashMap<(String, u32), ConsumerId> = HashMap::new();
        for (consumer, parts) in old {
            for p in parts {
                old_owner.insert((p.topic.clone(), p.partition_id), consumer.clone());
            }
        }

        // Build reverse map for new
        let mut new_owner: HashMap<(String, u32), ConsumerId> = HashMap::new();
        for (consumer, parts) in new {
            for p in parts {
                new_owner.insert((p.topic.clone(), p.partition_id), consumer.clone());
            }
        }

        // Find revoked: was in old but not same consumer in new
        for (consumer, parts) in old {
            for p in parts {
                let key = (p.topic.clone(), p.partition_id);
                match new_owner.get(&key) {
                    Some(new_consumer) if new_consumer == consumer => {
                        // Unchanged
                        unchanged.entry(consumer.clone()).or_default().push(p.clone());
                    }
                    _ => {
                        // Revoked from this consumer
                        revocations.entry(consumer.clone()).or_default().push(p.clone());
                        movement_count += 1;
                    }
                }
            }
        }

        // Find newly assigned: in new but not same consumer in old
        for (consumer, parts) in new {
            for p in parts {
                let key = (p.topic.clone(), p.partition_id);
                match old_owner.get(&key) {
                    Some(old_consumer) if old_consumer == consumer => {
                        // Already counted as unchanged
                    }
                    _ => {
                        new_assignments.entry(consumer.clone()).or_default().push(p.clone());
                    }
                }
            }
        }

        Self { revocations, new_assignments, unchanged, movement_count }
    }

    pub fn total_revocations(&self) -> usize {
        self.revocations.values().map(|v| v.len()).sum()
    }

    pub fn total_new_assignments(&self) -> usize {
        self.new_assignments.values().map(|v| v.len()).sum()
    }

    pub fn total_unchanged(&self) -> usize {
        self.unchanged.values().map(|v| v.len()).sum()
    }
}

impl std::fmt::Display for RebalancePlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RebalancePlan(moved={}, revoked={}, assigned={}, kept={})",
            self.movement_count,
            self.total_revocations(),
            self.total_new_assignments(),
            self.total_unchanged()
        )
    }
}

// ============================================================================
// Cooperative Sticky Assignor
// ============================================================================

/// Kafka KIP-429 style cooperative sticky assignor.
/// Minimizes partition movement across rebalances by preserving previous assignments
/// and only moving partitions when necessary for balance.
pub struct CooperativeStickyAssignor {
    /// Previous assignment cache for stickiness
    prev_assignments: HashMap<ConsumerId, Vec<PartitionAssignment>>,
    /// How much to weight stickiness vs balance (0.0 = pure balance, 1.0 = max sticky)
    stickiness_weight: f64,
}

impl CooperativeStickyAssignor {
    pub fn new() -> Self {
        Self {
            prev_assignments: HashMap::new(),
            stickiness_weight: 0.8,
        }
    }

    pub fn with_stickiness(mut self, weight: f64) -> Self {
        self.stickiness_weight = weight.clamp(0.0, 1.0);
        self
    }

    /// Update previous assignments (call after each rebalance completes)
    pub fn update_previous(&mut self, assignments: HashMap<ConsumerId, Vec<PartitionAssignment>>) {
        self.prev_assignments = assignments;
    }

    pub fn previous_assignments(&self) -> &HashMap<ConsumerId, Vec<PartitionAssignment>> {
        &self.prev_assignments
    }

    pub fn stickiness_weight(&self) -> f64 {
        self.stickiness_weight
    }

    /// Core cooperative assignment algorithm
    fn cooperative_assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[PartitionAssignment],
    ) -> HashMap<ConsumerId, Vec<PartitionAssignment>> {
        let mut result: HashMap<ConsumerId, Vec<PartitionAssignment>> = HashMap::new();
        for c in consumers {
            result.insert(c.clone(), Vec::new());
        }

        if consumers.is_empty() || partitions.is_empty() {
            return result;
        }

        let consumer_set: HashSet<&ConsumerId> = consumers.iter().collect();

        // Step 1: Keep sticky assignments for still-active consumers
        let mut assigned: HashSet<(String, u32)> = HashSet::new();

        for (consumer, prev_parts) in &self.prev_assignments {
            if consumer_set.contains(consumer) {
                for p in prev_parts {
                    let key = (p.topic.clone(), p.partition_id);
                    // Only keep if partition still exists
                    if partitions.iter().any(|pp| pp.topic == p.topic && pp.partition_id == p.partition_id) {
                        if !assigned.contains(&key) {
                            result.entry(consumer.clone()).or_default().push(p.clone());
                            assigned.insert(key);
                        }
                    }
                }
            }
        }

        // Step 2: Collect unassigned partitions
        let mut unassigned: Vec<&PartitionAssignment> = partitions.iter()
            .filter(|p| !assigned.contains(&(p.topic.clone(), p.partition_id)))
            .collect();

        // Step 3: Assign unassigned partitions to least-loaded consumers
        unassigned.sort_by_key(|p| (p.topic.clone(), p.partition_id));
        for p in unassigned {
            let least_loaded = consumers.iter()
                .min_by_key(|c| result.get(*c).map(|v| v.len()).unwrap_or(0))
                .unwrap();
            result.get_mut(least_loaded).unwrap().push(p.clone());
        }

        // Sort consumers by current load descending for stealing
        let mut sorted_consumers: Vec<ConsumerId> = consumers.to_vec();
        sorted_consumers.sort_by(|a, b| {
            let la = result.get(a).map(|v| v.len()).unwrap_or(0);
            let lb = result.get(b).map(|v| v.len()).unwrap_or(0);
            lb.cmp(&la) // descending
        });

        // Simple steal: from overloaded to underloaded
        loop {
            let loads: Vec<(ConsumerId, usize)> = sorted_consumers.iter()
                .map(|c| (c.clone(), result.get(c).map(|v| v.len()).unwrap_or(0)))
                .collect();

            let max_load = loads.iter().map(|(_, l)| *l).max().unwrap_or(0);
            let min_load = loads.iter().map(|(_, l)| *l).min().unwrap_or(0);

            if max_load <= min_load + 1 {
                break; // balanced enough
            }

            // Find most overloaded and most underloaded
            let overloaded = loads.iter().find(|(_, l)| *l == max_load).map(|(c, _)| c.clone());
            let underloaded = loads.iter().rev().find(|(_, l)| *l == min_load).map(|(c, _)| c.clone());

            if let (Some(from), Some(to)) = (overloaded, underloaded) {
                if from == to { break; }
                // Prefer moving non-sticky partitions first
                let parts = result.get(&from).cloned().unwrap_or_default();
                let non_sticky_idx = parts.iter().position(|p| {
                    !self.prev_assignments.get(&from)
                        .map(|prev| prev.iter().any(|pp| pp.topic == p.topic && pp.partition_id == p.partition_id))
                        .unwrap_or(false)
                });

                let move_idx = non_sticky_idx.unwrap_or(parts.len().saturating_sub(1));
                if let Some(parts_from) = result.get_mut(&from) {
                    if !parts_from.is_empty() {
                        let moved = parts_from.remove(move_idx.min(parts_from.len().saturating_sub(1)));
                        result.get_mut(&to).unwrap().push(moved);
                    } else {
                        break;
                    }
                }
            } else {
                break;
            }
        }

        result
    }

    /// Compute a RebalancePlan by diffing previous and new assignments
    pub fn plan_rebalance(
        &self,
        consumers: &[ConsumerId],
        partitions: &[PartitionAssignment],
    ) -> RebalancePlan {
        let new_assignments = self.cooperative_assign(consumers, partitions);
        RebalancePlan::compute(&self.prev_assignments, &new_assignments)
    }
}

impl PartitionAssignor for CooperativeStickyAssignor {
    fn assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[PartitionAssignment],
    ) -> HashMap<ConsumerId, Vec<PartitionAssignment>> {
        self.cooperative_assign(consumers, partitions)
    }

    fn name(&self) -> &str {
        "cooperative-sticky"
    }
}

// ============================================================================
// Rack-Aware Assignor
// ============================================================================

/// Partition metadata including rack location
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionRackInfo {
    pub topic: String,
    pub partition_id: u32,
    pub preferred_rack: Option<String>,
    pub replica_racks: Vec<String>,
}

/// Assigns partitions preferring consumers in the same rack as the partition leader
pub struct RackAwareAssignor {
    /// Consumer ID → rack ID mapping
    consumer_racks: HashMap<ConsumerId, String>,
    /// Partition → rack info
    partition_racks: Vec<PartitionRackInfo>,
    /// Fallback assignor when rack info unavailable
    fallback: CooperativeStickyAssignor,
}

impl RackAwareAssignor {
    pub fn new() -> Self {
        Self {
            consumer_racks: HashMap::new(),
            partition_racks: Vec::new(),
            fallback: CooperativeStickyAssignor::new(),
        }
    }

    pub fn set_consumer_rack(&mut self, consumer_id: &str, rack: &str) {
        self.consumer_racks.insert(consumer_id.to_string(), rack.to_string());
    }

    pub fn set_partition_rack(&mut self, info: PartitionRackInfo) {
        self.partition_racks.push(info);
    }

    pub fn consumer_racks(&self) -> &HashMap<ConsumerId, String> {
        &self.consumer_racks
    }

    fn rack_aware_assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[PartitionAssignment],
    ) -> HashMap<ConsumerId, Vec<PartitionAssignment>> {
        let mut result: HashMap<ConsumerId, Vec<PartitionAssignment>> = HashMap::new();
        for c in consumers {
            result.insert(c.clone(), Vec::new());
        }

        if consumers.is_empty() || partitions.is_empty() {
            return result;
        }

        // Build rack lookup for partitions
        let part_rack_map: HashMap<(String, u32), &str> = self.partition_racks.iter()
            .filter_map(|pi| {
                pi.preferred_rack.as_deref()
                    .map(|r| ((pi.topic.clone(), pi.partition_id), r))
            })
            .collect();

        let mut assigned: HashSet<(String, u32)> = HashSet::new();

        // Phase 1: Assign partitions to consumers in same rack
        for p in partitions {
            let key = (p.topic.clone(), p.partition_id);
            if let Some(preferred_rack) = part_rack_map.get(&key) {
                // Find consumer in same rack with fewest assignments
                let same_rack: Vec<&ConsumerId> = consumers.iter()
                    .filter(|c| self.consumer_racks.get(*c).map(|r| r.as_str()) == Some(*preferred_rack))
                    .collect();

                if !same_rack.is_empty() {
                    let best = same_rack.iter()
                        .min_by_key(|c| result.get(**c).map(|v| v.len()).unwrap_or(0))
                        .unwrap();
                    result.get_mut(*best).unwrap().push(p.clone());
                    assigned.insert(key);
                }
            }
        }

        // Phase 2: Assign remaining to least-loaded consumers
        for p in partitions {
            let key = (p.topic.clone(), p.partition_id);
            if !assigned.contains(&key) {
                let least_loaded = consumers.iter()
                    .min_by_key(|c| result.get(*c).map(|v| v.len()).unwrap_or(0))
                    .unwrap();
                result.get_mut(least_loaded).unwrap().push(p.clone());
            }
        }

        result
    }
}

impl PartitionAssignor for RackAwareAssignor {
    fn assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[PartitionAssignment],
    ) -> HashMap<ConsumerId, Vec<PartitionAssignment>> {
        // Fall back to cooperative-sticky if no rack info available
        if self.consumer_racks.is_empty() {
            return self.fallback.assign(consumers, partitions);
        }
        self.rack_aware_assign(consumers, partitions)
    }

    fn name(&self) -> &str {
        "rack-aware"
    }
}

// ============================================================================
// Rebalance Timeout & Backoff
// ============================================================================

/// Configures retry behavior for failed rebalances
#[derive(Debug, Clone)]
pub struct RebalanceRetryConfig {
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_multiplier: f64,
    pub jitter_factor: f64,
}

impl Default for RebalanceRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }
}

impl RebalanceRetryConfig {
    /// Calculate backoff duration for a given retry attempt
    pub fn backoff_duration(&self, attempt: u32) -> Duration {
        let base_ms = self.initial_backoff.as_millis() as f64
            * self.backoff_multiplier.powi(attempt as i32);
        let capped_ms = base_ms.min(self.max_backoff.as_millis() as f64);
        // Deterministic jitter: ±jitter_factor based on attempt number
        let jitter = capped_ms * self.jitter_factor * ((attempt % 3) as f64 - 1.0);
        let final_ms = (capped_ms + jitter).max(0.0);
        Duration::from_millis(final_ms as u64)
    }

    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_retries
    }
}

// ============================================================================
// Rebalance Tracker
// ============================================================================

/// Tracks rebalance history and metrics for a consumer group
#[derive(Debug, Clone)]
pub struct RebalanceTracker {
    group_id: String,
    protocol: RebalanceProtocol,
    member_epochs: HashMap<ConsumerId, MemberEpoch>,
    current_generation: u64,
    rebalance_count: u64,
    last_rebalance_at: Option<Instant>,
    last_rebalance_duration: Option<Duration>,
    total_movements: u64,
    retry_config: RebalanceRetryConfig,
    history: Vec<RebalanceRecord>,
}

/// A snapshot of a completed rebalance
#[derive(Debug, Clone)]
pub struct RebalanceRecord {
    pub generation: u64,
    pub timestamp: Instant,
    pub duration: Duration,
    pub protocol: RebalanceProtocol,
    pub member_count: usize,
    pub partition_count: usize,
    pub movements: usize,
    pub trigger: RebalanceTrigger,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RebalanceTrigger {
    MemberJoined(ConsumerId),
    MemberLeft(ConsumerId),
    MemberTimedOut(ConsumerId),
    TopicSubscriptionChanged,
    PartitionCountChanged,
    Manual,
}

impl std::fmt::Display for RebalanceTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RebalanceTrigger::MemberJoined(id) => write!(f, "MemberJoined({})", id),
            RebalanceTrigger::MemberLeft(id) => write!(f, "MemberLeft({})", id),
            RebalanceTrigger::MemberTimedOut(id) => write!(f, "MemberTimedOut({})", id),
            RebalanceTrigger::TopicSubscriptionChanged => write!(f, "TopicSubscriptionChanged"),
            RebalanceTrigger::PartitionCountChanged => write!(f, "PartitionCountChanged"),
            RebalanceTrigger::Manual => write!(f, "Manual"),
        }
    }
}

impl RebalanceTracker {
    pub fn new(group_id: &str, protocol: RebalanceProtocol) -> Self {
        Self {
            group_id: group_id.to_string(),
            protocol,
            member_epochs: HashMap::new(),
            current_generation: 0,
            rebalance_count: 0,
            last_rebalance_at: None,
            last_rebalance_duration: None,
            total_movements: 0,
            retry_config: RebalanceRetryConfig::default(),
            history: Vec::new(),
        }
    }

    pub fn with_retry_config(mut self, config: RebalanceRetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    pub fn protocol(&self) -> &RebalanceProtocol {
        &self.protocol
    }

    pub fn current_generation(&self) -> u64 {
        self.current_generation
    }

    pub fn rebalance_count(&self) -> u64 {
        self.rebalance_count
    }

    pub fn total_movements(&self) -> u64 {
        self.total_movements
    }

    pub fn last_rebalance_duration(&self) -> Option<Duration> {
        self.last_rebalance_duration
    }

    pub fn history(&self) -> &[RebalanceRecord] {
        &self.history
    }

    /// Register a member joining
    pub fn member_joined(&mut self, consumer_id: &str) {
        self.current_generation += 1;
        let epoch = MemberEpoch::new(consumer_id, self.current_generation);
        self.member_epochs.insert(consumer_id.to_string(), epoch);
    }

    /// Register a member leaving
    pub fn member_left(&mut self, consumer_id: &str) {
        self.member_epochs.remove(consumer_id);
        self.current_generation += 1;
    }

    /// Check if a request from a member is stale
    pub fn is_stale_request(&self, consumer_id: &str, generation: u64, epoch: u64) -> bool {
        self.member_epochs.get(consumer_id)
            .map(|me| me.is_stale(generation, epoch))
            .unwrap_or(true) // unknown member = stale
    }

    /// Record a completed rebalance
    pub fn record_rebalance(
        &mut self,
        duration: Duration,
        member_count: usize,
        partition_count: usize,
        movements: usize,
        trigger: RebalanceTrigger,
    ) {
        self.rebalance_count += 1;
        self.total_movements += movements as u64;
        self.last_rebalance_at = Some(Instant::now());
        self.last_rebalance_duration = Some(duration);

        // Advance epochs for all members
        for epoch in self.member_epochs.values_mut() {
            epoch.update_generation(self.current_generation);
        }

        self.history.push(RebalanceRecord {
            generation: self.current_generation,
            timestamp: Instant::now(),
            duration,
            protocol: self.protocol.clone(),
            member_count,
            partition_count,
            movements,
            trigger,
        });

        // Keep only last 100 records
        if self.history.len() > 100 {
            self.history.drain(0..self.history.len() - 100);
        }
    }

    /// Get retry config
    pub fn retry_config(&self) -> &RebalanceRetryConfig {
        &self.retry_config
    }

    /// Average movements per rebalance
    pub fn avg_movements_per_rebalance(&self) -> f64 {
        if self.rebalance_count == 0 {
            0.0
        } else {
            self.total_movements as f64 / self.rebalance_count as f64
        }
    }

    /// Average rebalance duration
    pub fn avg_rebalance_duration(&self) -> Option<Duration> {
        if self.history.is_empty() {
            return None;
        }
        let total_ms: u64 = self.history.iter()
            .map(|r| r.duration.as_millis() as u64)
            .sum();
        Some(Duration::from_millis(total_ms / self.history.len() as u64))
    }
}

impl std::fmt::Display for RebalanceTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RebalanceTracker(group={}, gen={}, rebalances={}, total_moves={}, protocol={})",
            self.group_id, self.current_generation, self.rebalance_count,
            self.total_movements, self.protocol
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_partitions(topic: &str, count: u32) -> Vec<PartitionAssignment> {
        (0..count).map(|i| PartitionAssignment {
            topic: topic.to_string(),
            partition_id: i,
        }).collect()
    }

    fn make_consumers(count: usize) -> Vec<ConsumerId> {
        (0..count).map(|i| format!("consumer-{}", i)).collect()
    }

    // --- RebalanceProtocol ---

    #[test]
    fn test_rebalance_protocol_display() {
        assert_eq!(RebalanceProtocol::Eager.to_string(), "Eager");
        assert_eq!(RebalanceProtocol::Cooperative.to_string(), "Cooperative");
    }

    // --- MemberEpoch ---

    #[test]
    fn test_member_epoch_new() {
        let epoch = MemberEpoch::new("c1", 1);
        assert_eq!(epoch.member_id, "c1");
        assert_eq!(epoch.generation, 1);
        assert_eq!(epoch.epoch, 0);
    }

    #[test]
    fn test_member_epoch_advance() {
        let mut epoch = MemberEpoch::new("c1", 1);
        epoch.advance_epoch();
        assert_eq!(epoch.epoch, 1);
        epoch.advance_epoch();
        assert_eq!(epoch.epoch, 2);
    }

    #[test]
    fn test_member_epoch_update_generation() {
        let mut epoch = MemberEpoch::new("c1", 1);
        epoch.advance_epoch();
        epoch.advance_epoch();
        assert_eq!(epoch.epoch, 2);

        epoch.update_generation(2);
        assert_eq!(epoch.generation, 2);
        assert_eq!(epoch.epoch, 0); // reset on generation change
    }

    #[test]
    fn test_member_epoch_stale_detection() {
        let mut epoch = MemberEpoch::new("c1", 5);
        epoch.advance_epoch(); // epoch now = 1

        assert!(epoch.is_stale(4, 0)); // older generation
        assert!(epoch.is_stale(5, 0)); // same gen, older epoch
        assert!(!epoch.is_stale(5, 1)); // current gen+epoch
        assert!(!epoch.is_stale(5, 2)); // same gen, newer epoch
        assert!(!epoch.is_stale(6, 0)); // newer generation
    }

    // --- ConsumerMetadata ---

    #[test]
    fn test_consumer_metadata_builder() {
        let meta = ConsumerMetadata::new("c1")
            .with_rack("us-east-1a")
            .with_host("10.0.0.1")
            .with_custom("version", "2.0");

        assert_eq!(meta.consumer_id, "c1");
        assert_eq!(meta.rack_id.as_deref(), Some("us-east-1a"));
        assert_eq!(meta.client_host.as_deref(), Some("10.0.0.1"));
        assert_eq!(meta.custom.get("version").unwrap(), "2.0");
    }

    #[test]
    fn test_consumer_metadata_default_assignors() {
        let meta = ConsumerMetadata::new("c1");
        assert!(meta.supported_assignors.contains(&"cooperative-sticky".to_string()));
    }

    // --- RebalancePlan ---

    #[test]
    fn test_rebalance_plan_empty() {
        let plan = RebalancePlan::empty();
        assert_eq!(plan.movement_count, 0);
        assert_eq!(plan.total_revocations(), 0);
        assert_eq!(plan.total_new_assignments(), 0);
    }

    #[test]
    fn test_rebalance_plan_compute_no_change() {
        let mut old = HashMap::new();
        old.insert("c1".to_string(), vec![
            PartitionAssignment { topic: "t".into(), partition_id: 0 },
        ]);
        let new = old.clone();

        let plan = RebalancePlan::compute(&old, &new);
        assert_eq!(plan.movement_count, 0);
        assert_eq!(plan.total_unchanged(), 1);
    }

    #[test]
    fn test_rebalance_plan_compute_movement() {
        let mut old = HashMap::new();
        old.insert("c1".to_string(), vec![
            PartitionAssignment { topic: "t".into(), partition_id: 0 },
            PartitionAssignment { topic: "t".into(), partition_id: 1 },
        ]);

        let mut new = HashMap::new();
        new.insert("c1".to_string(), vec![
            PartitionAssignment { topic: "t".into(), partition_id: 0 },
        ]);
        new.insert("c2".to_string(), vec![
            PartitionAssignment { topic: "t".into(), partition_id: 1 },
        ]);

        let plan = RebalancePlan::compute(&old, &new);
        assert_eq!(plan.movement_count, 1); // p1 moved from c1 to c2
        assert_eq!(plan.total_unchanged(), 1); // p0 stayed with c1
        assert_eq!(plan.total_new_assignments(), 1); // p1 assigned to c2
    }

    #[test]
    fn test_rebalance_plan_display() {
        let plan = RebalancePlan::empty();
        let display = plan.to_string();
        assert!(display.contains("RebalancePlan"));
        assert!(display.contains("moved=0"));
    }

    // --- CooperativeStickyAssignor ---

    #[test]
    fn test_cooperative_sticky_initial_assignment() {
        let assignor = CooperativeStickyAssignor::new();
        let consumers = make_consumers(3);
        let partitions = make_partitions("orders", 6);

        let result = assignor.assign(&consumers, &partitions);
        assert_eq!(result.len(), 3);
        assert_eq!(result["consumer-0"].len(), 2);
        assert_eq!(result["consumer-1"].len(), 2);
        assert_eq!(result["consumer-2"].len(), 2);
    }

    #[test]
    fn test_cooperative_sticky_preserves_assignments() {
        let mut assignor = CooperativeStickyAssignor::new();
        let consumers = make_consumers(2);
        let partitions = make_partitions("orders", 4);

        // Initial assignment
        let result1 = assignor.assign(&consumers, &partitions);
        assignor.update_previous(result1.clone());

        // Same consumers, same partitions — should be identical
        let result2 = assignor.assign(&consumers, &partitions);
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_cooperative_sticky_new_consumer_joins() {
        let mut assignor = CooperativeStickyAssignor::new();
        let partitions = make_partitions("orders", 6);

        // 2 consumers initially
        let consumers_2 = make_consumers(2);
        let result1 = assignor.assign(&consumers_2, &partitions);
        assignor.update_previous(result1.clone());

        // 3rd consumer joins
        let consumers_3 = make_consumers(3);
        let result2 = assignor.assign(&consumers_3, &partitions);

        // New consumer should get some partitions
        assert!(!result2["consumer-2"].is_empty());

        // Total should still be 6
        let total: usize = result2.values().map(|v| v.len()).sum();
        assert_eq!(total, 6);
    }

    #[test]
    fn test_cooperative_sticky_consumer_leaves() {
        let mut assignor = CooperativeStickyAssignor::new();
        let partitions = make_partitions("orders", 6);

        // 3 consumers initially
        let consumers_3 = make_consumers(3);
        let result1 = assignor.assign(&consumers_3, &partitions);
        assignor.update_previous(result1.clone());

        // Consumer 2 leaves
        let consumers_2 = make_consumers(2);
        let result2 = assignor.assign(&consumers_2, &partitions);

        // All 6 partitions should still be assigned
        let total: usize = result2.values().map(|v| v.len()).sum();
        assert_eq!(total, 6);
    }

    #[test]
    fn test_cooperative_sticky_empty_consumers() {
        let assignor = CooperativeStickyAssignor::new();
        let result = assignor.assign(&[], &make_partitions("t", 3));
        assert!(result.is_empty());
    }

    #[test]
    fn test_cooperative_sticky_empty_partitions() {
        let assignor = CooperativeStickyAssignor::new();
        let result = assignor.assign(&make_consumers(2), &[]);
        assert!(result["consumer-0"].is_empty());
        assert!(result["consumer-1"].is_empty());
    }

    #[test]
    fn test_cooperative_sticky_plan_rebalance() {
        let mut assignor = CooperativeStickyAssignor::new();
        let partitions = make_partitions("orders", 4);

        let consumers_2 = make_consumers(2);
        let result1 = assignor.assign(&consumers_2, &partitions);
        assignor.update_previous(result1);

        // 3rd consumer joins — should have minimal movements
        let consumers_3 = make_consumers(3);
        let plan = assignor.plan_rebalance(&consumers_3, &partitions);

        // At most 2 partitions should move (one from each existing consumer)
        assert!(plan.movement_count <= 2);
    }

    #[test]
    fn test_cooperative_sticky_stickiness_weight() {
        let assignor = CooperativeStickyAssignor::new().with_stickiness(0.5);
        assert!((assignor.stickiness_weight() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cooperative_sticky_name() {
        let assignor = CooperativeStickyAssignor::new();
        assert_eq!(assignor.name(), "cooperative-sticky");
    }

    // --- RackAwareAssignor ---

    #[test]
    fn test_rack_aware_same_rack_preference() {
        let mut assignor = RackAwareAssignor::new();
        assignor.set_consumer_rack("c1", "rack-a");
        assignor.set_consumer_rack("c2", "rack-b");

        assignor.set_partition_rack(PartitionRackInfo {
            topic: "t".into(), partition_id: 0,
            preferred_rack: Some("rack-a".into()), replica_racks: vec![],
        });
        assignor.set_partition_rack(PartitionRackInfo {
            topic: "t".into(), partition_id: 1,
            preferred_rack: Some("rack-b".into()), replica_racks: vec![],
        });

        let consumers = vec!["c1".to_string(), "c2".to_string()];
        let partitions = make_partitions("t", 2);

        let result = assignor.assign(&consumers, &partitions);

        // c1 should get partition 0 (rack-a), c2 should get partition 1 (rack-b)
        assert!(result["c1"].iter().any(|p| p.partition_id == 0));
        assert!(result["c2"].iter().any(|p| p.partition_id == 1));
    }

    #[test]
    fn test_rack_aware_fallback_no_rack_info() {
        let assignor = RackAwareAssignor::new();
        let consumers = make_consumers(2);
        let partitions = make_partitions("t", 4);

        let result = assignor.assign(&consumers, &partitions);
        // Falls back to cooperative-sticky
        let total: usize = result.values().map(|v| v.len()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_rack_aware_unmatched_partitions() {
        let mut assignor = RackAwareAssignor::new();
        assignor.set_consumer_rack("c1", "rack-a");
        assignor.set_consumer_rack("c2", "rack-a");

        // Partition prefers rack-b but no consumers in rack-b
        assignor.set_partition_rack(PartitionRackInfo {
            topic: "t".into(), partition_id: 0,
            preferred_rack: Some("rack-b".into()), replica_racks: vec![],
        });

        let consumers = vec!["c1".to_string(), "c2".to_string()];
        let partitions = make_partitions("t", 1);

        let result = assignor.assign(&consumers, &partitions);
        // Should still be assigned to someone
        let total: usize = result.values().map(|v| v.len()).sum();
        assert_eq!(total, 1);
    }

    #[test]
    fn test_rack_aware_name() {
        let assignor = RackAwareAssignor::new();
        assert_eq!(assignor.name(), "rack-aware");
    }

    // --- RebalanceRetryConfig ---

    #[test]
    fn test_retry_config_defaults() {
        let config = RebalanceRetryConfig::default();
        assert_eq!(config.max_retries, 5);
        assert!(config.should_retry(0));
        assert!(config.should_retry(4));
        assert!(!config.should_retry(5));
    }

    #[test]
    fn test_retry_backoff_exponential() {
        let config = RebalanceRetryConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0, // no jitter for predictable test
            max_retries: 5,
        };

        let d0 = config.backoff_duration(0);
        let d1 = config.backoff_duration(1);
        let d2 = config.backoff_duration(2);

        assert_eq!(d0.as_millis(), 100);
        assert_eq!(d1.as_millis(), 200);
        assert_eq!(d2.as_millis(), 400);
    }

    #[test]
    fn test_retry_backoff_capped() {
        let config = RebalanceRetryConfig {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 10.0,
            jitter_factor: 0.0,
            max_retries: 10,
        };

        let d3 = config.backoff_duration(3);
        assert!(d3 <= Duration::from_secs(5));
    }

    // --- RebalanceTracker ---

    #[test]
    fn test_tracker_new() {
        let tracker = RebalanceTracker::new("group-1", RebalanceProtocol::Cooperative);
        assert_eq!(tracker.current_generation(), 0);
        assert_eq!(tracker.rebalance_count(), 0);
        assert_eq!(tracker.total_movements(), 0);
    }

    #[test]
    fn test_tracker_member_lifecycle() {
        let mut tracker = RebalanceTracker::new("group-1", RebalanceProtocol::Cooperative);

        tracker.member_joined("c1");
        assert_eq!(tracker.current_generation(), 1);

        tracker.member_joined("c2");
        assert_eq!(tracker.current_generation(), 2);

        tracker.member_left("c1");
        assert_eq!(tracker.current_generation(), 3);
    }

    #[test]
    fn test_tracker_stale_request() {
        let mut tracker = RebalanceTracker::new("group-1", RebalanceProtocol::Cooperative);
        tracker.member_joined("c1"); // gen=1

        assert!(!tracker.is_stale_request("c1", 1, 0));
        assert!(tracker.is_stale_request("c1", 0, 0)); // older gen
        assert!(tracker.is_stale_request("unknown", 1, 0)); // unknown member
    }

    #[test]
    fn test_tracker_record_rebalance() {
        let mut tracker = RebalanceTracker::new("group-1", RebalanceProtocol::Cooperative);
        tracker.member_joined("c1");
        tracker.member_joined("c2");

        tracker.record_rebalance(
            Duration::from_millis(50),
            2, 6, 3,
            RebalanceTrigger::MemberJoined("c2".to_string()),
        );

        assert_eq!(tracker.rebalance_count(), 1);
        assert_eq!(tracker.total_movements(), 3);
        assert!(tracker.last_rebalance_duration().is_some());
        assert_eq!(tracker.history().len(), 1);
    }

    #[test]
    fn test_tracker_avg_movements() {
        let mut tracker = RebalanceTracker::new("group-1", RebalanceProtocol::Cooperative);
        tracker.member_joined("c1");

        tracker.record_rebalance(Duration::from_millis(10), 2, 6, 2, RebalanceTrigger::Manual);
        tracker.record_rebalance(Duration::from_millis(20), 3, 6, 4, RebalanceTrigger::Manual);

        assert!((tracker.avg_movements_per_rebalance() - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_tracker_display() {
        let tracker = RebalanceTracker::new("group-1", RebalanceProtocol::Cooperative);
        let display = tracker.to_string();
        assert!(display.contains("group-1"));
        assert!(display.contains("Cooperative"));
    }

    #[test]
    fn test_rebalance_trigger_display() {
        let trigger = RebalanceTrigger::MemberJoined("c1".to_string());
        assert_eq!(trigger.to_string(), "MemberJoined(c1)");

        let trigger = RebalanceTrigger::Manual;
        assert_eq!(trigger.to_string(), "Manual");
    }

    #[test]
    fn test_tracker_avg_duration() {
        let mut tracker = RebalanceTracker::new("g", RebalanceProtocol::Eager);
        tracker.member_joined("c1");

        tracker.record_rebalance(Duration::from_millis(100), 1, 3, 0, RebalanceTrigger::Manual);
        tracker.record_rebalance(Duration::from_millis(200), 1, 3, 0, RebalanceTrigger::Manual);

        let avg = tracker.avg_rebalance_duration().unwrap();
        assert_eq!(avg.as_millis(), 150);
    }
}
