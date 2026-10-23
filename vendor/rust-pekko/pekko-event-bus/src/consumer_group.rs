// consumer_group.rs - Consumer Group Coordinator
// Partition assignment, offset tracking, rebalancing protocol

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::bus_config::{ConsumerGroupConfig, RebalanceStrategy};
use crate::bus_errors::EventBusError;

// ============================================================================
// Consumer Identity
// ============================================================================

pub type ConsumerId = String;
pub type GroupId = String;

#[derive(Debug, Clone)]
pub struct ConsumerMember {
    pub consumer_id: ConsumerId,
    pub joined_at: Instant,
    pub last_heartbeat: Instant,
    pub assigned_partitions: Vec<PartitionAssignment>,
    pub metadata: HashMap<String, String>,
}

impl ConsumerMember {
    pub fn new(consumer_id: &str) -> Self {
        let now = Instant::now();
        Self {
            consumer_id: consumer_id.to_string(),
            joined_at: now,
            last_heartbeat: now,
            assigned_partitions: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
    }

    pub fn is_alive(&self, session_timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() < session_timeout
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionAssignment {
    pub topic: String,
    pub partition_id: u32,
}

// ============================================================================
// Offset Tracking
// ============================================================================

/// Tracks committed offsets for a consumer group
#[derive(Debug, Clone)]
pub struct OffsetTracker {
    /// Map of (topic, partition) -> committed offset
    offsets: HashMap<(String, u32), u64>,
}

impl OffsetTracker {
    pub fn new() -> Self {
        Self {
            offsets: HashMap::new(),
        }
    }

    /// Commit an offset for a topic-partition
    pub fn commit(&mut self, topic: &str, partition: u32, offset: u64) {
        let key = (topic.to_string(), partition);
        let current = self.offsets.get(&key).copied().unwrap_or(0);
        // Only advance (never go backward)
        if offset >= current {
            self.offsets.insert(key, offset);
        }
    }

    /// Get the committed offset for a topic-partition
    pub fn committed_offset(&self, topic: &str, partition: u32) -> Option<u64> {
        self.offsets.get(&(topic.to_string(), partition)).copied()
    }

    /// Get next offset to read from (committed + 1, or 0 if not committed)
    pub fn next_read_offset(&self, topic: &str, partition: u32) -> u64 {
        self.committed_offset(topic, partition)
            .map(|o| o + 1)
            .unwrap_or(0)
    }

    /// All committed offsets
    pub fn all_offsets(&self) -> &HashMap<(String, u32), u64> {
        &self.offsets
    }

    /// Reset offsets for a topic
    pub fn reset_topic(&mut self, topic: &str) {
        self.offsets.retain(|(t, _), _| t != topic);
    }

    /// Reset all offsets
    pub fn reset_all(&mut self) {
        self.offsets.clear();
    }
}

impl Default for OffsetTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Rebalance Protocol
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum RebalanceState {
    Stable,
    Preparing,  // revoke phase
    Assigning,  // assign phase
}

#[derive(Debug, Clone)]
pub struct RebalanceEvent {
    pub event_type: RebalanceEventType,
    pub group_id: GroupId,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RebalanceEventType {
    /// Partitions being revoked from a consumer
    Revoke { consumer_id: ConsumerId, partitions: Vec<PartitionAssignment> },
    /// Partitions being assigned to a consumer
    Assign { consumer_id: ConsumerId, partitions: Vec<PartitionAssignment> },
    /// Rebalance completed
    Complete,
}

// ============================================================================
// Partition Assignors
// ============================================================================

/// Assigns partitions to consumers
pub trait PartitionAssignor: Send + Sync {
    fn assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[PartitionAssignment],
    ) -> HashMap<ConsumerId, Vec<PartitionAssignment>>;

    fn name(&self) -> &str;
}

/// Range assignor: sequential partition ranges per consumer
pub struct RangeAssignor;

impl PartitionAssignor for RangeAssignor {
    fn assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[PartitionAssignment],
    ) -> HashMap<ConsumerId, Vec<PartitionAssignment>> {
        let mut result: HashMap<ConsumerId, Vec<PartitionAssignment>> = HashMap::new();
        if consumers.is_empty() || partitions.is_empty() {
            return result;
        }

        let partitions_per_consumer = partitions.len() / consumers.len();
        let remainder = partitions.len() % consumers.len();
        let mut idx = 0;

        for (i, consumer) in consumers.iter().enumerate() {
            let extra = if i < remainder { 1 } else { 0 };
            let count = partitions_per_consumer + extra;
            let assigned: Vec<PartitionAssignment> = partitions[idx..idx + count].to_vec();
            result.insert(consumer.clone(), assigned);
            idx += count;
        }

        result
    }

    fn name(&self) -> &str {
        "range"
    }
}

/// Round-robin assignor: distributes partitions one-by-one
pub struct RoundRobinAssignor;

impl PartitionAssignor for RoundRobinAssignor {
    fn assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[PartitionAssignment],
    ) -> HashMap<ConsumerId, Vec<PartitionAssignment>> {
        let mut result: HashMap<ConsumerId, Vec<PartitionAssignment>> = HashMap::new();
        if consumers.is_empty() {
            return result;
        }

        for consumer in consumers {
            result.insert(consumer.clone(), Vec::new());
        }

        for (i, partition) in partitions.iter().enumerate() {
            let consumer = &consumers[i % consumers.len()];
            result.get_mut(consumer).unwrap().push(partition.clone());
        }

        result
    }

    fn name(&self) -> &str {
        "round-robin"
    }
}

/// Sticky assignor: minimizes partition movement during rebalance
pub struct StickyAssignor;

impl PartitionAssignor for StickyAssignor {
    fn assign(
        &self,
        consumers: &[ConsumerId],
        partitions: &[PartitionAssignment],
    ) -> HashMap<ConsumerId, Vec<PartitionAssignment>> {
        // For initial assignment, use round-robin; sticky behavior
        // is most relevant during rebalancing where we'd compare
        // previous assignments — simplified here
        let rr = RoundRobinAssignor;
        rr.assign(consumers, partitions)
    }

    fn name(&self) -> &str {
        "sticky"
    }
}

/// Create an assignor from config
pub fn create_assignor(strategy: &RebalanceStrategy) -> Box<dyn PartitionAssignor> {
    match strategy {
        RebalanceStrategy::Range => Box::new(RangeAssignor),
        RebalanceStrategy::RoundRobin => Box::new(RoundRobinAssignor),
        RebalanceStrategy::Sticky => Box::new(StickyAssignor),
    }
}

// ============================================================================
// Consumer Group Coordinator
// ============================================================================

/// Manages a single consumer group: membership, offsets, rebalancing
pub struct ConsumerGroupCoordinator {
    pub group_id: GroupId,
    config: ConsumerGroupConfig,
    members: HashMap<ConsumerId, ConsumerMember>,
    subscriptions: Vec<String>, // subscribed topics
    offsets: OffsetTracker,
    assignor: Box<dyn PartitionAssignor>,
    state: RebalanceState,
    generation: u64,
    rebalance_history: Vec<RebalanceEvent>,
}

impl ConsumerGroupCoordinator {
    pub fn new(group_id: &str, config: ConsumerGroupConfig) -> Self {
        let assignor = create_assignor(&config.rebalance_strategy);
        Self {
            group_id: group_id.to_string(),
            config,
            members: HashMap::new(),
            subscriptions: Vec::new(),
            offsets: OffsetTracker::new(),
            assignor,
            state: RebalanceState::Stable,
            generation: 0,
            rebalance_history: Vec::new(),
        }
    }

    // --- Membership ---

    /// A consumer joins the group, triggers rebalance
    pub fn join(
        &mut self,
        consumer_id: &str,
    ) -> Result<u64, EventBusError> {
        if self.members.contains_key(consumer_id) {
            return Err(EventBusError::Internal(
                format!("Consumer '{}' already in group '{}'", consumer_id, self.group_id),
            ));
        }

        self.members.insert(consumer_id.to_string(), ConsumerMember::new(consumer_id));
        self.generation += 1;

        Ok(self.generation)
    }

    /// A consumer leaves the group, triggers rebalance
    pub fn leave(&mut self, consumer_id: &str) -> Result<(), EventBusError> {
        if self.members.remove(consumer_id).is_none() {
            return Err(EventBusError::Internal(
                format!("Consumer '{}' not in group '{}'", consumer_id, self.group_id),
            ));
        }
        self.generation += 1;
        Ok(())
    }

    /// Process heartbeat from a consumer
    pub fn heartbeat(&mut self, consumer_id: &str) -> Result<(), EventBusError> {
        let member = self.members.get_mut(consumer_id)
            .ok_or_else(|| EventBusError::Internal(
                format!("Consumer '{}' not in group '{}'", consumer_id, self.group_id),
            ))?;
        member.heartbeat();
        Ok(())
    }

    /// Check for expired consumers and remove them
    pub fn check_session_timeouts(&mut self) -> Vec<ConsumerId> {
        let timeout = self.config.session_timeout;
        let expired: Vec<ConsumerId> = self.members.iter()
            .filter(|(_, m)| !m.is_alive(timeout))
            .map(|(id, _)| id.clone())
            .collect();

        for id in &expired {
            self.members.remove(id);
        }

        if !expired.is_empty() {
            self.generation += 1;
        }

        expired
    }

    // --- Subscriptions ---

    /// Subscribe the group to topics
    pub fn subscribe(&mut self, topics: Vec<String>) {
        self.subscriptions = topics;
    }

    pub fn subscriptions(&self) -> &[String] {
        &self.subscriptions
    }

    // --- Partition Assignment (Rebalance) ---

    /// Trigger a rebalance: assign partitions to active consumers
    pub fn rebalance(
        &mut self,
        available_partitions: &HashMap<String, u32>, // topic → num_partitions
    ) -> Vec<RebalanceEvent> {
        self.state = RebalanceState::Preparing;
        let mut events = Vec::new();

        // Phase 1: Revoke all current assignments
        for (consumer_id, member) in &self.members {
            if !member.assigned_partitions.is_empty() {
                events.push(RebalanceEvent {
                    event_type: RebalanceEventType::Revoke {
                        consumer_id: consumer_id.clone(),
                        partitions: member.assigned_partitions.clone(),
                    },
                    group_id: self.group_id.clone(),
                    generation: self.generation,
                });
            }
        }

        // Build full partition list from subscribed topics
        let mut all_partitions = Vec::new();
        for topic in &self.subscriptions {
            if let Some(&num_partitions) = available_partitions.get(topic) {
                for p in 0..num_partitions {
                    all_partitions.push(PartitionAssignment {
                        topic: topic.clone(),
                        partition_id: p,
                    });
                }
            }
        }

        // Phase 2: Assign
        self.state = RebalanceState::Assigning;
        let consumer_ids: Vec<ConsumerId> = self.members.keys().cloned().collect();
        let assignments = self.assignor.assign(&consumer_ids, &all_partitions);

        for (consumer_id, partitions) in &assignments {
            if let Some(member) = self.members.get_mut(consumer_id) {
                member.assigned_partitions = partitions.clone();
            }
            events.push(RebalanceEvent {
                event_type: RebalanceEventType::Assign {
                    consumer_id: consumer_id.clone(),
                    partitions: partitions.clone(),
                },
                group_id: self.group_id.clone(),
                generation: self.generation,
            });
        }

        // Phase 3: Complete
        self.state = RebalanceState::Stable;
        events.push(RebalanceEvent {
            event_type: RebalanceEventType::Complete,
            group_id: self.group_id.clone(),
            generation: self.generation,
        });

        self.rebalance_history.extend(events.clone());
        events
    }

    // --- Offset Management ---

    /// Commit an offset
    pub fn commit_offset(&mut self, topic: &str, partition: u32, offset: u64) {
        self.offsets.commit(topic, partition, offset);
    }

    /// Get committed offset
    pub fn committed_offset(&self, topic: &str, partition: u32) -> Option<u64> {
        self.offsets.committed_offset(topic, partition)
    }

    /// Get next read offset
    pub fn next_read_offset(&self, topic: &str, partition: u32) -> u64 {
        self.offsets.next_read_offset(topic, partition)
    }

    pub fn offset_tracker(&self) -> &OffsetTracker {
        &self.offsets
    }

    // --- State ---

    pub fn state(&self) -> &RebalanceState {
        &self.state
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    pub fn members(&self) -> Vec<&ConsumerMember> {
        self.members.values().collect()
    }

    pub fn get_member(&self, consumer_id: &str) -> Option<&ConsumerMember> {
        self.members.get(consumer_id)
    }

    pub fn config(&self) -> &ConsumerGroupConfig {
        &self.config
    }

    pub fn rebalance_count(&self) -> usize {
        self.rebalance_history.iter()
            .filter(|e| e.event_type == RebalanceEventType::Complete)
            .count()
    }
}

// ============================================================================
// Consumer Group Registry
// ============================================================================

/// Manages all consumer groups
pub struct ConsumerGroupRegistry {
    groups: HashMap<GroupId, ConsumerGroupCoordinator>,
    default_config: ConsumerGroupConfig,
}

impl ConsumerGroupRegistry {
    pub fn new(default_config: ConsumerGroupConfig) -> Self {
        Self {
            groups: HashMap::new(),
            default_config,
        }
    }

    /// Create a new consumer group
    pub fn create_group(
        &mut self,
        group_id: &str,
        config: Option<ConsumerGroupConfig>,
    ) -> Result<(), EventBusError> {
        if self.groups.contains_key(group_id) {
            return Err(EventBusError::ConsumerGroupAlreadyExists(group_id.to_string()));
        }

        let cfg = config.unwrap_or_else(|| self.default_config.clone());
        self.groups.insert(
            group_id.to_string(),
            ConsumerGroupCoordinator::new(group_id, cfg),
        );
        Ok(())
    }

    /// Get a mutable reference to a group coordinator
    pub fn get_group_mut(&mut self, group_id: &str) -> Result<&mut ConsumerGroupCoordinator, EventBusError> {
        self.groups.get_mut(group_id)
            .ok_or_else(|| EventBusError::ConsumerGroupNotFound(group_id.to_string()))
    }

    /// Get a reference to a group coordinator
    pub fn get_group(&self, group_id: &str) -> Result<&ConsumerGroupCoordinator, EventBusError> {
        self.groups.get(group_id)
            .ok_or_else(|| EventBusError::ConsumerGroupNotFound(group_id.to_string()))
    }

    /// Delete a consumer group
    pub fn delete_group(&mut self, group_id: &str) -> Result<(), EventBusError> {
        if self.groups.remove(group_id).is_none() {
            return Err(EventBusError::ConsumerGroupNotFound(group_id.to_string()));
        }
        Ok(())
    }

    /// List all group IDs
    pub fn list_groups(&self) -> Vec<&str> {
        self.groups.keys().map(|s| s.as_str()).collect()
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> ConsumerGroupConfig {
        ConsumerGroupConfig::default()
    }

    fn test_partitions() -> HashMap<String, u32> {
        let mut m = HashMap::new();
        m.insert("orders".to_string(), 6);
        m.insert("payments".to_string(), 3);
        m
    }

    // --- OffsetTracker tests ---

    #[test]
    fn test_offset_tracker_commit_and_read() {
        let mut tracker = OffsetTracker::new();
        tracker.commit("orders", 0, 42);
        assert_eq!(tracker.committed_offset("orders", 0), Some(42));
        assert_eq!(tracker.next_read_offset("orders", 0), 43);
    }

    #[test]
    fn test_offset_tracker_no_backward() {
        let mut tracker = OffsetTracker::new();
        tracker.commit("orders", 0, 10);
        tracker.commit("orders", 0, 5); // should not go backward
        assert_eq!(tracker.committed_offset("orders", 0), Some(10));
    }

    #[test]
    fn test_offset_tracker_no_commit() {
        let tracker = OffsetTracker::new();
        assert_eq!(tracker.committed_offset("orders", 0), None);
        assert_eq!(tracker.next_read_offset("orders", 0), 0);
    }

    #[test]
    fn test_offset_tracker_reset_topic() {
        let mut tracker = OffsetTracker::new();
        tracker.commit("orders", 0, 10);
        tracker.commit("orders", 1, 20);
        tracker.commit("payments", 0, 5);

        tracker.reset_topic("orders");
        assert_eq!(tracker.committed_offset("orders", 0), None);
        assert_eq!(tracker.committed_offset("orders", 1), None);
        assert_eq!(tracker.committed_offset("payments", 0), Some(5));
    }

    #[test]
    fn test_offset_tracker_reset_all() {
        let mut tracker = OffsetTracker::new();
        tracker.commit("orders", 0, 10);
        tracker.commit("payments", 0, 5);
        tracker.reset_all();
        assert!(tracker.all_offsets().is_empty());
    }

    // --- Partition Assignor tests ---

    #[test]
    fn test_range_assignor_even() {
        let assignor = RangeAssignor;
        let consumers = vec!["c1".to_string(), "c2".to_string()];
        let partitions = vec![
            PartitionAssignment { topic: "t".into(), partition_id: 0 },
            PartitionAssignment { topic: "t".into(), partition_id: 1 },
            PartitionAssignment { topic: "t".into(), partition_id: 2 },
            PartitionAssignment { topic: "t".into(), partition_id: 3 },
        ];

        let result = assignor.assign(&consumers, &partitions);
        assert_eq!(result["c1"].len(), 2);
        assert_eq!(result["c2"].len(), 2);
    }

    #[test]
    fn test_range_assignor_uneven() {
        let assignor = RangeAssignor;
        let consumers = vec!["c1".to_string(), "c2".to_string()];
        let partitions = vec![
            PartitionAssignment { topic: "t".into(), partition_id: 0 },
            PartitionAssignment { topic: "t".into(), partition_id: 1 },
            PartitionAssignment { topic: "t".into(), partition_id: 2 },
        ];

        let result = assignor.assign(&consumers, &partitions);
        assert_eq!(result["c1"].len(), 2); // gets remainder
        assert_eq!(result["c2"].len(), 1);
    }

    #[test]
    fn test_round_robin_assignor() {
        let assignor = RoundRobinAssignor;
        let consumers = vec!["c1".to_string(), "c2".to_string(), "c3".to_string()];
        let partitions: Vec<PartitionAssignment> = (0..6).map(|i| PartitionAssignment {
            topic: "t".into(), partition_id: i,
        }).collect();

        let result = assignor.assign(&consumers, &partitions);
        assert_eq!(result["c1"].len(), 2); // 0, 3
        assert_eq!(result["c2"].len(), 2); // 1, 4
        assert_eq!(result["c3"].len(), 2); // 2, 5
    }

    #[test]
    fn test_assignor_empty_consumers() {
        let assignor = RangeAssignor;
        let result = assignor.assign(&[], &[PartitionAssignment { topic: "t".into(), partition_id: 0 }]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_assignor_empty_partitions() {
        let assignor = RangeAssignor;
        let result = assignor.assign(&["c1".to_string()], &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_sticky_assignor() {
        let assignor = StickyAssignor;
        let consumers = vec!["c1".to_string(), "c2".to_string()];
        let partitions: Vec<PartitionAssignment> = (0..4).map(|i| PartitionAssignment {
            topic: "t".into(), partition_id: i,
        }).collect();

        let result = assignor.assign(&consumers, &partitions);
        assert_eq!(result["c1"].len(), 2);
        assert_eq!(result["c2"].len(), 2);
    }

    #[test]
    fn test_assignor_names() {
        assert_eq!(RangeAssignor.name(), "range");
        assert_eq!(RoundRobinAssignor.name(), "round-robin");
        assert_eq!(StickyAssignor.name(), "sticky");
    }

    // --- ConsumerGroupCoordinator tests ---

    #[test]
    fn test_coordinator_join_and_leave() {
        let mut coord = ConsumerGroupCoordinator::new("group-1", default_config());
        let gen = coord.join("c1").unwrap();
        assert_eq!(gen, 1);
        assert_eq!(coord.member_count(), 1);

        coord.join("c2").unwrap();
        assert_eq!(coord.member_count(), 2);

        coord.leave("c1").unwrap();
        assert_eq!(coord.member_count(), 1);
    }

    #[test]
    fn test_coordinator_duplicate_join() {
        let mut coord = ConsumerGroupCoordinator::new("group-1", default_config());
        coord.join("c1").unwrap();
        let result = coord.join("c1");
        assert!(result.is_err());
    }

    #[test]
    fn test_coordinator_leave_nonmember() {
        let mut coord = ConsumerGroupCoordinator::new("group-1", default_config());
        let result = coord.leave("c1");
        assert!(result.is_err());
    }

    #[test]
    fn test_coordinator_heartbeat() {
        let mut coord = ConsumerGroupCoordinator::new("group-1", default_config());
        coord.join("c1").unwrap();
        assert!(coord.heartbeat("c1").is_ok());
        assert!(coord.heartbeat("c-unknown").is_err());
    }

    #[test]
    fn test_coordinator_subscribe() {
        let mut coord = ConsumerGroupCoordinator::new("group-1", default_config());
        coord.subscribe(vec!["orders".to_string(), "payments".to_string()]);
        assert_eq!(coord.subscriptions().len(), 2);
    }

    #[test]
    fn test_coordinator_rebalance() {
        let mut coord = ConsumerGroupCoordinator::new("group-1", default_config());
        coord.join("c1").unwrap();
        coord.join("c2").unwrap();
        coord.subscribe(vec!["orders".to_string()]);

        let events = coord.rebalance(&test_partitions());

        // Should have: 2 assigns (no previous revoke) + complete
        let assigns: Vec<_> = events.iter().filter(|e| matches!(e.event_type, RebalanceEventType::Assign { .. })).collect();
        let completes: Vec<_> = events.iter().filter(|e| e.event_type == RebalanceEventType::Complete).collect();

        assert_eq!(assigns.len(), 2);
        assert_eq!(completes.len(), 1);
        assert_eq!(*coord.state(), RebalanceState::Stable);
    }

    #[test]
    fn test_coordinator_rebalance_assigns_all_partitions() {
        let mut coord = ConsumerGroupCoordinator::new("group-1", default_config());
        coord.join("c1").unwrap();
        coord.join("c2").unwrap();
        coord.subscribe(vec!["orders".to_string()]); // 6 partitions

        coord.rebalance(&test_partitions());

        let c1 = coord.get_member("c1").unwrap();
        let c2 = coord.get_member("c2").unwrap();

        let total_assigned = c1.assigned_partitions.len() + c2.assigned_partitions.len();
        assert_eq!(total_assigned, 6); // all 6 partitions assigned
    }

    #[test]
    fn test_coordinator_generation_increments() {
        let mut coord = ConsumerGroupCoordinator::new("group-1", default_config());
        assert_eq!(coord.generation(), 0);
        coord.join("c1").unwrap();
        assert_eq!(coord.generation(), 1);
        coord.join("c2").unwrap();
        assert_eq!(coord.generation(), 2);
        coord.leave("c1").unwrap();
        assert_eq!(coord.generation(), 3);
    }

    #[test]
    fn test_coordinator_offset_management() {
        let mut coord = ConsumerGroupCoordinator::new("group-1", default_config());
        coord.commit_offset("orders", 0, 42);
        assert_eq!(coord.committed_offset("orders", 0), Some(42));
        assert_eq!(coord.next_read_offset("orders", 0), 43);
    }

    #[test]
    fn test_coordinator_rebalance_count() {
        let mut coord = ConsumerGroupCoordinator::new("group-1", default_config());
        coord.join("c1").unwrap();
        coord.subscribe(vec!["orders".to_string()]);

        assert_eq!(coord.rebalance_count(), 0);
        coord.rebalance(&test_partitions());
        assert_eq!(coord.rebalance_count(), 1);
        coord.rebalance(&test_partitions());
        assert_eq!(coord.rebalance_count(), 2);
    }

    // --- Session timeout tests ---

    #[test]
    fn test_session_timeout_detection() {
        let config = ConsumerGroupConfig {
            session_timeout: Duration::from_millis(10),
            ..default_config()
        };
        let mut coord = ConsumerGroupCoordinator::new("group-1", config);
        coord.join("c1").unwrap();

        std::thread::sleep(Duration::from_millis(20));

        let expired = coord.check_session_timeouts();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], "c1");
        assert_eq!(coord.member_count(), 0);
    }

    // --- ConsumerGroupRegistry tests ---

    #[test]
    fn test_registry_create_and_list() {
        let mut registry = ConsumerGroupRegistry::new(default_config());
        registry.create_group("billing", None).unwrap();
        registry.create_group("analytics", None).unwrap();

        assert_eq!(registry.group_count(), 2);
        let groups = registry.list_groups();
        assert!(groups.contains(&"billing"));
        assert!(groups.contains(&"analytics"));
    }

    #[test]
    fn test_registry_duplicate_group() {
        let mut registry = ConsumerGroupRegistry::new(default_config());
        registry.create_group("billing", None).unwrap();
        let result = registry.create_group("billing", None);
        assert!(matches!(result, Err(EventBusError::ConsumerGroupAlreadyExists(_))));
    }

    #[test]
    fn test_registry_delete_group() {
        let mut registry = ConsumerGroupRegistry::new(default_config());
        registry.create_group("billing", None).unwrap();
        registry.delete_group("billing").unwrap();
        assert_eq!(registry.group_count(), 0);
    }

    #[test]
    fn test_registry_get_group() {
        let mut registry = ConsumerGroupRegistry::new(default_config());
        registry.create_group("billing", None).unwrap();

        let group = registry.get_group("billing").unwrap();
        assert_eq!(group.group_id, "billing");

        assert!(registry.get_group("nope").is_err());
    }

    #[test]
    fn test_registry_get_group_mut() {
        let mut registry = ConsumerGroupRegistry::new(default_config());
        registry.create_group("billing", None).unwrap();

        let group = registry.get_group_mut("billing").unwrap();
        group.join("c1").unwrap();
        assert_eq!(group.member_count(), 1);
    }

    // --- ConsumerMember tests ---

    #[test]
    fn test_consumer_member_heartbeat() {
        let mut member = ConsumerMember::new("c1");
        assert!(member.is_alive(Duration::from_secs(30)));
        member.heartbeat();
        assert!(member.is_alive(Duration::from_secs(30)));
    }

    #[test]
    fn test_consumer_member_metadata() {
        let mut member = ConsumerMember::new("c1");
        member.metadata.insert("host".to_string(), "node-1".to_string());
        assert_eq!(member.metadata.get("host").unwrap(), "node-1");
    }
}
