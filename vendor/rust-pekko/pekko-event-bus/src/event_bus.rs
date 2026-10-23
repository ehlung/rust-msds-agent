// event_bus.rs - EventBus Router (Top-level API)
// Topic management, publish routing, subscription coordination

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::bus_config::{EventBusConfig, TopicConfig};
use crate::bus_errors::EventBusError;
use crate::partition_strategy::{self, PartitionKey, PartitionStrategy};
use crate::topic_partition::{TopicPartition, PublishResult, EventEnvelope};

// ============================================================================
// EventBus
// ============================================================================

/// The main EventBus — manages topics, partitions, and routing.
/// Thread-safe via Arc<RwLock<_>> for internal state.
pub struct EventBus {
    topics: HashMap<String, TopicState>,
    is_shutdown: bool,
}

struct TopicState {
    config: TopicConfig,
    partitions: Vec<TopicPartition>,
    strategy: Box<dyn PartitionStrategy>,
}

impl EventBus {
    // --- Construction ---

    pub fn builder() -> EventBusBuilder {
        EventBusBuilder::new()
    }

    fn new(config: EventBusConfig) -> Result<Self, EventBusError> {
        let mut topics = HashMap::new();

        for (name, topic_config) in &config.topics {
            let strategy = partition_strategy::create_strategy(
                &topic_config.partition_strategy,
                topic_config.num_partitions,
            );

            let partitions: Vec<TopicPartition> = (0..topic_config.num_partitions)
                .map(|i| TopicPartition::new(name, i, topic_config))
                .collect();

            topics.insert(name.clone(), TopicState {
                config: topic_config.clone(),
                partitions,
                strategy,
            });
        }

        Ok(Self {
            topics,
            is_shutdown: false,
        })
    }

    // --- Topic Management ---

    /// Create a new topic at runtime
    pub fn create_topic(&mut self, config: TopicConfig) -> Result<(), EventBusError> {
        if self.is_shutdown {
            return Err(EventBusError::ShutdownInProgress);
        }

        config.validate().map_err(|reason| EventBusError::InvalidTopicConfig {
            topic: config.name.clone(),
            reason,
        })?;

        if self.topics.contains_key(&config.name) {
            return Err(EventBusError::TopicAlreadyExists(config.name.clone()));
        }

        let strategy = partition_strategy::create_strategy(
            &config.partition_strategy,
            config.num_partitions,
        );

        let partitions: Vec<TopicPartition> = (0..config.num_partitions)
            .map(|i| TopicPartition::new(&config.name, i, &config))
            .collect();

        self.topics.insert(config.name.clone(), TopicState {
            config,
            partitions,
            strategy,
        });

        Ok(())
    }

    /// Delete a topic
    pub fn delete_topic(&mut self, topic: &str) -> Result<(), EventBusError> {
        if !self.topics.contains_key(topic) {
            return Err(EventBusError::TopicNotFound(topic.to_string()));
        }
        self.topics.remove(topic);
        Ok(())
    }

    /// Check if a topic exists
    pub fn topic_exists(&self, topic: &str) -> bool {
        self.topics.contains_key(topic)
    }

    /// List all topic names
    pub fn list_topics(&self) -> Vec<&str> {
        self.topics.keys().map(|s| s.as_str()).collect()
    }

    /// Get topic configuration
    pub fn topic_config(&self, topic: &str) -> Option<&TopicConfig> {
        self.topics.get(topic).map(|t| &t.config)
    }

    /// Number of partitions for a topic
    pub fn num_partitions(&self, topic: &str) -> Option<u32> {
        self.topics.get(topic).map(|t| t.config.num_partitions)
    }

    // --- Publish ---

    /// Publish an event to a topic using the configured partition strategy
    pub fn publish(
        &mut self,
        topic: &str,
        key: &PartitionKey,
        payload: Vec<u8>,
    ) -> Result<PublishResult, EventBusError> {
        if self.is_shutdown {
            return Err(EventBusError::ShutdownInProgress);
        }

        let topic_state = self.topics.get_mut(topic)
            .ok_or_else(|| EventBusError::TopicNotFound(topic.to_string()))?;

        let num_partitions = topic_state.config.num_partitions;
        let partition_id = topic_state.strategy.select_partition(key, num_partitions);

        let key_str = match key {
            PartitionKey::String(s) => Some(s.clone()),
            PartitionKey::Numeric(n) => Some(n.to_string()),
            PartitionKey::Composite(parts) => Some(parts.join(":")),
            PartitionKey::None => None,
        };

        let partition = topic_state.partitions.get_mut(partition_id as usize)
            .ok_or_else(|| EventBusError::PartitionNotFound {
                topic: topic.to_string(),
                partition: partition_id,
            })?;

        partition.publish(key_str, payload)
    }

    /// Publish to a specific partition (bypass strategy)
    pub fn publish_to_partition(
        &mut self,
        topic: &str,
        partition_id: u32,
        key: Option<String>,
        payload: Vec<u8>,
    ) -> Result<PublishResult, EventBusError> {
        if self.is_shutdown {
            return Err(EventBusError::ShutdownInProgress);
        }

        let topic_state = self.topics.get_mut(topic)
            .ok_or_else(|| EventBusError::TopicNotFound(topic.to_string()))?;

        let partition = topic_state.partitions.get_mut(partition_id as usize)
            .ok_or_else(|| EventBusError::PartitionNotFound {
                topic: topic.to_string(),
                partition: partition_id,
            })?;

        partition.publish(key, payload)
    }

    // --- Read ---

    /// Read events from a specific partition
    pub fn read(
        &self,
        topic: &str,
        partition_id: u32,
        from_offset: u64,
        max_count: usize,
    ) -> Result<Vec<&EventEnvelope>, EventBusError> {
        let topic_state = self.topics.get(topic)
            .ok_or_else(|| EventBusError::TopicNotFound(topic.to_string()))?;

        let partition = topic_state.partitions.get(partition_id as usize)
            .ok_or_else(|| EventBusError::PartitionNotFound {
                topic: topic.to_string(),
                partition: partition_id,
            })?;

        Ok(partition.read(from_offset, max_count))
    }

    /// Get the latest offset for a partition
    pub fn latest_offset(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Result<Option<u64>, EventBusError> {
        let topic_state = self.topics.get(topic)
            .ok_or_else(|| EventBusError::TopicNotFound(topic.to_string()))?;

        let partition = topic_state.partitions.get(partition_id as usize)
            .ok_or_else(|| EventBusError::PartitionNotFound {
                topic: topic.to_string(),
                partition: partition_id,
            })?;

        Ok(partition.latest_offset())
    }

    // --- Flush ---

    /// Flush all partitions for a topic
    pub fn flush_topic(&mut self, topic: &str) -> Result<usize, EventBusError> {
        let topic_state = self.topics.get_mut(topic)
            .ok_or_else(|| EventBusError::TopicNotFound(topic.to_string()))?;

        let mut total = 0;
        for partition in &mut topic_state.partitions {
            total += partition.flush_buffer();
        }
        Ok(total)
    }

    /// Flush all topics
    pub fn flush_all(&mut self) -> usize {
        let mut total = 0;
        for topic_state in self.topics.values_mut() {
            for partition in &mut topic_state.partitions {
                total += partition.flush_buffer();
            }
        }
        total
    }

    // --- Lifecycle ---

    /// Graceful shutdown: drain all buffers, stop accepting writes
    pub fn shutdown(&mut self) -> usize {
        self.is_shutdown = true;
        self.flush_all()
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    // --- Stats ---

    /// Get stats for a specific topic
    pub fn topic_stats(&self, topic: &str) -> Option<TopicStats> {
        let topic_state = self.topics.get(topic)?;

        let mut total_events = 0u64;
        let mut total_log_size = 0usize;
        let mut total_buffer_size = 0usize;
        let mut partition_stats = Vec::new();

        for p in &topic_state.partitions {
            total_events += p.events_published();
            total_log_size += p.log_size();
            total_buffer_size += p.buffer_size();
            partition_stats.push(PartitionStats {
                partition_id: p.partition_id,
                events_published: p.events_published(),
                log_size: p.log_size(),
                buffer_size: p.buffer_size(),
                latest_offset: p.latest_offset(),
            });
        }

        Some(TopicStats {
            topic: topic.to_string(),
            num_partitions: topic_state.config.num_partitions,
            total_events,
            total_log_size,
            total_buffer_size,
            partitions: partition_stats,
        })
    }

    /// Aggregate stats across all topics
    pub fn bus_stats(&self) -> BusStats {
        let mut total_events = 0u64;
        let mut total_topics = 0u32;
        let mut total_partitions = 0u32;

        for topic_state in self.topics.values() {
            total_topics += 1;
            total_partitions += topic_state.config.num_partitions;
            for p in &topic_state.partitions {
                total_events += p.events_published();
            }
        }

        BusStats {
            total_topics,
            total_partitions,
            total_events,
            is_shutdown: self.is_shutdown,
        }
    }
}

// ============================================================================
// Stats Types
// ============================================================================

#[derive(Debug, Clone)]
pub struct TopicStats {
    pub topic: String,
    pub num_partitions: u32,
    pub total_events: u64,
    pub total_log_size: usize,
    pub total_buffer_size: usize,
    pub partitions: Vec<PartitionStats>,
}

#[derive(Debug, Clone)]
pub struct PartitionStats {
    pub partition_id: u32,
    pub events_published: u64,
    pub log_size: usize,
    pub buffer_size: usize,
    pub latest_offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct BusStats {
    pub total_topics: u32,
    pub total_partitions: u32,
    pub total_events: u64,
    pub is_shutdown: bool,
}

// ============================================================================
// EventBus Builder
// ============================================================================

pub struct EventBusBuilder {
    config: EventBusConfig,
}

impl EventBusBuilder {
    pub fn new() -> Self {
        Self {
            config: EventBusConfig::default(),
        }
    }

    pub fn add_topic(mut self, config: TopicConfig) -> Self {
        self.config.topics.insert(config.name.clone(), config);
        self
    }

    pub fn config(mut self, config: EventBusConfig) -> Self {
        self.config = config;
        self
    }

    pub fn build(self) -> Result<EventBus, EventBusError> {
        // Validate all topics
        if let Err(errors) = self.config.validate() {
            let reason = errors.join("; ");
            return Err(EventBusError::Internal(format!("Config validation failed: {}", reason)));
        }

        EventBus::new(self.config)
    }
}

impl Default for EventBusBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Thread-Safe Wrapper
// ============================================================================

/// Thread-safe handle to the EventBus
#[derive(Clone)]
pub struct EventBusHandle {
    inner: Arc<RwLock<EventBus>>,
}

impl EventBusHandle {
    pub fn new(bus: EventBus) -> Self {
        Self {
            inner: Arc::new(RwLock::new(bus)),
        }
    }

    pub fn publish(
        &self,
        topic: &str,
        key: &PartitionKey,
        payload: Vec<u8>,
    ) -> Result<PublishResult, EventBusError> {
        let mut bus = self.inner.write().map_err(|_| {
            EventBusError::Internal("Failed to acquire write lock".into())
        })?;
        bus.publish(topic, key, payload)
    }

    pub fn create_topic(&self, config: TopicConfig) -> Result<(), EventBusError> {
        let mut bus = self.inner.write().map_err(|_| {
            EventBusError::Internal("Failed to acquire write lock".into())
        })?;
        bus.create_topic(config)
    }

    pub fn flush_all(&self) -> Result<usize, EventBusError> {
        let mut bus = self.inner.write().map_err(|_| {
            EventBusError::Internal("Failed to acquire write lock".into())
        })?;
        Ok(bus.flush_all())
    }

    pub fn bus_stats(&self) -> Result<BusStats, EventBusError> {
        let bus = self.inner.read().map_err(|_| {
            EventBusError::Internal("Failed to acquire read lock".into())
        })?;
        Ok(bus.bus_stats())
    }

    pub fn shutdown(&self) -> Result<usize, EventBusError> {
        let mut bus = self.inner.write().map_err(|_| {
            EventBusError::Internal("Failed to acquire write lock".into())
        })?;
        Ok(bus.shutdown())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus_config::PartitionStrategyType;
    use std::time::Duration;

    fn build_test_bus() -> EventBus {
        EventBus::builder()
            .add_topic(
                TopicConfig::new("orders")
                    .partitions(3)
                    .batch_size(5)
            )
            .add_topic(
                TopicConfig::new("payments")
                    .partitions(2)
                    .batch_size(5)
            )
            .build()
            .unwrap()
    }

    // --- Builder tests ---

    #[test]
    fn test_builder_creates_bus() {
        let bus = build_test_bus();
        assert!(bus.topic_exists("orders"));
        assert!(bus.topic_exists("payments"));
        assert!(!bus.topic_exists("nonexistent"));
    }

    #[test]
    fn test_builder_validates_config() {
        let result = EventBus::builder()
            .add_topic(TopicConfig::new("").partitions(3)) // empty name
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_builder_default() {
        let bus = EventBusBuilder::default().build().unwrap();
        assert_eq!(bus.list_topics().len(), 0);
    }

    // --- Topic management tests ---

    #[test]
    fn test_create_topic_runtime() {
        let mut bus = build_test_bus();
        bus.create_topic(TopicConfig::new("events").partitions(4)).unwrap();
        assert!(bus.topic_exists("events"));
        assert_eq!(bus.num_partitions("events"), Some(4));
    }

    #[test]
    fn test_create_duplicate_topic() {
        let mut bus = build_test_bus();
        let result = bus.create_topic(TopicConfig::new("orders").partitions(3));
        assert!(matches!(result, Err(EventBusError::TopicAlreadyExists(_))));
    }

    #[test]
    fn test_delete_topic() {
        let mut bus = build_test_bus();
        bus.delete_topic("orders").unwrap();
        assert!(!bus.topic_exists("orders"));
    }

    #[test]
    fn test_delete_nonexistent_topic() {
        let mut bus = build_test_bus();
        let result = bus.delete_topic("nope");
        assert!(matches!(result, Err(EventBusError::TopicNotFound(_))));
    }

    #[test]
    fn test_list_topics() {
        let bus = build_test_bus();
        let topics = bus.list_topics();
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"orders"));
        assert!(topics.contains(&"payments"));
    }

    #[test]
    fn test_topic_config() {
        let bus = build_test_bus();
        let config = bus.topic_config("orders").unwrap();
        assert_eq!(config.num_partitions, 3);
    }

    // --- Publish tests ---

    #[test]
    fn test_publish_event() {
        let mut bus = build_test_bus();
        let key = PartitionKey::from_str("order-123");
        let result = bus.publish("orders", &key, vec![1, 2, 3]).unwrap();
        match result {
            PublishResult::Accepted { .. } => {}
            other => panic!("Expected Accepted, got {:?}", other),
        }
    }

    #[test]
    fn test_publish_to_nonexistent_topic() {
        let mut bus = build_test_bus();
        let result = bus.publish("nope", &PartitionKey::None, vec![]);
        assert!(matches!(result, Err(EventBusError::TopicNotFound(_))));
    }

    #[test]
    fn test_publish_multiple_events() {
        let mut bus = build_test_bus();
        for i in 0..20 {
            let key = PartitionKey::from_str(&format!("k{}", i));
            bus.publish("orders", &key, vec![i as u8]).unwrap();
        }

        // Stats should reflect 20 events
        let stats = bus.topic_stats("orders").unwrap();
        assert_eq!(stats.total_events, 20);
    }

    #[test]
    fn test_publish_to_specific_partition() {
        let mut bus = build_test_bus();
        let result = bus.publish_to_partition("orders", 1, Some("k1".into()), vec![42]).unwrap();
        match result {
            PublishResult::Accepted { .. } => {}
            other => panic!("Expected Accepted, got {:?}", other),
        }
    }

    #[test]
    fn test_publish_to_invalid_partition() {
        let mut bus = build_test_bus();
        let result = bus.publish_to_partition("orders", 99, None, vec![]);
        assert!(matches!(result, Err(EventBusError::PartitionNotFound { .. })));
    }

    // --- Read tests ---

    #[test]
    fn test_read_events() {
        let mut bus = build_test_bus();

        // Publish to specific partition and flush
        for i in 0..5 {
            bus.publish_to_partition("orders", 0, None, vec![i]).unwrap();
        }
        bus.flush_topic("orders").unwrap();

        let events = bus.read("orders", 0, 0, 100).unwrap();
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn test_read_nonexistent_topic() {
        let bus = build_test_bus();
        let result = bus.read("nope", 0, 0, 10);
        assert!(matches!(result, Err(EventBusError::TopicNotFound(_))));
    }

    #[test]
    fn test_latest_offset() {
        let mut bus = build_test_bus();
        assert_eq!(bus.latest_offset("orders", 0).unwrap(), None);

        bus.publish_to_partition("orders", 0, None, vec![1]).unwrap();
        bus.flush_topic("orders").unwrap();
        assert_eq!(bus.latest_offset("orders", 0).unwrap(), Some(0));
    }

    // --- Flush tests ---

    #[test]
    fn test_flush_topic() {
        let mut bus = build_test_bus();
        bus.publish_to_partition("orders", 0, None, vec![1]).unwrap();
        bus.publish_to_partition("orders", 1, None, vec![2]).unwrap();

        let flushed = bus.flush_topic("orders").unwrap();
        assert_eq!(flushed, 2);
    }

    #[test]
    fn test_flush_all() {
        let mut bus = build_test_bus();
        bus.publish_to_partition("orders", 0, None, vec![1]).unwrap();
        bus.publish_to_partition("payments", 0, None, vec![2]).unwrap();

        let flushed = bus.flush_all();
        assert_eq!(flushed, 2);
    }

    // --- Lifecycle tests ---

    #[test]
    fn test_shutdown() {
        let mut bus = build_test_bus();
        bus.publish("orders", &PartitionKey::None, vec![1]).unwrap();
        let flushed = bus.shutdown();
        assert!(flushed >= 1);
        assert!(bus.is_shutdown());
    }

    #[test]
    fn test_publish_after_shutdown() {
        let mut bus = build_test_bus();
        bus.shutdown();
        let result = bus.publish("orders", &PartitionKey::None, vec![]);
        assert!(matches!(result, Err(EventBusError::ShutdownInProgress)));
    }

    #[test]
    fn test_create_topic_after_shutdown() {
        let mut bus = build_test_bus();
        bus.shutdown();
        let result = bus.create_topic(TopicConfig::new("new").partitions(1));
        assert!(matches!(result, Err(EventBusError::ShutdownInProgress)));
    }

    // --- Stats tests ---

    #[test]
    fn test_topic_stats() {
        let mut bus = build_test_bus();
        for i in 0..10 {
            bus.publish("orders", &PartitionKey::from_str(&format!("k{}", i)), vec![i as u8]).unwrap();
        }

        let stats = bus.topic_stats("orders").unwrap();
        assert_eq!(stats.topic, "orders");
        assert_eq!(stats.num_partitions, 3);
        assert_eq!(stats.total_events, 10);
        assert_eq!(stats.partitions.len(), 3);
    }

    #[test]
    fn test_bus_stats() {
        let bus = build_test_bus();
        let stats = bus.bus_stats();
        assert_eq!(stats.total_topics, 2);
        assert_eq!(stats.total_partitions, 5); // 3 + 2
        assert_eq!(stats.total_events, 0);
        assert!(!stats.is_shutdown);
    }

    #[test]
    fn test_topic_stats_nonexistent() {
        let bus = build_test_bus();
        assert!(bus.topic_stats("nope").is_none());
    }

    // --- Thread-safe handle tests ---

    #[test]
    fn test_event_bus_handle() {
        let bus = build_test_bus();
        let handle = EventBusHandle::new(bus);

        handle.publish("orders", &PartitionKey::from_str("k1"), vec![1]).unwrap();
        let stats = handle.bus_stats().unwrap();
        assert_eq!(stats.total_events, 1);
    }

    #[test]
    fn test_event_bus_handle_clone() {
        let bus = build_test_bus();
        let handle1 = EventBusHandle::new(bus);
        let handle2 = handle1.clone();

        handle1.publish("orders", &PartitionKey::from_str("k1"), vec![1]).unwrap();
        let stats = handle2.bus_stats().unwrap();
        assert_eq!(stats.total_events, 1);
    }

    #[test]
    fn test_event_bus_handle_shutdown() {
        let bus = build_test_bus();
        let handle = EventBusHandle::new(bus);
        handle.shutdown().unwrap();

        let stats = handle.bus_stats().unwrap();
        assert!(stats.is_shutdown);
    }

    // --- Strategy integration tests ---

    #[test]
    fn test_consistent_hash_routing_consistency() {
        let mut bus = EventBus::builder()
            .add_topic(
                TopicConfig::new("events")
                    .partitions(6)
                    .batch_size(100)
                    .strategy(PartitionStrategyType::ConsistentHash)
            )
            .build()
            .unwrap();

        // Same key should always route to same partition
        let key = PartitionKey::from_str("user-42");
        for _ in 0..10 {
            bus.publish("events", &key, vec![1]).unwrap();
        }
        bus.flush_topic("events").unwrap();

        // Count which partitions got events
        let stats = bus.topic_stats("events").unwrap();
        let non_empty: Vec<_> = stats.partitions.iter()
            .filter(|p| p.events_published > 0)
            .collect();

        // All 10 events should be on a single partition (same key)
        assert_eq!(non_empty.len(), 1);
        assert_eq!(non_empty[0].events_published, 10);
    }

    #[test]
    fn test_round_robin_routing_distribution() {
        let mut bus = EventBus::builder()
            .add_topic(
                TopicConfig::new("events")
                    .partitions(3)
                    .batch_size(100)
                    .strategy(PartitionStrategyType::RoundRobin)
            )
            .build()
            .unwrap();

        for _ in 0..9 {
            bus.publish("events", &PartitionKey::None, vec![1]).unwrap();
        }
        bus.flush_topic("events").unwrap();

        let stats = bus.topic_stats("events").unwrap();
        // RoundRobin should distribute evenly: 3 each
        for p in &stats.partitions {
            assert_eq!(p.events_published, 3);
        }
    }
}
