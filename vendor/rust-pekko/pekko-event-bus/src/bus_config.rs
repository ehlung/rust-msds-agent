// bus_config.rs - EventBus Configuration
// TopicConfig, BusConfig, Dispatcher isolation settings

use std::collections::HashMap;
use std::time::Duration;

// ============================================================================
// Topic Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub name: String,
    pub num_partitions: u32,
    pub replication_factor: u32,
    pub retention: RetentionConfig,
    pub partition_strategy: PartitionStrategyType,
    pub write_buffer: WriteBufferConfig,
    pub watermark: WatermarkConfig,
}

impl TopicConfig {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            num_partitions: 6,
            replication_factor: 1,
            retention: RetentionConfig::default(),
            partition_strategy: PartitionStrategyType::ConsistentHash,
            write_buffer: WriteBufferConfig::default(),
            watermark: WatermarkConfig::default(),
        }
    }

    pub fn partitions(mut self, n: u32) -> Self {
        self.num_partitions = n;
        self
    }

    pub fn replication(mut self, n: u32) -> Self {
        self.replication_factor = n;
        self
    }

    pub fn retention_max_events(mut self, max: u64) -> Self {
        self.retention.max_events_per_partition = Some(max);
        self
    }

    pub fn retention_max_age(mut self, duration: Duration) -> Self {
        self.retention.max_age = Some(duration);
        self
    }

    pub fn strategy(mut self, strategy: PartitionStrategyType) -> Self {
        self.partition_strategy = strategy;
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.write_buffer.batch_size = size;
        self
    }

    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.write_buffer.flush_interval = interval;
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("Topic name cannot be empty".into());
        }
        if self.num_partitions == 0 {
            return Err("Partitions must be > 0".into());
        }
        if self.num_partitions > 10_000 {
            return Err("Partitions cannot exceed 10,000".into());
        }
        if self.replication_factor == 0 {
            return Err("Replication factor must be > 0".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PartitionStrategyType {
    ConsistentHash,
    RoundRobin,
    Sticky { duration: Duration },
}

// ============================================================================
// Retention Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct RetentionConfig {
    pub max_events_per_partition: Option<u64>,
    pub max_age: Option<Duration>,
    pub cleanup_interval: Duration,
    pub compaction_enabled: bool,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_events_per_partition: None,
            max_age: Some(Duration::from_secs(7 * 24 * 3600)), // 7 days
            cleanup_interval: Duration::from_secs(300), // 5 minutes
            compaction_enabled: false,
        }
    }
}

// ============================================================================
// Write Buffer Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct WriteBufferConfig {
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub max_buffer_size: usize,
    pub retry_on_failure: bool,
    pub max_retries: u32,
}

impl Default for WriteBufferConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            flush_interval: Duration::from_millis(10),
            max_buffer_size: 10_000,
            retry_on_failure: true,
            max_retries: 3,
        }
    }
}

// ============================================================================
// Watermark Configuration (Backpressure)
// ============================================================================

#[derive(Debug, Clone)]
pub struct WatermarkConfig {
    pub high_watermark: f64,    // 0.0 - 1.0, triggers SlowDown
    pub max_watermark: f64,     // 0.0 - 1.0, triggers Reject
    pub low_watermark: f64,     // 0.0 - 1.0, triggers Resume
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            high_watermark: 0.80,
            max_watermark: 0.95,
            low_watermark: 0.50,
        }
    }
}

impl WatermarkConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.low_watermark >= self.high_watermark {
            return Err("low_watermark must be < high_watermark".into());
        }
        if self.high_watermark >= self.max_watermark {
            return Err("high_watermark must be < max_watermark".into());
        }
        if self.max_watermark > 1.0 || self.low_watermark < 0.0 {
            return Err("Watermarks must be between 0.0 and 1.0".into());
        }
        Ok(())
    }

    /// Determine backpressure state based on current fill ratio
    pub fn pressure_state(&self, fill_ratio: f64) -> PressureState {
        if fill_ratio >= self.max_watermark {
            PressureState::Reject
        } else if fill_ratio >= self.high_watermark {
            PressureState::SlowDown
        } else if fill_ratio <= self.low_watermark {
            PressureState::Normal
        } else {
            PressureState::Normal
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PressureState {
    Normal,
    SlowDown,
    Reject,
}

// ============================================================================
// Dispatcher Configuration (Isolation)
// ============================================================================

#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    pub dispatcher_type: DispatcherType,
    pub parallelism: usize,
    pub throughput: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DispatcherType {
    ForkJoin,
    Pinned,
    CallingThread,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            dispatcher_type: DispatcherType::ForkJoin,
            parallelism: num_cpus(),
            throughput: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventBusDispatcherConfig {
    pub partition_dispatcher: DispatcherConfig,
    pub consumer_dispatcher: DispatcherConfig,
    pub router_dispatcher: DispatcherConfig,
}

impl Default for EventBusDispatcherConfig {
    fn default() -> Self {
        let cpus = num_cpus();
        Self {
            partition_dispatcher: DispatcherConfig {
                dispatcher_type: DispatcherType::ForkJoin,
                parallelism: cpus,
                throughput: 100,
            },
            consumer_dispatcher: DispatcherConfig {
                dispatcher_type: DispatcherType::ForkJoin,
                parallelism: cpus * 2,
                throughput: 50,
            },
            router_dispatcher: DispatcherConfig {
                dispatcher_type: DispatcherType::Pinned,
                parallelism: 2,
                throughput: 200,
            },
        }
    }
}

// ============================================================================
// Consumer Group Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct ConsumerGroupConfig {
    pub delivery_semantic: DeliverySemantic,
    pub auto_commit: bool,
    pub auto_commit_interval: Duration,
    pub max_poll_records: usize,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub rebalance_strategy: RebalanceStrategy,
}

impl Default for ConsumerGroupConfig {
    fn default() -> Self {
        Self {
            delivery_semantic: DeliverySemantic::AtLeastOnce,
            auto_commit: true,
            auto_commit_interval: Duration::from_secs(5),
            max_poll_records: 500,
            session_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(3),
            rebalance_strategy: RebalanceStrategy::Sticky,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeliverySemantic {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RebalanceStrategy {
    Range,
    RoundRobin,
    Sticky,
}

// ============================================================================
// EventBus Configuration (Top-level)
// ============================================================================

#[derive(Debug, Clone)]
pub struct EventBusConfig {
    pub topics: HashMap<String, TopicConfig>,
    pub dispatchers: EventBusDispatcherConfig,
    pub default_consumer_config: ConsumerGroupConfig,
    pub metrics_collection_interval: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            topics: HashMap::new(),
            dispatchers: EventBusDispatcherConfig::default(),
            default_consumer_config: ConsumerGroupConfig::default(),
            metrics_collection_interval: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl EventBusConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_topic(mut self, config: TopicConfig) -> Self {
        self.topics.insert(config.name.clone(), config);
        self
    }

    pub fn with_dispatchers(mut self, dispatchers: EventBusDispatcherConfig) -> Self {
        self.dispatchers = dispatchers;
        self
    }

    pub fn with_consumer_config(mut self, config: ConsumerGroupConfig) -> Self {
        self.default_consumer_config = config;
        self
    }

    pub fn metrics_interval(mut self, interval: Duration) -> Self {
        self.metrics_collection_interval = interval;
        self
    }

    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        for (name, config) in &self.topics {
            if let Err(e) = config.validate() {
                errors.push(format!("Topic '{}': {}", name, e));
            }
        }
        if !errors.is_empty() {
            Err(errors)
        } else {
            Ok(())
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_config_builder() {
        let config = TopicConfig::new("orders")
            .partitions(12)
            .replication(3)
            .retention_max_events(1_000_000);

        assert_eq!(config.name, "orders");
        assert_eq!(config.num_partitions, 12);
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.retention.max_events_per_partition, Some(1_000_000));
    }

    #[test]
    fn test_topic_config_validation() {
        let config = TopicConfig::new("orders").partitions(6);
        assert!(config.validate().is_ok());

        let config = TopicConfig::new("").partitions(6);
        assert!(config.validate().is_err());

        let config = TopicConfig::new("orders").partitions(0);
        assert!(config.validate().is_err());

        let config = TopicConfig::new("orders").partitions(20_000);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_topic_config_defaults() {
        let config = TopicConfig::new("test");
        assert_eq!(config.num_partitions, 6);
        assert_eq!(config.replication_factor, 1);
        assert_eq!(config.partition_strategy, PartitionStrategyType::ConsistentHash);
    }

    #[test]
    fn test_retention_config_defaults() {
        let config = RetentionConfig::default();
        assert!(config.max_age.is_some());
        assert!(config.max_events_per_partition.is_none());
        assert!(!config.compaction_enabled);
    }

    #[test]
    fn test_write_buffer_config_defaults() {
        let config = WriteBufferConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.flush_interval, Duration::from_millis(10));
        assert_eq!(config.max_buffer_size, 10_000);
    }

    #[test]
    fn test_watermark_config_validation() {
        let config = WatermarkConfig::default();
        assert!(config.validate().is_ok());

        let bad = WatermarkConfig { low_watermark: 0.9, high_watermark: 0.8, max_watermark: 0.95 };
        assert!(bad.validate().is_err());

        let bad = WatermarkConfig { low_watermark: 0.5, high_watermark: 0.96, max_watermark: 0.95 };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn test_watermark_pressure_state() {
        let config = WatermarkConfig::default();
        assert_eq!(config.pressure_state(0.30), PressureState::Normal);
        assert_eq!(config.pressure_state(0.85), PressureState::SlowDown);
        assert_eq!(config.pressure_state(0.96), PressureState::Reject);
    }

    #[test]
    fn test_dispatcher_config_defaults() {
        let config = EventBusDispatcherConfig::default();
        assert_eq!(config.router_dispatcher.dispatcher_type, DispatcherType::Pinned);
        assert_eq!(config.router_dispatcher.parallelism, 2);
        assert_eq!(config.partition_dispatcher.dispatcher_type, DispatcherType::ForkJoin);
    }

    #[test]
    fn test_consumer_group_config_defaults() {
        let config = ConsumerGroupConfig::default();
        assert_eq!(config.delivery_semantic, DeliverySemantic::AtLeastOnce);
        assert!(config.auto_commit);
        assert_eq!(config.rebalance_strategy, RebalanceStrategy::Sticky);
    }

    #[test]
    fn test_event_bus_config_builder() {
        let config = EventBusConfig::new()
            .add_topic(TopicConfig::new("orders").partitions(6))
            .add_topic(TopicConfig::new("payments").partitions(3))
            .metrics_interval(Duration::from_secs(10));

        assert_eq!(config.topics.len(), 2);
        assert!(config.topics.contains_key("orders"));
        assert!(config.topics.contains_key("payments"));
        assert_eq!(config.metrics_collection_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_event_bus_config_validation() {
        let config = EventBusConfig::new()
            .add_topic(TopicConfig::new("orders").partitions(6));
        assert!(config.validate().is_ok());

        let config = EventBusConfig::new()
            .add_topic(TopicConfig::new("").partitions(6));
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_delivery_semantic_equality() {
        assert_eq!(DeliverySemantic::AtLeastOnce, DeliverySemantic::AtLeastOnce);
        assert_ne!(DeliverySemantic::AtLeastOnce, DeliverySemantic::ExactlyOnce);
    }

    #[test]
    fn test_partition_strategy_sticky() {
        let strategy = PartitionStrategyType::Sticky { duration: Duration::from_secs(30) };
        match strategy {
            PartitionStrategyType::Sticky { duration } => {
                assert_eq!(duration, Duration::from_secs(30));
            },
            _ => panic!("Expected Sticky"),
        }
    }
}
