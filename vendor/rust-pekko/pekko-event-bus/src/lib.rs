//! # Pekko Event Bus - Internal Kafka-like Event Streaming
//!
//! Provides Kafka-like event streaming capabilities without external dependencies,
//! using Pekko actor primitives for topic-partitioned, durable event logs.
//!
//! ## Architecture
//! - **TopicPartition**: Per-partition durable event log with write-behind buffering
//! - **EventBus**: Topic management, publish routing, partition strategy
//! - **ConsumerGroup**: Partition assignment, offset tracking, rebalancing
//! - **Backpressure**: Watermark-based flow control (SlowDown/Reject/Resume)
//!
//! ## Quick Start
//! ```rust
//! use pekko_event_bus::*;
//!
//! let mut bus = EventBus::builder()
//!     .add_topic(TopicConfig::new("orders").partitions(6))
//!     .build()
//!     .unwrap();
//!
//! bus.publish("orders", &PartitionKey::from_str("user-42"), vec![1, 2, 3]).unwrap();
//! bus.flush_all();
//! ```

pub mod bus_config;
pub mod bus_errors;
pub mod partition_strategy;
pub mod topic_partition;
pub mod event_bus;
pub mod consumer_group;
pub mod bus_metrics;
pub mod bus_testing;
pub mod transaction;
pub mod compaction;
pub mod schema_registry;
pub mod exactly_once;
pub mod consumer_rebalancing;
pub mod tiered_storage;
pub mod stream_integration;
pub mod quota_manager;
pub mod mirror_maker;
pub mod admin_client;

// ============================================================================
// Re-exports
// ============================================================================

// Config
pub use bus_config::{
    TopicConfig, EventBusConfig, EventBusDispatcherConfig,
    PartitionStrategyType, RetentionConfig, WriteBufferConfig,
    WatermarkConfig, PressureState,
    DispatcherConfig, DispatcherType,
    ConsumerGroupConfig, DeliverySemantic, RebalanceStrategy,
};

// Errors
pub use bus_errors::EventBusError;

// Partition Strategy
pub use partition_strategy::{
    PartitionKey, PartitionStrategy,
    ConsistentHashStrategy, RoundRobinStrategy, StickyStrategy,
    KeyExtractor, NoKeyExtractor, FnKeyExtractor,
};

// Topic Partition
pub use topic_partition::{
    EventEnvelope, WriteBuffer, PartitionLog,
    TopicPartition, PartitionState, PublishResult,
    HandoffSnapshot,
};

// Event Bus
pub use event_bus::{
    EventBus, EventBusBuilder, EventBusHandle,
    TopicStats, PartitionStats, BusStats,
};

// Consumer Group
pub use consumer_group::{
    ConsumerMember, PartitionAssignment, OffsetTracker,
    RebalanceState, RebalanceEvent, RebalanceEventType,
    PartitionAssignor, RangeAssignor, RoundRobinAssignor, StickyAssignor,
    ConsumerGroupCoordinator, ConsumerGroupRegistry,
};

// Metrics
pub use bus_metrics::{
    Counter, Gauge, RateMeter,
    ConsumerLag, PartitionHealth, PartitionHealthCheck,
    Alert, AlertSeverity, AlertRule,
    BusMetricsCollector, BusMetricsSnapshot,
};

// Testing
pub use bus_testing::{
    RecordedEvent, MockEventBus, EventBusTestProbe,
    MockBusAssertions,
    mock_event_bus, test_probe,
};

// Transaction
pub use transaction::{
    TransactionId, TransactionState, Transaction,
    TransactionRecord, TransactionCoordinator, TransactionStats,
    WriteAheadLog, WalEntry, WalAction,
};

// Compaction
pub use compaction::{
    CompactionStrategy, CompactionRecord, CompactionConfig,
    CompactionResult, LogCompactor,
    Segment, SegmentCleaner,
};

// Schema Registry
pub use schema_registry::{
    SchemaId, SchemaType, SchemaDefinition, SchemaField, FieldType,
    SchemaReference, CompatibilityMode, CompatibilityChecker,
    CompatibilityResult, CompatibilityIssue, IssueSeverity,
    SchemaRegistry,
};

// Exactly-Once
pub use exactly_once::{
    ProducerId, ProducerStateManager, SequenceCheckResult,
    IdempotentProducer, TransactionalConsumer,
    ConsumerProcessingState, IsolationLevel,
    ExactlyOnceCoordinator, EosStats,
};

// Consumer Rebalancing (Phase 3)
pub use consumer_rebalancing::{
    RebalanceProtocol, MemberEpoch, ConsumerMetadata,
    RebalancePlan, CooperativeStickyAssignor,
    PartitionRackInfo, RackAwareAssignor,
    RebalanceRetryConfig, RebalanceTracker,
    RebalanceRecord, RebalanceTrigger,
};

// Tiered Storage (Phase 3)
pub use tiered_storage::{
    StorageTier, StorageBackend, TierConfig,
    SegmentMetadata, MigrationPolicy,
    MigrationAction, MigrationReason,
    TieredStorageManager, TieredStorageStats,
};

// Stream Integration (Phase 3)
pub use stream_integration::{
    StartPosition, EventSourceConfig, EventSource, SourceState,
    EventSinkConfig, EventSink, SinkState,
    StreamConsumerGroup, StreamProcessingState,
    StreamError, StreamHealthSummary,
};

// Quota Manager (Phase 4)
pub use quota_manager::{
    QuotaType, QuotaEntity, QuotaLimit, TokenBucket,
    SlidingWindowTracker, ThrottleResult, QuotaViolation,
    QuotaManager, QuotaManagerStats, EntityQuotaStats,
    QuotaManagerBuilder,
};

// Mirror Maker (Phase 4)
pub use mirror_maker::{
    MirrorConfig, TopicMirrorRule, TopicPattern, MirrorFilter,
    MirrorRecord, MirrorState, TopicMirrorStatus,
    MirrorMaker, MirrorBatchResult, MirrorMakerStats, MirrorError,
};

// Admin Client (Phase 4)
pub use admin_client::{
    AdminOperation, TopicDescription, PartitionInfo,
    ConsumerGroupDescription, ConsumerGroupState, MemberDescription,
    OffsetResetStrategy, ClusterInfo, NodeInfo,
    AdminResult, AdminClient, AdminClientStats, AdminLogEntry,
};
