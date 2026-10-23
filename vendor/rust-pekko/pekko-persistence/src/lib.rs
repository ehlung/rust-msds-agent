//! # Pekko Persistence - Event Sourcing for Actors
//! 
//! This module provides event sourcing capabilities for Pekko actors, including:
//! 
//! - Persistent actors that can store and replay events
//! - Journal backends (Cassandra/ScyllaDB)
//! - Snapshot support for performance optimization
//! - Pluggable serialization (JSON, Protocol Buffers)
//! - Recovery and replay mechanisms

pub mod actor;
pub mod journal;
pub mod snapshot;
pub mod serialization;
pub mod recovery;
#[cfg(feature = "scylla-driver")]
pub mod cassandra;
pub mod postgres;
#[cfg(feature = "coredb-backend")]
pub mod coredb;
pub mod config;
pub mod errors;
pub mod query;
pub mod event_adapter;
pub mod projection;
pub mod retention;
pub mod persistence_metrics;
pub mod durable_state;
pub mod at_least_once_delivery;
pub mod persistence_lifecycle;
pub mod persistence_testing;
pub mod multi_journal;
pub mod tagged_events;
pub mod persistence_healthcheck;
pub mod saga;
pub mod event_upcasting;
pub mod persistence_testkit_ext;

#[cfg(test)]
mod integration_tests;

// Generated protobuf types
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/pekko.persistence.rs"));
}

pub use actor::{PersistentActor, PersistentContext, PersistentRepr, SnapshotMetadata, SnapshotPolicy, AutoSnapshotResult};
pub use journal::*;
pub use snapshot::*;
pub use serialization::*;
pub use recovery::*;
#[cfg(feature = "scylla-driver")]
pub use cassandra::*;
#[cfg(feature = "postgres")]
pub use postgres::*;
#[cfg(feature = "coredb-backend")]
pub use coredb::*;
pub use config::*;
pub use errors::*;
pub use query::{ReadJournal, JournalReadJournal, EventEnvelope, Offset, LiveEventSource, TagEventSource};
pub use event_adapter::{EventAdapter, EventAdapterInput, EventAdapterOutput, VersionedEvent, VersionMigrator, VersioningAdapter, EventAdapterChain, EventAdapterRegistry, IdentityAdapter};
pub use projection::{ProjectionHandler, StatefulProjectionHandler, OffsetStore, InMemoryOffsetStore, Projection, ProjectionHandle, ProjectionConfig, ProjectionStats, ProjectionStatus, CollectingHandler, FnProjectionHandler};

// Re-export retention types
pub use retention::{
    RetentionCriteria, RetentionStrategy, RetentionState,
    SnapshotCountRetention, EventSequenceRetention, TimeBasedRetention,
    EventCountRetention, CompositeRetention, RetentionResult, RetentionStats,
};

// Re-export persistence metrics types
pub use persistence_metrics::{
    Histogram, HistogramSnapshot, Timer, JournalMetrics, SnapshotMetrics as PersistenceSnapshotMetrics,
    RecoveryMetrics as PersistenceRecoveryMetrics, PersistenceMetricsCollector, PersistenceMetricsSnapshot,
};

// Re-export durable state types
pub use durable_state::{
    DurableStateStore, InMemoryDurableStateStore, DurableStateEntry,
    DurableStateChange, DurableStateManager, DurableStateManagerStats, DurableStateError,
};

// Re-export at-least-once delivery types
pub use at_least_once_delivery::{
    AtLeastOnceDelivery, DeliveryConfig, DeliveryBuilder, DeliveryAttempt,
    DeliveryStatus, DeliveryEvent, DeliveryEventType, DeliveryId, DeliveryError,
    DeliveryStats, DeliverySnapshot, UnconfirmedDelivery, MessageDeduplicator,
};

// Re-export persistence lifecycle types
pub use persistence_lifecycle::{
    PersistenceLifecycleManager, LifecycleEvent, LifecycleStats, HookResult,
    PrePersistHook, PostPersistHook, SnapshotHook,
    EventTypeValidator, MetadataTagHook, PersistTimingHook, SnapshotRateLimiter,
};

// Re-export persistence testing types
pub use persistence_testing::{
    MockJournal, MockSnapshotStore, MockJournalError,
    PersistenceTestProbe, RecoveryResult as TestRecoveryResult,
    TestEvent, TestSnapshot, PersistenceAssertions, assert_persistence,
};

// Re-export multi-journal types
pub use multi_journal::{
    JournalRoutingStrategy, JournalBackend, MultiJournalRouter, MultiJournalStats,
    ReplicationManager, ReplicationMode, ReplicationLag, ReplicationStats,
};

// Re-export tagged events types
pub use tagged_events::{
    Tag, TagPattern, TagQuery, TaggedEvent, TagIndex, TagIndexStats,
};

// Re-export persistence healthcheck types
pub use persistence_healthcheck::{
    HealthStatus, ComponentHealth, PersistenceHealthCheck, HealthThresholds, HealthSnapshot,
    CleanupConfig, CleanupPlugin, CleanupResult, CleanupRecommendation, CleanupAction,
};

// Re-export saga types
pub use saga::{
    Saga, SagaBuilder, SagaStep, SagaConfig, SagaState, SagaAction, SagaError,
    StepStatus, StepExecution, SagaLogEntry, SagaLogEvent,
};

// Re-export event upcasting types
pub use event_upcasting::{
    EventUpcaster, EventVersion, VersionedPayload, UpcastResult, UpcastError,
    UpcastStep, UpcastStats,
};

// Re-export persistence testkit extension types
pub use persistence_testkit_ext::{
    PersistenceScenario, ScenarioStep, ScenarioStepType, ScenarioEvent, StateSnapshot,
    RecoveryVerifier, RecoveryVerification, EventStreamAssertion,
};

// Re-export commonly used types
pub use chrono::{DateTime, Utc};
pub use serde::{Deserialize, Serialize};
pub use uuid::Uuid;

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_persistent_repr_creation() {
        let repr = PersistentRepr::new(
            "test-actor".to_string(),
            1,
            "TestEvent".to_string(),
            vec![1, 2, 3, 4],
        );
        
        assert_eq!(repr.persistence_id, "test-actor");
        assert_eq!(repr.sequence_nr, 1);
        assert_eq!(repr.manifest, "TestEvent");
    }
}

