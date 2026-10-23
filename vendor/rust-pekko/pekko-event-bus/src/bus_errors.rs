// bus_errors.rs - EventBus Error Types

use std::fmt;

pub type TopicName = String;
pub type PartitionId = u32;
pub type ConsumerGroupId = String;

#[derive(Debug, Clone, PartialEq)]
pub enum EventBusError {
    // Topic errors
    TopicNotFound(TopicName),
    TopicAlreadyExists(TopicName),
    InvalidTopicConfig { topic: TopicName, reason: String },

    // Partition errors
    PartitionNotFound { topic: TopicName, partition: PartitionId },
    PartitionFull { topic: TopicName, partition: PartitionId },

    // Publish errors
    PublishFailed { topic: TopicName, reason: String },
    BufferOverflow { topic: TopicName, partition: PartitionId },
    FlushFailed { topic: TopicName, partition: PartitionId, reason: String },

    // Consumer errors
    ConsumerGroupNotFound(ConsumerGroupId),
    ConsumerGroupAlreadyExists(ConsumerGroupId),
    ConsumerAlreadySubscribed { group: ConsumerGroupId, topic: TopicName },
    OffsetOutOfRange { topic: TopicName, partition: PartitionId, offset: u64 },

    // Rebalancing errors
    RebalancingInProgress(ConsumerGroupId),
    CoordinatorUnavailable,

    // Serialization errors
    SerializationError(String),
    DeserializationError(String),

    // System errors
    ShutdownInProgress,
    Timeout { operation: String, duration_ms: u64 },
    Internal(String),
}

impl fmt::Display for EventBusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventBusError::TopicNotFound(t) => write!(f, "Topic not found: {}", t),
            EventBusError::TopicAlreadyExists(t) => write!(f, "Topic already exists: {}", t),
            EventBusError::InvalidTopicConfig { topic, reason } =>
                write!(f, "Invalid config for topic '{}': {}", topic, reason),
            EventBusError::PartitionNotFound { topic, partition } =>
                write!(f, "Partition {}/{} not found", topic, partition),
            EventBusError::PartitionFull { topic, partition } =>
                write!(f, "Partition {}/{} is full", topic, partition),
            EventBusError::PublishFailed { topic, reason } =>
                write!(f, "Publish to '{}' failed: {}", topic, reason),
            EventBusError::BufferOverflow { topic, partition } =>
                write!(f, "Buffer overflow on {}/{}", topic, partition),
            EventBusError::FlushFailed { topic, partition, reason } =>
                write!(f, "Flush failed on {}/{}: {}", topic, partition, reason),
            EventBusError::ConsumerGroupNotFound(g) =>
                write!(f, "Consumer group not found: {}", g),
            EventBusError::ConsumerGroupAlreadyExists(g) =>
                write!(f, "Consumer group already exists: {}", g),
            EventBusError::ConsumerAlreadySubscribed { group, topic } =>
                write!(f, "Consumer group '{}' already subscribed to '{}'", group, topic),
            EventBusError::OffsetOutOfRange { topic, partition, offset } =>
                write!(f, "Offset {} out of range for {}/{}", offset, topic, partition),
            EventBusError::RebalancingInProgress(g) =>
                write!(f, "Rebalancing in progress for group '{}'", g),
            EventBusError::CoordinatorUnavailable =>
                write!(f, "Consumer group coordinator unavailable"),
            EventBusError::SerializationError(msg) =>
                write!(f, "Serialization error: {}", msg),
            EventBusError::DeserializationError(msg) =>
                write!(f, "Deserialization error: {}", msg),
            EventBusError::ShutdownInProgress =>
                write!(f, "EventBus shutdown in progress"),
            EventBusError::Timeout { operation, duration_ms } =>
                write!(f, "Timeout after {}ms on: {}", duration_ms, operation),
            EventBusError::Internal(msg) =>
                write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for EventBusError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = EventBusError::TopicNotFound("orders".into());
        assert_eq!(err.to_string(), "Topic not found: orders");
    }

    #[test]
    fn test_partition_error_display() {
        let err = EventBusError::PartitionNotFound { topic: "orders".into(), partition: 3 };
        assert_eq!(err.to_string(), "Partition orders/3 not found");
    }

    #[test]
    fn test_publish_error() {
        let err = EventBusError::PublishFailed { topic: "orders".into(), reason: "journal write failed".into() };
        assert!(err.to_string().contains("journal write failed"));
    }

    #[test]
    fn test_consumer_error() {
        let err = EventBusError::ConsumerAlreadySubscribed { group: "billing".into(), topic: "orders".into() };
        assert!(err.to_string().contains("billing"));
        assert!(err.to_string().contains("orders"));
    }

    #[test]
    fn test_error_equality() {
        assert_eq!(
            EventBusError::TopicNotFound("a".into()),
            EventBusError::TopicNotFound("a".into())
        );
        assert_ne!(
            EventBusError::TopicNotFound("a".into()),
            EventBusError::TopicNotFound("b".into())
        );
    }

    #[test]
    fn test_timeout_error() {
        let err = EventBusError::Timeout { operation: "publish".into(), duration_ms: 5000 };
        assert!(err.to_string().contains("5000ms"));
    }
}
