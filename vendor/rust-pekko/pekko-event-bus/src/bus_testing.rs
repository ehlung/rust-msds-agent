// bus_testing.rs - EventBus Testing Utilities
// MockEventBus, TestProbe, Assertions DSL

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::bus_errors::EventBusError;
use crate::partition_strategy::PartitionKey;
use crate::topic_partition::PublishResult;

// ============================================================================
// Recorded Event
// ============================================================================

/// A captured event for test assertions
#[derive(Debug, Clone)]
pub struct RecordedEvent {
    pub topic: String,
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub partition_hint: Option<u32>,
}

impl RecordedEvent {
    pub fn payload_as_string(&self) -> Option<String> {
        String::from_utf8(self.payload.clone()).ok()
    }
}

// ============================================================================
// MockEventBus
// ============================================================================

/// A mock event bus for unit testing — records all publishes without real partitions
#[derive(Clone)]
pub struct MockEventBus {
    events: Arc<Mutex<Vec<RecordedEvent>>>,
    fail_on_topic: Arc<Mutex<Option<String>>>,
    next_offset: Arc<Mutex<u64>>,
    is_shutdown: Arc<Mutex<bool>>,
}

impl MockEventBus {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            fail_on_topic: Arc::new(Mutex::new(None)),
            next_offset: Arc::new(Mutex::new(0)),
            is_shutdown: Arc::new(Mutex::new(false)),
        }
    }

    /// Configure the mock to fail on a specific topic
    pub fn fail_on_topic(&self, topic: &str) {
        *self.fail_on_topic.lock().unwrap() = Some(topic.to_string());
    }

    /// Clear the failure configuration
    pub fn clear_failure(&self) {
        *self.fail_on_topic.lock().unwrap() = None;
    }

    /// Publish an event (records it)
    pub fn publish(
        &self,
        topic: &str,
        key: &PartitionKey,
        payload: Vec<u8>,
    ) -> Result<PublishResult, EventBusError> {
        if *self.is_shutdown.lock().unwrap() {
            return Err(EventBusError::ShutdownInProgress);
        }

        if let Some(ref fail_topic) = *self.fail_on_topic.lock().unwrap() {
            if fail_topic == topic {
                return Err(EventBusError::PublishFailed {
                    topic: topic.to_string(),
                    reason: "Mock failure".into(),
                });
            }
        }

        let key_str = match key {
            PartitionKey::String(s) => Some(s.clone()),
            PartitionKey::Numeric(n) => Some(n.to_string()),
            PartitionKey::Composite(parts) => Some(parts.join(":")),
            PartitionKey::None => None,
        };

        let event = RecordedEvent {
            topic: topic.to_string(),
            key: key_str,
            payload,
            partition_hint: None,
        };

        self.events.lock().unwrap().push(event);
        let offset = {
            let mut o = self.next_offset.lock().unwrap();
            let current = *o;
            *o += 1;
            current
        };

        Ok(PublishResult::Accepted { offset })
    }

    /// Shutdown the mock bus
    pub fn shutdown(&self) {
        *self.is_shutdown.lock().unwrap() = true;
    }

    /// Get all recorded events
    pub fn recorded_events(&self) -> Vec<RecordedEvent> {
        self.events.lock().unwrap().clone()
    }

    /// Get events for a specific topic
    pub fn events_for_topic(&self, topic: &str) -> Vec<RecordedEvent> {
        self.events.lock().unwrap()
            .iter()
            .filter(|e| e.topic == topic)
            .cloned()
            .collect()
    }

    /// Total number of published events
    pub fn event_count(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    /// Clear all recorded events
    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    /// Create an assertion builder for this mock
    pub fn assert_that(&self) -> MockBusAssertions {
        MockBusAssertions {
            events: self.recorded_events(),
        }
    }
}

impl Default for MockEventBus {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// EventBus Test Probe
// ============================================================================

/// A test probe that collects events from a specific topic
pub struct EventBusTestProbe {
    topic: String,
    received: VecDeque<RecordedEvent>,
    expected_count: Option<usize>,
}

impl EventBusTestProbe {
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            received: VecDeque::new(),
            expected_count: None,
        }
    }

    /// Feed an event to the probe
    pub fn receive(&mut self, event: RecordedEvent) {
        if event.topic == self.topic {
            self.received.push_back(event);
        }
    }

    /// Feed multiple events
    pub fn receive_all(&mut self, events: Vec<RecordedEvent>) {
        for event in events {
            self.receive(event);
        }
    }

    /// Set expected count for assertions
    pub fn expect_count(mut self, count: usize) -> Self {
        self.expected_count = Some(count);
        self
    }

    /// Get the next received event
    pub fn next_event(&mut self) -> Option<RecordedEvent> {
        self.received.pop_front()
    }

    /// Peek at the next event without consuming
    pub fn peek(&self) -> Option<&RecordedEvent> {
        self.received.front()
    }

    /// Number of events received
    pub fn received_count(&self) -> usize {
        self.received.len()
    }

    /// Whether the probe has received events
    pub fn has_events(&self) -> bool {
        !self.received.is_empty()
    }

    /// All received events (cloned)
    pub fn all_events(&self) -> Vec<RecordedEvent> {
        self.received.iter().cloned().collect()
    }

    /// Verify expected count
    pub fn verify(&self) -> Result<(), String> {
        if let Some(expected) = self.expected_count {
            if self.received.len() != expected {
                return Err(format!(
                    "Expected {} events on topic '{}', got {}",
                    expected, self.topic, self.received.len()
                ));
            }
        }
        Ok(())
    }
}

// ============================================================================
// Assertion DSL
// ============================================================================

/// Fluent assertions for MockEventBus
pub struct MockBusAssertions {
    events: Vec<RecordedEvent>,
}

impl MockBusAssertions {
    /// Assert total event count
    pub fn has_event_count(self, expected: usize) -> Self {
        assert_eq!(
            self.events.len(), expected,
            "Expected {} events, got {}", expected, self.events.len()
        );
        self
    }

    /// Assert events exist for a topic
    pub fn has_events_for_topic(self, topic: &str) -> Self {
        let count = self.events.iter().filter(|e| e.topic == topic).count();
        assert!(count > 0, "No events found for topic '{}'", topic);
        self
    }

    /// Assert event count for a specific topic
    pub fn has_topic_event_count(self, topic: &str, expected: usize) -> Self {
        let count = self.events.iter().filter(|e| e.topic == topic).count();
        assert_eq!(
            count, expected,
            "Expected {} events for topic '{}', got {}", expected, topic, count
        );
        self
    }

    /// Assert no events for a topic
    pub fn has_no_events_for_topic(self, topic: &str) -> Self {
        let count = self.events.iter().filter(|e| e.topic == topic).count();
        assert_eq!(
            count, 0,
            "Expected no events for topic '{}', got {}", topic, count
        );
        self
    }

    /// Assert an event with a specific key exists
    pub fn has_event_with_key(self, key: &str) -> Self {
        let found = self.events.iter().any(|e| e.key.as_deref() == Some(key));
        assert!(found, "No event found with key '{}'", key);
        self
    }

    /// Assert an event with specific payload exists
    pub fn has_event_with_payload(self, payload: &[u8]) -> Self {
        let found = self.events.iter().any(|e| e.payload == payload);
        assert!(found, "No event found with payload {:?}", payload);
        self
    }

    /// Assert the latest event matches criteria
    pub fn latest_event_has_topic(self, topic: &str) -> Self {
        let last = self.events.last().expect("No events recorded");
        assert_eq!(last.topic, topic, "Latest event topic mismatch");
        self
    }

    /// Assert events are ordered by topic
    pub fn events_ordered_by_topic(self) -> Self {
        let topics: Vec<&str> = self.events.iter().map(|e| e.topic.as_str()).collect();
        let mut sorted = topics.clone();
        sorted.sort();
        assert_eq!(topics, sorted, "Events not ordered by topic");
        self
    }

    /// Get the filtered events for further inspection
    pub fn events(&self) -> &[RecordedEvent] {
        &self.events
    }
}

/// Convenience function to create a mock bus
pub fn mock_event_bus() -> MockEventBus {
    MockEventBus::new()
}

/// Convenience function to create a test probe
pub fn test_probe(topic: &str) -> EventBusTestProbe {
    EventBusTestProbe::new(topic)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- MockEventBus tests ---

    #[test]
    fn test_mock_bus_publish() {
        let bus = mock_event_bus();
        let key = PartitionKey::from_str("k1");
        let result = bus.publish("orders", &key, vec![1, 2, 3]).unwrap();
        match result {
            PublishResult::Accepted { offset } => assert_eq!(offset, 0),
            _ => panic!("Expected Accepted"),
        }
        assert_eq!(bus.event_count(), 1);
    }

    #[test]
    fn test_mock_bus_multiple_publishes() {
        let bus = mock_event_bus();
        for i in 0..5 {
            bus.publish("orders", &PartitionKey::from_str(&format!("k{}", i)), vec![i]).unwrap();
        }
        assert_eq!(bus.event_count(), 5);
        assert_eq!(bus.events_for_topic("orders").len(), 5);
    }

    #[test]
    fn test_mock_bus_multiple_topics() {
        let bus = mock_event_bus();
        bus.publish("orders", &PartitionKey::None, vec![1]).unwrap();
        bus.publish("payments", &PartitionKey::None, vec![2]).unwrap();
        bus.publish("orders", &PartitionKey::None, vec![3]).unwrap();

        assert_eq!(bus.events_for_topic("orders").len(), 2);
        assert_eq!(bus.events_for_topic("payments").len(), 1);
    }

    #[test]
    fn test_mock_bus_fail_on_topic() {
        let bus = mock_event_bus();
        bus.fail_on_topic("orders");

        let result = bus.publish("orders", &PartitionKey::None, vec![]);
        assert!(result.is_err());

        // Other topics still work
        let result = bus.publish("payments", &PartitionKey::None, vec![]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_bus_clear_failure() {
        let bus = mock_event_bus();
        bus.fail_on_topic("orders");
        bus.clear_failure();

        let result = bus.publish("orders", &PartitionKey::None, vec![]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_bus_shutdown() {
        let bus = mock_event_bus();
        bus.shutdown();
        let result = bus.publish("orders", &PartitionKey::None, vec![]);
        assert!(matches!(result, Err(EventBusError::ShutdownInProgress)));
    }

    #[test]
    fn test_mock_bus_clear() {
        let bus = mock_event_bus();
        bus.publish("orders", &PartitionKey::None, vec![1]).unwrap();
        assert_eq!(bus.event_count(), 1);
        bus.clear();
        assert_eq!(bus.event_count(), 0);
    }

    #[test]
    fn test_mock_bus_clone_shares_state() {
        let bus1 = mock_event_bus();
        let bus2 = bus1.clone();

        bus1.publish("orders", &PartitionKey::None, vec![1]).unwrap();
        assert_eq!(bus2.event_count(), 1);
    }

    // --- RecordedEvent tests ---

    #[test]
    fn test_recorded_event_payload_as_string() {
        let event = RecordedEvent {
            topic: "t".into(),
            key: None,
            payload: b"hello world".to_vec(),
            partition_hint: None,
        };
        assert_eq!(event.payload_as_string(), Some("hello world".to_string()));
    }

    // --- TestProbe tests ---

    #[test]
    fn test_probe_receive() {
        let mut probe = test_probe("orders");
        probe.receive(RecordedEvent {
            topic: "orders".into(),
            key: Some("k1".into()),
            payload: vec![1],
            partition_hint: None,
        });
        probe.receive(RecordedEvent {
            topic: "payments".into(), // wrong topic, ignored
            key: None,
            payload: vec![2],
            partition_hint: None,
        });

        assert_eq!(probe.received_count(), 1);
        assert!(probe.has_events());
    }

    #[test]
    fn test_probe_next_event() {
        let mut probe = test_probe("orders");
        probe.receive(RecordedEvent {
            topic: "orders".into(),
            key: Some("k1".into()),
            payload: vec![1],
            partition_hint: None,
        });

        let event = probe.next_event().unwrap();
        assert_eq!(event.key, Some("k1".to_string()));
        assert_eq!(probe.received_count(), 0);
    }

    #[test]
    fn test_probe_peek() {
        let mut probe = test_probe("orders");
        probe.receive(RecordedEvent {
            topic: "orders".into(),
            key: Some("k1".into()),
            payload: vec![1],
            partition_hint: None,
        });

        let peeked = probe.peek().unwrap();
        assert_eq!(peeked.key, Some("k1".to_string()));
        assert_eq!(probe.received_count(), 1); // not consumed
    }

    #[test]
    fn test_probe_verify_pass() {
        let mut probe = test_probe("orders").expect_count(2);
        for _ in 0..2 {
            probe.receive(RecordedEvent {
                topic: "orders".into(),
                key: None,
                payload: vec![],
                partition_hint: None,
            });
        }
        assert!(probe.verify().is_ok());
    }

    #[test]
    fn test_probe_verify_fail() {
        let probe = test_probe("orders").expect_count(5);
        let result = probe.verify();
        assert!(result.is_err());
    }

    #[test]
    fn test_probe_receive_all() {
        let mut probe = test_probe("orders");
        let events = vec![
            RecordedEvent { topic: "orders".into(), key: None, payload: vec![1], partition_hint: None },
            RecordedEvent { topic: "orders".into(), key: None, payload: vec![2], partition_hint: None },
        ];
        probe.receive_all(events);
        assert_eq!(probe.received_count(), 2);
    }

    // --- Assertion DSL tests ---

    #[test]
    fn test_assertions_event_count() {
        let bus = mock_event_bus();
        bus.publish("orders", &PartitionKey::None, vec![1]).unwrap();
        bus.publish("orders", &PartitionKey::None, vec![2]).unwrap();

        bus.assert_that()
            .has_event_count(2);
    }

    #[test]
    fn test_assertions_topic_events() {
        let bus = mock_event_bus();
        bus.publish("orders", &PartitionKey::None, vec![1]).unwrap();
        bus.publish("payments", &PartitionKey::None, vec![2]).unwrap();

        bus.assert_that()
            .has_events_for_topic("orders")
            .has_events_for_topic("payments")
            .has_topic_event_count("orders", 1)
            .has_topic_event_count("payments", 1);
    }

    #[test]
    fn test_assertions_no_events() {
        let bus = mock_event_bus();
        bus.publish("orders", &PartitionKey::None, vec![1]).unwrap();

        bus.assert_that()
            .has_no_events_for_topic("payments");
    }

    #[test]
    fn test_assertions_key_and_payload() {
        let bus = mock_event_bus();
        bus.publish("orders", &PartitionKey::from_str("user-42"), vec![1, 2, 3]).unwrap();

        bus.assert_that()
            .has_event_with_key("user-42")
            .has_event_with_payload(&[1, 2, 3]);
    }

    #[test]
    fn test_assertions_latest_event() {
        let bus = mock_event_bus();
        bus.publish("orders", &PartitionKey::None, vec![1]).unwrap();
        bus.publish("payments", &PartitionKey::None, vec![2]).unwrap();

        bus.assert_that()
            .latest_event_has_topic("payments");
    }

    #[test]
    fn test_assertions_chaining() {
        let bus = mock_event_bus();
        bus.publish("orders", &PartitionKey::from_str("k1"), vec![1]).unwrap();
        bus.publish("orders", &PartitionKey::from_str("k2"), vec![2]).unwrap();
        bus.publish("payments", &PartitionKey::from_str("k3"), vec![3]).unwrap();

        bus.assert_that()
            .has_event_count(3)
            .has_topic_event_count("orders", 2)
            .has_topic_event_count("payments", 1)
            .has_event_with_key("k1")
            .has_event_with_key("k2")
            .has_event_with_key("k3")
            .latest_event_has_topic("payments");
    }
}
