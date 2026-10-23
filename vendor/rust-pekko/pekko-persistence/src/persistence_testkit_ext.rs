// persistence_testkit_ext.rs - Extended Persistence TestKit
// Provides scenario-based testing, recovery verification, and event stream assertions

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// PersistenceScenario
// ============================================================================

/// A scenario-based test that simulates a sequence of commands and verifies events/state
#[derive(Debug)]
pub struct PersistenceScenario {
    name: String,
    steps: Vec<ScenarioStep>,
    executed_steps: usize,
    events_produced: Vec<ScenarioEvent>,
    state_snapshots: Vec<StateSnapshot>,
    assertions_passed: usize,
    assertions_failed: usize,
    errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ScenarioStep {
    pub name: String,
    pub step_type: ScenarioStepType,
    pub expected_events: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScenarioStepType {
    Command(String),
    Query(String),
    Snapshot,
    Recovery,
    Wait(Duration),
    AssertState(String),
    AssertEventCount(usize),
}

#[derive(Debug, Clone)]
pub struct ScenarioEvent {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub event_type: String,
    pub payload: String,
    pub timestamp: Instant,
}

#[derive(Debug, Clone)]
pub struct StateSnapshot {
    pub step_name: String,
    pub state: HashMap<String, String>,
    pub sequence_nr: u64,
    pub timestamp: Instant,
}

impl PersistenceScenario {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            steps: Vec::new(),
            executed_steps: 0,
            events_produced: Vec::new(),
            state_snapshots: Vec::new(),
            assertions_passed: 0,
            assertions_failed: 0,
            errors: Vec::new(),
        }
    }

    pub fn add_step(&mut self, step: ScenarioStep) {
        self.steps.push(step);
    }

    pub fn command(mut self, name: &str, command: &str, expected_events: usize) -> Self {
        self.steps.push(ScenarioStep {
            name: name.to_string(),
            step_type: ScenarioStepType::Command(command.to_string()),
            expected_events,
        });
        self
    }

    pub fn query(mut self, name: &str, query: &str) -> Self {
        self.steps.push(ScenarioStep {
            name: name.to_string(),
            step_type: ScenarioStepType::Query(query.to_string()),
            expected_events: 0,
        });
        self
    }

    pub fn snapshot(mut self, name: &str) -> Self {
        self.steps.push(ScenarioStep {
            name: name.to_string(),
            step_type: ScenarioStepType::Snapshot,
            expected_events: 0,
        });
        self
    }

    pub fn recovery(mut self, name: &str) -> Self {
        self.steps.push(ScenarioStep {
            name: name.to_string(),
            step_type: ScenarioStepType::Recovery,
            expected_events: 0,
        });
        self
    }

    pub fn assert_state(mut self, name: &str, expected: &str) -> Self {
        self.steps.push(ScenarioStep {
            name: name.to_string(),
            step_type: ScenarioStepType::AssertState(expected.to_string()),
            expected_events: 0,
        });
        self
    }

    pub fn assert_event_count(mut self, name: &str, count: usize) -> Self {
        self.steps.push(ScenarioStep {
            name: name.to_string(),
            step_type: ScenarioStepType::AssertEventCount(count),
            expected_events: 0,
        });
        self
    }

    /// Execute a step and record the event
    pub fn execute_step(&mut self, persistence_id: &str, events: Vec<(String, String)>) {
        if self.executed_steps >= self.steps.len() {
            self.errors.push("No more steps to execute".to_string());
            return;
        }

        let step = &self.steps[self.executed_steps];
        let base_seq = self.events_produced.len() as u64 + 1;

        for (i, (event_type, payload)) in events.iter().enumerate() {
            self.events_produced.push(ScenarioEvent {
                persistence_id: persistence_id.to_string(),
                sequence_nr: base_seq + i as u64,
                event_type: event_type.clone(),
                payload: payload.clone(),
                timestamp: Instant::now(),
            });
        }

        // Check expected event count
        if step.expected_events > 0 && events.len() != step.expected_events {
            self.assertions_failed += 1;
            self.errors.push(format!(
                "Step '{}': expected {} events, got {}",
                step.name, step.expected_events, events.len()
            ));
        } else if step.expected_events > 0 {
            self.assertions_passed += 1;
        }

        self.executed_steps += 1;
    }

    /// Record a state snapshot
    pub fn record_state(&mut self, state: HashMap<String, String>, sequence_nr: u64) {
        let step_name = if self.executed_steps > 0 {
            self.steps[self.executed_steps - 1].name.clone()
        } else {
            "initial".to_string()
        };
        self.state_snapshots.push(StateSnapshot {
            step_name,
            state,
            sequence_nr,
            timestamp: Instant::now(),
        });
    }

    /// Assert current event count
    pub fn check_event_count(&mut self, expected: usize) -> bool {
        if self.events_produced.len() == expected {
            self.assertions_passed += 1;
            true
        } else {
            self.assertions_failed += 1;
            self.errors.push(format!(
                "Event count: expected {}, got {}",
                expected,
                self.events_produced.len()
            ));
            false
        }
    }

    /// Assert that a specific event type was produced
    pub fn check_event_type_exists(&mut self, event_type: &str) -> bool {
        if self.events_produced.iter().any(|e| e.event_type == event_type) {
            self.assertions_passed += 1;
            true
        } else {
            self.assertions_failed += 1;
            self.errors.push(format!("Event type '{}' not found", event_type));
            false
        }
    }

    /// Assert event ordering
    pub fn check_event_order(&mut self, expected_types: &[&str]) -> bool {
        let actual: Vec<&str> = self.events_produced.iter().map(|e| e.event_type.as_str()).collect();
        if actual.len() >= expected_types.len()
            && actual.iter().zip(expected_types.iter()).all(|(a, e)| a == e)
        {
            self.assertions_passed += 1;
            true
        } else {
            self.assertions_failed += 1;
            self.errors.push(format!("Event order mismatch: expected {:?}, got {:?}", expected_types, actual));
            false
        }
    }

    // ── Queries ──────────────────────────────────────────

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn steps(&self) -> &[ScenarioStep] {
        &self.steps
    }

    pub fn executed_steps(&self) -> usize {
        self.executed_steps
    }

    pub fn events(&self) -> &[ScenarioEvent] {
        &self.events_produced
    }

    pub fn snapshots(&self) -> &[StateSnapshot] {
        &self.state_snapshots
    }

    pub fn assertions_passed(&self) -> usize {
        self.assertions_passed
    }

    pub fn assertions_failed(&self) -> usize {
        self.assertions_failed
    }

    pub fn errors(&self) -> &[String] {
        &self.errors
    }

    pub fn is_complete(&self) -> bool {
        !self.steps.is_empty() && self.executed_steps >= self.steps.len()
    }

    pub fn is_success(&self) -> bool {
        self.assertions_failed == 0 && self.errors.is_empty()
    }

    pub fn total_events(&self) -> usize {
        self.events_produced.len()
    }

    pub fn events_by_type(&self, event_type: &str) -> Vec<&ScenarioEvent> {
        self.events_produced.iter().filter(|e| e.event_type == event_type).collect()
    }

    pub fn last_sequence_nr(&self) -> u64 {
        self.events_produced.last().map(|e| e.sequence_nr).unwrap_or(0)
    }

    pub fn latest_state(&self) -> Option<&StateSnapshot> {
        self.state_snapshots.last()
    }
}

// ============================================================================
// RecoveryVerifier
// ============================================================================

/// Verifies that recovery produces the correct state from persisted events
pub struct RecoveryVerifier {
    persistence_id: String,
    events: Vec<(String, String)>,
    expected_state: HashMap<String, String>,
    expected_sequence_nr: u64,
    recovery_count: u32,
    results: Vec<RecoveryVerification>,
}

#[derive(Debug, Clone)]
pub struct RecoveryVerification {
    pub attempt: u32,
    pub events_replayed: usize,
    pub final_sequence_nr: u64,
    pub state_match: bool,
    pub duration: Duration,
    pub errors: Vec<String>,
}

impl RecoveryVerifier {
    pub fn new(persistence_id: &str) -> Self {
        Self {
            persistence_id: persistence_id.to_string(),
            events: Vec::new(),
            expected_state: HashMap::new(),
            expected_sequence_nr: 0,
            recovery_count: 0,
            results: Vec::new(),
        }
    }

    pub fn with_events(mut self, events: Vec<(String, String)>) -> Self {
        self.expected_sequence_nr = events.len() as u64;
        self.events = events;
        self
    }

    pub fn with_expected_state(mut self, state: HashMap<String, String>) -> Self {
        self.expected_state = state;
        self
    }

    pub fn with_expected_sequence_nr(mut self, nr: u64) -> Self {
        self.expected_sequence_nr = nr;
        self
    }

    /// Simulate a recovery and verify
    pub fn verify_recovery(
        &mut self,
        recovered_state: &HashMap<String, String>,
        recovered_sequence_nr: u64,
    ) -> &RecoveryVerification {
        self.recovery_count += 1;
        let start = Instant::now();
        let mut errors = Vec::new();

        let state_match = if self.expected_state.is_empty() {
            true
        } else {
            if recovered_state != &self.expected_state {
                errors.push(format!(
                    "State mismatch: expected {:?}, got {:?}",
                    self.expected_state, recovered_state
                ));
                false
            } else {
                true
            }
        };

        if recovered_sequence_nr != self.expected_sequence_nr {
            errors.push(format!(
                "Sequence nr mismatch: expected {}, got {}",
                self.expected_sequence_nr, recovered_sequence_nr
            ));
        }

        self.results.push(RecoveryVerification {
            attempt: self.recovery_count,
            events_replayed: self.events.len(),
            final_sequence_nr: recovered_sequence_nr,
            state_match,
            duration: start.elapsed(),
            errors,
        });

        self.results.last().unwrap()
    }

    pub fn persistence_id(&self) -> &str {
        &self.persistence_id
    }

    pub fn results(&self) -> &[RecoveryVerification] {
        &self.results
    }

    pub fn all_passed(&self) -> bool {
        self.results.iter().all(|r| r.state_match && r.errors.is_empty())
    }

    pub fn total_recoveries(&self) -> u32 {
        self.recovery_count
    }
}

impl fmt::Debug for RecoveryVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveryVerifier")
            .field("persistence_id", &self.persistence_id)
            .field("events", &self.events.len())
            .field("recoveries", &self.recovery_count)
            .field("all_passed", &self.all_passed())
            .finish()
    }
}

// ============================================================================
// EventStreamAssertion
// ============================================================================

/// Fluent assertions for event streams
pub struct EventStreamAssertion {
    events: Vec<(String, String, u64)>, // (type, payload, seq_nr)
    assertions_passed: usize,
    assertions_failed: usize,
    errors: Vec<String>,
}

impl EventStreamAssertion {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            assertions_passed: 0,
            assertions_failed: 0,
            errors: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event_type: &str, payload: &str, seq_nr: u64) {
        self.events.push((event_type.to_string(), payload.to_string(), seq_nr));
    }

    pub fn from_events(events: Vec<(String, String, u64)>) -> Self {
        Self {
            events,
            assertions_passed: 0,
            assertions_failed: 0,
            errors: Vec::new(),
        }
    }

    /// Assert total number of events
    pub fn has_count(&mut self, expected: usize) -> &mut Self {
        if self.events.len() == expected {
            self.assertions_passed += 1;
        } else {
            self.assertions_failed += 1;
            self.errors.push(format!("Count: expected {}, got {}", expected, self.events.len()));
        }
        self
    }

    /// Assert event at specific index
    pub fn at_index(&mut self, idx: usize, expected_type: &str) -> &mut Self {
        if let Some((t, _, _)) = self.events.get(idx) {
            if t == expected_type {
                self.assertions_passed += 1;
            } else {
                self.assertions_failed += 1;
                self.errors.push(format!("At index {}: expected '{}', got '{}'", idx, expected_type, t));
            }
        } else {
            self.assertions_failed += 1;
            self.errors.push(format!("Index {} out of bounds (len={})", idx, self.events.len()));
        }
        self
    }

    /// Assert first event type
    pub fn first_is(&mut self, expected_type: &str) -> &mut Self {
        self.at_index(0, expected_type)
    }

    /// Assert last event type
    pub fn last_is(&mut self, expected_type: &str) -> &mut Self {
        if self.events.is_empty() {
            self.assertions_failed += 1;
            self.errors.push("No events for last_is check".to_string());
        } else {
            let idx = self.events.len() - 1;
            self.at_index(idx, expected_type);
        }
        self
    }

    /// Assert no events of a specific type
    pub fn has_no(&mut self, event_type: &str) -> &mut Self {
        if self.events.iter().any(|(t, _, _)| t == event_type) {
            self.assertions_failed += 1;
            self.errors.push(format!("Unexpected event type: '{}'", event_type));
        } else {
            self.assertions_passed += 1;
        }
        self
    }

    /// Assert contains event of specific type
    pub fn contains(&mut self, event_type: &str) -> &mut Self {
        if self.events.iter().any(|(t, _, _)| t == event_type) {
            self.assertions_passed += 1;
        } else {
            self.assertions_failed += 1;
            self.errors.push(format!("Missing event type: '{}'", event_type));
        }
        self
    }

    /// Assert sequence numbers are monotonically increasing
    pub fn has_monotonic_sequence(&mut self) -> &mut Self {
        let mut prev = 0u64;
        let mut ok = true;
        for (_, _, seq) in &self.events {
            if *seq <= prev && prev > 0 {
                ok = false;
                break;
            }
            prev = *seq;
        }
        if ok {
            self.assertions_passed += 1;
        } else {
            self.assertions_failed += 1;
            self.errors.push("Sequence numbers not monotonically increasing".to_string());
        }
        self
    }

    /// Assert all events match a predicate
    pub fn all_match(&mut self, predicate: impl Fn(&str, &str, u64) -> bool) -> &mut Self {
        if self.events.iter().all(|(t, p, s)| predicate(t, p, *s)) {
            self.assertions_passed += 1;
        } else {
            self.assertions_failed += 1;
            self.errors.push("Not all events match predicate".to_string());
        }
        self
    }

    /// Count events of a specific type
    pub fn count_of(&self, event_type: &str) -> usize {
        self.events.iter().filter(|(t, _, _)| t == event_type).count()
    }

    pub fn is_success(&self) -> bool {
        self.assertions_failed == 0
    }

    pub fn passed(&self) -> usize {
        self.assertions_passed
    }

    pub fn failed(&self) -> usize {
        self.assertions_failed
    }

    pub fn errors(&self) -> &[String] {
        &self.errors
    }

    pub fn total_events(&self) -> usize {
        self.events.len()
    }
}

impl Default for EventStreamAssertion {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for EventStreamAssertion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStreamAssertion")
            .field("events", &self.events.len())
            .field("passed", &self.assertions_passed)
            .field("failed", &self.assertions_failed)
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scenario_creation() {
        let scenario = PersistenceScenario::new("order-test");
        assert_eq!(scenario.name(), "order-test");
        assert_eq!(scenario.executed_steps(), 0);
        assert!(!scenario.is_complete());
    }

    #[test]
    fn test_scenario_builder() {
        let scenario = PersistenceScenario::new("test")
            .command("create", "CreateOrder", 1)
            .command("pay", "ProcessPayment", 1)
            .snapshot("save-snapshot")
            .assert_event_count("check", 2);
        assert_eq!(scenario.steps().len(), 4);
    }

    #[test]
    fn test_scenario_execute_step() {
        let mut scenario = PersistenceScenario::new("test")
            .command("create", "CreateOrder", 1);

        scenario.execute_step("order-1", vec![("OrderCreated".into(), "{}".into())]);
        assert_eq!(scenario.executed_steps(), 1);
        assert_eq!(scenario.total_events(), 1);
        assert!(scenario.is_complete());
        assert!(scenario.is_success());
    }

    #[test]
    fn test_scenario_event_count_mismatch() {
        let mut scenario = PersistenceScenario::new("test")
            .command("create", "CreateOrder", 2); // expects 2 events

        scenario.execute_step("order-1", vec![("OrderCreated".into(), "{}".into())]); // only 1
        assert_eq!(scenario.assertions_failed(), 1);
        assert!(!scenario.is_success());
    }

    #[test]
    fn test_scenario_check_event_count() {
        let mut scenario = PersistenceScenario::new("test")
            .command("s1", "cmd", 1);
        scenario.execute_step("id", vec![("E1".into(), "d".into())]);
        assert!(scenario.check_event_count(1));
        assert!(!scenario.check_event_count(2));
    }

    #[test]
    fn test_scenario_check_event_type() {
        let mut scenario = PersistenceScenario::new("test")
            .command("s1", "cmd", 0);
        scenario.execute_step("id", vec![("OrderCreated".into(), "d".into())]);
        assert!(scenario.check_event_type_exists("OrderCreated"));
        assert!(!scenario.check_event_type_exists("OrderDeleted"));
    }

    #[test]
    fn test_scenario_check_event_order() {
        let mut scenario = PersistenceScenario::new("test")
            .command("s1", "cmd", 0);
        scenario.execute_step("id", vec![
            ("A".into(), "d".into()),
            ("B".into(), "d".into()),
            ("C".into(), "d".into()),
        ]);
        assert!(scenario.check_event_order(&["A", "B", "C"]));
        assert!(!scenario.check_event_order(&["A", "C", "B"]));
    }

    #[test]
    fn test_scenario_events_by_type() {
        let mut scenario = PersistenceScenario::new("test")
            .command("s1", "cmd", 0);
        scenario.execute_step("id", vec![
            ("A".into(), "d1".into()),
            ("B".into(), "d2".into()),
            ("A".into(), "d3".into()),
        ]);
        assert_eq!(scenario.events_by_type("A").len(), 2);
        assert_eq!(scenario.events_by_type("B").len(), 1);
    }

    #[test]
    fn test_scenario_last_sequence_nr() {
        let mut scenario = PersistenceScenario::new("test")
            .command("s1", "cmd", 0);
        assert_eq!(scenario.last_sequence_nr(), 0);
        scenario.execute_step("id", vec![("E1".into(), "d".into()), ("E2".into(), "d".into())]);
        assert_eq!(scenario.last_sequence_nr(), 2);
    }

    #[test]
    fn test_scenario_state_snapshot() {
        let mut scenario = PersistenceScenario::new("test")
            .command("s1", "cmd", 0);
        scenario.execute_step("id", vec![("E".into(), "d".into())]);

        let mut state = HashMap::new();
        state.insert("count".to_string(), "1".to_string());
        scenario.record_state(state, 1);

        assert_eq!(scenario.snapshots().len(), 1);
        assert_eq!(scenario.latest_state().unwrap().sequence_nr, 1);
    }

    #[test]
    fn test_recovery_verifier() {
        let mut verifier = RecoveryVerifier::new("test-actor")
            .with_events(vec![("E1".into(), "d".into()), ("E2".into(), "d".into())])
            .with_expected_sequence_nr(2);

        let mut expected = HashMap::new();
        expected.insert("count".to_string(), "2".to_string());
        let mut verifier = verifier.with_expected_state(expected.clone());

        let result = verifier.verify_recovery(&expected, 2);
        assert!(result.state_match);
        assert!(result.errors.is_empty());
        assert!(verifier.all_passed());
    }

    #[test]
    fn test_recovery_state_mismatch() {
        let mut expected = HashMap::new();
        expected.insert("count".to_string(), "2".to_string());
        let mut verifier = RecoveryVerifier::new("test")
            .with_expected_state(expected);

        let mut actual = HashMap::new();
        actual.insert("count".to_string(), "1".to_string());
        let result = verifier.verify_recovery(&actual, 0);
        assert!(!result.state_match);
        assert!(!verifier.all_passed());
    }

    #[test]
    fn test_recovery_sequence_mismatch() {
        let mut verifier = RecoveryVerifier::new("test")
            .with_expected_sequence_nr(5);
        let state = HashMap::new();
        let result = verifier.verify_recovery(&state, 3);
        assert!(!result.errors.is_empty());
    }

    #[test]
    fn test_recovery_verifier_debug() {
        let verifier = RecoveryVerifier::new("test");
        let debug = format!("{:?}", verifier);
        assert!(debug.contains("RecoveryVerifier"));
    }

    #[test]
    fn test_event_stream_has_count() {
        let mut assertion = EventStreamAssertion::from_events(vec![
            ("A".into(), "d".into(), 1),
            ("B".into(), "d".into(), 2),
        ]);
        assertion.has_count(2);
        assert!(assertion.is_success());
    }

    #[test]
    fn test_event_stream_at_index() {
        let mut assertion = EventStreamAssertion::from_events(vec![
            ("OrderCreated".into(), "d".into(), 1),
            ("OrderPaid".into(), "d".into(), 2),
        ]);
        assertion.at_index(0, "OrderCreated").at_index(1, "OrderPaid");
        assert!(assertion.is_success());
    }

    #[test]
    fn test_event_stream_first_last() {
        let mut assertion = EventStreamAssertion::from_events(vec![
            ("First".into(), "d".into(), 1),
            ("Middle".into(), "d".into(), 2),
            ("Last".into(), "d".into(), 3),
        ]);
        assertion.first_is("First").last_is("Last");
        assert!(assertion.is_success());
    }

    #[test]
    fn test_event_stream_contains_has_no() {
        let mut assertion = EventStreamAssertion::from_events(vec![
            ("A".into(), "d".into(), 1),
            ("B".into(), "d".into(), 2),
        ]);
        assertion.contains("A").has_no("C");
        assert!(assertion.is_success());
    }

    #[test]
    fn test_event_stream_monotonic_sequence() {
        let mut assertion = EventStreamAssertion::from_events(vec![
            ("A".into(), "d".into(), 1),
            ("B".into(), "d".into(), 2),
            ("C".into(), "d".into(), 3),
        ]);
        assertion.has_monotonic_sequence();
        assert!(assertion.is_success());

        let mut bad = EventStreamAssertion::from_events(vec![
            ("A".into(), "d".into(), 1),
            ("B".into(), "d".into(), 3),
            ("C".into(), "d".into(), 2),
        ]);
        bad.has_monotonic_sequence();
        assert!(!bad.is_success());
    }

    #[test]
    fn test_event_stream_all_match() {
        let mut assertion = EventStreamAssertion::from_events(vec![
            ("Order".into(), "d".into(), 1),
            ("Order".into(), "d".into(), 2),
        ]);
        assertion.all_match(|t, _, _| t == "Order");
        assert!(assertion.is_success());
    }

    #[test]
    fn test_event_stream_count_of() {
        let assertion = EventStreamAssertion::from_events(vec![
            ("A".into(), "d".into(), 1),
            ("B".into(), "d".into(), 2),
            ("A".into(), "d".into(), 3),
        ]);
        assert_eq!(assertion.count_of("A"), 2);
        assert_eq!(assertion.count_of("B"), 1);
    }

    #[test]
    fn test_event_stream_failed() {
        let mut assertion = EventStreamAssertion::from_events(vec![
            ("A".into(), "d".into(), 1),
        ]);
        assertion.has_count(5); // wrong
        assert!(!assertion.is_success());
        assert_eq!(assertion.failed(), 1);
        assert!(!assertion.errors().is_empty());
    }

    #[test]
    fn test_event_stream_debug() {
        let assertion = EventStreamAssertion::new();
        let debug = format!("{:?}", assertion);
        assert!(debug.contains("EventStreamAssertion"));
    }

    #[test]
    fn test_event_stream_empty_last() {
        let mut assertion = EventStreamAssertion::new();
        assertion.last_is("X");
        assert!(!assertion.is_success());
    }

    #[test]
    fn test_event_stream_add_event() {
        let mut assertion = EventStreamAssertion::new();
        assertion.add_event("A", "data", 1);
        assertion.add_event("B", "data", 2);
        assert_eq!(assertion.total_events(), 2);
    }

    #[test]
    fn test_scenario_query_step() {
        let scenario = PersistenceScenario::new("test")
            .query("check-state", "GetState");
        assert_eq!(scenario.steps()[0].step_type, ScenarioStepType::Query("GetState".into()));
    }

    #[test]
    fn test_scenario_recovery_step() {
        let scenario = PersistenceScenario::new("test")
            .recovery("recover");
        assert_eq!(scenario.steps()[0].step_type, ScenarioStepType::Recovery);
    }
}
