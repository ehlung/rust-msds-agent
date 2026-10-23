//! # Finite State Machine (FSM)
//!
//! Explicit state machine definitions for actors, inspired by Pekko's FSM trait.
//!
//! Provides:
//! - Declarative state/event/transition definitions
//! - Builder pattern for FSM construction
//! - Entry/exit actions per state
//! - State timeout handling
//! - Transition listeners
//! - Unhandled event fallback

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Core traits
// ---------------------------------------------------------------------------

/// Marker trait for FSM states
pub trait FsmState: Clone + Eq + std::hash::Hash + fmt::Debug + Send + Sync + 'static {}

/// Marker trait for FSM events
pub trait FsmEvent: Clone + fmt::Debug + Send + Sync + 'static {}

// Blanket implementations
impl<T: Clone + Eq + std::hash::Hash + fmt::Debug + Send + Sync + 'static> FsmState for T {}
impl<T: Clone + fmt::Debug + Send + Sync + 'static> FsmEvent for T {}

/// Result of a transition handler
#[derive(Debug, Clone)]
pub enum TransitionResult<S, D> {
    /// Stay in the current state, optionally update data
    Stay(Option<D>),
    /// Go to a new state, optionally update data
    GoTo(S, Option<D>),
    /// Stop the FSM
    Stop(StopReason),
}

/// Reason for stopping the FSM
#[derive(Debug, Clone, PartialEq)]
pub enum StopReason {
    /// Normal completion
    Normal,
    /// Shutdown requested
    Shutdown,
    /// Error occurred
    Failure(String),
}

impl fmt::Display for StopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StopReason::Normal => write!(f, "Normal"),
            StopReason::Shutdown => write!(f, "Shutdown"),
            StopReason::Failure(msg) => write!(f, "Failure: {}", msg),
        }
    }
}

/// Record of a state transition
#[derive(Debug, Clone)]
pub struct TransitionRecord<S> {
    pub from: S,
    pub to: S,
    pub timestamp: Instant,
}

// ---------------------------------------------------------------------------
// StateMachine
// ---------------------------------------------------------------------------

/// Type alias for transition handler
type TransitionHandler<S, D, E> = Box<dyn Fn(&S, &D, &E) -> TransitionResult<S, D> + Send + Sync>;

/// Type alias for action callback
type ActionCallback<S, D> = Box<dyn Fn(&S, &D) + Send + Sync>;

/// Type alias for transition listener
type TransitionListener<S> = Box<dyn Fn(&S, &S) + Send + Sync>;

/// State configuration
struct StateConfig<S, D, E> {
    /// Event handlers: event discriminator → handler
    handlers: Vec<(Box<dyn Fn(&E) -> bool + Send + Sync>, TransitionHandler<S, D, E>)>,
    /// Entry action
    on_entry: Option<ActionCallback<S, D>>,
    /// Exit action
    on_exit: Option<ActionCallback<S, D>>,
    /// State timeout
    timeout: Option<Duration>,
    /// Timeout action
    on_timeout: Option<Box<dyn Fn(&S, &D) -> TransitionResult<S, D> + Send + Sync>>,
}

/// Finite State Machine
pub struct StateMachine<S, D, E> {
    current_state: S,
    data: D,
    states: HashMap<S, StateConfig<S, D, E>>,
    transition_listeners: Vec<TransitionListener<S>>,
    unhandled_handler: Option<Box<dyn Fn(&S, &D, &E) -> TransitionResult<S, D> + Send + Sync>>,
    history: Vec<TransitionRecord<S>>,
    max_history: usize,
    is_stopped: bool,
    stop_reason: Option<StopReason>,
    state_entered_at: Instant,
    total_transitions: u64,
}

impl<S: FsmState, D: Clone + Send + Sync, E: FsmEvent> StateMachine<S, D, E> {
    /// Process an event
    pub fn handle_event(&mut self, event: &E) -> &S {
        if self.is_stopped {
            return &self.current_state;
        }

        // Check state timeout first
        if let Some(config) = self.states.get(&self.current_state) {
            if let Some(timeout) = config.timeout {
                if self.state_entered_at.elapsed() > timeout {
                    if let Some(ref handler) = config.on_timeout {
                        let result = handler(&self.current_state, &self.data);
                        self.apply_result(result);
                        return &self.current_state;
                    }
                }
            }
        }

        // Find matching handler
        let result = if let Some(config) = self.states.get(&self.current_state) {
            let mut found = None;
            for (matcher, handler) in &config.handlers {
                if matcher(event) {
                    found = Some(handler(&self.current_state, &self.data, event));
                    break;
                }
            }
            found
        } else {
            None
        };

        // Apply result or try unhandled handler
        if let Some(result) = result {
            self.apply_result(result);
        } else if let Some(ref handler) = self.unhandled_handler {
            let result = handler(&self.current_state, &self.data, event);
            self.apply_result(result);
        }

        &self.current_state
    }

    fn apply_result(&mut self, result: TransitionResult<S, D>) {
        match result {
            TransitionResult::Stay(new_data) => {
                if let Some(d) = new_data {
                    self.data = d;
                }
            }
            TransitionResult::GoTo(new_state, new_data) => {
                let old_state = self.current_state.clone();

                // Exit action
                if let Some(config) = self.states.get(&old_state) {
                    if let Some(ref on_exit) = config.on_exit {
                        on_exit(&old_state, &self.data);
                    }
                }

                // Update data
                if let Some(d) = new_data {
                    self.data = d;
                }

                // Transition
                self.current_state = new_state.clone();
                self.state_entered_at = Instant::now();
                self.total_transitions += 1;

                // Record history
                if self.history.len() >= self.max_history {
                    self.history.remove(0);
                }
                self.history.push(TransitionRecord {
                    from: old_state.clone(),
                    to: new_state.clone(),
                    timestamp: Instant::now(),
                });

                // Notify listeners
                for listener in &self.transition_listeners {
                    listener(&old_state, &new_state);
                }

                // Entry action
                if let Some(config) = self.states.get(&new_state) {
                    if let Some(ref on_entry) = config.on_entry {
                        on_entry(&new_state, &self.data);
                    }
                }
            }
            TransitionResult::Stop(reason) => {
                self.is_stopped = true;
                self.stop_reason = Some(reason);
            }
        }
    }

    /// Get current state
    pub fn state(&self) -> &S {
        &self.current_state
    }

    /// Get current data
    pub fn data(&self) -> &D {
        &self.data
    }

    /// Update data without changing state
    pub fn set_data(&mut self, data: D) {
        self.data = data;
    }

    /// Check if FSM is stopped
    pub fn is_stopped(&self) -> bool {
        self.is_stopped
    }

    /// Get stop reason
    pub fn stop_reason(&self) -> Option<&StopReason> {
        self.stop_reason.as_ref()
    }

    /// Get transition history
    pub fn history(&self) -> &[TransitionRecord<S>] {
        &self.history
    }

    /// Get total transitions
    pub fn total_transitions(&self) -> u64 {
        self.total_transitions
    }

    /// Time spent in current state
    pub fn state_duration(&self) -> Duration {
        self.state_entered_at.elapsed()
    }

    /// Force transition to a state (for testing/recovery)
    pub fn force_state(&mut self, state: S) {
        self.current_state = state;
        self.state_entered_at = Instant::now();
    }
}

// ---------------------------------------------------------------------------
// StateMachineBuilder
// ---------------------------------------------------------------------------

/// Builder for constructing a StateMachine
pub struct StateMachineBuilder<S, D, E> {
    initial_state: S,
    initial_data: D,
    states: HashMap<S, StateConfig<S, D, E>>,
    transition_listeners: Vec<TransitionListener<S>>,
    unhandled_handler: Option<Box<dyn Fn(&S, &D, &E) -> TransitionResult<S, D> + Send + Sync>>,
    max_history: usize,
}

impl<S: FsmState, D: Clone + Send + Sync, E: FsmEvent> StateMachineBuilder<S, D, E> {
    pub fn new(initial_state: S, initial_data: D) -> Self {
        Self {
            initial_state,
            initial_data,
            states: HashMap::new(),
            transition_listeners: Vec::new(),
            unhandled_handler: None,
            max_history: 100,
        }
    }

    /// Configure a state
    pub fn state(self, state: S) -> StateBuilder<S, D, E> {
        StateBuilder {
            fsm_builder: self,
            state,
            handlers: Vec::new(),
            on_entry: None,
            on_exit: None,
            timeout: None,
            on_timeout: None,
        }
    }    

    /// Add a transition listener
    pub fn on_transition<F>(mut self, listener: F) -> Self
    where
        F: Fn(&S, &S) + Send + Sync + 'static,
    {
        self.transition_listeners.push(Box::new(listener));
        self
    }

    /// Set unhandled event handler
    pub fn when_unhandled<F>(mut self, handler: F) -> Self
    where
        F: Fn(&S, &D, &E) -> TransitionResult<S, D> + Send + Sync + 'static,
    {
        self.unhandled_handler = Some(Box::new(handler));
        self
    }

    /// Set max history entries
    pub fn max_history(mut self, max: usize) -> Self {
        self.max_history = max;
        self
    }

    /// Build the state machine
    pub fn build(self) -> StateMachine<S, D, E> {
        StateMachine {
            current_state: self.initial_state,
            data: self.initial_data,
            states: self.states,
            transition_listeners: self.transition_listeners,
            unhandled_handler: self.unhandled_handler,
            history: Vec::new(),
            max_history: self.max_history,
            is_stopped: false,
            stop_reason: None,
            state_entered_at: Instant::now(),
            total_transitions: 0,
        }
    }
}

/// Builder for configuring a single state
pub struct StateBuilder<S, D, E> {
    fsm_builder: StateMachineBuilder<S, D, E>,
    state: S,
    handlers: Vec<(Box<dyn Fn(&E) -> bool + Send + Sync>, TransitionHandler<S, D, E>)>,
    on_entry: Option<ActionCallback<S, D>>,
    on_exit: Option<ActionCallback<S, D>>,
    timeout: Option<Duration>,
    on_timeout: Option<Box<dyn Fn(&S, &D) -> TransitionResult<S, D> + Send + Sync>>,
}

impl<S: FsmState, D: Clone + Send + Sync, E: FsmEvent> StateBuilder<S, D, E> {
    /// Add an event handler for this state
    pub fn on_event<M, H>(mut self, matcher: M, handler: H) -> Self
    where
        M: Fn(&E) -> bool + Send + Sync + 'static,
        H: Fn(&S, &D, &E) -> TransitionResult<S, D> + Send + Sync + 'static,
    {
        self.handlers.push((Box::new(matcher), Box::new(handler)));
        self
    }

    /// Set entry action
    pub fn on_entry<F>(mut self, action: F) -> Self
    where
        F: Fn(&S, &D) + Send + Sync + 'static,
    {
        self.on_entry = Some(Box::new(action));
        self
    }

    /// Set exit action
    pub fn on_exit<F>(mut self, action: F) -> Self
    where
        F: Fn(&S, &D) + Send + Sync + 'static,
    {
        self.on_exit = Some(Box::new(action));
        self
    }

    /// Set state timeout
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Set timeout handler
    pub fn on_timeout<F>(mut self, handler: F) -> Self
    where
        F: Fn(&S, &D) -> TransitionResult<S, D> + Send + Sync + 'static,
    {
        self.on_timeout = Some(Box::new(handler));
        self
    }

    /// Finish configuring this state and return to FSM builder
    pub fn end_state(mut self) -> StateMachineBuilder<S, D, E> {
        let config = StateConfig {
            handlers: self.handlers,
            on_entry: self.on_entry,
            on_exit: self.on_exit,
            timeout: self.timeout,
            on_timeout: self.on_timeout,
        };
        self.fsm_builder.states.insert(self.state, config);
        self.fsm_builder
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    // --- Test state & event types ---

    #[derive(Debug, Clone, Hash, Eq, PartialEq)]
    enum DoorState {
        Locked,
        Closed,
        Open,
    }

    #[derive(Debug, Clone)]
    enum DoorEvent {
        Lock,
        Unlock,
        Push,
        Pull,
    }

    type DoorData = u32; // open count

    fn door_fsm() -> StateMachine<DoorState, DoorData, DoorEvent> {
        StateMachineBuilder::new(DoorState::Locked, 0u32)
            .state(DoorState::Locked)
                .on_event(
                    |e| matches!(e, DoorEvent::Unlock),
                    |_s, d, _e| TransitionResult::GoTo(DoorState::Closed, Some(*d)),
                )
                .end_state()
            .state(DoorState::Closed)
                .on_event(
                    |e| matches!(e, DoorEvent::Push),
                    |_s, d, _e| TransitionResult::GoTo(DoorState::Open, Some(d + 1)),
                )
                .on_event(
                    |e| matches!(e, DoorEvent::Lock),
                    |_s, d, _e| TransitionResult::GoTo(DoorState::Locked, Some(*d)),
                )
                .end_state()
            .state(DoorState::Open)
                .on_event(
                    |e| matches!(e, DoorEvent::Pull),
                    |_s, d, _e| TransitionResult::GoTo(DoorState::Closed, Some(*d)),
                )
                .end_state()
            .build()
    }

    #[test]
    fn test_initial_state() {
        let fsm = door_fsm();
        assert_eq!(*fsm.state(), DoorState::Locked);
        assert_eq!(*fsm.data(), 0);
    }

    #[test]
    fn test_basic_transitions() {
        let mut fsm = door_fsm();

        fsm.handle_event(&DoorEvent::Unlock);
        assert_eq!(*fsm.state(), DoorState::Closed);

        fsm.handle_event(&DoorEvent::Push);
        assert_eq!(*fsm.state(), DoorState::Open);
        assert_eq!(*fsm.data(), 1);

        fsm.handle_event(&DoorEvent::Pull);
        assert_eq!(*fsm.state(), DoorState::Closed);
    }

    #[test]
    fn test_unhandled_event_stays() {
        let mut fsm = door_fsm();
        // Push while locked → no handler, stays locked
        fsm.handle_event(&DoorEvent::Push);
        assert_eq!(*fsm.state(), DoorState::Locked);
    }

    #[test]
    fn test_transition_count() {
        let mut fsm = door_fsm();
        fsm.handle_event(&DoorEvent::Unlock);
        fsm.handle_event(&DoorEvent::Push);
        fsm.handle_event(&DoorEvent::Pull);
        assert_eq!(fsm.total_transitions(), 3);
    }

    #[test]
    fn test_history() {
        let mut fsm = door_fsm();
        fsm.handle_event(&DoorEvent::Unlock);
        fsm.handle_event(&DoorEvent::Push);

        let history = fsm.history();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].from, DoorState::Locked);
        assert_eq!(history[0].to, DoorState::Closed);
        assert_eq!(history[1].from, DoorState::Closed);
        assert_eq!(history[1].to, DoorState::Open);
    }

    #[test]
    fn test_max_history() {
        let mut fsm = StateMachineBuilder::new(DoorState::Locked, 0u32)
            .max_history(2)
            .state(DoorState::Locked)
                .on_event(|e| matches!(e, DoorEvent::Unlock), |_s, d, _e| TransitionResult::GoTo(DoorState::Closed, Some(*d)))
                .end_state()
            .state(DoorState::Closed)
                .on_event(|e| matches!(e, DoorEvent::Push), |_s, d, _e| TransitionResult::GoTo(DoorState::Open, Some(*d)))
                .on_event(|e| matches!(e, DoorEvent::Lock), |_s, d, _e| TransitionResult::GoTo(DoorState::Locked, Some(*d)))
                .end_state()
            .state(DoorState::Open)
                .on_event(|e| matches!(e, DoorEvent::Pull), |_s, d, _e| TransitionResult::GoTo(DoorState::Closed, Some(*d)))
                .end_state()
            .build();

        fsm.handle_event(&DoorEvent::Unlock);
        fsm.handle_event(&DoorEvent::Push);
        fsm.handle_event(&DoorEvent::Pull);

        assert_eq!(fsm.history().len(), 2); // Oldest evicted
    }

    #[test]
    fn test_stop() {
        let mut fsm = StateMachineBuilder::new(DoorState::Open, 0u32)
            .state(DoorState::Open)
                .on_event(
                    |e| matches!(e, DoorEvent::Lock),
                    |_s, _d, _e| TransitionResult::Stop(StopReason::Normal),
                )
                .end_state()
            .build();

        fsm.handle_event(&DoorEvent::Lock);
        assert!(fsm.is_stopped());
        assert_eq!(fsm.stop_reason(), Some(&StopReason::Normal));
    }

    #[test]
    fn test_stopped_ignores_events() {
        let mut fsm = StateMachineBuilder::new(DoorState::Open, 0u32)
            .state(DoorState::Open)
                .on_event(|e| matches!(e, DoorEvent::Lock), |_s, _d, _e| TransitionResult::Stop(StopReason::Normal))
                .on_event(|e| matches!(e, DoorEvent::Pull), |_s, d, _e| TransitionResult::GoTo(DoorState::Closed, Some(*d)))
                .end_state()
            .build();

        fsm.handle_event(&DoorEvent::Lock);
        fsm.handle_event(&DoorEvent::Pull); // Should be ignored
        assert_eq!(*fsm.state(), DoorState::Open);
    }

    #[test]
    fn test_stay_result() {
        let mut fsm = StateMachineBuilder::new(DoorState::Open, 0u32)
            .state(DoorState::Open)
                .on_event(
                    |e| matches!(e, DoorEvent::Push),
                    |_s, d, _e| TransitionResult::Stay(Some(d + 1)),
                )
                .end_state()
            .build();

        fsm.handle_event(&DoorEvent::Push);
        assert_eq!(*fsm.state(), DoorState::Open);
        assert_eq!(*fsm.data(), 1);
    }

    #[test]
    fn test_entry_exit_actions() {
        let entry_count = Arc::new(AtomicU32::new(0));
        let exit_count = Arc::new(AtomicU32::new(0));
        let ec = entry_count.clone();
        let xc = exit_count.clone();

        let mut fsm = StateMachineBuilder::new(DoorState::Locked, 0u32)
            .state(DoorState::Locked)
                .on_event(|e| matches!(e, DoorEvent::Unlock), |_s, d, _e| TransitionResult::GoTo(DoorState::Closed, Some(*d)))
                .on_exit(move |_s, _d| { xc.fetch_add(1, Ordering::Relaxed); })
                .end_state()
            .state(DoorState::Closed)
                .on_entry(move |_s, _d| { ec.fetch_add(1, Ordering::Relaxed); })
                .end_state()
            .build();

        fsm.handle_event(&DoorEvent::Unlock);
        assert_eq!(entry_count.load(Ordering::Relaxed), 1);
        assert_eq!(exit_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_transition_listener() {
        let transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
        let t = transitions.clone();

        let mut fsm = StateMachineBuilder::new(DoorState::Locked, 0u32)
            .on_transition(move |from: &DoorState, to: &DoorState| {
                t.lock().unwrap().push((from.clone(), to.clone()));
            })
            .state(DoorState::Locked)
                .on_event(|e| matches!(e, DoorEvent::Unlock), |_s, d, _e| TransitionResult::GoTo(DoorState::Closed, Some(*d)))
                .end_state()
            .build();

        fsm.handle_event(&DoorEvent::Unlock);

        let t = transitions.lock().unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t[0], (DoorState::Locked, DoorState::Closed));
    }

    #[test]
    fn test_unhandled_handler() {
        let mut fsm = StateMachineBuilder::new(DoorState::Locked, 0u32)
            .when_unhandled(|_s, d, _e| TransitionResult::Stay(Some(d + 100)))
            .state(DoorState::Locked)
                .end_state()
            .build();

        fsm.handle_event(&DoorEvent::Push); // Unhandled
        assert_eq!(*fsm.data(), 100);
    }

    #[test]
    fn test_force_state() {
        let mut fsm = door_fsm();
        fsm.force_state(DoorState::Open);
        assert_eq!(*fsm.state(), DoorState::Open);
    }

    #[test]
    fn test_set_data() {
        let mut fsm = door_fsm();
        fsm.set_data(42);
        assert_eq!(*fsm.data(), 42);
    }

    #[test]
    fn test_state_duration() {
        let fsm = door_fsm();
        std::thread::sleep(Duration::from_millis(5));
        assert!(fsm.state_duration() >= Duration::from_millis(5));
    }

    #[test]
    fn test_stop_reason_display() {
        assert_eq!(StopReason::Normal.to_string(), "Normal");
        assert_eq!(StopReason::Shutdown.to_string(), "Shutdown");
        assert_eq!(StopReason::Failure("oops".into()).to_string(), "Failure: oops");
    }

    #[test]
    fn test_complex_workflow() {
        #[derive(Debug, Clone, Hash, Eq, PartialEq)]
        enum OrderState { New, Paid, Shipped, Delivered, Cancelled }

        #[derive(Debug, Clone)]
        enum OrderEvent { Pay, Ship, Deliver, Cancel }

        let mut fsm = StateMachineBuilder::new(OrderState::New, 0u32)
            .state(OrderState::New)
                .on_event(|e| matches!(e, OrderEvent::Pay), |_s, d, _e| TransitionResult::GoTo(OrderState::Paid, Some(d + 1)))
                .on_event(|e| matches!(e, OrderEvent::Cancel), |_s, _d, _e| TransitionResult::GoTo(OrderState::Cancelled, None))
                .end_state()
            .state(OrderState::Paid)
                .on_event(|e| matches!(e, OrderEvent::Ship), |_s, d, _e| TransitionResult::GoTo(OrderState::Shipped, Some(*d)))
                .on_event(|e| matches!(e, OrderEvent::Cancel), |_s, _d, _e| TransitionResult::GoTo(OrderState::Cancelled, None))
                .end_state()
            .state(OrderState::Shipped)
                .on_event(|e| matches!(e, OrderEvent::Deliver), |_s, d, _e| TransitionResult::GoTo(OrderState::Delivered, Some(*d)))
                .end_state()
            .state(OrderState::Delivered)
                .end_state()
            .state(OrderState::Cancelled)
                .end_state()
            .build();

        fsm.handle_event(&OrderEvent::Pay);
        assert_eq!(*fsm.state(), OrderState::Paid);

        fsm.handle_event(&OrderEvent::Ship);
        assert_eq!(*fsm.state(), OrderState::Shipped);

        fsm.handle_event(&OrderEvent::Deliver);
        assert_eq!(*fsm.state(), OrderState::Delivered);
        assert_eq!(fsm.total_transitions(), 3);
    }
}
