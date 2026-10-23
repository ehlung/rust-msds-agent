//! Behavior - Dynamic behavior switching for actors
//!
//! This module provides the `become`/`unbecome` pattern that allows actors
//! to dynamically change how they handle messages at runtime.
//!
//! ## Use Cases
//!
//! - **State Machine**: Switch between different states
//! - **Initialization**: Different behavior during init vs running
//! - **Authentication**: Different behavior for authenticated vs guest users
//!
//! ## Example
//!
//! ```rust,ignore
//! use pekko_actor::{Actor, BehaviorStack, Behavior};
//!
//! struct ConnectionActor {
//!     behaviors: BehaviorStack<Self>,
//! }
//!
//! impl ConnectionActor {
//!     // Disconnected behavior
//!     fn disconnected(&mut self, msg: ConnMessage) -> Behavior<Self> {
//!         match msg {
//!             ConnMessage::Connect(addr) => {
//!                 self.do_connect(&addr);
//!                 Behavior::Become(Box::new(Self::connected))
//!             }
//!             _ => {
//!                 println!("Cannot process: not connected");
//!                 Behavior::Same
//!             }
//!         }
//!     }
//!
//!     // Connected behavior
//!     fn connected(&mut self, msg: ConnMessage) -> Behavior<Self> {
//!         match msg {
//!             ConnMessage::Send(data) => {
//!                 self.send_data(&data);
//!                 Behavior::Same
//!             }
//!             ConnMessage::Disconnect => {
//!                 self.close();
//!                 Behavior::Unbecome
//!             }
//!             _ => Behavior::Same
//!         }
//!     }
//! }
//! ```

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

/// The result of processing a message, indicating what behavior to use next
pub enum Behavior<A, M> {
    /// Keep the current behavior
    Same,
    /// Switch to a new behavior (push onto stack)
    Become(BehaviorFn<A, M>),
    /// Switch to a new behavior, replacing current (no stack push)
    BecomeReplace(BehaviorFn<A, M>),
    /// Revert to the previous behavior (pop from stack)
    Unbecome,
    /// Stop the actor
    Stop,
    /// Stop the actor with a reason
    StopWithReason(String),
}

impl<A, M> fmt::Debug for Behavior<A, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Behavior::Same => write!(f, "Behavior::Same"),
            Behavior::Become(_) => write!(f, "Behavior::Become(...)"),
            Behavior::BecomeReplace(_) => write!(f, "Behavior::BecomeReplace(...)"),
            Behavior::Unbecome => write!(f, "Behavior::Unbecome"),
            Behavior::Stop => write!(f, "Behavior::Stop"),
            Behavior::StopWithReason(r) => write!(f, "Behavior::StopWithReason({:?})", r),
        }
    }
}

/// A behavior function that processes messages
/// Returns a future that resolves to the next Behavior
pub type BehaviorFn<A, M> = Box<
    dyn FnMut(&mut A, M) -> Pin<Box<dyn Future<Output = Behavior<A, M>> + Send + '_>>
        + Send
        + Sync,
>;

/// Synchronous behavior function (simpler, no async)
pub type SyncBehaviorFn<A, M> = Box<dyn FnMut(&mut A, M) -> Behavior<A, M> + Send + Sync>;

/// A stack of behaviors for implementing become/unbecome
pub struct BehaviorStack<A, M> {
    stack: VecDeque<SyncBehaviorFn<A, M>>,
    default_behavior: Option<SyncBehaviorFn<A, M>>,
}

impl<A, M> BehaviorStack<A, M> {
    /// Create a new behavior stack with a default behavior
    pub fn new<F>(default: F) -> Self
    where
        F: FnMut(&mut A, M) -> Behavior<A, M> + Send + Sync + 'static,
    {
        Self {
            stack: VecDeque::new(),
            default_behavior: Some(Box::new(default)),
        }
    }

    /// Create an empty behavior stack (must call become before processing)
    pub fn empty() -> Self {
        Self {
            stack: VecDeque::new(),
            default_behavior: None,
        }
    }

    /// Push a new behavior onto the stack (become)
    pub fn push<F>(&mut self, behavior: F)
    where
        F: FnMut(&mut A, M) -> Behavior<A, M> + Send + Sync + 'static,
    {
        self.stack.push_back(Box::new(behavior));
        tracing::debug!("Behavior stack: push (depth: {})", self.stack.len());
    }

    /// Replace the current behavior (doesn't push onto stack)
    pub fn replace<F>(&mut self, behavior: F)
    where
        F: FnMut(&mut A, M) -> Behavior<A, M> + Send + Sync + 'static,
    {
        if !self.stack.is_empty() {
            self.stack.pop_back();
        }
        self.stack.push_back(Box::new(behavior));
        tracing::debug!(
            "Behavior stack: replace (depth: {})",
            self.stack.len()
        );
    }

    /// Pop the current behavior and revert to the previous one (unbecome)
    pub fn pop(&mut self) -> bool {
        if self.stack.len() > 1 {
            self.stack.pop_back();
            tracing::debug!("Behavior stack: pop (depth: {})", self.stack.len());
            true
        } else if !self.stack.is_empty() && self.default_behavior.is_some() {
            self.stack.pop_back();
            tracing::debug!("Behavior stack: pop to default");
            true
        } else {
            tracing::warn!("Behavior stack: cannot pop (already at bottom)");
            false
        }
    }

    /// Process a message with the current behavior
    pub fn receive(&mut self, actor: &mut A, msg: M) -> Behavior<A, M> {
        if let Some(behavior) = self.stack.back_mut() {
            behavior(actor, msg)
        } else if let Some(ref mut default) = self.default_behavior {
            default(actor, msg)
        } else {
            tracing::error!("No behavior defined!");
            Behavior::Stop
        }
    }

    /// Process a message and automatically handle the behavior result
    pub fn process(&mut self, actor: &mut A, msg: M) -> ProcessResult {
        let behavior_result = self.receive(actor, msg);
        self.apply(behavior_result)
    }

    /// Apply a behavior result
    pub fn apply(&mut self, behavior: Behavior<A, M>) -> ProcessResult {
        match behavior {
            Behavior::Same => ProcessResult::Continue,
            Behavior::Become(_b) => {
                // Convert async to sync by wrapping
                // For now, we only support sync behaviors in BehaviorStack
                tracing::warn!("Async Become not directly supported in BehaviorStack, use push() method");
                ProcessResult::Continue
            }
            Behavior::BecomeReplace(_b) => {
                tracing::warn!(
                    "Async BecomeReplace not directly supported in BehaviorStack, use replace() method"
                );
                ProcessResult::Continue
            }
            Behavior::Unbecome => {
                self.pop();
                ProcessResult::Continue
            }
            Behavior::Stop => ProcessResult::Stop,
            Behavior::StopWithReason(reason) => ProcessResult::StopWithReason(reason),
        }
    }

    /// Get the current stack depth
    pub fn depth(&self) -> usize {
        self.stack.len()
    }

    /// Check if the stack is empty (using default behavior)
    pub fn is_at_default(&self) -> bool {
        self.stack.is_empty()
    }

    /// Clear all behaviors and reset to default
    pub fn reset(&mut self) {
        self.stack.clear();
        tracing::debug!("Behavior stack: reset");
    }
}

impl<A, M> fmt::Debug for BehaviorStack<A, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BehaviorStack")
            .field("depth", &self.stack.len())
            .field("has_default", &self.default_behavior.is_some())
            .finish()
    }
}

/// Result of processing a message
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessResult {
    /// Continue processing messages
    Continue,
    /// Stop the actor
    Stop,
    /// Stop the actor with a reason
    StopWithReason(String),
}

/// A simple state machine built on top of behaviors
pub struct StateMachine<S, M> {
    current_state: S,
    transitions: Vec<Box<dyn Fn(&S, &M) -> Option<S> + Send + Sync>>,
}

impl<S: Clone + PartialEq + fmt::Debug, M> StateMachine<S, M> {
    pub fn new(initial_state: S) -> Self {
        Self {
            current_state: initial_state,
            transitions: Vec::new(),
        }
    }

    /// Add a transition rule
    pub fn add_transition<F>(&mut self, transition: F)
    where
        F: Fn(&S, &M) -> Option<S> + Send + Sync + 'static,
    {
        self.transitions.push(Box::new(transition));
    }

    /// Process an event and potentially transition to a new state
    pub fn process(&mut self, event: &M) -> bool {
        for transition in &self.transitions {
            if let Some(new_state) = transition(&self.current_state, event) {
                tracing::debug!(
                    "State transition: {:?} -> {:?}",
                    self.current_state,
                    new_state
                );
                self.current_state = new_state;
                return true;
            }
        }
        false
    }

    /// Get the current state
    pub fn state(&self) -> &S {
        &self.current_state
    }

    /// Check if in a specific state
    pub fn is_in_state(&self, state: &S) -> bool {
        &self.current_state == state
    }

    /// Force transition to a state
    pub fn goto(&mut self, state: S) {
        tracing::debug!(
            "State forced transition: {:?} -> {:?}",
            self.current_state,
            state
        );
        self.current_state = state;
    }
}

impl<S: fmt::Debug, M> fmt::Debug for StateMachine<S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateMachine")
            .field("current_state", &self.current_state)
            .field("num_transitions", &self.transitions.len())
            .finish()
    }
}

/// Builder for creating behavior-based actors
pub struct BehaviorBuilder<A: 'static, M: 'static> {
    behaviors: Vec<(String, SyncBehaviorFn<A, M>)>,
    initial: Option<String>,
}

impl<A: 'static, M: 'static> BehaviorBuilder<A, M> {
    pub fn new() -> Self {
        Self {
            behaviors: Vec::new(),
            initial: None,
        }
    }

    /// Add a named behavior
    pub fn with_behavior<F>(mut self, name: &str, behavior: F) -> Self
    where
        F: FnMut(&mut A, M) -> Behavior<A, M> + Send + Sync + 'static,
    {
        self.behaviors.push((name.to_string(), Box::new(behavior)));
        self
    }

    /// Set the initial behavior by name
    pub fn initial(mut self, name: &str) -> Self {
        self.initial = Some(name.to_string());
        self
    }

    /// Build the behavior stack
    pub fn build(mut self) -> Option<BehaviorStack<A, M>> {
        let initial_name = self.initial?;
        let idx = self.behaviors.iter().position(|(n, _)| n == &initial_name)?;
        let (_, initial_behavior) = self.behaviors.remove(idx);

        let mut stack = BehaviorStack::empty();
        stack.push(initial_behavior);
        Some(stack)
    }
}

impl<A: 'static, M: 'static> Default for BehaviorBuilder<A, M> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestActor {
        value: i32,
        log: Vec<String>,
    }

    #[derive(Debug, Clone)]
    enum TestMessage {
        Increment,
        Decrement,
        SetValue(i32),
        GetValue,
        SwitchToDoubling,
        SwitchBack,
        Stop,
    }

    fn normal_behavior(actor: &mut TestActor, msg: TestMessage) -> Behavior<TestActor, TestMessage> {
        match msg {
            TestMessage::Increment => {
                actor.value += 1;
                actor.log.push(format!("increment -> {}", actor.value));
                Behavior::Same
            }
            TestMessage::Decrement => {
                actor.value -= 1;
                actor.log.push(format!("decrement -> {}", actor.value));
                Behavior::Same
            }
            TestMessage::SetValue(v) => {
                actor.value = v;
                actor.log.push(format!("set -> {}", actor.value));
                Behavior::Same
            }
            TestMessage::GetValue => {
                actor.log.push(format!("get: {}", actor.value));
                Behavior::Same
            }
            TestMessage::SwitchToDoubling => {
                actor.log.push("switching to doubling".to_string());
                Behavior::Same // We'll handle the actual switch outside
            }
            TestMessage::SwitchBack => {
                actor.log.push("already in normal".to_string());
                Behavior::Same
            }
            TestMessage::Stop => Behavior::Stop,
        }
    }

    fn doubling_behavior(
        actor: &mut TestActor,
        msg: TestMessage,
    ) -> Behavior<TestActor, TestMessage> {
        match msg {
            TestMessage::Increment => {
                actor.value += 2; // Double the increment
                actor.log.push(format!("double increment -> {}", actor.value));
                Behavior::Same
            }
            TestMessage::Decrement => {
                actor.value -= 2; // Double the decrement
                actor.log.push(format!("double decrement -> {}", actor.value));
                Behavior::Same
            }
            TestMessage::SwitchBack => {
                actor.log.push("switching back to normal".to_string());
                Behavior::Unbecome
            }
            _ => {
                // Delegate to normal behavior for other messages
                normal_behavior(actor, msg)
            }
        }
    }

    #[test]
    fn test_behavior_stack_basic() {
        let mut actor = TestActor {
            value: 0,
            log: Vec::new(),
        };
        let mut stack = BehaviorStack::new(normal_behavior);

        stack.process(&mut actor, TestMessage::Increment);
        assert_eq!(actor.value, 1);

        stack.process(&mut actor, TestMessage::Increment);
        assert_eq!(actor.value, 2);

        stack.process(&mut actor, TestMessage::Decrement);
        assert_eq!(actor.value, 1);
    }

    #[test]
    fn test_push_pop() {
        let mut actor = TestActor {
            value: 0,
            log: Vec::new(),
        };
        let mut stack = BehaviorStack::new(normal_behavior);

        // Normal increment
        stack.process(&mut actor, TestMessage::Increment);
        assert_eq!(actor.value, 1);

        // Switch to doubling behavior
        stack.push(doubling_behavior);

        // Doubling increment
        stack.process(&mut actor, TestMessage::Increment);
        assert_eq!(actor.value, 3); // +2

        stack.process(&mut actor, TestMessage::Increment);
        assert_eq!(actor.value, 5); // +2

        // Switch back
        stack.process(&mut actor, TestMessage::SwitchBack);
        // SwitchBack returns Unbecome, which we need to process
        stack.pop();

        // Back to normal increment
        stack.process(&mut actor, TestMessage::Increment);
        assert_eq!(actor.value, 6); // +1
    }

    #[test]
    fn test_behavior_stack_depth() {
        let mut stack: BehaviorStack<TestActor, TestMessage> = BehaviorStack::new(normal_behavior);

        assert_eq!(stack.depth(), 0);
        assert!(stack.is_at_default());

        stack.push(doubling_behavior);
        assert_eq!(stack.depth(), 1);
        assert!(!stack.is_at_default());

        stack.push(normal_behavior);
        assert_eq!(stack.depth(), 2);

        stack.pop();
        assert_eq!(stack.depth(), 1);

        stack.pop();
        assert_eq!(stack.depth(), 0);
        assert!(stack.is_at_default());
    }

    #[test]
    fn test_replace() {
        let mut stack: BehaviorStack<TestActor, TestMessage> = BehaviorStack::new(normal_behavior);

        stack.push(doubling_behavior);
        assert_eq!(stack.depth(), 1);

        // Replace instead of push
        stack.replace(normal_behavior);
        assert_eq!(stack.depth(), 1); // Still 1, not 2
    }

    #[test]
    fn test_reset() {
        let mut stack: BehaviorStack<TestActor, TestMessage> = BehaviorStack::new(normal_behavior);

        stack.push(doubling_behavior);
        stack.push(normal_behavior);
        assert_eq!(stack.depth(), 2);

        stack.reset();
        assert_eq!(stack.depth(), 0);
        assert!(stack.is_at_default());
    }

    #[test]
    fn test_stop_behavior() {
        let mut actor = TestActor {
            value: 0,
            log: Vec::new(),
        };
        let mut stack = BehaviorStack::new(normal_behavior);

        let result = stack.process(&mut actor, TestMessage::Stop);
        assert_eq!(result, ProcessResult::Stop);
    }

    #[test]
    fn test_state_machine() {
        #[derive(Debug, Clone, PartialEq)]
        enum State {
            Idle,
            Running,
            Paused,
            Stopped,
        }

        #[derive(Debug)]
        enum Event {
            Start,
            Pause,
            Resume,
            Stop,
        }

        let mut sm = StateMachine::new(State::Idle);

        sm.add_transition(|state, event| match (state, event) {
            (State::Idle, Event::Start) => Some(State::Running),
            (State::Running, Event::Pause) => Some(State::Paused),
            (State::Paused, Event::Resume) => Some(State::Running),
            (State::Running, Event::Stop) | (State::Paused, Event::Stop) => Some(State::Stopped),
            _ => None,
        });

        assert!(sm.is_in_state(&State::Idle));

        sm.process(&Event::Start);
        assert!(sm.is_in_state(&State::Running));

        sm.process(&Event::Pause);
        assert!(sm.is_in_state(&State::Paused));

        sm.process(&Event::Resume);
        assert!(sm.is_in_state(&State::Running));

        sm.process(&Event::Stop);
        assert!(sm.is_in_state(&State::Stopped));
    }

    #[test]
    fn test_state_machine_invalid_transition() {
        #[derive(Debug, Clone, PartialEq)]
        enum State {
            A,
            B,
        }

        #[derive(Debug)]
        enum Event {
            GoToB,
        }

        let mut sm = StateMachine::new(State::A);
        sm.add_transition(|state, event| match (state, event) {
            (State::A, Event::GoToB) => Some(State::B),
            _ => None,
        });

        // Valid transition
        assert!(sm.process(&Event::GoToB));
        assert!(sm.is_in_state(&State::B));

        // Invalid transition (already in B)
        assert!(!sm.process(&Event::GoToB));
        assert!(sm.is_in_state(&State::B)); // Still in B
    }

    #[test]
    fn test_state_machine_goto() {
        #[derive(Debug, Clone, PartialEq)]
        enum State {
            A,
            B,
            C,
        }

        let mut sm: StateMachine<State, ()> = StateMachine::new(State::A);

        sm.goto(State::C);
        assert!(sm.is_in_state(&State::C));
    }

    #[test]
    fn test_behavior_builder() {
        let mut actor = TestActor {
            value: 0,
            log: Vec::new(),
        };

        let mut stack = BehaviorBuilder::new()
            .with_behavior("normal", normal_behavior)
            .with_behavior("doubling", doubling_behavior)
            .initial("normal")
            .build()
            .unwrap();

        stack.process(&mut actor, TestMessage::Increment);
        assert_eq!(actor.value, 1);
    }

    #[test]
    fn test_empty_stack_with_no_default() {
        let mut actor = TestActor {
            value: 0,
            log: Vec::new(),
        };
        let mut stack: BehaviorStack<TestActor, TestMessage> = BehaviorStack::empty();

        // Should return Stop when no behavior is defined
        let result = stack.process(&mut actor, TestMessage::Increment);
        assert_eq!(result, ProcessResult::Stop);
    }

    #[test]
    fn test_pop_at_bottom() {
        let mut stack: BehaviorStack<TestActor, TestMessage> = BehaviorStack::new(normal_behavior);

        // Can't pop when at default
        assert!(!stack.pop());
    }
}
