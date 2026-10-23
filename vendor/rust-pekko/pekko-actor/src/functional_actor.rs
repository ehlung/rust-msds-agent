//! Functional programming style actor implementation
//! 
//! This module provides a functional programming approach to actors where:
//! - State is immutable
//! - State transitions are pure functions
//! - Side effects are explicitly modeled as Effects
//! - Actors return new state instead of mutating existing state

use std::any::Any;
use std::sync::Arc;
use crate::actor_system::ActorSystem;
use crate::actor_path::ActorPath;

/// Effect represents side effects that an actor wants to perform
/// Effects are applied after state transition, keeping the receive function pure
/// 
/// Note: Effect doesn't need to know about the actor type, making it more flexible
#[derive(Debug)]
pub enum Effect {
    /// No effect
    None,
    
    /// Send a message to another actor
    Send {
        target: String,
        message: Box<dyn Any + Send + Sync>,
    },
    
    /// Spawn a child actor
    /// Note: In a real implementation, this would use a factory function
    Spawn {
        name: String,
        // For functional actors, we use a factory function instead of Props
        // Factory is not Clone, so we'll handle this differently
    },
    
    /// Stop the actor
    Stop,
    
    /// Stop a child actor
    StopChild {
        name: String,
    },
    
    /// Schedule a message to be sent after a delay
    Schedule {
        delay_ms: u64,
        message: Box<dyn Any + Send + Sync>,
    },
    
    /// Multiple effects to be applied in sequence
    Batch {
        effects: Vec<Effect>,
    },
    
    /// Become a new state (state machine transition)
    Become {
        new_state: Box<dyn Any + Send + Sync>,
    },
    
    /// Stash a message for later processing
    Stash {
        message: Box<dyn Any + Send + Sync>,
    },
    
    /// Unstash all stashed messages
    UnstashAll,
}

impl Effect {
    /// Create an empty effect
    pub fn none() -> Self {
        Effect::None
    }
    
    /// Create a send effect
    pub fn send<M: Send + Sync + 'static>(target: String, message: M) -> Self {
        Effect::Send {
            target,
            message: Box::new(message),
        }
    }
    
    /// Create a spawn effect
    pub fn spawn(name: String) -> Self {
        Effect::Spawn { name }
    }
    
    /// Create a batch of effects
    pub fn batch(effects: Vec<Effect>) -> Self {
        Effect::Batch { effects }
    }
}

/// Functional actor context - provides access to system and self reference
/// Similar to ActorContext but doesn't require Actor trait
pub struct FunctionalActorContext {
    system: Arc<ActorSystem>,
    self_path: ActorPath,
}

impl FunctionalActorContext {
    pub fn new(system: Arc<ActorSystem>, self_path: ActorPath) -> Self {
        Self { system, self_path }
    }
    
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }
    
    pub fn path(&self) -> &ActorPath {
        &self.self_path
    }
}

/// Functional actor trait - pure functional approach to actors
/// 
/// Key principles:
/// 1. State is immutable - receive returns new state
/// 2. Side effects are explicit - returned as Effects
/// 3. Functions are pure - same input always produces same output
/// 4. State transitions are composable
pub trait FunctionalActor: Send + Sync + 'static {
    /// The state type for this actor (must be Clone for immutability)
    type State: Clone + Send + Sync + 'static;
    
    /// The message type this actor can receive
    type Message: Send + 'static;
    
    /// Initialize the actor state
    /// This is called when the actor starts
    fn init(ctx: &FunctionalActorContext) -> Self::State;
    
    /// Handle a message - pure function that returns new state and effects
    /// 
    /// This is a pure function: given the same state and message,
    /// it will always return the same new state and effects.
    /// No side effects should occur in this function.
    fn receive(
        state: Self::State,
        msg: Self::Message,
        ctx: &FunctionalActorContext,
    ) -> (Self::State, Effect);
    
    /// Apply an effect - this is where side effects happen
    /// 
    /// Effects are applied after state transition to keep receive pure.
    /// This function can perform async operations, I/O, etc.
    fn apply_effect(
        effect: Effect,
        _ctx: &mut FunctionalActorContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            match effect {
                Effect::None => {
                    // No effect to apply
                }
                Effect::Send { target, message: _message } => {
                    // In a real implementation, this would send the message
                    tracing::debug!("Effect: Send message to {}", target);
                }
                Effect::Spawn { name } => {
                    tracing::debug!("Effect: Spawn child actor {}", name);
                }
                Effect::Stop => {
                    tracing::debug!("Effect: Stop actor");
                }
                Effect::StopChild { name } => {
                    tracing::debug!("Effect: Stop child actor {}", name);
                }
                Effect::Schedule { delay_ms, .. } => {
                    tracing::debug!("Effect: Schedule message after {}ms", delay_ms);
                }
                Effect::Batch { effects } => {
                    for _effect in effects {
                        // Recursively apply batch effects
                        tracing::debug!("Effect: Apply batch effect");
                    }
                }
                Effect::Become { .. } => {
                    tracing::debug!("Effect: Become new state");
                }
                Effect::Stash { .. } => {
                    tracing::debug!("Effect: Stash message");
                }
                Effect::UnstashAll => {
                    tracing::debug!("Effect: Unstash all messages");
                }
            }
        })
    }
    
    /// Called when the actor is about to stop
    /// Can perform cleanup operations
    fn terminate(state: Self::State, _ctx: &FunctionalActorContext) -> Self::State {
        // Default implementation returns state unchanged
        state
    }
    
    /// Handle actor lifecycle events
    fn handle_supervisor_strategy(
        _state: Self::State,
        _error: Box<dyn std::error::Error + Send + Sync>,
        ctx: &FunctionalActorContext,
    ) -> (Self::State, Effect) {
        // Default: restart by returning initial state
        (Self::init(ctx), Effect::None)
    }
}

/// Adapter to use FunctionalActor as regular Actor
/// This allows functional actors to work with existing actor system
pub struct FunctionalActorAdapter<A: FunctionalActor> {
    state: A::State,
    actor_type: std::marker::PhantomData<A>,
}

impl<A: FunctionalActor> FunctionalActorAdapter<A> {
    pub fn new(initial_state: A::State) -> Self {
        Self {
            state: initial_state,
            actor_type: std::marker::PhantomData,
        }
    }
    
    pub fn state(&self) -> &A::State {
        &self.state
    }
    
    pub fn state_mut(&mut self) -> &mut A::State {
        &mut self.state
    }
    
    /// Update state (used internally)
    pub fn set_state(&mut self, new_state: A::State) {
        self.state = new_state;
    }
}

/// Helper trait for creating functional actors
/// This allows building actors with custom initialization logic
pub trait FunctionalActorBuilder<A: FunctionalActor> {
    fn build(ctx: &FunctionalActorContext) -> A::State;
}

/// State reducer pattern - common functional programming pattern
pub trait StateReducer {
    type State: Clone + Send + Sync + 'static;
    type Action: Send + 'static;
    
    /// Reduce state based on action
    fn reduce(state: Self::State, action: Self::Action) -> Self::State;
    
    /// Get initial state
    fn initial_state() -> Self::State;
}

/// Compose multiple reducers
pub struct ComposedReducer<R1: StateReducer, R2: StateReducer> {
    _r1: std::marker::PhantomData<R1>,
    _r2: std::marker::PhantomData<R2>,
}

impl<R1: StateReducer, R2: StateReducer> StateReducer for ComposedReducer<R1, R2> {
    type State = (R1::State, R2::State);
    type Action = (R1::Action, R2::Action);
    
    fn reduce(state: Self::State, action: Self::Action) -> Self::State {
        (
            R1::reduce(state.0, action.0),
            R2::reduce(state.1, action.1),
        )
    }
    
    fn initial_state() -> Self::State {
        (R1::initial_state(), R2::initial_state())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    /// Example: Functional counter actor
    #[derive(Debug, Clone, PartialEq)]
    struct CounterState {
        count: u32,
    }
    
    #[derive(Debug, Clone)]
    enum CounterMessage {
        Increment,
        Decrement,
        GetCount,
        Reset,
    }
    
    struct CounterActor;
    
    impl FunctionalActor for CounterActor {
        type State = CounterState;
        type Message = CounterMessage;
        
        fn init(_ctx: &FunctionalActorContext) -> Self::State {
            CounterState { count: 0 }
        }
        
        fn receive(
            state: Self::State,
            msg: Self::Message,
            _ctx: &FunctionalActorContext,
        ) -> (Self::State, Effect) {
            match msg {
                CounterMessage::Increment => (
                    CounterState { count: state.count + 1 },
                    Effect::None,
                ),
                CounterMessage::Decrement => (
                    CounterState {
                        count: state.count.saturating_sub(1),
                    },
                    Effect::None,
                ),
                CounterMessage::GetCount => {
                    // In a real implementation, we'd send a response
                    (state, Effect::None)
                }
                CounterMessage::Reset => (
                    CounterState { count: 0 },
                    Effect::None,
                ),
            }
        }
    }
    
    /// Example: State reducer for counter
    impl StateReducer for CounterActor {
        type State = CounterState;
        type Action = CounterMessage;
        
        fn reduce(state: Self::State, action: Self::Action) -> Self::State {
            match action {
                CounterMessage::Increment => CounterState { count: state.count + 1 },
                CounterMessage::Decrement => CounterState {
                    count: state.count.saturating_sub(1),
                },
                CounterMessage::Reset => CounterState { count: 0 },
                CounterMessage::GetCount => state, // No state change
            }
        }
        
        fn initial_state() -> Self::State {
            CounterState { count: 0 }
        }
    }
    
    #[test]
    fn test_functional_actor_receive() {
        // Test that receive is pure - same input produces same output
        let state1 = CounterState { count: 5 };
        let state2 = CounterState { count: 5 };
        let msg = CounterMessage::Increment;
        
        // Create a minimal context for testing
        use crate::actor_system::ActorSystem;
        use crate::actor_path::{ActorPath, ActorAddress};
        let system = Arc::new(ActorSystem::new("test"));
        let address = ActorAddress::local("test");
        let path = ActorPath::new(address, vec!["user".to_string(), "test".to_string()]);
        let ctx = FunctionalActorContext::new(system, path);
        
        let (new_state1, _) = CounterActor::receive(state1, msg.clone(), &ctx);
        let (new_state2, _) = CounterActor::receive(state2, msg, &ctx);
        
        // Same input should produce same output (purity)
        assert_eq!(new_state1, new_state2);
        assert_eq!(new_state1.count, 6);
    }
    
    #[test]
    fn test_state_reducer() {
        let state = CounterActor::initial_state();
        assert_eq!(state.count, 0);
        
        let state1 = CounterActor::reduce(state, CounterMessage::Increment);
        assert_eq!(state1.count, 1);
        
        let state2 = CounterActor::reduce(state1, CounterMessage::Increment);
        assert_eq!(state2.count, 2);
        
        let state3 = CounterActor::reduce(state2, CounterMessage::Decrement);
        assert_eq!(state3.count, 1);
        
        let state4 = CounterActor::reduce(state3, CounterMessage::Reset);
        assert_eq!(state4.count, 0);
    }
    
    #[test]
    fn test_effect_creation() {
        let effect = Effect::none();
        assert!(matches!(effect, Effect::None));
        
        let send_effect = Effect::send("target".to_string(), "message");
        assert!(matches!(send_effect, Effect::Send { .. }));
        
        let batch = Effect::batch(vec![Effect::none(), Effect::none()]);
        assert!(matches!(batch, Effect::Batch { .. }));
    }
    
    #[test]
    fn test_functional_actor_immutability() {
        // Test that state is not mutated
        let original_state = CounterState { count: 10 };
        let msg = CounterMessage::Increment;
        
        // Create a proper context for testing
        use crate::actor_system::ActorSystem;
        use crate::actor_path::{ActorPath, ActorAddress};
        let system = Arc::new(ActorSystem::new("test"));
        let address = ActorAddress::local("test");
        let path = ActorPath::new(address, vec!["user".to_string(), "test".to_string()]);
        let ctx = FunctionalActorContext::new(system, path);
        
        let (new_state, _) = CounterActor::receive(original_state.clone(), msg, &ctx);
        
        // Original state should be unchanged
        assert_eq!(original_state.count, 10);
        // New state should be different
        assert_eq!(new_state.count, 11);
        assert_ne!(original_state, new_state);
    }
}
