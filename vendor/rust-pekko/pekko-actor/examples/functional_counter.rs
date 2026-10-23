//! Functional Counter Actor Example
//! 
//! This example demonstrates how to use functional programming style actors
//! where state is immutable and transitions are pure functions.

use pekko_actor::{
    FunctionalActor, FunctionalActorContext, Effect,
    ActorSystem, ActorPath, ActorAddress,
};
use std::sync::Arc;

/// Counter state - immutable
#[derive(Debug, Clone, PartialEq)]
struct CounterState {
    count: u32,
}

/// Counter messages
#[derive(Debug, Clone)]
enum CounterMessage {
    Increment,
    Decrement,
    GetCount,
    Reset,
    SetValue(u32),
}

/// Functional counter actor
struct CounterActor;

impl FunctionalActor for CounterActor {
    type State = CounterState;
    type Message = CounterMessage;
    
    fn init(_ctx: &FunctionalActorContext) -> Self::State {
        println!("Counter actor initialized");
        CounterState { count: 0 }
    }
    
    fn receive(
        state: Self::State,
        msg: Self::Message,
        _ctx: &FunctionalActorContext,
    ) -> (Self::State, Effect) {
        match msg {
            CounterMessage::Increment => {
                println!("Incrementing counter from {} to {}", state.count, state.count + 1);
                (
                    CounterState { count: state.count + 1 },
                    Effect::None,
                )
            }
            CounterMessage::Decrement => {
                let new_count = state.count.saturating_sub(1);
                println!("Decrementing counter from {} to {}", state.count, new_count);
                (
                    CounterState { count: new_count },
                    Effect::None,
                )
            }
            CounterMessage::GetCount => {
                println!("Current count: {}", state.count);
                (state, Effect::None)
            }
            CounterMessage::Reset => {
                println!("Resetting counter from {} to 0", state.count);
                (CounterState { count: 0 }, Effect::None)
            }
            CounterMessage::SetValue(value) => {
                println!("Setting counter from {} to {}", state.count, value);
                (CounterState { count: value }, Effect::None)
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Functional Counter Actor Example ===\n");
    
    // Create actor system
    let system = Arc::new(ActorSystem::new("functional-system"));
    let address = ActorAddress::local("functional-system");
    let path = ActorPath::new(address, vec!["user".to_string(), "counter".to_string()]);
    let ctx = FunctionalActorContext::new(system.clone(), path);
    
    // Initialize state
    let mut state = CounterActor::init(&ctx);
    println!("Initial state: {:?}\n", state);
    
    // Process messages - demonstrating immutability
    let messages = vec![
        CounterMessage::Increment,
        CounterMessage::Increment,
        CounterMessage::Increment,
        CounterMessage::Decrement,
        CounterMessage::GetCount,
        CounterMessage::SetValue(100),
        CounterMessage::Reset,
    ];
    
    for msg in messages {
        let (new_state, effect) = CounterActor::receive(state.clone(), msg, &ctx);
        
        // Original state is unchanged (immutability)
        println!("Original state: {:?}", state);
        println!("New state: {:?}", new_state);
        println!("Effect: {:?}\n", effect);
        
        // Update to new state
        state = new_state;
        
        // Apply effect (in real implementation, this would be async)
        // CounterActor::apply_effect(effect, &mut ctx).await;
    }
    
    println!("Final state: {:?}", state);
    println!("\n=== Example completed ===");
    
    Ok(())
}
