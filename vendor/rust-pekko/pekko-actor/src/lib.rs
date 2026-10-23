//! # Pekko Actor - Core Actor System
//! 
//! A Rust implementation of the actor model based on Apache Pekko.
//! 
//! ## Features
//! 
//! - Type-safe actor references
//! - Hierarchical actor supervision
//! - Fault tolerance with restart/stop directives
//! - Async message processing
//! - Configurable dispatchers and mailboxes
//! - Ask pattern (request-response)
//! 
//! ## Ask Pattern
//! 
//! The ask pattern allows you to send a message and receive a response:
//! 
//! ```rust,ignore
//! use pekko_actor::{Actor, ActorRef, ReplyTo};
//! 
//! // Define message with ReplyTo
//! enum MyMessage {
//!     GetValue { reply_to: ReplyTo<i32> },
//! }
//! 
//! // Ask the actor
//! let result = actor_ref.ask(|reply_to| MyMessage::GetValue { reply_to }).await?;
//! ```

pub mod actor;
pub mod actor_path;
pub mod actor_ref;
pub mod actor_system;
pub mod actor_context;
pub mod functional_actor;
pub mod ask;
pub mod supervision;
pub mod death_watch;
pub mod stash;
pub mod scheduler;
pub mod behavior;
pub mod router;
pub mod event_bus;
pub mod mailbox;
pub mod dispatcher;
pub mod metrics;
pub mod circuit_breaker;
pub mod pipe_to;
pub mod coordinated_shutdown;
pub mod fsm;
pub mod inbox;
pub mod graceful_stop;
pub mod aggregator;
pub mod receptionist;
pub mod message_adapter;
pub mod watch_with;
pub mod actor_selection;
pub mod behavior_composition;
pub mod typed_props;
pub mod router_pool;
pub mod work_stealing;

#[cfg(test)]
mod ask_integration_tests;

#[cfg(test)]
mod supervision_integration_tests;

#[cfg(test)]
mod death_watch_integration_tests;

// Re-export specific types to avoid conflicts
pub use actor::{Actor, ActorLifecycle, ActorBehavior, Props};
pub use actor_path::{ActorPath, ActorAddress};
pub use actor_ref::{ActorRef, ActorError};
pub use actor_system::ActorSystem;
pub use actor_context::ActorContext;

// Re-export ask pattern types
pub use ask::{ReplyTo, AskFuture, AskError, ask_channel, ask_channel_with_timeout, DEFAULT_ASK_TIMEOUT};

// Re-export functional actor types
pub use functional_actor::{
    FunctionalActor, FunctionalActorContext, Effect, FunctionalActorAdapter, 
    StateReducer, ComposedReducer,
};

// Re-export supervision types
pub use supervision::{
    Directive, Decider, ChildFailure, RestartStatistics, SupervisionResult,
    SupervisorStrategy, SupervisorState,
    OneForOneStrategy, OneForAllStrategy, RestForOneStrategy,
    default_decider,
    // Backoff types
    BackoffConfig, BackoffState, BackoffSupervisorStrategy, BackoffSupervisorState,
};

// Re-export death watch types
pub use death_watch::{
    Terminated, SystemMessage, DeathWatchRegistry, WithSystemMessage,
};

// Re-export stash types
pub use stash::{
    Stash, StashConfig, StashOverflowStrategy, StashResult,
    StashSupport, BoundedStash, AnyStash,
};

// Re-export scheduler types
pub use scheduler::{
    Scheduler, Cancellable, ActorTimer, ScheduleBuilder, ScheduleExt,
};

// Re-export behavior types
pub use behavior::{
    Behavior, BehaviorFn, SyncBehaviorFn, BehaviorStack, BehaviorBuilder,
    ProcessResult, StateMachine,
};

// Re-export router types
pub use router::{
    Routee, RoutingStrategy, Router, RouteResult, Routers,
    RoundRobinStrategy, RandomStrategy, BroadcastStrategy,
    ScatterGatherStrategy, ConsistentHashStrategy, FirstAvailableStrategy,
    AdaptiveStrategy,
};

// Re-export event bus types
pub use event_bus::{
    EventBus, Subscriber, CallbackSubscriber, Subscription,
    ChannelEventBus, Event, DeadLetterBus, DeadLetter,
};

// Re-export mailbox types
pub use mailbox::{
    Mailbox as MailboxTrait, MailboxConfig, MailboxType, MailboxOverflowStrategy, MailboxError,
    UnboundedMailbox, BoundedMailbox, PriorityMailbox, PriorityFn,
    ControlAwareMailbox, ControlAwareMessage, DeadLetterMailbox,
};

// Re-export dispatcher types
pub use dispatcher::{
    Dispatcher, DispatcherConfig, DispatcherType, DispatcherStats, BoxFuture,
    DefaultDispatcher, PinnedDispatcher, CallingThreadDispatcher, DispatcherSelector,
};

// Re-export metrics types
pub use metrics::{
    ActorMetrics, ActorMetricsSnapshot, SystemMetrics, SystemMetricsSnapshot,
    MetricsCollector, MetricsReportHandle, ProcessingTimer,
};

// Re-export circuit breaker types
pub use circuit_breaker::{
    CircuitBreaker, CircuitBreakerState, CircuitBreakerError,
    CircuitBreakerBuilder, CircuitBreakerStats,
};

// Re-export pipe_to types
pub use pipe_to::{
    PipeTarget, PipeError, ChannelTarget, PipeToExt, PipeBuilder,
    PipeStatus, pipe_to, pipe_to_mapped, pipe_to_with_timeout, pipe_status, pipe_all,
};

// Re-export coordinated shutdown types
pub use coordinated_shutdown::{
    CoordinatedShutdown, ShutdownPhase, ShutdownReason,
    ShutdownResult, PhaseResult,
};

// Re-export FSM types
pub use fsm::{
    StateMachine as FsmStateMachine, StateMachineBuilder,
    TransitionResult, StopReason as FsmStopReason, TransitionRecord,
};

// Re-export inbox types
pub use inbox::{
    Inbox, InboxSender, InboxMessage, InboxError, InboxAssertion,
};

// Re-export graceful stop types
pub use graceful_stop::{
    GracefulStopper, GracefulStopConfig, GracefulStopResult,
    StopMessage, PreStopHook, CascadingStop, graceful_stop,
};

// Re-export aggregator types
pub use aggregator::{
    Aggregator, AggregatorConfig, AggregatorBuilder, AggregateResult, AggregateStatus,
    CompletionStrategy, ResponseStatus, AggregatorError, AggregationReducer, ReducedResult,
    AggregationProgress, RetryState,
};

// Re-export receptionist types
pub use receptionist::{
    Receptionist, ServiceKey, ServiceInstance, Listing, ReceptionistEvent,
    Subscription as ReceptionistSubscription, ReceptionistError, ReceptionistStats, LookupStrategy,
};

// Re-export message adapter types
pub use message_adapter::{
    MessageAdapter, AdapterStats, AdapterChain, MessageEnvelope,
    SpawnRequest, SpawnConfig, SpawnResponse, BufferedAdapter, BufferOverflow,
    AdapterError, ProtocolBridge,
};

// Re-export watch_with types
pub use watch_with::{
    DeathPactException, WatchWithConfig, WatchWithRegistry, WatchWithStats,
    WatchWithBuilder, DeathPactMode, DeathPactConfig, DeathPactOutcome,
    DeathPactManager, DeathPactStats,
};

// Re-export actor_selection types
pub use actor_selection::{
    SelectionPattern, PatternSegment, ActorSelection, ActorPathRegistry,
    PathMetadata, PathQuery,
};

// Re-export behavior_composition types
pub use behavior_composition::{
    HandleResult, HandlerFn, OrElse, AndThen, CombineStrategy,
    Intercept, InterceptDecision, InterceptStats, HandlerChain, ChainStats,
    MessageTransform, TransformStats, BehaviorGuard, GuardStats, CompositionStats,
};

// Re-export typed_props types
pub use typed_props::{
    Protocol, TypedActorRef, UntypedActorRef, TypedProps,
    ProtocolRegistry, ProtocolRegistryStats,
};

// Re-export router_pool types
pub use router_pool::{
    RouterPool, PoolRoutee, PoolRoutingStrategy, RouteeState,
    ScalingPolicy, ScalingDecision, HealthCheck, HealthStatus,
    RouterPoolStats, ScaleEvent,
};

// Re-export work_stealing types
pub use work_stealing::{
    WorkStealingDispatcher, WorkStealingConfig, WorkStealingStats,
    WorkerQueue, WorkerState, Task, TaskPriority,
    StealingStrategy,
};

// Re-export commonly used types
pub use tokio;
pub use tracing;

#[cfg(test)]
mod tests {
    #[test]
    fn test_actor_trait_compiles() {
        // This test just ensures the trait implementation compiles
        assert!(true);
    }
}