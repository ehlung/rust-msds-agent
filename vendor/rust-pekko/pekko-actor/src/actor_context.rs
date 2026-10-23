//! Actor context implementation

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::actor::Actor;
use crate::actor_ref::{ActorRef, ActorError};
use crate::actor_system::ActorSystem;
use crate::actor_path::ActorPath;
use crate::supervision::{SupervisorState, SupervisorStrategy, ChildFailure, Directive, OneForOneStrategy};
use crate::death_watch::Terminated;
use std::time::Duration;

/// Actor context providing access to actor system and self reference
pub struct ActorContext<A: Actor + ?Sized> {
    system: Arc<ActorSystem>,
    self_ref: ActorRef<A>,
    parent: Option<Box<dyn std::any::Any + Send + Sync>>,
    children: Arc<RwLock<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>>,
    /// Supervision state for managing child failures
    supervisor_state: Option<Arc<SupervisorState>>,
}

impl<A: Actor + ?Sized> ActorContext<A> {
    /// Create a new actor context
    pub fn new(
        system: Arc<ActorSystem>,
        self_ref: ActorRef<A>,
        parent: Option<Box<dyn std::any::Any + Send + Sync>>,
    ) -> Self {
        Self {
            system,
            self_ref,
            parent,
            children: Arc::new(RwLock::new(HashMap::new())),
            supervisor_state: None,
        }
    }

    /// Create a new actor context with supervision
    pub fn with_supervision(
        system: Arc<ActorSystem>,
        self_ref: ActorRef<A>,
        parent: Option<Box<dyn std::any::Any + Send + Sync>>,
        strategy: Arc<dyn SupervisorStrategy>,
    ) -> Self {
        Self {
            system,
            self_ref,
            parent,
            children: Arc::new(RwLock::new(HashMap::new())),
            supervisor_state: Some(Arc::new(SupervisorState::new(strategy))),
        }
    }

    /// Set a supervision strategy (default: OneForOne with 10 retries in 60 seconds)
    pub fn set_supervision_strategy(&mut self, strategy: Arc<dyn SupervisorStrategy>) {
        self.supervisor_state = Some(Arc::new(SupervisorState::new(strategy)));
    }

    /// Use default supervision strategy (OneForOne, 10 retries, 60 seconds)
    pub fn use_default_supervision(&mut self) {
        let strategy = Arc::new(OneForOneStrategy::with_defaults(10, Duration::from_secs(60)));
        self.supervisor_state = Some(Arc::new(SupervisorState::new(strategy)));
    }

    /// Get the supervisor state
    pub fn supervisor_state(&self) -> Option<&Arc<SupervisorState>> {
        self.supervisor_state.as_ref()
    }

    /// Get the actor system
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Get the self reference
    pub fn self_ref(&self) -> &ActorRef<A> {
        &self.self_ref
    }

    /// Get the parent actor reference (if any)
    pub fn parent<T: Actor>(&self) -> Option<&ActorRef<T>> {
        self.parent.as_ref()
            .and_then(|any| any.downcast_ref::<ActorRef<T>>())
    }

    /// Spawn a child actor
    pub async fn spawn<B: Actor>(
        &self,
        actor: B,
        name: &str,
    ) -> Result<ActorRef<B>, ActorError> {
        let child_ref = self.system.spawn(actor, name).await?;
        
        // Store the child reference
        {
            let mut children = self.children.write().await;
            children.insert(name.to_string(), Box::new(child_ref.clone()));
        }

        // Register child with supervisor if supervision is enabled
        if let Some(ref supervisor_state) = self.supervisor_state {
            supervisor_state.register_child(name).await;
            tracing::debug!("Registered child '{}' with supervisor", name);
        }
        
        Ok(child_ref)
    }

    /// Handle a child failure according to the supervision strategy
    /// Returns the directive that was applied
    pub async fn handle_child_failure(
        &self,
        child_name: &str,
        error: &dyn std::error::Error,
    ) -> Result<Directive, ActorError> {
        let supervisor_state = self.supervisor_state.as_ref()
            .ok_or_else(|| ActorError::Failure("No supervision strategy configured".to_string()))?;

        let failure = ChildFailure::new(child_name, error);
        let result = supervisor_state.handle_child_failure(&failure).await;
        match result.directive {
            Directive::Resume => {
                // Do nothing, actor continues with current state
                tracing::debug!("Resuming child '{}'", child_name);
            }
            Directive::Restart => {
                // Restart the affected children
                for child in &result.children_to_restart {
                    // Note: Actual restart logic will be implemented in actor_system
                    // This requires the actor factory (Props) to recreate the actor
                    self.system.restart_actor(child).await?;
                }
            }
            Directive::Stop => {
                // Stop the child permanently
                tracing::warn!("Stopping child '{}' due to failure", child_name);
                self.stop_child(child_name).await?;
            }
            Directive::Escalate => {
                // Escalate to parent - this should be handled by the parent's supervision
                tracing::warn!("Escalating failure from child '{}' to parent", child_name);
                return Err(ActorError::Failure(format!(
                    "Escalated failure from child '{}': {}",
                    child_name,
                    error
                )));
            }
        }

        Ok(result.directive)
    }

    /// Notify that a child has recovered successfully
    /// This resets the restart statistics for that child
    pub async fn notify_child_recovered(&self, child_name: &str) {
        if let Some(ref supervisor_state) = self.supervisor_state {
            supervisor_state.reset_child_stats(child_name).await;
            tracing::debug!("Reset restart statistics for child '{}'", child_name);
        }
    }

    /// Get a child actor reference
    pub async fn get_child<B: Actor>(&self, name: &str) -> Option<ActorRef<B>> {
        let children = self.children.read().await;
        children.get(name)
            .and_then(|any| any.downcast_ref::<ActorRef<B>>())
            .map(|ref_| ref_.clone())
    }

    /// Stop a child actor
    pub async fn stop_child(&self, name: &str) -> Result<(), ActorError> {
        // Unregister from supervisor state
        if let Some(ref supervisor_state) = self.supervisor_state {
            supervisor_state.unregister_child(name).await;
        }

        let mut children = self.children.write().await;
        if let Some(_child_ref) = children.remove(name) {
            // Also remove from the system
            self.system.stop_actor(name).await?;
            tracing::debug!("Stopped child actor '{}'", name);
            Ok(())
        } else {
            Err(ActorError::NotFound)
        }
    }

    /// Get the actor path
    pub fn path(&self) -> &ActorPath {
        self.self_ref.path()
    }

    /// Check if the actor is terminated
    pub fn is_terminated(&self) -> bool {
        self.self_ref.is_terminated()
    }

    /// Send a message to another actor by name
    pub async fn send_message<B: Actor>(
        &self,
        target_name: &str,
        message: B::Message,
    ) -> Result<(), ActorError> {
        // Use the actor system to send the message
        self.system.send_message::<B>(target_name, message).await
    }

    // ============================================================
    // DeathWatch methods
    // ============================================================

    /// Watch another actor for termination
    /// When the watched actor terminates, this actor will receive a Terminated message
    pub async fn watch<B: Actor>(&self, target: &ActorRef<B>) {
        self.system
            .death_watch()
            .watch(self.path(), target.path())
            .await;
        tracing::debug!(
            "Actor '{}' is now watching '{}'",
            self.path(),
            target.path()
        );
    }

    /// Watch an actor by path
    pub async fn watch_path(&self, target_path: &ActorPath) {
        self.system
            .death_watch()
            .watch(self.path(), target_path)
            .await;
    }

    /// Stop watching another actor
    pub async fn unwatch<B: Actor>(&self, target: &ActorRef<B>) {
        self.system
            .death_watch()
            .unwatch(self.path(), target.path())
            .await;
        tracing::debug!(
            "Actor '{}' stopped watching '{}'",
            self.path(),
            target.path()
        );
    }

    /// Stop watching an actor by path
    pub async fn unwatch_path(&self, target_path: &ActorPath) {
        self.system
            .death_watch()
            .unwatch(self.path(), target_path)
            .await;
    }

    /// Check if this actor is watching another actor
    pub async fn is_watching<B: Actor>(&self, target: &ActorRef<B>) -> bool {
        self.system
            .death_watch()
            .is_watching(self.path(), target.path())
            .await
    }

    /// Get all actors this actor is watching
    pub async fn get_watched_actors(&self) -> Vec<String> {
        self.system
            .death_watch()
            .get_watched_by(self.path())
            .await
    }

    /// Register a callback to be invoked when a specific actor terminates
    /// This is an alternative to receiving Terminated messages
    pub async fn on_termination<F, B: Actor>(&self, target: &ActorRef<B>, callback: F)
    where
        F: Fn(Terminated) + Send + Sync + 'static,
    {
        // Also set up the watch relationship
        self.watch(target).await;
        
        self.system
            .death_watch()
            .on_termination(target.path(), callback)
            .await;
    }
}

impl<A: Actor + ?Sized> std::fmt::Debug for ActorContext<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorContext")
            .field("self_ref", &self.self_ref)
            .field("parent", &self.parent.is_some())
            .finish()
    }
}
