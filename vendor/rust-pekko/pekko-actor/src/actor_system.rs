//! Actor system implementation

use std::collections::HashMap;
use std::sync::Arc;
use std::any::Any;
use tokio::sync::RwLock;
use crate::actor::{Actor, Props};
use crate::actor_ref::{ActorRef, ActorError};
use crate::actor_path::{ActorPath, ActorAddress};
use crate::actor_context::ActorContext;
use crate::death_watch::{DeathWatchRegistry, Terminated};
use crate::scheduler::Scheduler;

/// Trait for type-erased actor mailbox
/// This allows us to store different actor types in the same HashMap
pub trait ActorMailbox: Send + Sync {
    /// Send a message to the actor (type-erased)
    fn send_any(&self, msg: Box<dyn Any + Send>) -> Result<(), ActorError>;

    /// Get the actor path
    fn path(&self) -> &ActorPath;

    /// Check if actor is terminated
    fn is_terminated(&self) -> bool;

    /// Restart the actor (if factory is available)
    fn restart(&self) -> Result<(), ActorError>;
}

/// Trait for type-erased actor factory
/// Allows storing Props<A> for different actor types
pub trait ActorFactory: Send + Sync {
    /// Create a new actor instance and spawn it
    fn respawn(&self, system: Arc<ActorSystem>, name: String) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ActorError>> + Send>>;
}

/// Concrete implementation of ActorMailbox for a specific actor type
struct TypedMailbox<A: Actor> {
    actor_ref: ActorRef<A>,
}

impl<A: Actor> TypedMailbox<A> {
    fn new(actor_ref: ActorRef<A>) -> Self {
        Self { actor_ref }
    }
}

impl<A: Actor> ActorMailbox for TypedMailbox<A> {
    fn send_any(&self, msg: Box<dyn Any + Send>) -> Result<(), ActorError> {
        // Try to downcast the message to the correct type
        match msg.downcast::<A::Message>() {
            Ok(typed_msg) => self.actor_ref.tell(*typed_msg),
            Err(_) => Err(ActorError::Failure(
                "Message type mismatch".to_string()
            )),
        }
    }

    fn path(&self) -> &ActorPath {
        self.actor_ref.path()
    }

    fn is_terminated(&self) -> bool {
        self.actor_ref.is_terminated()
    }

    fn restart(&self) -> Result<(), ActorError> {
        // Restart is handled by ActorSystem using the factory
        // This is just a placeholder for the trait
        Err(ActorError::Failure("Use ActorSystem::restart_actor instead".to_string()))
    }
}

/// Concrete implementation of ActorFactory for a specific actor type
struct TypedFactory<A: Actor> {
    props: Props<A>,
}

impl<A: Actor> TypedFactory<A> {
    fn new(props: Props<A>) -> Self {
        Self { props }
    }
}

impl<A: Actor> ActorFactory for TypedFactory<A> {
    fn respawn(&self, system: Arc<ActorSystem>, name: String) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ActorError>> + Send>> {
        // Create actor now (synchronously) since Props factory needs &self
        let actor_opt = self.props.create_actor();
        
        Box::pin(async move {
            if let Some(actor) = actor_opt {
                // First stop the old actor
                let _ = system.stop_actor(&name).await;
                
                // Then spawn the new one
                system.spawn(actor, &name).await?;
                Ok(())
            } else {
                Err(ActorError::Failure(format!(
                    "Cannot restart actor '{}': no factory available in Props",
                    name
                )))
            }
        })
    }
}

/// The actor system manages actors and their lifecycle
pub struct ActorSystem {
    name: String,
    actors: Arc<RwLock<HashMap<String, Arc<dyn ActorMailbox>>>>,
    /// Actor factories for restart capability
    factories: Arc<RwLock<HashMap<String, Arc<dyn ActorFactory>>>>,
    root_path: ActorPath,
    /// Death watch registry for monitoring actor termination
    death_watch: Arc<DeathWatchRegistry>,
    /// Scheduler for delayed and periodic tasks
    scheduler: Arc<Scheduler>,
}

impl ActorSystem {
    /// Create a new actor system
    pub fn new(name: &str) -> Self {
        let address = ActorAddress::local(name);
        let root_path = ActorPath::root(address);
        Self {
            name: name.to_string(),
            actors: Arc::new(RwLock::new(HashMap::new())),
            factories: Arc::new(RwLock::new(HashMap::new())),
            root_path,
            death_watch: Arc::new(DeathWatchRegistry::new()),
            scheduler: Arc::new(Scheduler::new()),
        }
    }

    /// Get the death watch registry
    pub fn death_watch(&self) -> &Arc<DeathWatchRegistry> {
        &self.death_watch
    }

    /// Get the scheduler
    pub fn scheduler(&self) -> &Arc<Scheduler> {
        &self.scheduler
    }

    /// Get the system name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the root path
    pub fn root_path(&self) -> &ActorPath {
        &self.root_path
    }

    /// Send a message to an actor by name
    /// This is the main method for sending typed messages to actors
    pub async fn send_message<A: Actor + 'static>(
        &self,
        actor_name: &str,
        message: A::Message,
    ) -> Result<(), ActorError> {
        let actors = self.actors.read().await;
        let mailbox = actors.get(actor_name)
            .ok_or(ActorError::NotFound)?;

        // Send the message through the mailbox
        mailbox.send_any(Box::new(message))?;
        Ok(())
    }

    /// Spawn a new actor
    pub async fn spawn<A: Actor>(
        &self,
        mut actor: A,
        name: &str,
    ) -> Result<ActorRef<A>, ActorError> {
        // Check if actor already exists
        {
            let actors = self.actors.read().await;
            if actors.contains_key(name) {
                return Err(ActorError::ActorAlreadyExists(name.to_string()));
            }
        }

        let path = self.root_path.child(name);
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        let actor_ref = ActorRef::new(path.clone(), sender);

        // Create typed mailbox and store it
        {
            let mut actors = self.actors.write().await;
            let mailbox = Arc::new(TypedMailbox::new(actor_ref.clone()));
            actors.insert(name.to_string(), mailbox);
        }

        // Start the actor task
        let actor_ref_clone = actor_ref.clone();
        let system = Arc::new(self.clone());
        tokio::spawn(async move {
            let mut ctx = ActorContext::new(
                system,
                actor_ref_clone.clone(),
                None, // No parent for root actors
            );

            actor.pre_start().await;

            while let Some(msg) = receiver.recv().await {
                actor.receive(msg, &mut ctx).await;
            }

            actor.post_stop().await;
            actor_ref_clone.terminate();
        });

        Ok(actor_ref)
    }

    /// Get an actor mailbox by name
    /// Note: This returns the mailbox, not the ActorRef directly
    /// Use send_message to communicate with the actor
    pub async fn get_mailbox(&self, name: &str) -> Option<Arc<dyn ActorMailbox>> {
        let actors = self.actors.read().await;
        actors.get(name).map(|mailbox| mailbox.clone())
    }

    /// Stop an actor
    pub async fn stop_actor(&self, name: &str) -> Result<(), ActorError> {
        let mailbox = {
            let mut actors = self.actors.write().await;
            actors.remove(name)
        };

        if let Some(mailbox) = mailbox {
            // Notify death watch watchers
            let terminated = Terminated::new(mailbox.path().clone());
            let _watchers = self.death_watch.notify_terminated(&terminated).await;
            
            // Also clean up if this actor was watching others
            self.death_watch.watcher_terminated(mailbox.path()).await;
            
            tracing::debug!("Stopped actor '{}'", name);
            Ok(())
        } else {
            Err(ActorError::NotFound)
        }
    }

    /// Stop an actor with failure flag (for supervision)
    pub async fn stop_actor_with_failure(&self, name: &str, reason: Option<String>) -> Result<(), ActorError> {
        let mailbox = {
            let mut actors = self.actors.write().await;
            actors.remove(name)
        };

        if let Some(mailbox) = mailbox {
            // Notify death watch watchers with failure info
            let mut terminated = Terminated::new(mailbox.path().clone()).with_failure(true);
            if let Some(r) = reason {
                terminated = terminated.with_reason(r);
            }
            let _watchers = self.death_watch.notify_terminated(&terminated).await;
            
            // Also clean up if this actor was watching others
            self.death_watch.watcher_terminated(mailbox.path()).await;
            
            tracing::debug!("Stopped actor '{}' (failure)", name);
            Ok(())
        } else {
            Err(ActorError::NotFound)
        }
    }

    /// Spawn a new actor with Props (enables restart capability)
    pub async fn spawn_with_props<A: Actor>(
        &self,
        props: Props<A>,
        name: &str,
    ) -> Result<ActorRef<A>, ActorError> {
        // Create actor from props
        let actor = props.create_actor()
            .ok_or_else(|| ActorError::Failure(
                "Props does not contain a factory function".to_string()
            ))?;

        // Store the factory for restart capability
        {
            let mut factories = self.factories.write().await;
            factories.insert(name.to_string(), Arc::new(TypedFactory::new(props)));
        }

        // Spawn the actor
        self.spawn(actor, name).await
    }

    /// Restart an actor using its stored factory
    pub async fn restart_actor(&self, name: &str) -> Result<(), ActorError> {
        let factory = {
            let factories = self.factories.read().await;
            factories.get(name).cloned()
        };

        match factory {
            Some(factory) => {
                let system = Arc::new(self.clone());
                factory.respawn(system, name.to_string()).await
            }
            None => {
                Err(ActorError::Failure(format!(
                    "Cannot restart actor '{}': no factory registered. Use spawn_with_props to enable restart.",
                    name
                )))
            }
        }
    }

    /// Check if an actor can be restarted (has a factory)
    pub async fn can_restart(&self, name: &str) -> bool {
        let factories = self.factories.read().await;
        factories.contains_key(name)
    }

    /// Remove factory (disable restart for this actor)
    pub async fn remove_factory(&self, name: &str) {
        let mut factories = self.factories.write().await;
        factories.remove(name);
    }
}

impl Clone for ActorSystem {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            actors: self.actors.clone(),
            factories: self.factories.clone(),
            root_path: self.root_path.clone(),
            death_watch: self.death_watch.clone(),
            scheduler: self.scheduler.clone(),
        }
    }
}

impl std::fmt::Debug for ActorSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorSystem")
            .field("name", &self.name)
            .field("root_path", &self.root_path)
            .finish()
    }
}
