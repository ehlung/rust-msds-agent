//! DeathWatch - Monitor actor termination
//!
//! DeathWatch allows actors to monitor other actors and receive notifications
//! when they terminate. This is essential for building fault-tolerant systems
//! where actors need to react to the death of their collaborators.
//!
//! ## Example
//!
//! ```rust,ignore
//! use pekko_actor::{Actor, ActorContext, Terminated, SystemMessage};
//!
//! impl Actor for MyActor {
//!     type Message = MyMessage;
//!
//!     async fn receive(&mut self, msg: MyMessage, ctx: &mut ActorContext<Self>) {
//!         match msg {
//!             MyMessage::WatchWorker(worker_ref) => {
//!                 ctx.watch(&worker_ref).await;
//!             }
//!             MyMessage::System(SystemMessage::Terminated(terminated)) => {
//!                 println!("Actor {} died!", terminated.actor_path);
//!                 // Handle the death - maybe restart, failover, etc.
//!             }
//!             // ...
//!         }
//!     }
//! }
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::actor_path::ActorPath;

/// Message sent to watchers when a watched actor terminates
#[derive(Debug, Clone)]
pub struct Terminated {
    /// Path of the actor that terminated
    pub actor_path: ActorPath,
    /// Whether the termination was due to a failure
    pub failure: bool,
    /// Optional reason for termination
    pub reason: Option<String>,
}

impl Terminated {
    pub fn new(actor_path: ActorPath) -> Self {
        Self {
            actor_path,
            failure: false,
            reason: None,
        }
    }

    pub fn with_failure(mut self, failure: bool) -> Self {
        self.failure = failure;
        self
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

/// System messages that can be sent to any actor
#[derive(Debug, Clone)]
pub enum SystemMessage {
    /// Notification that a watched actor has terminated
    Terminated(Terminated),
    /// Request to watch another actor
    Watch { target: ActorPath, watcher: ActorPath },
    /// Request to stop watching another actor
    Unwatch { target: ActorPath, watcher: ActorPath },
    /// Poison pill - causes the actor to stop
    PoisonPill,
    /// Kill - causes immediate termination
    Kill,
}

/// Callback type for termination notifications
pub type TerminationCallback = Arc<dyn Fn(Terminated) + Send + Sync>;

/// Registry that tracks who is watching whom
#[derive(Default)]
pub struct DeathWatchRegistry {
    /// Map from watched actor path to set of watcher paths
    watchers: RwLock<HashMap<String, HashSet<String>>>,
    /// Map from watcher path to set of watched paths
    watching: RwLock<HashMap<String, HashSet<String>>>,
    /// Callbacks to invoke when termination occurs
    callbacks: RwLock<HashMap<String, Vec<TerminationCallback>>>,
}

impl std::fmt::Debug for DeathWatchRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeathWatchRegistry")
            .field("watchers", &"...")
            .field("watching", &"...")
            .field("callbacks", &"<callbacks>")
            .finish()
    }
}

impl DeathWatchRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register that `watcher` wants to watch `target`
    pub async fn watch(&self, watcher: &ActorPath, target: &ActorPath) {
        let watcher_key = watcher.to_string();
        let target_key = target.to_string();

        // Add to watchers map (target -> watchers)
        {
            let mut watchers = self.watchers.write().await;
            watchers
                .entry(target_key.clone())
                .or_insert_with(HashSet::new)
                .insert(watcher_key.clone());
        }

        // Add to watching map (watcher -> targets)
        {
            let mut watching = self.watching.write().await;
            watching
                .entry(watcher_key.clone())
                .or_insert_with(HashSet::new)
                .insert(target_key.clone());
        }

        tracing::debug!(
            "DeathWatch: {} is now watching {}",
            watcher_key,
            target_key
        );
    }

    /// Unregister watch relationship
    pub async fn unwatch(&self, watcher: &ActorPath, target: &ActorPath) {
        let watcher_key = watcher.to_string();
        let target_key = target.to_string();

        // Remove from watchers map
        {
            let mut watchers = self.watchers.write().await;
            if let Some(watcher_set) = watchers.get_mut(&target_key) {
                watcher_set.remove(&watcher_key);
                if watcher_set.is_empty() {
                    watchers.remove(&target_key);
                }
            }
        }

        // Remove from watching map
        {
            let mut watching = self.watching.write().await;
            if let Some(target_set) = watching.get_mut(&watcher_key) {
                target_set.remove(&target_key);
                if target_set.is_empty() {
                    watching.remove(&watcher_key);
                }
            }
        }

        tracing::debug!(
            "DeathWatch: {} stopped watching {}",
            watcher_key,
            target_key
        );
    }

    /// Register a callback to be invoked when an actor terminates
    pub async fn on_termination<F>(&self, target: &ActorPath, callback: F)
    where
        F: Fn(Terminated) + Send + Sync + 'static,
    {
        let target_key = target.to_string();
        let mut callbacks = self.callbacks.write().await;
        callbacks
            .entry(target_key)
            .or_insert_with(Vec::new)
            .push(Arc::new(callback));
    }

    /// Notify all watchers that an actor has terminated
    /// Returns the list of watcher paths that need to be notified
    pub async fn notify_terminated(&self, terminated: &Terminated) -> Vec<String> {
        let target_key = terminated.actor_path.to_string();

        // Get all watchers
        let watcher_paths: Vec<String> = {
            let watchers = self.watchers.read().await;
            watchers
                .get(&target_key)
                .map(|set| set.iter().cloned().collect())
                .unwrap_or_default()
        };

        // Invoke callbacks
        {
            let callbacks = self.callbacks.read().await;
            if let Some(cbs) = callbacks.get(&target_key) {
                for cb in cbs {
                    cb(terminated.clone());
                }
            }
        }

        // Cleanup: remove the terminated actor from all maps
        {
            let mut watchers = self.watchers.write().await;
            watchers.remove(&target_key);
        }

        // Also remove from watching map for all watchers
        {
            let mut watching = self.watching.write().await;
            for watcher_path in &watcher_paths {
                if let Some(targets) = watching.get_mut(watcher_path) {
                    targets.remove(&target_key);
                }
            }
        }

        // Cleanup callbacks
        {
            let mut callbacks = self.callbacks.write().await;
            callbacks.remove(&target_key);
        }

        tracing::debug!(
            "DeathWatch: {} terminated, notifying {} watchers",
            target_key,
            watcher_paths.len()
        );

        watcher_paths
    }

    /// Called when a watcher itself terminates - cleanup all its watches
    pub async fn watcher_terminated(&self, watcher: &ActorPath) {
        let watcher_key = watcher.to_string();

        // Get all targets this watcher was watching
        let targets: Vec<String> = {
            let watching = self.watching.read().await;
            watching
                .get(&watcher_key)
                .map(|set| set.iter().cloned().collect())
                .unwrap_or_default()
        };

        // Remove watcher from all target's watcher sets
        {
            let mut watchers = self.watchers.write().await;
            for target_key in &targets {
                if let Some(watcher_set) = watchers.get_mut(target_key) {
                    watcher_set.remove(&watcher_key);
                    if watcher_set.is_empty() {
                        watchers.remove(target_key);
                    }
                }
            }
        }

        // Remove watcher's entry from watching map
        {
            let mut watching = self.watching.write().await;
            watching.remove(&watcher_key);
        }

        tracing::debug!(
            "DeathWatch: watcher {} terminated, cleaned up {} watches",
            watcher_key,
            targets.len()
        );
    }

    /// Check if `watcher` is watching `target`
    pub async fn is_watching(&self, watcher: &ActorPath, target: &ActorPath) -> bool {
        let watcher_key = watcher.to_string();
        let target_key = target.to_string();

        let watching = self.watching.read().await;
        watching
            .get(&watcher_key)
            .map(|targets| targets.contains(&target_key))
            .unwrap_or(false)
    }

    /// Get all actors being watched by a given watcher
    pub async fn get_watched_by(&self, watcher: &ActorPath) -> Vec<String> {
        let watcher_key = watcher.to_string();
        let watching = self.watching.read().await;
        watching
            .get(&watcher_key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get all watchers of a given actor
    pub async fn get_watchers_of(&self, target: &ActorPath) -> Vec<String> {
        let target_key = target.to_string();
        let watchers = self.watchers.read().await;
        watchers
            .get(&target_key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get count of active watches
    pub async fn watch_count(&self) -> usize {
        let watching = self.watching.read().await;
        watching.values().map(|set| set.len()).sum()
    }
}

/// Extension trait for message types that can include system messages
pub trait WithSystemMessage: Sized {
    /// Create a message from a system message
    fn from_system(msg: SystemMessage) -> Option<Self>;
    
    /// Try to extract a system message
    fn as_system(&self) -> Option<&SystemMessage>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor_path::ActorAddress;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    fn make_path(name: &str) -> ActorPath {
        let address = ActorAddress::local("test-system");
        ActorPath::root(address).child(name)
    }

    #[tokio::test]
    async fn test_basic_watch() {
        let registry = DeathWatchRegistry::new();

        let watcher = make_path("watcher");
        let target = make_path("target");

        registry.watch(&watcher, &target).await;

        assert!(registry.is_watching(&watcher, &target).await);
    }

    #[tokio::test]
    async fn test_unwatch() {
        let registry = DeathWatchRegistry::new();

        let watcher = make_path("watcher");
        let target = make_path("target");

        registry.watch(&watcher, &target).await;
        assert!(registry.is_watching(&watcher, &target).await);

        registry.unwatch(&watcher, &target).await;
        assert!(!registry.is_watching(&watcher, &target).await);
    }

    #[tokio::test]
    async fn test_notify_terminated() {
        let registry = DeathWatchRegistry::new();

        let watcher1 = make_path("watcher1");
        let watcher2 = make_path("watcher2");
        let target = make_path("target");

        registry.watch(&watcher1, &target).await;
        registry.watch(&watcher2, &target).await;

        let terminated = Terminated::new(target.clone());
        let notified = registry.notify_terminated(&terminated).await;

        assert_eq!(notified.len(), 2);
        assert!(notified.contains(&watcher1.to_string()));
        assert!(notified.contains(&watcher2.to_string()));

        // Watch should be cleaned up
        assert!(!registry.is_watching(&watcher1, &target).await);
        assert!(!registry.is_watching(&watcher2, &target).await);
    }

    #[tokio::test]
    async fn test_callback_on_termination() {
        let registry = DeathWatchRegistry::new();
        let target = make_path("target");

        let callback_called = Arc::new(AtomicBool::new(false));
        let cc = callback_called.clone();

        registry
            .on_termination(&target, move |_| {
                cc.store(true, Ordering::SeqCst);
            })
            .await;

        let terminated = Terminated::new(target.clone());
        registry.notify_terminated(&terminated).await;

        assert!(callback_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_multiple_callbacks() {
        let registry = DeathWatchRegistry::new();
        let target = make_path("target");

        let counter = Arc::new(AtomicU32::new(0));

        for _ in 0..3 {
            let c = counter.clone();
            registry
                .on_termination(&target, move |_| {
                    c.fetch_add(1, Ordering::SeqCst);
                })
                .await;
        }

        let terminated = Terminated::new(target.clone());
        registry.notify_terminated(&terminated).await;

        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_watcher_terminated_cleanup() {
        let registry = DeathWatchRegistry::new();

        let watcher = make_path("watcher");
        let target1 = make_path("target1");
        let target2 = make_path("target2");

        registry.watch(&watcher, &target1).await;
        registry.watch(&watcher, &target2).await;

        assert!(registry.is_watching(&watcher, &target1).await);
        assert!(registry.is_watching(&watcher, &target2).await);

        // Watcher terminates
        registry.watcher_terminated(&watcher).await;

        // All watches should be cleaned up
        assert!(!registry.is_watching(&watcher, &target1).await);
        assert!(!registry.is_watching(&watcher, &target2).await);

        // Targets should have no watchers
        assert!(registry.get_watchers_of(&target1).await.is_empty());
        assert!(registry.get_watchers_of(&target2).await.is_empty());
    }

    #[tokio::test]
    async fn test_get_watched_by() {
        let registry = DeathWatchRegistry::new();

        let watcher = make_path("watcher");
        let target1 = make_path("target1");
        let target2 = make_path("target2");
        let target3 = make_path("target3");

        registry.watch(&watcher, &target1).await;
        registry.watch(&watcher, &target2).await;
        registry.watch(&watcher, &target3).await;

        let watched = registry.get_watched_by(&watcher).await;
        assert_eq!(watched.len(), 3);
    }

    #[tokio::test]
    async fn test_get_watchers_of() {
        let registry = DeathWatchRegistry::new();

        let watcher1 = make_path("watcher1");
        let watcher2 = make_path("watcher2");
        let target = make_path("target");

        registry.watch(&watcher1, &target).await;
        registry.watch(&watcher2, &target).await;

        let watchers = registry.get_watchers_of(&target).await;
        assert_eq!(watchers.len(), 2);
    }

    #[tokio::test]
    async fn test_watch_count() {
        let registry = DeathWatchRegistry::new();

        let watcher1 = make_path("watcher1");
        let watcher2 = make_path("watcher2");
        let target1 = make_path("target1");
        let target2 = make_path("target2");

        assert_eq!(registry.watch_count().await, 0);

        registry.watch(&watcher1, &target1).await;
        assert_eq!(registry.watch_count().await, 1);

        registry.watch(&watcher1, &target2).await;
        assert_eq!(registry.watch_count().await, 2);

        registry.watch(&watcher2, &target1).await;
        assert_eq!(registry.watch_count().await, 3);

        registry.unwatch(&watcher1, &target1).await;
        assert_eq!(registry.watch_count().await, 2);
    }

    #[tokio::test]
    async fn test_terminated_with_failure() {
        let target = make_path("target");
        
        let terminated = Terminated::new(target.clone())
            .with_failure(true)
            .with_reason("out of memory");

        assert!(terminated.failure);
        assert_eq!(terminated.reason, Some("out of memory".to_string()));
    }

    #[tokio::test]
    async fn test_duplicate_watch_idempotent() {
        let registry = DeathWatchRegistry::new();

        let watcher = make_path("watcher");
        let target = make_path("target");

        registry.watch(&watcher, &target).await;
        registry.watch(&watcher, &target).await;
        registry.watch(&watcher, &target).await;

        // Should still only have one watch relationship
        let watchers = registry.get_watchers_of(&target).await;
        assert_eq!(watchers.len(), 1);

        let terminated = Terminated::new(target.clone());
        let notified = registry.notify_terminated(&terminated).await;
        assert_eq!(notified.len(), 1);
    }

    #[test]
    fn test_system_message_variants() {
        let target = make_path("target");
        let watcher = make_path("watcher");

        let terminated = SystemMessage::Terminated(Terminated::new(target.clone()));
        let watch = SystemMessage::Watch {
            target: target.clone(),
            watcher: watcher.clone(),
        };
        let unwatch = SystemMessage::Unwatch {
            target: target.clone(),
            watcher: watcher.clone(),
        };
        let poison = SystemMessage::PoisonPill;
        let kill = SystemMessage::Kill;

        // Just verify they can be created
        assert!(matches!(terminated, SystemMessage::Terminated(_)));
        assert!(matches!(watch, SystemMessage::Watch { .. }));
        assert!(matches!(unwatch, SystemMessage::Unwatch { .. }));
        assert!(matches!(poison, SystemMessage::PoisonPill));
        assert!(matches!(kill, SystemMessage::Kill));
    }
}
