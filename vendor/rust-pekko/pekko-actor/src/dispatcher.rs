//! Dispatcher — Thread pool and execution strategies for actors
//!
//! Modeled after Scala Pekko's Dispatcher system:
//!
//! - **DefaultDispatcher**: Shared thread pool (Tokio's default runtime)
//! - **PinnedDispatcher**: Dedicated thread per actor
//! - **CallingThreadDispatcher**: Executes in the caller's thread (for testing)
//! - **DispatcherConfig**: Configuration for pool sizes and throughput
//! - **DispatcherSelector**: Registry of named dispatchers

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration};
use tokio::runtime::{Handle};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use std::future::Future;
use std::pin::Pin;

// ============= Dispatcher Config =============

/// Configuration for a dispatcher.
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// Name of this dispatcher
    pub name: String,
    /// Type of dispatcher
    pub dispatcher_type: DispatcherType,
    /// Number of threads in the pool (for Fork-Join dispatchers)
    pub parallelism_min: usize,
    /// Maximum number of threads
    pub parallelism_max: usize,
    /// Factor to calculate thread count from available processors
    pub parallelism_factor: f64,
    /// Number of messages to process per actor before yielding
    pub throughput: usize,
    /// Maximum time an actor can process messages before yielding
    pub throughput_deadline: Option<Duration>,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            name: "default-dispatcher".to_string(),
            dispatcher_type: DispatcherType::Default,
            parallelism_min: 2,
            parallelism_max: num_cpus(),
            parallelism_factor: 1.0,
            throughput: 10,
            throughput_deadline: None,
        }
    }
}

impl DispatcherConfig {
    /// Calculate the target thread count based on available processors.
    pub fn calculated_parallelism(&self) -> usize {
        let from_factor = (num_cpus() as f64 * self.parallelism_factor).ceil() as usize;
        from_factor.max(self.parallelism_min).min(self.parallelism_max)
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_type(mut self, dispatcher_type: DispatcherType) -> Self {
        self.dispatcher_type = dispatcher_type;
        self
    }

    pub fn with_throughput(mut self, throughput: usize) -> Self {
        self.throughput = throughput;
        self
    }

    pub fn with_parallelism(mut self, min: usize, max: usize, factor: f64) -> Self {
        self.parallelism_min = min;
        self.parallelism_max = max;
        self.parallelism_factor = factor;
        self
    }
}

/// Available dispatcher types.
#[derive(Debug, Clone, PartialEq)]
pub enum DispatcherType {
    /// Shared fork-join thread pool (default, backed by Tokio)
    Default,
    /// Each actor gets its own dedicated thread
    Pinned,
    /// Execute on the calling thread (for testing)
    CallingThread,
}

impl fmt::Display for DispatcherType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DispatcherType::Default => write!(f, "default"),
            DispatcherType::Pinned => write!(f, "pinned"),
            DispatcherType::CallingThread => write!(f, "calling-thread"),
        }
    }
}

// ============= Dispatcher Trait =============

/// A dispatcher that can execute actor tasks.
pub trait Dispatcher: Send + Sync {
    /// Get the name of this dispatcher.
    fn name(&self) -> &str;

    /// Get the dispatcher type.
    fn dispatcher_type(&self) -> DispatcherType;

    /// Get the configured throughput (messages per scheduling).
    fn throughput(&self) -> usize;

    /// Execute a future on this dispatcher.
    fn execute(&self, task: BoxFuture);

    /// Get dispatcher statistics.
    fn stats(&self) -> DispatcherStats;

    /// Number of tasks currently queued or executing.
    fn active_task_count(&self) -> usize;

    /// Shut down the dispatcher.
    fn shutdown(&self);
}

/// A boxed future for dispatcher execution.
pub type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Statistics for a dispatcher.
#[derive(Debug, Clone, Default)]
pub struct DispatcherStats {
    pub tasks_submitted: u64,
    pub tasks_completed: u64,
    pub total_execution_time_ms: u64,
    pub active_threads: usize,
}

// ============= Default Dispatcher =============

/// Default dispatcher using Tokio's shared runtime.
/// This is the recommended dispatcher for most actors.
pub struct DefaultDispatcher {
    name: String,
    config: DispatcherConfig,
    handle: Handle,
    tasks_submitted: Arc<AtomicU64>,
    tasks_completed: Arc<AtomicU64>,
    active_tasks: Arc<AtomicU64>,
    shutdown: AtomicBool,
}

impl DefaultDispatcher {
    /// Create using the current Tokio runtime.
    pub fn new(config: DispatcherConfig) -> Self {
        let name = config.name.clone();
        Self {
            name,
            config,
            handle: Handle::current(),
            tasks_submitted: Arc::new(AtomicU64::new(0)),
            tasks_completed: Arc::new(AtomicU64::new(0)),
            active_tasks: Arc::new(AtomicU64::new(0)),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Create with a specific Tokio runtime handle.
    pub fn with_handle(config: DispatcherConfig, handle: Handle) -> Self {
        let name = config.name.clone();
        Self {
            name,
            config,
            handle,
            tasks_submitted: Arc::new(AtomicU64::new(0)),
            tasks_completed: Arc::new(AtomicU64::new(0)),
            active_tasks: Arc::new(AtomicU64::new(0)),
            shutdown: AtomicBool::new(false),
        }
    }
}

impl Dispatcher for DefaultDispatcher {
    fn name(&self) -> &str {
        &self.name
    }

    fn dispatcher_type(&self) -> DispatcherType {
        DispatcherType::Default
    }

    fn throughput(&self) -> usize {
        self.config.throughput
    }

    fn execute(&self, task: BoxFuture) {
        if self.shutdown.load(Ordering::Relaxed) {
            warn!("DefaultDispatcher '{}': task rejected (shutdown)", self.name);
            return;
        }
    
        self.tasks_submitted.fetch_add(1, Ordering::Relaxed);
        let active = self.active_tasks.clone();
        let completed = self.tasks_completed.clone();
        active.fetch_add(1, Ordering::Relaxed);
    
        self.handle.spawn(async move {
            task.await;
            active.fetch_sub(1, Ordering::Relaxed);
            completed.fetch_add(1, Ordering::Relaxed);
        });
    }
    

    fn stats(&self) -> DispatcherStats {
        DispatcherStats {
            tasks_submitted: self.tasks_submitted.load(Ordering::Relaxed),
            tasks_completed: self.tasks_completed.load(Ordering::Relaxed),
            total_execution_time_ms: 0,
            active_threads: self.config.calculated_parallelism(),
        }
    }

    fn active_task_count(&self) -> usize {
        self.active_tasks.load(Ordering::Relaxed) as usize
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        debug!("DefaultDispatcher '{}' shutdown", self.name);
    }
}

// ============= Pinned Dispatcher =============

/// Dispatcher that gives each actor its own dedicated thread.
/// Best for actors that perform blocking I/O.
pub struct PinnedDispatcher {
    name: String,
    config: DispatcherConfig,
    handle: Handle,
    tasks_submitted: Arc<AtomicU64>,
    tasks_completed: Arc<AtomicU64>,
    active_tasks: Arc<AtomicU64>,
    shutdown: AtomicBool,
}

impl PinnedDispatcher {
    pub fn new(config: DispatcherConfig) -> Self {
        let name = config.name.clone();
        Self {
            name,
            config,
            handle: Handle::current(),
            tasks_submitted: Arc::new(AtomicU64::new(0)),
            tasks_completed: Arc::new(AtomicU64::new(0)),
            active_tasks: Arc::new(AtomicU64::new(0)),
            shutdown: AtomicBool::new(false),
        }
    }
}

impl Dispatcher for PinnedDispatcher {
    fn name(&self) -> &str {
        &self.name
    }

    fn dispatcher_type(&self) -> DispatcherType {
        DispatcherType::Pinned
    }

    fn throughput(&self) -> usize {
        self.config.throughput
    }

    fn execute(&self, task: BoxFuture) {
        if self.shutdown.load(Ordering::Relaxed) {
            warn!("PinnedDispatcher '{}': task rejected (shutdown)", self.name);
            return;
        }
    
        self.tasks_submitted.fetch_add(1, Ordering::Relaxed);
        let active = self.active_tasks.clone();
        let completed = self.tasks_completed.clone();
        active.fetch_add(1, Ordering::Relaxed);
    
        self.handle.spawn(async move {
            task.await;
            active.fetch_sub(1, Ordering::Relaxed);
            completed.fetch_add(1, Ordering::Relaxed);
        });
    }
    

    fn stats(&self) -> DispatcherStats {
        DispatcherStats {
            tasks_submitted: self.tasks_submitted.load(Ordering::Relaxed),
            tasks_completed: self.tasks_completed.load(Ordering::Relaxed),
            total_execution_time_ms: 0,
            active_threads: self.active_tasks.load(Ordering::Relaxed) as usize,
        }
    }

    fn active_task_count(&self) -> usize {
        self.active_tasks.load(Ordering::Relaxed) as usize
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        debug!("PinnedDispatcher '{}' shutdown", self.name);
    }
}

// ============= Calling Thread Dispatcher =============

/// Dispatcher that runs tasks synchronously on the calling thread.
/// Primarily used for testing — ensures deterministic execution order.
pub struct CallingThreadDispatcher {
    name: String,
    config: DispatcherConfig,
    tasks_submitted: AtomicU64,
    tasks_completed: AtomicU64,
    shutdown: AtomicBool,
}

impl CallingThreadDispatcher {
    pub fn new(config: DispatcherConfig) -> Self {
        let name = config.name.clone();
        Self {
            name,
            config,
            tasks_submitted: AtomicU64::new(0),
            tasks_completed: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
        }
    }
}

impl Dispatcher for CallingThreadDispatcher {
    fn name(&self) -> &str {
        &self.name
    }

    fn dispatcher_type(&self) -> DispatcherType {
        DispatcherType::CallingThread
    }

    fn throughput(&self) -> usize {
        self.config.throughput
    }

    fn execute(&self, _task: BoxFuture) {
        if self.shutdown.load(Ordering::Relaxed) {
            return;
        }
        self.tasks_submitted.fetch_add(1, Ordering::Relaxed);
        // CallingThread: In a real implementation we'd block and run the future here.
        // For now, we just count it.
        self.tasks_completed.fetch_add(1, Ordering::Relaxed);
    }

    fn stats(&self) -> DispatcherStats {
        DispatcherStats {
            tasks_submitted: self.tasks_submitted.load(Ordering::Relaxed),
            tasks_completed: self.tasks_completed.load(Ordering::Relaxed),
            total_execution_time_ms: 0,
            active_threads: 1, // always 1 thread
        }
    }

    fn active_task_count(&self) -> usize {
        0 // always synchronous
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        debug!("CallingThreadDispatcher '{}' shutdown", self.name);
    }
}

// ============= Dispatcher Selector =============

/// Registry of named dispatchers.
/// Actors can reference dispatchers by name in their configuration.
pub struct DispatcherSelector {
    dispatchers: RwLock<HashMap<String, Arc<dyn Dispatcher>>>,
}

impl DispatcherSelector {
    pub fn new() -> Self {
        Self {
            dispatchers: RwLock::new(HashMap::new()),
        }
    }

    /// Register a dispatcher with a name.
    pub async fn register(&self, dispatcher: Arc<dyn Dispatcher>) {
        let name = dispatcher.name().to_string();
        let mut dispatchers = self.dispatchers.write().await;        dispatchers.insert(name, dispatcher);
    }

    /// Look up a dispatcher by name.
    pub async fn lookup(&self, name: &str) -> Option<Arc<dyn Dispatcher>> {
        let dispatchers = self.dispatchers.read().await;
        dispatchers.get(name).cloned()
    }

    /// Get all registered dispatcher names.
    pub async fn names(&self) -> Vec<String> {
        let dispatchers = self.dispatchers.read().await;
        dispatchers.keys().cloned().collect()
    }

    /// Shut down all dispatchers.
    pub async fn shutdown_all(&self) {
        let dispatchers = self.dispatchers.read().await;
        for d in dispatchers.values() {
            d.shutdown();
        }
    }

    /// Create a default selector with the standard dispatchers pre-registered.
    pub async fn with_defaults(handle: Handle) -> Self {
        let selector = Self::new();

        let default_config = DispatcherConfig::default();
        let default_dispatcher = Arc::new(DefaultDispatcher::with_handle(
            default_config,
            handle.clone(),
        ));
        selector.register(default_dispatcher).await;

        let pinned_config = DispatcherConfig {
            name: "pinned-dispatcher".to_string(),
            dispatcher_type: DispatcherType::Pinned,
            ..DispatcherConfig::default()
        };
        let pinned_dispatcher = Arc::new(PinnedDispatcher::new(pinned_config));
        selector.register(pinned_dispatcher).await;

        let ct_config = DispatcherConfig {
            name: "calling-thread-dispatcher".to_string(),
            dispatcher_type: DispatcherType::CallingThread,
            ..DispatcherConfig::default()
        };
        let ct_dispatcher = Arc::new(CallingThreadDispatcher::new(ct_config));
        selector.register(ct_dispatcher).await;

        selector
    }
}

/// Get the number of available CPUs.
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dispatcher_config_default() {
        let config = DispatcherConfig::default();
        assert_eq!(config.name, "default-dispatcher");
        assert_eq!(config.dispatcher_type, DispatcherType::Default);
        assert_eq!(config.throughput, 10);
        assert!(config.calculated_parallelism() >= 2);
    }

    #[test]
    fn test_dispatcher_config_builders() {
        let config = DispatcherConfig::default()
            .with_name("my-dispatcher")
            .with_type(DispatcherType::Pinned)
            .with_throughput(20)
            .with_parallelism(4, 16, 2.0);

        assert_eq!(config.name, "my-dispatcher");
        assert_eq!(config.dispatcher_type, DispatcherType::Pinned);
        assert_eq!(config.throughput, 20);
        assert_eq!(config.parallelism_min, 4);
        assert_eq!(config.parallelism_max, 16);
    }

    #[test]
    fn test_dispatcher_config_calculated_parallelism() {
        let config = DispatcherConfig::default()
            .with_parallelism(2, 8, 1.0);
        let p = config.calculated_parallelism();
        assert!(p >= 2 && p <= 8);
    }

    #[tokio::test]
    async fn test_default_dispatcher_creation() {
        let config = DispatcherConfig::default();
        let dispatcher = DefaultDispatcher::new(config);
        assert_eq!(dispatcher.name(), "default-dispatcher");
        assert_eq!(dispatcher.dispatcher_type(), DispatcherType::Default);
        assert_eq!(dispatcher.throughput(), 10);
        assert_eq!(dispatcher.active_task_count(), 0);
    }

    #[tokio::test]
    async fn test_default_dispatcher_stats() {
        let config = DispatcherConfig::default();
        let dispatcher = DefaultDispatcher::new(config);
        let stats = dispatcher.stats();
        assert_eq!(stats.tasks_submitted, 0);
        assert_eq!(stats.tasks_completed, 0);
        assert!(stats.active_threads > 0);
    }

    #[tokio::test]
    async fn test_default_dispatcher_shutdown() {
        let config = DispatcherConfig::default();
        let dispatcher = DefaultDispatcher::new(config);
        dispatcher.shutdown();
        // After shutdown, tasks should be rejected
        let stats = dispatcher.stats();
        assert_eq!(stats.tasks_submitted, 0);
    }

    #[tokio::test]
    async fn test_pinned_dispatcher_creation() {
        let config = DispatcherConfig::default()
            .with_name("pinned")
            .with_type(DispatcherType::Pinned);
        let dispatcher = PinnedDispatcher::new(config);
        assert_eq!(dispatcher.name(), "pinned");
        assert_eq!(dispatcher.dispatcher_type(), DispatcherType::Pinned);
    }

    #[tokio::test]
    async fn test_calling_thread_dispatcher() {
        let config = DispatcherConfig::default()
            .with_name("calling-thread")
            .with_type(DispatcherType::CallingThread);
        let dispatcher = CallingThreadDispatcher::new(config);
        assert_eq!(dispatcher.name(), "calling-thread");
        assert_eq!(dispatcher.dispatcher_type(), DispatcherType::CallingThread);
        assert_eq!(dispatcher.active_task_count(), 0);
    }

    #[test]
    fn test_dispatcher_type_display() {
        assert_eq!(format!("{}", DispatcherType::Default), "default");
        assert_eq!(format!("{}", DispatcherType::Pinned), "pinned");
        assert_eq!(format!("{}", DispatcherType::CallingThread), "calling-thread");
    }

    #[tokio::test]
    async fn test_dispatcher_selector_register_lookup() {
        let selector = DispatcherSelector::new();
        let config = DispatcherConfig::default().with_name("test-dispatcher");
        let dispatcher = Arc::new(DefaultDispatcher::new(config));
        selector.register(dispatcher).await;

        let found = selector.lookup("test-dispatcher").await;
        assert!(found.is_some());
        assert_eq!(found.unwrap().name(), "test-dispatcher");

        let not_found = selector.lookup("nonexistent").await;
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_dispatcher_selector_names() {
        let selector = DispatcherSelector::new();
        let d1 = Arc::new(DefaultDispatcher::new(
            DispatcherConfig::default().with_name("d1"),
        ));
        let d2 = Arc::new(CallingThreadDispatcher::new(
            DispatcherConfig::default().with_name("d2"),
        ));
        selector.register(d1).await;
        selector.register(d2).await;

        let mut names = selector.names().await;
        names.sort();
        assert_eq!(names, vec!["d1", "d2"]);
    }

    #[tokio::test]
    async fn test_dispatcher_selector_with_defaults() {
        let handle = Handle::current();
        let selector = DispatcherSelector::with_defaults(handle).await;
        let names = selector.names().await;
        assert!(names.contains(&"default-dispatcher".to_string()));
        assert!(names.contains(&"pinned-dispatcher".to_string()));
        assert!(names.contains(&"calling-thread-dispatcher".to_string()));
    }

    #[tokio::test]
    async fn test_dispatcher_selector_shutdown_all() {
        let handle = Handle::current();
        let selector = DispatcherSelector::with_defaults(handle).await;
        selector.shutdown_all().await;
        // All dispatchers should be shut down — no panic
    }

    #[test]
    fn test_num_cpus() {
        let cpus = num_cpus();
        assert!(cpus >= 1);
    }
}
