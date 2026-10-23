//! # Graceful Stop
//!
//! Pattern for gracefully stopping individual actors with timeout guarantees.
//! Simpler alternative to CoordinatedShutdown for per-actor cleanup.
//!
//! Provides:
//! - Graceful stop with timeout
//! - Pre-stop hooks for cleanup
//! - Stop result tracking
//! - Cascading stop for actor hierarchies

use std::fmt;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Result of a graceful stop operation
#[derive(Debug, Clone, PartialEq)]
pub enum GracefulStopResult {
    /// Actor stopped successfully
    Stopped { duration: Duration },
    /// Stop timed out
    TimedOut { timeout: Duration },
    /// Actor was already stopped
    AlreadyStopped,
    /// Stop failed with error
    Failed(String),
}

impl GracefulStopResult {
    pub fn is_success(&self) -> bool {
        matches!(self, GracefulStopResult::Stopped { .. } | GracefulStopResult::AlreadyStopped)
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, GracefulStopResult::TimedOut { .. })
    }
}

impl fmt::Display for GracefulStopResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GracefulStopResult::Stopped { duration } => write!(f, "Stopped in {:?}", duration),
            GracefulStopResult::TimedOut { timeout } => write!(f, "Timed out after {:?}", timeout),
            GracefulStopResult::AlreadyStopped => write!(f, "Already stopped"),
            GracefulStopResult::Failed(msg) => write!(f, "Failed: {}", msg),
        }
    }
}

/// Stop message that can be sent to an actor
#[derive(Debug, Clone)]
pub enum StopMessage {
    /// Normal stop request
    GracefulStop,
    /// Stop with a reason
    StopWithReason(String),
    /// Stop with a custom message for cleanup
    StopAfterProcessing(Vec<u8>),
}

/// Configuration for graceful stop
#[derive(Debug, Clone)]
pub struct GracefulStopConfig {
    /// Maximum time to wait for stop
    pub timeout: Duration,
    /// Whether to stop children first
    pub stop_children_first: bool,
    /// Whether to process remaining messages before stopping
    pub drain_messages: bool,
    /// Custom stop message to send
    pub stop_message: Option<StopMessage>,
}

impl Default for GracefulStopConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            stop_children_first: true,
            drain_messages: false,
            stop_message: None,
        }
    }
}

impl GracefulStopConfig {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            ..Default::default()
        }
    }

    pub fn with_children_first(mut self, yes: bool) -> Self {
        self.stop_children_first = yes;
        self
    }

    pub fn with_drain_messages(mut self, yes: bool) -> Self {
        self.drain_messages = yes;
        self
    }

    pub fn with_stop_message(mut self, msg: StopMessage) -> Self {
        self.stop_message = Some(msg);
        self
    }
}

// ---------------------------------------------------------------------------
// Pre-stop hook
// ---------------------------------------------------------------------------

/// A hook that runs before the actor stops
pub struct PreStopHook {
    name: String,
    action: Box<dyn Fn() -> Result<(), String> + Send + Sync>,
}

impl PreStopHook {
    pub fn new<F>(name: impl Into<String>, action: F) -> Self
    where
        F: Fn() -> Result<(), String> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            action: Box::new(action),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn execute(&self) -> Result<(), String> {
        (self.action)()
    }
}

// ---------------------------------------------------------------------------
// GracefulStopper
// ---------------------------------------------------------------------------

/// Manages graceful stop operations for actors
pub struct GracefulStopper {
    config: GracefulStopConfig,
    pre_stop_hooks: Vec<PreStopHook>,
    results: Arc<Mutex<Vec<(String, GracefulStopResult)>>>,
    is_stopping: Arc<Mutex<bool>>,
}

impl GracefulStopper {
    pub fn new(config: GracefulStopConfig) -> Self {
        Self {
            config,
            pre_stop_hooks: Vec::new(),
            results: Arc::new(Mutex::new(Vec::new())),
            is_stopping: Arc::new(Mutex::new(false)),
        }
    }

    pub fn with_default() -> Self {
        Self::new(GracefulStopConfig::default())
    }

    /// Add a pre-stop hook
    pub fn add_hook(&mut self, hook: PreStopHook) {
        self.pre_stop_hooks.push(hook);
    }

    /// Execute graceful stop
    pub fn stop(&self, actor_name: &str) -> GracefulStopResult {
        // Check if already stopping
        {
            let mut stopping = self.is_stopping.lock().unwrap();
            if *stopping {
                return GracefulStopResult::AlreadyStopped;
            }
            *stopping = true;
        }

        let start = Instant::now();
        let timeout = self.config.timeout;

        // Execute pre-stop hooks
        for hook in &self.pre_stop_hooks {
            if start.elapsed() > timeout {
                let result = GracefulStopResult::TimedOut { timeout };
                self.record_result(actor_name, result.clone());
                return result;
            }

            if let Err(e) = hook.execute() {
                let result = GracefulStopResult::Failed(
                    format!("Hook '{}' failed: {}", hook.name(), e),
                );
                self.record_result(actor_name, result.clone());
                return result;
            }
        }

        let elapsed = start.elapsed();
        if elapsed > timeout {
            let result = GracefulStopResult::TimedOut { timeout };
            self.record_result(actor_name, result.clone());
            return result;
        }

        let result = GracefulStopResult::Stopped { duration: elapsed };
        self.record_result(actor_name, result.clone());

        // Reset stopping flag
        *self.is_stopping.lock().unwrap() = false;

        result
    }

    /// Get stop results
    pub fn results(&self) -> Vec<(String, GracefulStopResult)> {
        self.results.lock().unwrap().clone()
    }

    /// Check if currently stopping
    pub fn is_stopping(&self) -> bool {
        *self.is_stopping.lock().unwrap()
    }

    /// Get config
    pub fn config(&self) -> &GracefulStopConfig {
        &self.config
    }

    fn record_result(&self, name: &str, result: GracefulStopResult) {
        self.results.lock().unwrap().push((name.to_string(), result));
    }
}

// ---------------------------------------------------------------------------
// CascadingStop — stop actor hierarchy
// ---------------------------------------------------------------------------

/// Manages cascading stops for actor hierarchies
pub struct CascadingStop {
    config: GracefulStopConfig,
    /// Actor names in stop order (leaves first)
    stop_order: Vec<String>,
    results: Vec<(String, GracefulStopResult)>,
}

impl CascadingStop {
    pub fn new(config: GracefulStopConfig) -> Self {
        Self {
            config,
            stop_order: Vec::new(),
            results: Vec::new(),
        }
    }

    /// Add an actor to the stop order
    pub fn add_actor(&mut self, name: impl Into<String>) {
        self.stop_order.push(name.into());
    }

    /// Execute cascading stop (stops in reverse order — children first)
    pub fn execute(&mut self) -> &[(String, GracefulStopResult)] {
        let start = Instant::now();
        let total_timeout = self.config.timeout;

        // Per-actor timeout = total / count
        let per_actor_timeout = if self.stop_order.is_empty() {
            total_timeout
        } else {
            total_timeout / self.stop_order.len() as u32
        };

        // Stop in reverse order (children first)
        let order: Vec<String> = self.stop_order.iter().rev().cloned().collect();

        for name in &order {
            if start.elapsed() > total_timeout {
                self.results.push((
                    name.clone(),
                    GracefulStopResult::TimedOut { timeout: total_timeout },
                ));
                continue;
            }

            let actor_start = Instant::now();

            // Simulate stop (in real impl, would send stop message)
            let result = if actor_start.elapsed() > per_actor_timeout {
                GracefulStopResult::TimedOut { timeout: per_actor_timeout }
            } else {
                GracefulStopResult::Stopped { duration: actor_start.elapsed() }
            };

            self.results.push((name.clone(), result));
        }

        &self.results
    }

    /// Get results
    pub fn results(&self) -> &[(String, GracefulStopResult)] {
        &self.results
    }

    /// Check if all stopped successfully
    pub fn all_stopped(&self) -> bool {
        self.results.iter().all(|(_, r)| r.is_success())
    }

    /// Get failed stops
    pub fn failures(&self) -> Vec<&(String, GracefulStopResult)> {
        self.results.iter().filter(|(_, r)| !r.is_success()).collect()
    }
}

// ---------------------------------------------------------------------------
// Convenience function
// ---------------------------------------------------------------------------

/// Gracefully stop with default configuration
pub fn graceful_stop(actor_name: &str, timeout: Duration) -> GracefulStopResult {
    let stopper = GracefulStopper::new(GracefulStopConfig::new(timeout));
    stopper.stop(actor_name)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graceful_stop_basic() {
        let result = graceful_stop("actor-1", Duration::from_secs(5));
        assert!(result.is_success());
        assert!(matches!(result, GracefulStopResult::Stopped { .. }));
    }

    #[test]
    fn test_stop_result_display() {
        let r = GracefulStopResult::Stopped { duration: Duration::from_millis(50) };
        assert!(r.to_string().contains("Stopped"));

        let r = GracefulStopResult::TimedOut { timeout: Duration::from_secs(5) };
        assert!(r.to_string().contains("Timed out"));

        let r = GracefulStopResult::AlreadyStopped;
        assert_eq!(r.to_string(), "Already stopped");

        let r = GracefulStopResult::Failed("oops".into());
        assert!(r.to_string().contains("oops"));
    }

    #[test]
    fn test_stop_result_predicates() {
        assert!(GracefulStopResult::Stopped { duration: Duration::ZERO }.is_success());
        assert!(GracefulStopResult::AlreadyStopped.is_success());
        assert!(!GracefulStopResult::TimedOut { timeout: Duration::ZERO }.is_success());
        assert!(GracefulStopResult::TimedOut { timeout: Duration::ZERO }.is_timeout());
    }

    #[test]
    fn test_pre_stop_hooks() {
        let mut stopper = GracefulStopper::with_default();
        stopper.add_hook(PreStopHook::new("cleanup", || Ok(())));
        stopper.add_hook(PreStopHook::new("flush", || Ok(())));

        let result = stopper.stop("actor-1");
        assert!(result.is_success());
    }

    #[test]
    fn test_pre_stop_hook_failure() {
        let mut stopper = GracefulStopper::with_default();
        stopper.add_hook(PreStopHook::new("bad-hook", || Err("cleanup failed".into())));

        let result = stopper.stop("actor-1");
        assert!(matches!(result, GracefulStopResult::Failed(_)));
    }

    #[test]
    fn test_results_tracking() {
        let stopper = GracefulStopper::with_default();
        stopper.stop("a1");
        stopper.stop("a2");

        let results = stopper.results();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "a1");
        assert_eq!(results[1].0, "a2");
    }

    #[test]
    fn test_config_builder() {
        let config = GracefulStopConfig::new(Duration::from_secs(10))
            .with_children_first(true)
            .with_drain_messages(true)
            .with_stop_message(StopMessage::GracefulStop);

        assert_eq!(config.timeout, Duration::from_secs(10));
        assert!(config.stop_children_first);
        assert!(config.drain_messages);
        assert!(config.stop_message.is_some());
    }

    #[test]
    fn test_config_defaults() {
        let config = GracefulStopConfig::default();
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert!(config.stop_children_first);
        assert!(!config.drain_messages);
        assert!(config.stop_message.is_none());
    }

    #[test]
    fn test_cascading_stop() {
        let config = GracefulStopConfig::new(Duration::from_secs(10));
        let mut cascade = CascadingStop::new(config);

        cascade.add_actor("parent");
        cascade.add_actor("child-1");
        cascade.add_actor("child-2");

        cascade.execute();
        assert!(cascade.all_stopped());
        assert_eq!(cascade.results().len(), 3);

        // Children should be stopped first (reverse order)
        assert_eq!(cascade.results()[0].0, "child-2");
        assert_eq!(cascade.results()[1].0, "child-1");
        assert_eq!(cascade.results()[2].0, "parent");
    }

    #[test]
    fn test_cascading_stop_failures() {
        let cascade = CascadingStop::new(GracefulStopConfig::default());
        assert!(cascade.all_stopped()); // No actors = all stopped
        assert!(cascade.failures().is_empty());
    }

    #[test]
    fn test_stop_message_variants() {
        let _m1 = StopMessage::GracefulStop;
        let _m2 = StopMessage::StopWithReason("shutdown".into());
        let _m3 = StopMessage::StopAfterProcessing(vec![1, 2, 3]);
    }

    #[test]
    fn test_hook_name() {
        let hook = PreStopHook::new("my-hook", || Ok(()));
        assert_eq!(hook.name(), "my-hook");
    }

    #[test]
    fn test_hook_execute() {
        let hook = PreStopHook::new("ok", || Ok(()));
        assert!(hook.execute().is_ok());

        let hook = PreStopHook::new("fail", || Err("boom".into()));
        assert_eq!(hook.execute(), Err("boom".into()));
    }

    #[test]
    fn test_stopper_is_stopping() {
        let stopper = GracefulStopper::with_default();
        assert!(!stopper.is_stopping());
    }

    #[test]
    fn test_stopper_config() {
        let config = GracefulStopConfig::new(Duration::from_secs(30));
        let stopper = GracefulStopper::new(config);
        assert_eq!(stopper.config().timeout, Duration::from_secs(30));
    }
}
