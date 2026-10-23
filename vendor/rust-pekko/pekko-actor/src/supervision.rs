//! Supervision strategies for fault-tolerant actor systems
//!
//! This module implements the "let it crash" philosophy from Erlang/OTP,
//! allowing supervisors to automatically handle child actor failures.
//!
//! ## Strategies
//!
//! - **OneForOne**: Only the failed child is restarted
//! - **OneForAll**: All children are restarted when one fails
//! - **RestForOne**: The failed child and all children started after it are restarted
//!
//! ## Example
//!
//! ```rust,ignore
//! use pekko_actor::supervision::{OneForOneStrategy, Directive, Decider};
//! use std::time::Duration;
//!
//! let strategy = OneForOneStrategy::new(
//!     3,                          // max retries
//!     Duration::from_secs(60),    // within this window
//!     |error| {
//!         if error.to_string().contains("transient") {
//!             Directive::Restart
//!         } else {
//!             Directive::Stop
//!         }
//!     },
//! );
//! ```

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Directive returned by supervisor strategy when a child fails
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Directive {
    /// Resume the actor, keeping its internal state
    /// Use when the error is recoverable without restart
    Resume,

    /// Restart the actor, clearing its internal state
    /// The actor's pre_restart and post_restart hooks will be called
    Restart,

    /// Stop the actor permanently
    /// Use for unrecoverable errors
    Stop,

    /// Escalate the failure to the parent supervisor
    /// Use when this supervisor cannot handle the error
    Escalate,
}

impl fmt::Display for Directive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Directive::Resume => write!(f, "Resume"),
            Directive::Restart => write!(f, "Restart"),
            Directive::Stop => write!(f, "Stop"),
            Directive::Escalate => write!(f, "Escalate"),
        }
    }
}

/// A function that decides what directive to apply based on the error
pub type Decider = Arc<dyn Fn(&dyn Error) -> Directive + Send + Sync>;

/// Default decider that restarts on any error
pub fn default_decider() -> Decider {
    Arc::new(|_| Directive::Restart)
}

/// Failure info passed to supervision strategy
#[derive(Debug, Clone)]
pub struct ChildFailure {
    /// Name of the failed child
    pub child_name: String,
    /// Error message
    pub error_message: String,
    /// Timestamp of the failure
    pub timestamp: Instant,
}

impl ChildFailure {
    pub fn new(child_name: impl Into<String>, error: &dyn Error) -> Self {
        Self {
            child_name: child_name.into(),
            error_message: error.to_string(),
            timestamp: Instant::now(),
        }
    }
}

/// Tracks restart attempts for rate limiting
#[derive(Debug)]
pub struct RestartStatistics {
    failures: Vec<Instant>,
    max_retries: u32,
    within_window: Duration,
}

impl RestartStatistics {
    pub fn new(max_retries: u32, within_window: Duration) -> Self {
        Self {
            failures: Vec::new(),
            max_retries,
            within_window,
        }
    }

    /// Record a failure and check if we've exceeded the retry limit
    /// Returns true if restart is allowed, false if limit exceeded
    pub fn record_failure(&mut self) -> bool {
        let now = Instant::now();
        
        // Remove old failures outside the window
        self.failures.retain(|&t| now.duration_since(t) < self.within_window);
        
        // Add current failure
        self.failures.push(now);
        
        // Check if within limit
        self.failures.len() <= self.max_retries as usize
    }

    /// Reset statistics (e.g., after successful recovery)
    pub fn reset(&mut self) {
        self.failures.clear();
    }

    /// Get current failure count within window
    pub fn failure_count(&self) -> usize {
        let now = Instant::now();
        self.failures
            .iter()
            .filter(|&&t| now.duration_since(t) < self.within_window)
            .count()
    }
}

/// Result of applying supervision strategy
#[derive(Debug, Clone)]
pub struct SupervisionResult {
    /// The directive to apply
    pub directive: Directive,
    /// Children to restart (for OneForAll/RestForOne)
    pub children_to_restart: Vec<String>,
    /// Whether retry limit was exceeded (directive may be overridden to Stop)
    pub retry_limit_exceeded: bool,
}

/// Core trait for supervision strategies
pub trait SupervisorStrategy: Send + Sync + fmt::Debug {
    /// Handle a child failure and return the supervision result
    fn handle_failure(
        &self,
        failure: &ChildFailure,
        children: &[String],
        stats: &mut RestartStatistics,
    ) -> SupervisionResult;

    /// Get the decider function
    fn decider(&self) -> &Decider;

    /// Get max retries configuration
    fn max_retries(&self) -> u32;

    /// Get the time window for retry counting
    fn within_window(&self) -> Duration;
}

/// One-for-One Strategy: Only restart the failed child
///
/// Use when children are independent and one child's failure
/// doesn't affect others.
#[derive(Clone)]
pub struct OneForOneStrategy {
    max_retries: u32,
    within_window: Duration,
    decider: Decider,
}

impl OneForOneStrategy {
    pub fn new<F>(max_retries: u32, within_window: Duration, decider: F) -> Self
    where
        F: Fn(&dyn Error) -> Directive + Send + Sync + 'static,
    {
        Self {
            max_retries,
            within_window,
            decider: Arc::new(decider),
        }
    }

    /// Create with default decider (always restart)
    pub fn with_defaults(max_retries: u32, within_window: Duration) -> Self {
        Self {
            max_retries,
            within_window,
            decider: default_decider(),
        }
    }
}

impl fmt::Debug for OneForOneStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OneForOneStrategy")
            .field("max_retries", &self.max_retries)
            .field("within_window", &self.within_window)
            .finish()
    }
}

impl SupervisorStrategy for OneForOneStrategy {
    fn handle_failure(
        &self,
        failure: &ChildFailure,
        _children: &[String],
        stats: &mut RestartStatistics,
    ) -> SupervisionResult {
        // Create a simple error wrapper for the decider
        let error = SimpleError(failure.error_message.clone());
        let mut directive = (self.decider)(&error);

        let mut retry_limit_exceeded = false;

        // Check restart limits if directive is Restart
        if directive == Directive::Restart {
            if !stats.record_failure() {
                // Exceeded retry limit, stop instead
                directive = Directive::Stop;
                retry_limit_exceeded = true;
                tracing::warn!(
                    "Child '{}' exceeded restart limit ({} within {:?}), stopping",
                    failure.child_name,
                    self.max_retries,
                    self.within_window
                );
            }
        }

        SupervisionResult {
            directive,
            children_to_restart: if directive == Directive::Restart {
                vec![failure.child_name.clone()]
            } else {
                vec![]
            },
            retry_limit_exceeded,
        }
    }

    fn decider(&self) -> &Decider {
        &self.decider
    }

    fn max_retries(&self) -> u32 {
        self.max_retries
    }

    fn within_window(&self) -> Duration {
        self.within_window
    }
}

/// One-for-All Strategy: Restart all children when one fails
///
/// Use when children are interdependent and one child's failure
/// means others are likely in an inconsistent state.
#[derive(Clone)]
pub struct OneForAllStrategy {
    max_retries: u32,
    within_window: Duration,
    decider: Decider,
}

impl OneForAllStrategy {
    pub fn new<F>(max_retries: u32, within_window: Duration, decider: F) -> Self
    where
        F: Fn(&dyn Error) -> Directive + Send + Sync + 'static,
    {
        Self {
            max_retries,
            within_window,
            decider: Arc::new(decider),
        }
    }

    pub fn with_defaults(max_retries: u32, within_window: Duration) -> Self {
        Self {
            max_retries,
            within_window,
            decider: default_decider(),
        }
    }
}

impl fmt::Debug for OneForAllStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OneForAllStrategy")
            .field("max_retries", &self.max_retries)
            .field("within_window", &self.within_window)
            .finish()
    }
}

impl SupervisorStrategy for OneForAllStrategy {
    fn handle_failure(
        &self,
        failure: &ChildFailure,
        children: &[String],
        stats: &mut RestartStatistics,
    ) -> SupervisionResult {
        let error = SimpleError(failure.error_message.clone());
        let mut directive = (self.decider)(&error);

        let mut retry_limit_exceeded = false;

        if directive == Directive::Restart {
            if !stats.record_failure() {
                directive = Directive::Stop;
                retry_limit_exceeded = true;
                tracing::warn!(
                    "Supervisor exceeded restart limit ({} within {:?}), stopping all children",
                    self.max_retries,
                    self.within_window
                );
            }
        }

        SupervisionResult {
            directive,
            children_to_restart: if directive == Directive::Restart {
                children.to_vec() // All children
            } else {
                vec![]
            },
            retry_limit_exceeded,
        }
    }

    fn decider(&self) -> &Decider {
        &self.decider
    }

    fn max_retries(&self) -> u32 {
        self.max_retries
    }

    fn within_window(&self) -> Duration {
        self.within_window
    }
}

/// Rest-for-One Strategy: Restart the failed child and all children started after it
///
/// Use when children have sequential dependencies (e.g., B depends on A,
/// C depends on B). If B fails, B and C need to restart, but A is fine.
#[derive(Clone)]
pub struct RestForOneStrategy {
    max_retries: u32,
    within_window: Duration,
    decider: Decider,
}

impl RestForOneStrategy {
    pub fn new<F>(max_retries: u32, within_window: Duration, decider: F) -> Self
    where
        F: Fn(&dyn Error) -> Directive + Send + Sync + 'static,
    {
        Self {
            max_retries,
            within_window,
            decider: Arc::new(decider),
        }
    }

    pub fn with_defaults(max_retries: u32, within_window: Duration) -> Self {
        Self {
            max_retries,
            within_window,
            decider: default_decider(),
        }
    }
}

impl fmt::Debug for RestForOneStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestForOneStrategy")
            .field("max_retries", &self.max_retries)
            .field("within_window", &self.within_window)
            .finish()
    }
}

impl SupervisorStrategy for RestForOneStrategy {
    fn handle_failure(
        &self,
        failure: &ChildFailure,
        children: &[String],
        stats: &mut RestartStatistics,
    ) -> SupervisionResult {
        let error = SimpleError(failure.error_message.clone());
        let mut directive = (self.decider)(&error);

        let mut retry_limit_exceeded = false;

        if directive == Directive::Restart {
            if !stats.record_failure() {
                directive = Directive::Stop;
                retry_limit_exceeded = true;
                tracing::warn!(
                    "Supervisor exceeded restart limit ({} within {:?}), stopping affected children",
                    self.max_retries,
                    self.within_window
                );
            }
        }

        // Find the failed child and all children after it
        let children_to_restart = if directive == Directive::Restart {
            if let Some(pos) = children.iter().position(|c| c == &failure.child_name) {
                children[pos..].to_vec()
            } else {
                vec![failure.child_name.clone()]
            }
        } else {
            vec![]
        };

        SupervisionResult {
            directive,
            children_to_restart,
            retry_limit_exceeded,
        }
    }

    fn decider(&self) -> &Decider {
        &self.decider
    }

    fn max_retries(&self) -> u32 {
        self.max_retries
    }

    fn within_window(&self) -> Duration {
        self.within_window
    }
}

/// Simple error wrapper for decider functions
#[derive(Debug)]
struct SimpleError(String);

impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for SimpleError {}

/// Manages supervision state for a supervisor actor
#[derive(Debug)]
pub struct SupervisorState {
    /// The strategy to use
    strategy: Arc<dyn SupervisorStrategy>,
    /// Restart statistics per child
    child_stats: RwLock<HashMap<String, RestartStatistics>>,
    /// Ordered list of children (for RestForOne)
    child_order: RwLock<Vec<String>>,
}

impl SupervisorState {
    pub fn new(strategy: Arc<dyn SupervisorStrategy>) -> Self {
        Self {
            strategy,
            child_stats: RwLock::new(HashMap::new()),
            child_order: RwLock::new(Vec::new()),
        }
    }

    /// Register a new child
    pub async fn register_child(&self, name: &str) {
        let mut stats = self.child_stats.write().await;
        stats.insert(
            name.to_string(),
            RestartStatistics::new(self.strategy.max_retries(), self.strategy.within_window()),
        );

        let mut order = self.child_order.write().await;
        if !order.contains(&name.to_string()) {
            order.push(name.to_string());
        }
    }

    /// Unregister a child
    pub async fn unregister_child(&self, name: &str) {
        let mut stats = self.child_stats.write().await;
        stats.remove(name);

        let mut order = self.child_order.write().await;
        order.retain(|n| n != name);
    }

    /// Handle a child failure
    pub async fn handle_child_failure(&self, failure: &ChildFailure) -> SupervisionResult {
        let mut stats = self.child_stats.write().await;
        let order = self.child_order.read().await;

        let child_stats = stats
            .entry(failure.child_name.clone())
            .or_insert_with(|| {
                RestartStatistics::new(self.strategy.max_retries(), self.strategy.within_window())
            });

        self.strategy.handle_failure(failure, &order, child_stats)
    }

    /// Reset statistics for a child (after successful recovery)
    pub async fn reset_child_stats(&self, name: &str) {
        let mut stats = self.child_stats.write().await;
        if let Some(child_stats) = stats.get_mut(name) {
            child_stats.reset();
        }
    }

    /// Get the strategy
    pub fn strategy(&self) -> &Arc<dyn SupervisorStrategy> {
        &self.strategy
    }
}

// ============================================================
// Backoff Supervisor
// ============================================================

/// Backoff configuration for exponential restart delays
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial delay before first restart
    pub min_backoff: Duration,
    /// Maximum delay between restarts
    pub max_backoff: Duration,
    /// Multiplier for exponential growth (typically 2.0)
    pub multiplier: f64,
    /// Random jitter factor (0.0 to 1.0) to prevent thundering herd
    pub jitter: f64,
    /// Time after which backoff resets if no failures occur
    pub reset_after: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: 0.2,
            reset_after: Duration::from_secs(60),
        }
    }
}

impl BackoffConfig {
    pub fn new(min_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            min_backoff,
            max_backoff,
            ..Default::default()
        }
    }

    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    pub fn with_jitter(mut self, jitter: f64) -> Self {
        self.jitter = jitter.clamp(0.0, 1.0);
        self
    }

    pub fn with_reset_after(mut self, reset_after: Duration) -> Self {
        self.reset_after = reset_after;
        self
    }
}

/// Tracks backoff state for a single actor
#[derive(Debug)]
pub struct BackoffState {
    config: BackoffConfig,
    current_delay: Duration,
    last_failure: Option<Instant>,
    restart_count: u32,
}

impl BackoffState {
    pub fn new(config: BackoffConfig) -> Self {
        let initial_delay = config.min_backoff;
        Self {
            config,
            current_delay: initial_delay,
            last_failure: None,
            restart_count: 0,
        }
    }

    /// Calculate the next backoff delay and update state
    /// Returns the delay to wait before restarting
    pub fn next_backoff(&mut self) -> Duration {
        let now = Instant::now();

        // Check if we should reset backoff
        if let Some(last) = self.last_failure {
            if now.duration_since(last) > self.config.reset_after {
                self.reset();
            }
        }

        self.last_failure = Some(now);
        self.restart_count += 1;

        // Calculate delay with jitter
        let delay = self.apply_jitter(self.current_delay);

        // Increase delay for next time (exponential backoff)
        let next_delay_ms = (self.current_delay.as_millis() as f64 * self.config.multiplier) as u64;
        self.current_delay = Duration::from_millis(next_delay_ms).min(self.config.max_backoff);

        tracing::debug!(
            "Backoff: restart #{}, delay {:?} (next will be {:?})",
            self.restart_count,
            delay,
            self.current_delay
        );

        delay
    }

    /// Apply random jitter to prevent thundering herd
    fn apply_jitter(&self, delay: Duration) -> Duration {
        if self.config.jitter == 0.0 {
            return delay;
        }

        // Simple pseudo-random based on current time nanoseconds
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        
        let random_factor = (nanos % 1000) as f64 / 1000.0; // 0.0 to 1.0
        let jitter_range = delay.as_millis() as f64 * self.config.jitter;
        let jitter = (random_factor - 0.5) * 2.0 * jitter_range; // -jitter to +jitter

        let adjusted_ms = (delay.as_millis() as f64 + jitter).max(1.0) as u64;
        Duration::from_millis(adjusted_ms)
    }

    /// Reset backoff state (call after successful recovery)
    pub fn reset(&mut self) {
        self.current_delay = self.config.min_backoff;
        self.restart_count = 0;
        self.last_failure = None;
        tracing::debug!("Backoff reset");
    }

    /// Get current restart count
    pub fn restart_count(&self) -> u32 {
        self.restart_count
    }

    /// Get the current delay (without jitter)
    pub fn current_delay(&self) -> Duration {
        self.current_delay
    }

    /// Check if backoff should be reset based on time elapsed
    pub fn should_reset(&self) -> bool {
        self.last_failure
            .map(|last| Instant::now().duration_since(last) > self.config.reset_after)
            .unwrap_or(true)
    }
}

/// Backoff supervisor strategy that wraps another strategy
/// and adds exponential backoff delays between restarts
#[derive(Clone)]
pub struct BackoffSupervisorStrategy {
    /// The underlying strategy for deciding what to do
    inner: Arc<dyn SupervisorStrategy>,
    /// Backoff configuration
    config: BackoffConfig,
}

impl BackoffSupervisorStrategy {
    pub fn new(inner: Arc<dyn SupervisorStrategy>, config: BackoffConfig) -> Self {
        Self { inner, config }
    }

    /// Create with default backoff config
    pub fn with_defaults(inner: Arc<dyn SupervisorStrategy>) -> Self {
        Self {
            inner,
            config: BackoffConfig::default(),
        }
    }

    /// Get the backoff config
    pub fn config(&self) -> &BackoffConfig {
        &self.config
    }
}

impl fmt::Debug for BackoffSupervisorStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackoffSupervisorStrategy")
            .field("inner", &self.inner)
            .field("config", &self.config)
            .finish()
    }
}

impl SupervisorStrategy for BackoffSupervisorStrategy {
    fn handle_failure(
        &self,
        failure: &ChildFailure,
        children: &[String],
        stats: &mut RestartStatistics,
    ) -> SupervisionResult {
        // Delegate to inner strategy
        self.inner.handle_failure(failure, children, stats)
    }

    fn decider(&self) -> &Decider {
        self.inner.decider()
    }

    fn max_retries(&self) -> u32 {
        self.inner.max_retries()
    }

    fn within_window(&self) -> Duration {
        self.inner.within_window()
    }
}

/// Extended supervisor state that includes backoff tracking
#[derive(Debug)]
pub struct BackoffSupervisorState {
    /// Base supervisor state
    base: SupervisorState,
    /// Backoff state per child
    backoff_states: RwLock<HashMap<String, BackoffState>>,
    /// Backoff configuration
    config: BackoffConfig,
}

impl BackoffSupervisorState {
    pub fn new(strategy: Arc<dyn SupervisorStrategy>, config: BackoffConfig) -> Self {
        Self {
            base: SupervisorState::new(strategy),
            backoff_states: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Create with default backoff config
    pub fn with_defaults(strategy: Arc<dyn SupervisorStrategy>) -> Self {
        Self::new(strategy, BackoffConfig::default())
    }

    /// Register a new child
    pub async fn register_child(&self, name: &str) {
        self.base.register_child(name).await;
        
        let mut states = self.backoff_states.write().await;
        states.insert(name.to_string(), BackoffState::new(self.config.clone()));
    }

    /// Unregister a child
    pub async fn unregister_child(&self, name: &str) {
        self.base.unregister_child(name).await;
        
        let mut states = self.backoff_states.write().await;
        states.remove(name);
    }

    /// Handle a child failure with backoff
    /// Returns (SupervisionResult, backoff_delay)
    pub async fn handle_child_failure_with_backoff(
        &self,
        failure: &ChildFailure,
    ) -> (SupervisionResult, Option<Duration>) {
        let result = self.base.handle_child_failure(failure).await;

        // Only apply backoff for restarts
        let backoff_delay = if result.directive == Directive::Restart {
            let mut states = self.backoff_states.write().await;
            let backoff_state = states
                .entry(failure.child_name.clone())
                .or_insert_with(|| BackoffState::new(self.config.clone()));
            
            Some(backoff_state.next_backoff())
        } else {
            None
        };

        (result, backoff_delay)
    }

    /// Notify that a child has recovered successfully
    pub async fn notify_child_recovered(&self, child_name: &str) {
        self.base.reset_child_stats(child_name).await;
        
        let mut states = self.backoff_states.write().await;
        if let Some(state) = states.get_mut(child_name) {
            state.reset();
        }
    }

    /// Get the current backoff delay for a child (without incrementing)
    pub async fn get_current_backoff(&self, child_name: &str) -> Option<Duration> {
        let states = self.backoff_states.read().await;
        states.get(child_name).map(|s| s.current_delay())
    }

    /// Get restart count for a child
    pub async fn get_restart_count(&self, child_name: &str) -> Option<u32> {
        let states = self.backoff_states.read().await;
        states.get(child_name).map(|s| s.restart_count())
    }

    /// Get the underlying supervisor state
    pub fn base(&self) -> &SupervisorState {
        &self.base
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_directive_display() {
        assert_eq!(format!("{}", Directive::Resume), "Resume");
        assert_eq!(format!("{}", Directive::Restart), "Restart");
        assert_eq!(format!("{}", Directive::Stop), "Stop");
        assert_eq!(format!("{}", Directive::Escalate), "Escalate");
    }

    #[test]
    fn test_restart_statistics() {
        let mut stats = RestartStatistics::new(3, Duration::from_secs(60));

        // First 3 failures should be allowed
        assert!(stats.record_failure());
        assert!(stats.record_failure());
        assert!(stats.record_failure());

        // 4th failure should exceed limit
        assert!(!stats.record_failure());

        // Reset should allow failures again
        stats.reset();
        assert!(stats.record_failure());
    }

    #[test]
    fn test_one_for_one_strategy() {
        let strategy = OneForOneStrategy::with_defaults(3, Duration::from_secs(60));
        let mut stats = RestartStatistics::new(3, Duration::from_secs(60));

        let failure = ChildFailure {
            child_name: "child1".to_string(),
            error_message: "test error".to_string(),
            timestamp: Instant::now(),
        };

        let children = vec!["child1".to_string(), "child2".to_string(), "child3".to_string()];

        let result = strategy.handle_failure(&failure, &children, &mut stats);

        assert_eq!(result.directive, Directive::Restart);
        assert_eq!(result.children_to_restart, vec!["child1".to_string()]);
        assert!(!result.retry_limit_exceeded);
    }

    #[test]
    fn test_one_for_all_strategy() {
        let strategy = OneForAllStrategy::with_defaults(3, Duration::from_secs(60));
        let mut stats = RestartStatistics::new(3, Duration::from_secs(60));

        let failure = ChildFailure {
            child_name: "child1".to_string(),
            error_message: "test error".to_string(),
            timestamp: Instant::now(),
        };

        let children = vec!["child1".to_string(), "child2".to_string(), "child3".to_string()];

        let result = strategy.handle_failure(&failure, &children, &mut stats);

        assert_eq!(result.directive, Directive::Restart);
        assert_eq!(result.children_to_restart, children);
        assert!(!result.retry_limit_exceeded);
    }

    #[test]
    fn test_rest_for_one_strategy() {
        let strategy = RestForOneStrategy::with_defaults(3, Duration::from_secs(60));
        let mut stats = RestartStatistics::new(3, Duration::from_secs(60));

        let failure = ChildFailure {
            child_name: "child2".to_string(),
            error_message: "test error".to_string(),
            timestamp: Instant::now(),
        };

        let children = vec!["child1".to_string(), "child2".to_string(), "child3".to_string()];

        let result = strategy.handle_failure(&failure, &children, &mut stats);

        assert_eq!(result.directive, Directive::Restart);
        // child2 and child3 (after child2) should restart
        assert_eq!(
            result.children_to_restart,
            vec!["child2".to_string(), "child3".to_string()]
        );
        assert!(!result.retry_limit_exceeded);
    }

    #[test]
    fn test_custom_decider() {
        let strategy = OneForOneStrategy::new(3, Duration::from_secs(60), |error| {
            if error.to_string().contains("fatal") {
                Directive::Stop
            } else {
                Directive::Resume
            }
        });

        let mut stats = RestartStatistics::new(3, Duration::from_secs(60));
        let children = vec!["child1".to_string()];

        // Non-fatal error -> Resume
        let failure = ChildFailure {
            child_name: "child1".to_string(),
            error_message: "transient error".to_string(),
            timestamp: Instant::now(),
        };
        let result = strategy.handle_failure(&failure, &children, &mut stats);
        assert_eq!(result.directive, Directive::Resume);

        // Fatal error -> Stop
        let failure = ChildFailure {
            child_name: "child1".to_string(),
            error_message: "fatal error".to_string(),
            timestamp: Instant::now(),
        };
        let result = strategy.handle_failure(&failure, &children, &mut stats);
        assert_eq!(result.directive, Directive::Stop);
    }

    // ============================================================
    // Backoff Tests
    // ============================================================

    #[test]
    fn test_backoff_config_default() {
        let config = BackoffConfig::default();
        assert_eq!(config.min_backoff, Duration::from_secs(1));
        assert_eq!(config.max_backoff, Duration::from_secs(30));
        assert_eq!(config.multiplier, 2.0);
        assert_eq!(config.jitter, 0.2);
        assert_eq!(config.reset_after, Duration::from_secs(60));
    }

    #[test]
    fn test_backoff_config_builder() {
        let config = BackoffConfig::new(
            Duration::from_millis(500),
            Duration::from_secs(60),
        )
        .with_multiplier(3.0)
        .with_jitter(0.1)
        .with_reset_after(Duration::from_secs(120));

        assert_eq!(config.min_backoff, Duration::from_millis(500));
        assert_eq!(config.max_backoff, Duration::from_secs(60));
        assert_eq!(config.multiplier, 3.0);
        assert_eq!(config.jitter, 0.1);
        assert_eq!(config.reset_after, Duration::from_secs(120));
    }

    #[test]
    fn test_backoff_state_exponential_growth() {
        let config = BackoffConfig::new(
            Duration::from_secs(1),
            Duration::from_secs(30),
        )
        .with_jitter(0.0); // Disable jitter for predictable test

        let mut state = BackoffState::new(config);

        // First backoff: 1s
        let delay1 = state.next_backoff();
        assert_eq!(delay1, Duration::from_secs(1));
        assert_eq!(state.restart_count(), 1);

        // Second backoff: 2s
        let delay2 = state.next_backoff();
        assert_eq!(delay2, Duration::from_secs(2));
        assert_eq!(state.restart_count(), 2);

        // Third backoff: 4s
        let delay3 = state.next_backoff();
        assert_eq!(delay3, Duration::from_secs(4));
        assert_eq!(state.restart_count(), 3);

        // Fourth backoff: 8s
        let delay4 = state.next_backoff();
        assert_eq!(delay4, Duration::from_secs(8));
        assert_eq!(state.restart_count(), 4);
    }

    #[test]
    fn test_backoff_state_respects_max() {
        let config = BackoffConfig::new(
            Duration::from_secs(1),
            Duration::from_secs(5), // Low max for testing
        )
        .with_jitter(0.0);

        let mut state = BackoffState::new(config);

        // 1s -> 2s -> 4s -> should cap at 5s
        state.next_backoff(); // 1s
        state.next_backoff(); // 2s
        state.next_backoff(); // 4s
        
        let delay = state.next_backoff();
        assert_eq!(delay, Duration::from_secs(5)); // Capped at max

        let delay = state.next_backoff();
        assert_eq!(delay, Duration::from_secs(5)); // Still capped
    }

    #[test]
    fn test_backoff_state_reset() {
        let config = BackoffConfig::new(
            Duration::from_secs(1),
            Duration::from_secs(30),
        )
        .with_jitter(0.0);

        let mut state = BackoffState::new(config);

        // Build up some backoff
        state.next_backoff(); // 1s
        state.next_backoff(); // 2s
        state.next_backoff(); // 4s
        assert_eq!(state.restart_count(), 3);
        assert_eq!(state.current_delay(), Duration::from_secs(8));

        // Reset
        state.reset();

        // Should be back to initial state
        assert_eq!(state.restart_count(), 0);
        assert_eq!(state.current_delay(), Duration::from_secs(1));

        // Next backoff should be 1s again
        let delay = state.next_backoff();
        assert_eq!(delay, Duration::from_secs(1));
    }

    #[test]
    fn test_backoff_jitter_bounds() {
        let config = BackoffConfig::new(
            Duration::from_secs(10),
            Duration::from_secs(100),
        )
        .with_jitter(0.2); // 20% jitter

        let mut state = BackoffState::new(config);

        // Run multiple times and check bounds
        for _ in 0..10 {
            state.reset();
            let delay = state.next_backoff();
            
            // With 20% jitter on 10s: should be between 8s and 12s
            let min_expected = Duration::from_secs(8);
            let max_expected = Duration::from_secs(12);
            
            assert!(
                delay >= min_expected && delay <= max_expected,
                "Delay {:?} outside expected range [{:?}, {:?}]",
                delay, min_expected, max_expected
            );
        }
    }

    #[tokio::test]
    async fn test_backoff_supervisor_state_integration() {
        let strategy = Arc::new(OneForOneStrategy::with_defaults(10, Duration::from_secs(60)));
        let config = BackoffConfig::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
        )
        .with_jitter(0.0);

        let supervisor = BackoffSupervisorState::new(strategy, config);
        supervisor.register_child("worker").await;

        let failure = ChildFailure {
            child_name: "worker".to_string(),
            error_message: "error".to_string(),
            timestamp: Instant::now(),
        };

        // First failure: 100ms backoff
        let (result1, backoff1) = supervisor.handle_child_failure_with_backoff(&failure).await;
        assert_eq!(result1.directive, Directive::Restart);
        assert_eq!(backoff1, Some(Duration::from_millis(100)));

        // Second failure: 200ms backoff
        let (result2, backoff2) = supervisor.handle_child_failure_with_backoff(&failure).await;
        assert_eq!(result2.directive, Directive::Restart);
        assert_eq!(backoff2, Some(Duration::from_millis(200)));

        // Third failure: 400ms backoff
        let (result3, backoff3) = supervisor.handle_child_failure_with_backoff(&failure).await;
        assert_eq!(result3.directive, Directive::Restart);
        assert_eq!(backoff3, Some(Duration::from_millis(400)));

        // Verify restart count
        assert_eq!(supervisor.get_restart_count("worker").await, Some(3));
    }

    #[tokio::test]
    async fn test_backoff_supervisor_recovery_reset() {
        let strategy = Arc::new(OneForOneStrategy::with_defaults(10, Duration::from_secs(60)));
        let config = BackoffConfig::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
        )
        .with_jitter(0.0);

        let supervisor = BackoffSupervisorState::new(strategy, config);
        supervisor.register_child("worker").await;

        let failure = ChildFailure {
            child_name: "worker".to_string(),
            error_message: "error".to_string(),
            timestamp: Instant::now(),
        };

        // Build up backoff
        supervisor.handle_child_failure_with_backoff(&failure).await;
        supervisor.handle_child_failure_with_backoff(&failure).await;
        supervisor.handle_child_failure_with_backoff(&failure).await;

        // Current delay should be 800ms (next would be this)
        assert_eq!(
            supervisor.get_current_backoff("worker").await,
            Some(Duration::from_millis(800))
        );

        // Notify recovery
        supervisor.notify_child_recovered("worker").await;

        // Should be reset to initial
        assert_eq!(
            supervisor.get_current_backoff("worker").await,
            Some(Duration::from_millis(100))
        );
        assert_eq!(supervisor.get_restart_count("worker").await, Some(0));
    }

    #[tokio::test]
    async fn test_backoff_no_delay_for_non_restart_directives() {
        let strategy = Arc::new(OneForOneStrategy::new(
            10,
            Duration::from_secs(60),
            |error| {
                if error.to_string().contains("resume") {
                    Directive::Resume
                } else if error.to_string().contains("stop") {
                    Directive::Stop
                } else {
                    Directive::Restart
                }
            },
        ));

        let supervisor = BackoffSupervisorState::with_defaults(strategy);
        supervisor.register_child("worker").await;

        // Resume directive - no backoff
        let resume_failure = ChildFailure {
            child_name: "worker".to_string(),
            error_message: "resume this".to_string(),
            timestamp: Instant::now(),
        };
        let (result, backoff) = supervisor.handle_child_failure_with_backoff(&resume_failure).await;
        assert_eq!(result.directive, Directive::Resume);
        assert_eq!(backoff, None);

        // Stop directive - no backoff
        let stop_failure = ChildFailure {
            child_name: "worker".to_string(),
            error_message: "stop this".to_string(),
            timestamp: Instant::now(),
        };
        let (result, backoff) = supervisor.handle_child_failure_with_backoff(&stop_failure).await;
        assert_eq!(result.directive, Directive::Stop);
        assert_eq!(backoff, None);
    }
}
