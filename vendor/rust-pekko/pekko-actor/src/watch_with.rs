// watch_with.rs - WatchWith custom termination messages and DeathPact
// Extends death_watch with custom termination message mapping and DeathPact exception handling

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// DeathPactException
// ============================================================================

/// Exception thrown when a watched actor terminates and the watcher
/// did not handle the Terminated message (DeathPact violation).
#[derive(Debug, Clone)]
pub struct DeathPactException {
    /// Path of the terminated actor
    pub terminated_actor: String,
    /// Path of the watcher that didn't handle termination
    pub watcher_actor: String,
    /// When the termination was detected
    pub detected_at: Instant,
    /// Optional reason for termination
    pub reason: Option<String>,
    /// Whether the pact was explicitly configured
    pub explicit_pact: bool,
}

impl DeathPactException {
    pub fn new(terminated: &str, watcher: &str) -> Self {
        Self {
            terminated_actor: terminated.to_string(),
            watcher_actor: watcher.to_string(),
            detected_at: Instant::now(),
            reason: None,
            explicit_pact: false,
        }
    }

    pub fn with_reason(mut self, reason: &str) -> Self {
        self.reason = Some(reason.to_string());
        self
    }

    pub fn with_explicit_pact(mut self) -> Self {
        self.explicit_pact = true;
        self
    }
}

impl fmt::Display for DeathPactException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DeathPactException: watched actor '{}' terminated, watcher '{}' did not handle Terminated",
            self.terminated_actor, self.watcher_actor
        )?;
        if let Some(ref reason) = self.reason {
            write!(f, " (reason: {})", reason)?;
        }
        Ok(())
    }
}

impl std::error::Error for DeathPactException {}

// ============================================================================
// WatchWith - Custom termination message mapping
// ============================================================================

/// Configuration for watching an actor with a custom message.
/// Instead of receiving the standard Terminated message, the watcher
/// receives a custom message when the watched actor terminates.
#[derive(Clone)]
pub struct WatchWithConfig<M: Clone> {
    /// The target actor path being watched
    pub target: String,
    /// The watcher actor path
    pub watcher: String,
    /// Custom message to deliver on termination
    pub message: M,
    /// Whether to include failure info in the message
    pub include_failure_info: bool,
    /// Whether this watch should survive restarts
    pub survive_restart: bool,
}

impl<M: Clone + fmt::Debug> fmt::Debug for WatchWithConfig<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WatchWithConfig")
            .field("target", &self.target)
            .field("watcher", &self.watcher)
            .field("message", &self.message)
            .field("include_failure_info", &self.include_failure_info)
            .field("survive_restart", &self.survive_restart)
            .finish()
    }
}

/// Manages WatchWith registrations for an actor system.
/// Maps (watcher, target) pairs to custom messages.
pub struct WatchWithRegistry<M: Clone> {
    /// Map from (watcher_path, target_path) -> custom message
    watches: HashMap<(String, String), WatchWithEntry<M>>,
    /// Statistics
    stats: WatchWithStats,
}

#[derive(Clone)]
struct WatchWithEntry<M: Clone> {
    config: WatchWithConfig<M>,
    registered_at: Instant,
}

/// Statistics for the WatchWith registry
#[derive(Debug, Clone, Default)]
pub struct WatchWithStats {
    pub total_registered: u64,
    pub total_unregistered: u64,
    pub total_triggered: u64,
    pub total_death_pacts: u64,
    pub active_watches: usize,
}

impl<M: Clone> WatchWithRegistry<M> {
    pub fn new() -> Self {
        Self {
            watches: HashMap::new(),
            stats: WatchWithStats::default(),
        }
    }

    /// Register a WatchWith — when target terminates, watcher receives the custom message
    pub fn watch_with(&mut self, config: WatchWithConfig<M>) {
        let key = (config.watcher.clone(), config.target.clone());
        self.watches.insert(
            key,
            WatchWithEntry {
                config,
                registered_at: Instant::now(),
            },
        );
        self.stats.total_registered += 1;
        self.stats.active_watches = self.watches.len();
    }

    /// Unregister a WatchWith
    pub fn unwatch(&mut self, watcher: &str, target: &str) -> bool {
        let key = (watcher.to_string(), target.to_string());
        let removed = self.watches.remove(&key).is_some();
        if removed {
            self.stats.total_unregistered += 1;
            self.stats.active_watches = self.watches.len();
        }
        removed
    }

    /// When a target terminates, get all custom messages for watchers
    pub fn on_terminated(&mut self, target: &str) -> Vec<(String, M)> {
        let mut results = Vec::new();
        let mut to_remove = Vec::new();

        for (key, entry) in &self.watches {
            if key.1 == target {
                results.push((key.0.clone(), entry.config.message.clone()));
                to_remove.push(key.clone());
            }
        }

        for key in to_remove {
            self.watches.remove(&key);
        }

        self.stats.total_triggered += results.len() as u64;
        self.stats.active_watches = self.watches.len();
        results
    }

    /// Check if a specific watch exists
    pub fn is_watching(&self, watcher: &str, target: &str) -> bool {
        self.watches
            .contains_key(&(watcher.to_string(), target.to_string()))
    }

    /// Get all targets watched by a specific watcher
    pub fn watched_by(&self, watcher: &str) -> Vec<String> {
        self.watches
            .keys()
            .filter(|(w, _)| w == watcher)
            .map(|(_, t)| t.clone())
            .collect()
    }

    /// Get all watchers of a specific target
    pub fn watchers_of(&self, target: &str) -> Vec<String> {
        self.watches
            .keys()
            .filter(|(_, t)| t == target)
            .map(|(w, _)| w.clone())
            .collect()
    }

    /// Get config for a specific watch
    pub fn get_config(&self, watcher: &str, target: &str) -> Option<&WatchWithConfig<M>> {
        self.watches
            .get(&(watcher.to_string(), target.to_string()))
            .map(|e| &e.config)
    }

    /// Get the age of a watch registration
    pub fn watch_age(&self, watcher: &str, target: &str) -> Option<Duration> {
        self.watches
            .get(&(watcher.to_string(), target.to_string()))
            .map(|e| e.registered_at.elapsed())
    }

    pub fn stats(&self) -> &WatchWithStats {
        &self.stats
    }

    pub fn active_count(&self) -> usize {
        self.watches.len()
    }

    /// Clear all watches (e.g., on system shutdown)
    pub fn clear(&mut self) {
        self.watches.clear();
        self.stats.active_watches = 0;
    }
}

impl<M: Clone> Default for WatchWithRegistry<M> {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// DeathPact - Automatic stop when watched actor terminates
// ============================================================================

/// DeathPact mode determines what happens when a watched actor terminates
/// and the watcher doesn't handle the Terminated message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeathPactMode {
    /// Watcher is stopped (default Akka behavior)
    Stop,
    /// Watcher receives DeathPactException (escalated to supervisor)
    Escalate,
    /// Watcher is restarted
    Restart,
    /// Log warning but continue (lenient mode)
    LogAndContinue,
    /// Custom handler will be invoked
    Custom,
}

/// Configuration for a death pact between two actors
#[derive(Debug, Clone)]
pub struct DeathPactConfig {
    /// The watcher actor path
    pub watcher: String,
    /// The target actor path
    pub target: String,
    /// What to do when target terminates unhandled
    pub mode: DeathPactMode,
    /// Grace period before enforcing the pact
    pub grace_period: Option<Duration>,
    /// Maximum number of pact violations before escalation
    pub max_violations: u32,
    /// Whether the pact is bidirectional
    pub bidirectional: bool,
}

impl DeathPactConfig {
    pub fn new(watcher: &str, target: &str) -> Self {
        Self {
            watcher: watcher.to_string(),
            target: target.to_string(),
            mode: DeathPactMode::Stop,
            grace_period: None,
            max_violations: 1,
            bidirectional: false,
        }
    }

    pub fn with_mode(mut self, mode: DeathPactMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_grace_period(mut self, duration: Duration) -> Self {
        self.grace_period = Some(duration);
        self
    }

    pub fn with_max_violations(mut self, max: u32) -> Self {
        self.max_violations = max;
        self
    }

    pub fn bidirectional(mut self) -> Self {
        self.bidirectional = true;
        self
    }
}

/// Outcome of a death pact evaluation
#[derive(Debug, Clone)]
pub enum DeathPactOutcome {
    /// Stop the watcher
    StopWatcher(String),
    /// Escalate exception to supervisor
    EscalateException(DeathPactException),
    /// Restart the watcher
    RestartWatcher(String),
    /// Log and continue
    LogWarning(String),
    /// Custom action needed
    CustomAction { watcher: String, target: String },
    /// Within grace period, no action yet
    GracePeriodActive { remaining: Duration },
    /// Violation count not yet exceeded
    ViolationRecorded { count: u32, max: u32 },
    /// No pact configured
    NoPact,
}

/// Manages death pacts between actors
pub struct DeathPactManager {
    pacts: HashMap<(String, String), DeathPactState>,
    stats: DeathPactStats,
}

struct DeathPactState {
    config: DeathPactConfig,
    violations: u32,
    created_at: Instant,
    last_violation: Option<Instant>,
}

/// Statistics for the DeathPact manager
#[derive(Debug, Clone, Default)]
pub struct DeathPactStats {
    pub total_pacts_created: u64,
    pub total_pacts_removed: u64,
    pub total_violations: u64,
    pub total_stops: u64,
    pub total_escalations: u64,
    pub total_restarts: u64,
    pub active_pacts: usize,
}

impl DeathPactManager {
    pub fn new() -> Self {
        Self {
            pacts: HashMap::new(),
            stats: DeathPactStats::default(),
        }
    }

    /// Register a death pact
    pub fn register(&mut self, config: DeathPactConfig) {
        let key = (config.watcher.clone(), config.target.clone());
        let bidirectional = config.bidirectional;
        let reverse_config = if bidirectional {
            Some(DeathPactConfig {
                watcher: config.target.clone(),
                target: config.watcher.clone(),
                mode: config.mode,
                grace_period: config.grace_period,
                max_violations: config.max_violations,
                bidirectional: false, // prevent infinite recursion
            })
        } else {
            None
        };

        self.pacts.insert(
            key,
            DeathPactState {
                config,
                violations: 0,
                created_at: Instant::now(),
                last_violation: None,
            },
        );
        self.stats.total_pacts_created += 1;

        if let Some(rev) = reverse_config {
            let rev_key = (rev.watcher.clone(), rev.target.clone());
            self.pacts.insert(
                rev_key,
                DeathPactState {
                    config: rev,
                    violations: 0,
                    created_at: Instant::now(),
                    last_violation: None,
                },
            );
            self.stats.total_pacts_created += 1;
        }

        self.stats.active_pacts = self.pacts.len();
    }

    /// Remove a death pact
    pub fn remove(&mut self, watcher: &str, target: &str) -> bool {
        let key = (watcher.to_string(), target.to_string());
        let removed = self.pacts.remove(&key).is_some();
        if removed {
            self.stats.total_pacts_removed += 1;
            self.stats.active_pacts = self.pacts.len();
        }
        removed
    }

    /// Evaluate what should happen when target terminates
    pub fn on_terminated(&mut self, target: &str) -> Vec<DeathPactOutcome> {
        let mut outcomes = Vec::new();
        let mut to_process: Vec<(String, String)> = Vec::new();

        for key in self.pacts.keys() {
            if key.1 == target {
                to_process.push(key.clone());
            }
        }

        for key in to_process {
            if let Some(state) = self.pacts.get_mut(&key) {
                // Check grace period
                if let Some(grace) = state.config.grace_period {
                    if state.created_at.elapsed() < grace {
                        outcomes.push(DeathPactOutcome::GracePeriodActive {
                            remaining: grace - state.created_at.elapsed(),
                        });
                        continue;
                    }
                }

                state.violations += 1;
                state.last_violation = Some(Instant::now());
                self.stats.total_violations += 1;

                // Check if violations exceeded
                if state.violations < state.config.max_violations {
                    outcomes.push(DeathPactOutcome::ViolationRecorded {
                        count: state.violations,
                        max: state.config.max_violations,
                    });
                    continue;
                }

                let outcome = match state.config.mode {
                    DeathPactMode::Stop => {
                        self.stats.total_stops += 1;
                        DeathPactOutcome::StopWatcher(state.config.watcher.clone())
                    }
                    DeathPactMode::Escalate => {
                        self.stats.total_escalations += 1;
                        DeathPactOutcome::EscalateException(
                            DeathPactException::new(&state.config.target, &state.config.watcher)
                                .with_explicit_pact(),
                        )
                    }
                    DeathPactMode::Restart => {
                        self.stats.total_restarts += 1;
                        DeathPactOutcome::RestartWatcher(state.config.watcher.clone())
                    }
                    DeathPactMode::LogAndContinue => DeathPactOutcome::LogWarning(format!(
                        "Death pact: '{}' terminated, watcher '{}' continues",
                        state.config.target, state.config.watcher
                    )),
                    DeathPactMode::Custom => DeathPactOutcome::CustomAction {
                        watcher: state.config.watcher.clone(),
                        target: state.config.target.clone(),
                    },
                };
                outcomes.push(outcome);
            }
        }

        // Clean up terminated pacts
        self.pacts.retain(|key, _| key.1 != target);
        self.stats.active_pacts = self.pacts.len();

        outcomes
    }

    /// Check if a pact exists
    pub fn has_pact(&self, watcher: &str, target: &str) -> bool {
        self.pacts
            .contains_key(&(watcher.to_string(), target.to_string()))
    }

    /// Get pact config
    pub fn get_pact(&self, watcher: &str, target: &str) -> Option<&DeathPactConfig> {
        self.pacts
            .get(&(watcher.to_string(), target.to_string()))
            .map(|s| &s.config)
    }

    /// Get violation count for a pact
    pub fn violation_count(&self, watcher: &str, target: &str) -> u32 {
        self.pacts
            .get(&(watcher.to_string(), target.to_string()))
            .map(|s| s.violations)
            .unwrap_or(0)
    }

    pub fn stats(&self) -> &DeathPactStats {
        &self.stats
    }

    pub fn active_count(&self) -> usize {
        self.pacts.len()
    }

    /// Clear all pacts
    pub fn clear(&mut self) {
        self.pacts.clear();
        self.stats.active_pacts = 0;
    }
}

impl Default for DeathPactManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// WatchWith Builder
// ============================================================================

/// Builder for creating WatchWith configurations
pub struct WatchWithBuilder<M: Clone> {
    target: String,
    watcher: String,
    message: Option<M>,
    include_failure_info: bool,
    survive_restart: bool,
}

impl<M: Clone> WatchWithBuilder<M> {
    pub fn new(watcher: &str, target: &str) -> Self {
        Self {
            watcher: watcher.to_string(),
            target: target.to_string(),
            message: None,
            include_failure_info: false,
            survive_restart: false,
        }
    }

    pub fn with_message(mut self, msg: M) -> Self {
        self.message = Some(msg);
        self
    }

    pub fn include_failure_info(mut self) -> Self {
        self.include_failure_info = true;
        self
    }

    pub fn survive_restart(mut self) -> Self {
        self.survive_restart = true;
        self
    }

    pub fn build(self) -> Option<WatchWithConfig<M>> {
        self.message.map(|msg| WatchWithConfig {
            target: self.target,
            watcher: self.watcher,
            message: msg,
            include_failure_info: self.include_failure_info,
            survive_restart: self.survive_restart,
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- DeathPactException tests --

    #[test]
    fn test_death_pact_exception_creation() {
        let ex = DeathPactException::new("/user/worker", "/user/parent");
        assert_eq!(ex.terminated_actor, "/user/worker");
        assert_eq!(ex.watcher_actor, "/user/parent");
        assert!(ex.reason.is_none());
        assert!(!ex.explicit_pact);
    }

    #[test]
    fn test_death_pact_exception_with_reason() {
        let ex = DeathPactException::new("/user/a", "/user/b").with_reason("crash");
        assert_eq!(ex.reason, Some("crash".to_string()));
    }

    #[test]
    fn test_death_pact_exception_display() {
        let ex = DeathPactException::new("/user/worker", "/user/parent");
        let msg = format!("{}", ex);
        assert!(msg.contains("DeathPactException"));
        assert!(msg.contains("/user/worker"));
    }

    #[test]
    fn test_death_pact_exception_explicit() {
        let ex = DeathPactException::new("/a", "/b").with_explicit_pact();
        assert!(ex.explicit_pact);
    }

    // -- WatchWithRegistry tests --

    #[test]
    fn test_watch_with_register() {
        let mut reg: WatchWithRegistry<String> = WatchWithRegistry::new();
        reg.watch_with(WatchWithConfig {
            target: "/user/target".to_string(),
            watcher: "/user/watcher".to_string(),
            message: "TargetDown".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });
        assert!(reg.is_watching("/user/watcher", "/user/target"));
        assert_eq!(reg.active_count(), 1);
    }

    #[test]
    fn test_watch_with_unregister() {
        let mut reg: WatchWithRegistry<String> = WatchWithRegistry::new();
        reg.watch_with(WatchWithConfig {
            target: "/user/t".to_string(),
            watcher: "/user/w".to_string(),
            message: "Down".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });
        assert!(reg.unwatch("/user/w", "/user/t"));
        assert!(!reg.is_watching("/user/w", "/user/t"));
        assert_eq!(reg.active_count(), 0);
    }

    #[test]
    fn test_watch_with_on_terminated() {
        let mut reg: WatchWithRegistry<String> = WatchWithRegistry::new();
        reg.watch_with(WatchWithConfig {
            target: "/user/target".to_string(),
            watcher: "/user/watcher1".to_string(),
            message: "Msg1".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });
        reg.watch_with(WatchWithConfig {
            target: "/user/target".to_string(),
            watcher: "/user/watcher2".to_string(),
            message: "Msg2".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });

        let results = reg.on_terminated("/user/target");
        assert_eq!(results.len(), 2);
        assert_eq!(reg.active_count(), 0);
    }

    #[test]
    fn test_watch_with_no_match() {
        let mut reg: WatchWithRegistry<String> = WatchWithRegistry::new();
        reg.watch_with(WatchWithConfig {
            target: "/user/a".to_string(),
            watcher: "/user/w".to_string(),
            message: "M".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });
        let results = reg.on_terminated("/user/b");
        assert!(results.is_empty());
        assert_eq!(reg.active_count(), 1);
    }

    #[test]
    fn test_watched_by() {
        let mut reg: WatchWithRegistry<String> = WatchWithRegistry::new();
        reg.watch_with(WatchWithConfig {
            target: "/t1".to_string(),
            watcher: "/w".to_string(),
            message: "M1".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });
        reg.watch_with(WatchWithConfig {
            target: "/t2".to_string(),
            watcher: "/w".to_string(),
            message: "M2".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });
        let targets = reg.watched_by("/w");
        assert_eq!(targets.len(), 2);
    }

    #[test]
    fn test_watchers_of() {
        let mut reg: WatchWithRegistry<String> = WatchWithRegistry::new();
        reg.watch_with(WatchWithConfig {
            target: "/t".to_string(),
            watcher: "/w1".to_string(),
            message: "M1".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });
        reg.watch_with(WatchWithConfig {
            target: "/t".to_string(),
            watcher: "/w2".to_string(),
            message: "M2".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });
        let watchers = reg.watchers_of("/t");
        assert_eq!(watchers.len(), 2);
    }

    #[test]
    fn test_watch_with_config_access() {
        let mut reg: WatchWithRegistry<String> = WatchWithRegistry::new();
        reg.watch_with(WatchWithConfig {
            target: "/t".to_string(),
            watcher: "/w".to_string(),
            message: "CustomMsg".to_string(),
            include_failure_info: true,
            survive_restart: true,
        });
        let config = reg.get_config("/w", "/t").unwrap();
        assert!(config.include_failure_info);
        assert!(config.survive_restart);
    }

    #[test]
    fn test_watch_with_stats() {
        let mut reg: WatchWithRegistry<String> = WatchWithRegistry::new();
        reg.watch_with(WatchWithConfig {
            target: "/t".to_string(),
            watcher: "/w".to_string(),
            message: "M".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });
        reg.on_terminated("/t");
        let stats = reg.stats();
        assert_eq!(stats.total_registered, 1);
        assert_eq!(stats.total_triggered, 1);
    }

    #[test]
    fn test_watch_with_clear() {
        let mut reg: WatchWithRegistry<String> = WatchWithRegistry::new();
        reg.watch_with(WatchWithConfig {
            target: "/t".to_string(),
            watcher: "/w".to_string(),
            message: "M".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });
        reg.clear();
        assert_eq!(reg.active_count(), 0);
    }

    #[test]
    fn test_watch_age() {
        let mut reg: WatchWithRegistry<String> = WatchWithRegistry::new();
        reg.watch_with(WatchWithConfig {
            target: "/t".to_string(),
            watcher: "/w".to_string(),
            message: "M".to_string(),
            include_failure_info: false,
            survive_restart: false,
        });
        let age = reg.watch_age("/w", "/t");
        assert!(age.is_some());
    }

    // -- DeathPactManager tests --

    #[test]
    fn test_death_pact_register() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w", "/t"));
        assert!(mgr.has_pact("/w", "/t"));
        assert_eq!(mgr.active_count(), 1);
    }

    #[test]
    fn test_death_pact_remove() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w", "/t"));
        assert!(mgr.remove("/w", "/t"));
        assert!(!mgr.has_pact("/w", "/t"));
    }

    #[test]
    fn test_death_pact_stop_mode() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w", "/t").with_mode(DeathPactMode::Stop));
        let outcomes = mgr.on_terminated("/t");
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(outcomes[0], DeathPactOutcome::StopWatcher(_)));
    }

    #[test]
    fn test_death_pact_escalate_mode() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w", "/t").with_mode(DeathPactMode::Escalate));
        let outcomes = mgr.on_terminated("/t");
        assert!(matches!(outcomes[0], DeathPactOutcome::EscalateException(_)));
    }

    #[test]
    fn test_death_pact_restart_mode() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w", "/t").with_mode(DeathPactMode::Restart));
        let outcomes = mgr.on_terminated("/t");
        assert!(matches!(outcomes[0], DeathPactOutcome::RestartWatcher(_)));
    }

    #[test]
    fn test_death_pact_log_mode() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w", "/t").with_mode(DeathPactMode::LogAndContinue));
        let outcomes = mgr.on_terminated("/t");
        assert!(matches!(outcomes[0], DeathPactOutcome::LogWarning(_)));
    }

    #[test]
    fn test_death_pact_custom_mode() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w", "/t").with_mode(DeathPactMode::Custom));
        let outcomes = mgr.on_terminated("/t");
        assert!(matches!(outcomes[0], DeathPactOutcome::CustomAction { .. }));
    }

    #[test]
    fn test_death_pact_bidirectional() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/a", "/b").bidirectional());
        assert!(mgr.has_pact("/a", "/b"));
        assert!(mgr.has_pact("/b", "/a"));
        assert_eq!(mgr.active_count(), 2);
    }

    #[test]
    fn test_death_pact_max_violations() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w", "/t").with_max_violations(3));
        // First termination — violation recorded, not exceeded
        // We need to re-register after termination since pacts are cleaned up
        let outcomes = mgr.on_terminated("/t");
        assert_eq!(outcomes.len(), 1);
        // With max_violations=3, first violation should be recorded
        match &outcomes[0] {
            DeathPactOutcome::ViolationRecorded { count, max } => {
                assert_eq!(*count, 1);
                assert_eq!(*max, 3);
            }
            other => panic!("Expected ViolationRecorded, got {:?}", other),
        }
    }

    #[test]
    fn test_death_pact_no_match() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w", "/a"));
        let outcomes = mgr.on_terminated("/b");
        assert!(outcomes.is_empty());
    }

    #[test]
    fn test_death_pact_multiple_watchers() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w1", "/t"));
        mgr.register(DeathPactConfig::new("/w2", "/t"));
        let outcomes = mgr.on_terminated("/t");
        assert_eq!(outcomes.len(), 2);
    }

    #[test]
    fn test_death_pact_stats() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w", "/t").with_mode(DeathPactMode::Stop));
        mgr.on_terminated("/t");
        let stats = mgr.stats();
        assert_eq!(stats.total_pacts_created, 1);
        assert_eq!(stats.total_stops, 1);
        assert_eq!(stats.total_violations, 1);
    }

    #[test]
    fn test_death_pact_violation_count() {
        let mgr = DeathPactManager::new();
        assert_eq!(mgr.violation_count("/w", "/t"), 0);
    }

    #[test]
    fn test_death_pact_clear() {
        let mut mgr = DeathPactManager::new();
        mgr.register(DeathPactConfig::new("/w1", "/t1"));
        mgr.register(DeathPactConfig::new("/w2", "/t2"));
        mgr.clear();
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn test_death_pact_get_config() {
        let mut mgr = DeathPactManager::new();
        mgr.register(
            DeathPactConfig::new("/w", "/t")
                .with_mode(DeathPactMode::Restart)
                .with_max_violations(5),
        );
        let config = mgr.get_pact("/w", "/t").unwrap();
        assert_eq!(config.mode, DeathPactMode::Restart);
        assert_eq!(config.max_violations, 5);
    }

    // -- WatchWithBuilder tests --

    #[test]
    fn test_builder_basic() {
        let config = WatchWithBuilder::<String>::new("/w", "/t")
            .with_message("Down".to_string())
            .build();
        assert!(config.is_some());
        let c = config.unwrap();
        assert_eq!(c.message, "Down");
    }

    #[test]
    fn test_builder_no_message() {
        let config = WatchWithBuilder::<String>::new("/w", "/t").build();
        assert!(config.is_none());
    }

    #[test]
    fn test_builder_full() {
        let config = WatchWithBuilder::new("/w", "/t")
            .with_message(42u32)
            .include_failure_info()
            .survive_restart()
            .build()
            .unwrap();
        assert_eq!(config.message, 42);
        assert!(config.include_failure_info);
        assert!(config.survive_restart);
    }

    #[test]
    fn test_registry_default() {
        let reg: WatchWithRegistry<String> = WatchWithRegistry::default();
        assert_eq!(reg.active_count(), 0);
    }

    #[test]
    fn test_manager_default() {
        let mgr = DeathPactManager::default();
        assert_eq!(mgr.active_count(), 0);
    }
}
