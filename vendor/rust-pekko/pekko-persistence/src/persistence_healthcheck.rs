// persistence_healthcheck.rs - Health check, diagnostics, and CleanupPlugin
// Monitors persistence subsystem health and provides automated cleanup

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// Health Status
// ============================================================================

/// Overall health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// All systems nominal
    Healthy,
    /// Some degradation but functional
    Degraded,
    /// Critical issues, may lose data
    Unhealthy,
    /// Cannot determine (check failed)
    Unknown,
}

impl HealthStatus {
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    pub fn is_degraded(&self) -> bool {
        matches!(self, HealthStatus::Degraded)
    }

    pub fn worst(a: HealthStatus, b: HealthStatus) -> HealthStatus {
        match (a, b) {
            (HealthStatus::Unhealthy, _) | (_, HealthStatus::Unhealthy) => HealthStatus::Unhealthy,
            (HealthStatus::Unknown, _) | (_, HealthStatus::Unknown) => HealthStatus::Unknown,
            (HealthStatus::Degraded, _) | (_, HealthStatus::Degraded) => HealthStatus::Degraded,
            _ => HealthStatus::Healthy,
        }
    }
}

impl fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "HEALTHY"),
            HealthStatus::Degraded => write!(f, "DEGRADED"),
            HealthStatus::Unhealthy => write!(f, "UNHEALTHY"),
            HealthStatus::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

// ============================================================================
// Component Health Check
// ============================================================================

/// Health check result for a single component
#[derive(Debug, Clone)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub message: Option<String>,
    pub latency: Option<Duration>,
    pub checked_at: Instant,
    pub details: HashMap<String, String>,
}

impl ComponentHealth {
    pub fn healthy(name: &str) -> Self {
        Self {
            name: name.to_string(),
            status: HealthStatus::Healthy,
            message: None,
            latency: None,
            checked_at: Instant::now(),
            details: HashMap::new(),
        }
    }

    pub fn degraded(name: &str, msg: &str) -> Self {
        Self {
            name: name.to_string(),
            status: HealthStatus::Degraded,
            message: Some(msg.to_string()),
            latency: None,
            checked_at: Instant::now(),
            details: HashMap::new(),
        }
    }

    pub fn unhealthy(name: &str, msg: &str) -> Self {
        Self {
            name: name.to_string(),
            status: HealthStatus::Unhealthy,
            message: Some(msg.to_string()),
            latency: None,
            checked_at: Instant::now(),
            details: HashMap::new(),
        }
    }

    pub fn unknown(name: &str, msg: &str) -> Self {
        Self {
            name: name.to_string(),
            status: HealthStatus::Unknown,
            message: Some(msg.to_string()),
            latency: None,
            checked_at: Instant::now(),
            details: HashMap::new(),
        }
    }

    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = Some(latency);
        self
    }

    pub fn with_detail(mut self, key: &str, value: &str) -> Self {
        self.details.insert(key.to_string(), value.to_string());
        self
    }

    pub fn age(&self) -> Duration {
        self.checked_at.elapsed()
    }
}

// ============================================================================
// PersistenceHealthCheck
// ============================================================================

/// Aggregated health check for the entire persistence subsystem
pub struct PersistenceHealthCheck {
    components: Vec<ComponentHealth>,
    thresholds: HealthThresholds,
    history: Vec<HealthSnapshot>,
    max_history: usize,
}

/// Thresholds for health evaluation
#[derive(Debug, Clone)]
pub struct HealthThresholds {
    /// Max acceptable journal write latency
    pub max_write_latency: Duration,
    /// Max acceptable read latency
    pub max_read_latency: Duration,
    /// Max acceptable snapshot size (bytes)
    pub max_snapshot_size: u64,
    /// Max events before requiring compaction
    pub max_events_before_compaction: u64,
    /// Max acceptable replication lag
    pub max_replication_lag: u64,
    /// Max staleness of health check
    pub max_check_staleness: Duration,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            max_write_latency: Duration::from_millis(500),
            max_read_latency: Duration::from_millis(200),
            max_snapshot_size: 10 * 1024 * 1024, // 10MB
            max_events_before_compaction: 100_000,
            max_replication_lag: 1000,
            max_check_staleness: Duration::from_secs(30),
        }
    }
}

/// A snapshot of health at a point in time
#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub status: HealthStatus,
    pub component_count: usize,
    pub healthy_count: usize,
    pub degraded_count: usize,
    pub unhealthy_count: usize,
    pub timestamp: Instant,
}

impl PersistenceHealthCheck {
    pub fn new() -> Self {
        Self {
            components: Vec::new(),
            thresholds: HealthThresholds::default(),
            history: Vec::new(),
            max_history: 100,
        }
    }

    pub fn with_thresholds(mut self, thresholds: HealthThresholds) -> Self {
        self.thresholds = thresholds;
        self
    }

    /// Add or update a component health check
    pub fn update_component(&mut self, health: ComponentHealth) {
        if let Some(existing) = self.components.iter_mut().find(|c| c.name == health.name) {
            *existing = health;
        } else {
            self.components.push(health);
        }
    }

    /// Check journal health based on latency
    pub fn check_journal_latency(&mut self, name: &str, write_latency: Duration) {
        let health = if write_latency <= self.thresholds.max_write_latency {
            ComponentHealth::healthy(name).with_latency(write_latency)
        } else if write_latency <= self.thresholds.max_write_latency * 2 {
            ComponentHealth::degraded(name, "High write latency")
                .with_latency(write_latency)
        } else {
            ComponentHealth::unhealthy(name, "Write latency critically high")
                .with_latency(write_latency)
        };
        self.update_component(health);
    }

    /// Check snapshot health
    pub fn check_snapshot_size(&mut self, name: &str, size_bytes: u64) {
        let health = if size_bytes <= self.thresholds.max_snapshot_size {
            ComponentHealth::healthy(name)
                .with_detail("size_bytes", &size_bytes.to_string())
        } else if size_bytes <= self.thresholds.max_snapshot_size * 2 {
            ComponentHealth::degraded(name, "Snapshot size large")
                .with_detail("size_bytes", &size_bytes.to_string())
        } else {
            ComponentHealth::unhealthy(name, "Snapshot size critically large")
                .with_detail("size_bytes", &size_bytes.to_string())
        };
        self.update_component(health);
    }

    /// Check event count for compaction need
    pub fn check_event_count(&mut self, name: &str, event_count: u64) {
        let health = if event_count <= self.thresholds.max_events_before_compaction {
            ComponentHealth::healthy(name)
                .with_detail("event_count", &event_count.to_string())
        } else {
            ComponentHealth::degraded(name, "Compaction recommended")
                .with_detail("event_count", &event_count.to_string())
        };
        self.update_component(health);
    }

    /// Get overall health status
    pub fn overall_status(&self) -> HealthStatus {
        self.components
            .iter()
            .map(|c| c.status)
            .fold(HealthStatus::Healthy, HealthStatus::worst)
    }

    /// Take a health snapshot
    pub fn snapshot(&mut self) -> HealthSnapshot {
        let snap = HealthSnapshot {
            status: self.overall_status(),
            component_count: self.components.len(),
            healthy_count: self.components.iter().filter(|c| c.status == HealthStatus::Healthy).count(),
            degraded_count: self.components.iter().filter(|c| c.status == HealthStatus::Degraded).count(),
            unhealthy_count: self.components.iter().filter(|c| c.status == HealthStatus::Unhealthy).count(),
            timestamp: Instant::now(),
        };
        self.history.push(snap.clone());
        if self.history.len() > self.max_history {
            self.history.remove(0);
        }
        snap
    }

    pub fn components(&self) -> &[ComponentHealth] {
        &self.components
    }

    pub fn get_component(&self, name: &str) -> Option<&ComponentHealth> {
        self.components.iter().find(|c| c.name == name)
    }

    pub fn thresholds(&self) -> &HealthThresholds {
        &self.thresholds
    }

    pub fn history(&self) -> &[HealthSnapshot] {
        &self.history
    }

    pub fn component_count(&self) -> usize {
        self.components.len()
    }
}

impl Default for PersistenceHealthCheck {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// CleanupPlugin - Automated journal/snapshot cleanup
// ============================================================================

/// Configuration for the cleanup plugin
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// Delete events older than this
    pub max_event_age: Option<Duration>,
    /// Keep at most this many events per persistence ID
    pub max_events_per_id: Option<u64>,
    /// Delete snapshots older than this
    pub max_snapshot_age: Option<Duration>,
    /// Keep at most this many snapshots per persistence ID
    pub max_snapshots_per_id: Option<u32>,
    /// Dry run (report but don't delete)
    pub dry_run: bool,
    /// Batch size for deletions
    pub batch_size: usize,
    /// Only clean up specific persistence ID patterns
    pub id_patterns: Vec<String>,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            max_event_age: None,
            max_events_per_id: None,
            max_snapshot_age: None,
            max_snapshots_per_id: Some(3),
            dry_run: false,
            batch_size: 1000,
            id_patterns: Vec::new(),
        }
    }
}

impl CleanupConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_event_age(mut self, age: Duration) -> Self {
        self.max_event_age = Some(age);
        self
    }

    pub fn with_max_events_per_id(mut self, max: u64) -> Self {
        self.max_events_per_id = Some(max);
        self
    }

    pub fn with_max_snapshot_age(mut self, age: Duration) -> Self {
        self.max_snapshot_age = Some(age);
        self
    }

    pub fn with_max_snapshots_per_id(mut self, max: u32) -> Self {
        self.max_snapshots_per_id = Some(max);
        self
    }

    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_id_pattern(mut self, pattern: &str) -> Self {
        self.id_patterns.push(pattern.to_string());
        self
    }
}

/// Result of a cleanup operation
#[derive(Debug, Clone, Default)]
pub struct CleanupResult {
    pub events_deleted: u64,
    pub snapshots_deleted: u64,
    pub persistence_ids_processed: u64,
    pub errors: Vec<String>,
    pub duration: Option<Duration>,
    pub dry_run: bool,
}

impl CleanupResult {
    pub fn is_success(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn total_deleted(&self) -> u64 {
        self.events_deleted + self.snapshots_deleted
    }
}

/// The cleanup plugin that manages periodic cleanup
pub struct CleanupPlugin {
    config: CleanupConfig,
    last_cleanup: Option<Instant>,
    total_cleanups: u64,
    total_events_cleaned: u64,
    total_snapshots_cleaned: u64,
    history: Vec<CleanupResult>,
}

impl CleanupPlugin {
    pub fn new(config: CleanupConfig) -> Self {
        Self {
            config,
            last_cleanup: None,
            total_cleanups: 0,
            total_events_cleaned: 0,
            total_snapshots_cleaned: 0,
            history: Vec::new(),
        }
    }

    /// Evaluate cleanup for a persistence ID
    pub fn evaluate_cleanup(
        &self,
        _persistence_id: &str,
        event_count: u64,
        oldest_event_age: Option<Duration>,
        snapshot_count: u32,
        oldest_snapshot_age: Option<Duration>,
    ) -> CleanupRecommendation {
        let mut actions = Vec::new();

        // Check event count
        if let Some(max) = self.config.max_events_per_id {
            if event_count > max {
                actions.push(CleanupAction::DeleteEvents {
                    count: event_count - max,
                    reason: format!("Exceeds max {} events", max),
                });
            }
        }

        // Check event age
        if let (Some(max_age), Some(age)) = (self.config.max_event_age, oldest_event_age) {
            if age > max_age {
                actions.push(CleanupAction::DeleteOldEvents {
                    older_than: max_age,
                    reason: "Events exceed max age".to_string(),
                });
            }
        }

        // Check snapshot count
        if let Some(max) = self.config.max_snapshots_per_id {
            if snapshot_count > max {
                actions.push(CleanupAction::DeleteSnapshots {
                    count: (snapshot_count - max) as u64,
                    reason: format!("Exceeds max {} snapshots", max),
                });
            }
        }

        // Check snapshot age
        if let (Some(max_age), Some(age)) = (self.config.max_snapshot_age, oldest_snapshot_age) {
            if age > max_age {
                actions.push(CleanupAction::DeleteOldSnapshots {
                    older_than: max_age,
                    reason: "Snapshots exceed max age".to_string(),
                });
            }
        }

        CleanupRecommendation {
            actions,
            dry_run: self.config.dry_run,
        }
    }

    /// Record a completed cleanup
    pub fn record_cleanup(&mut self, result: CleanupResult) {
        self.total_cleanups += 1;
        self.total_events_cleaned += result.events_deleted;
        self.total_snapshots_cleaned += result.snapshots_deleted;
        self.last_cleanup = Some(Instant::now());
        self.history.push(result);
        if self.history.len() > 50 {
            self.history.remove(0);
        }
    }

    /// Check if a persistence ID matches the configured patterns
    pub fn matches_id(&self, persistence_id: &str) -> bool {
        if self.config.id_patterns.is_empty() {
            return true; // No patterns means match all
        }
        self.config.id_patterns.iter().any(|p| {
            if p.ends_with('*') {
                persistence_id.starts_with(&p[..p.len() - 1])
            } else {
                persistence_id == p
            }
        })
    }

    pub fn config(&self) -> &CleanupConfig {
        &self.config
    }

    pub fn last_cleanup(&self) -> Option<Instant> {
        self.last_cleanup
    }

    pub fn total_cleanups(&self) -> u64 {
        self.total_cleanups
    }

    pub fn total_events_cleaned(&self) -> u64 {
        self.total_events_cleaned
    }

    pub fn total_snapshots_cleaned(&self) -> u64 {
        self.total_snapshots_cleaned
    }

    pub fn history(&self) -> &[CleanupResult] {
        &self.history
    }
}

/// Recommended cleanup actions
#[derive(Debug, Clone)]
pub struct CleanupRecommendation {
    pub actions: Vec<CleanupAction>,
    pub dry_run: bool,
}

impl CleanupRecommendation {
    pub fn has_actions(&self) -> bool {
        !self.actions.is_empty()
    }

    pub fn action_count(&self) -> usize {
        self.actions.len()
    }
}

/// Individual cleanup action
#[derive(Debug, Clone)]
pub enum CleanupAction {
    DeleteEvents { count: u64, reason: String },
    DeleteOldEvents { older_than: Duration, reason: String },
    DeleteSnapshots { count: u64, reason: String },
    DeleteOldSnapshots { older_than: Duration, reason: String },
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- HealthStatus tests --

    #[test]
    fn test_health_status_display() {
        assert_eq!(format!("{}", HealthStatus::Healthy), "HEALTHY");
        assert_eq!(format!("{}", HealthStatus::Degraded), "DEGRADED");
        assert_eq!(format!("{}", HealthStatus::Unhealthy), "UNHEALTHY");
        assert_eq!(format!("{}", HealthStatus::Unknown), "UNKNOWN");
    }

    #[test]
    fn test_health_status_worst() {
        assert_eq!(HealthStatus::worst(HealthStatus::Healthy, HealthStatus::Healthy), HealthStatus::Healthy);
        assert_eq!(HealthStatus::worst(HealthStatus::Healthy, HealthStatus::Degraded), HealthStatus::Degraded);
        assert_eq!(HealthStatus::worst(HealthStatus::Degraded, HealthStatus::Unhealthy), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_status_checks() {
        assert!(HealthStatus::Healthy.is_healthy());
        assert!(HealthStatus::Degraded.is_degraded());
        assert!(!HealthStatus::Healthy.is_degraded());
    }

    // -- ComponentHealth tests --

    #[test]
    fn test_component_healthy() {
        let h = ComponentHealth::healthy("journal");
        assert_eq!(h.status, HealthStatus::Healthy);
        assert!(h.message.is_none());
    }

    #[test]
    fn test_component_degraded() {
        let h = ComponentHealth::degraded("journal", "slow");
        assert_eq!(h.status, HealthStatus::Degraded);
        assert_eq!(h.message.as_deref(), Some("slow"));
    }

    #[test]
    fn test_component_unhealthy() {
        let h = ComponentHealth::unhealthy("journal", "down");
        assert_eq!(h.status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_component_unknown() {
        let h = ComponentHealth::unknown("journal", "timeout");
        assert_eq!(h.status, HealthStatus::Unknown);
    }

    #[test]
    fn test_component_with_latency() {
        let h = ComponentHealth::healthy("j").with_latency(Duration::from_millis(50));
        assert_eq!(h.latency, Some(Duration::from_millis(50)));
    }

    #[test]
    fn test_component_with_detail() {
        let h = ComponentHealth::healthy("j").with_detail("version", "1.0");
        assert_eq!(h.details.get("version").unwrap(), "1.0");
    }

    // -- PersistenceHealthCheck tests --

    #[test]
    fn test_healthcheck_new() {
        let hc = PersistenceHealthCheck::new();
        assert_eq!(hc.overall_status(), HealthStatus::Healthy);
        assert_eq!(hc.component_count(), 0);
    }

    #[test]
    fn test_healthcheck_update_component() {
        let mut hc = PersistenceHealthCheck::new();
        hc.update_component(ComponentHealth::healthy("journal"));
        assert_eq!(hc.component_count(), 1);
        assert!(hc.get_component("journal").is_some());
    }

    #[test]
    fn test_healthcheck_overall_healthy() {
        let mut hc = PersistenceHealthCheck::new();
        hc.update_component(ComponentHealth::healthy("journal"));
        hc.update_component(ComponentHealth::healthy("snapshot"));
        assert_eq!(hc.overall_status(), HealthStatus::Healthy);
    }

    #[test]
    fn test_healthcheck_overall_degraded() {
        let mut hc = PersistenceHealthCheck::new();
        hc.update_component(ComponentHealth::healthy("journal"));
        hc.update_component(ComponentHealth::degraded("snapshot", "slow"));
        assert_eq!(hc.overall_status(), HealthStatus::Degraded);
    }

    #[test]
    fn test_healthcheck_journal_latency_healthy() {
        let mut hc = PersistenceHealthCheck::new();
        hc.check_journal_latency("journal", Duration::from_millis(100));
        assert_eq!(hc.get_component("journal").unwrap().status, HealthStatus::Healthy);
    }

    #[test]
    fn test_healthcheck_journal_latency_degraded() {
        let mut hc = PersistenceHealthCheck::new();
        hc.check_journal_latency("journal", Duration::from_millis(700));
        assert_eq!(hc.get_component("journal").unwrap().status, HealthStatus::Degraded);
    }

    #[test]
    fn test_healthcheck_journal_latency_unhealthy() {
        let mut hc = PersistenceHealthCheck::new();
        hc.check_journal_latency("journal", Duration::from_millis(1500));
        assert_eq!(hc.get_component("journal").unwrap().status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_healthcheck_snapshot_size() {
        let mut hc = PersistenceHealthCheck::new();
        hc.check_snapshot_size("snapshot", 1024);
        assert_eq!(hc.get_component("snapshot").unwrap().status, HealthStatus::Healthy);
    }

    #[test]
    fn test_healthcheck_event_count() {
        let mut hc = PersistenceHealthCheck::new();
        hc.check_event_count("events", 200_000);
        assert_eq!(hc.get_component("events").unwrap().status, HealthStatus::Degraded);
    }

    #[test]
    fn test_healthcheck_snapshot() {
        let mut hc = PersistenceHealthCheck::new();
        hc.update_component(ComponentHealth::healthy("j"));
        let snap = hc.snapshot();
        assert_eq!(snap.status, HealthStatus::Healthy);
        assert_eq!(snap.healthy_count, 1);
        assert_eq!(hc.history().len(), 1);
    }

    // -- CleanupPlugin tests --

    #[test]
    fn test_cleanup_config_default() {
        let config = CleanupConfig::default();
        assert!(config.max_event_age.is_none());
        assert_eq!(config.max_snapshots_per_id, Some(3));
        assert!(!config.dry_run);
    }

    #[test]
    fn test_cleanup_config_builder() {
        let config = CleanupConfig::new()
            .with_max_events_per_id(1000)
            .with_max_snapshot_age(Duration::from_secs(86400))
            .dry_run();
        assert_eq!(config.max_events_per_id, Some(1000));
        assert!(config.dry_run);
    }

    #[test]
    fn test_cleanup_evaluate_no_action() {
        let plugin = CleanupPlugin::new(CleanupConfig::new().with_max_events_per_id(1000));
        let rec = plugin.evaluate_cleanup("order-1", 500, None, 1, None);
        assert!(!rec.has_actions());
    }

    #[test]
    fn test_cleanup_evaluate_events_exceeded() {
        let plugin = CleanupPlugin::new(CleanupConfig::new().with_max_events_per_id(1000));
        let rec = plugin.evaluate_cleanup("order-1", 1500, None, 1, None);
        assert!(rec.has_actions());
        assert_eq!(rec.action_count(), 1);
    }

    #[test]
    fn test_cleanup_evaluate_snapshots_exceeded() {
        let plugin = CleanupPlugin::new(CleanupConfig::new().with_max_snapshots_per_id(3));
        let rec = plugin.evaluate_cleanup("order-1", 100, None, 5, None);
        assert!(rec.has_actions());
    }

    #[test]
    fn test_cleanup_evaluate_old_events() {
        let plugin = CleanupPlugin::new(
            CleanupConfig::new().with_max_event_age(Duration::from_secs(3600)),
        );
        let rec = plugin.evaluate_cleanup(
            "order-1",
            100,
            Some(Duration::from_secs(7200)),
            1,
            None,
        );
        assert!(rec.has_actions());
    }

    #[test]
    fn test_cleanup_matches_id() {
        let plugin = CleanupPlugin::new(
            CleanupConfig::new().with_id_pattern("order-*"),
        );
        assert!(plugin.matches_id("order-123"));
        assert!(!plugin.matches_id("user-456"));
    }

    #[test]
    fn test_cleanup_matches_all_when_no_pattern() {
        let plugin = CleanupPlugin::new(CleanupConfig::new());
        assert!(plugin.matches_id("anything"));
    }

    #[test]
    fn test_cleanup_record() {
        let mut plugin = CleanupPlugin::new(CleanupConfig::new());
        plugin.record_cleanup(CleanupResult {
            events_deleted: 50,
            snapshots_deleted: 2,
            persistence_ids_processed: 10,
            errors: Vec::new(),
            duration: Some(Duration::from_millis(100)),
            dry_run: false,
        });
        assert_eq!(plugin.total_cleanups(), 1);
        assert_eq!(plugin.total_events_cleaned(), 50);
        assert_eq!(plugin.total_snapshots_cleaned(), 2);
    }

    #[test]
    fn test_cleanup_result_success() {
        let result = CleanupResult {
            events_deleted: 10,
            snapshots_deleted: 2,
            errors: Vec::new(),
            ..Default::default()
        };
        assert!(result.is_success());
        assert_eq!(result.total_deleted(), 12);
    }

    #[test]
    fn test_cleanup_result_failure() {
        let result = CleanupResult {
            errors: vec!["timeout".to_string()],
            ..Default::default()
        };
        assert!(!result.is_success());
    }

    #[test]
    fn test_cleanup_dry_run() {
        let plugin = CleanupPlugin::new(CleanupConfig::new().dry_run().with_max_events_per_id(10));
        let rec = plugin.evaluate_cleanup("o-1", 100, None, 0, None);
        assert!(rec.dry_run);
        assert!(rec.has_actions());
    }

    #[test]
    fn test_healthcheck_default() {
        let hc = PersistenceHealthCheck::default();
        assert_eq!(hc.component_count(), 0);
    }
}
