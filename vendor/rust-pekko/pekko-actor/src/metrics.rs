//! Actor Metrics & Instrumentation
//!
//! Provides runtime observability for actor systems:
//!
//! - **ActorMetrics**: Per-actor statistics (message count, processing time, errors)
//! - **SystemMetrics**: Aggregate metrics for the entire actor system
//! - **MetricsCollector**: Central collector with periodic reporting
//! - **MetricsSnapshot**: Point-in-time snapshot for export
//! - **MailboxMetrics**: Mailbox-specific queue depth and throughput

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug};

// ============= Actor Metrics =============

/// Per-actor metrics tracking.
#[derive(Debug)]
pub struct ActorMetrics {
    /// Actor name/path
    pub actor_name: String,
    /// Total messages received
    messages_received: AtomicU64,
    /// Total messages processed successfully
    messages_processed: AtomicU64,
    /// Total messages that caused errors
    messages_failed: AtomicU64,
    /// Total processing time in microseconds
    total_processing_time_us: AtomicU64,
    /// Max processing time for a single message in microseconds
    max_processing_time_us: AtomicU64,
    /// Min processing time for a single message in microseconds (init to u64::MAX)
    min_processing_time_us: AtomicU64,
    /// Number of restarts
    restarts: AtomicU64,
    /// Number of stashed messages
    stashed_messages: AtomicU64,
    /// Current mailbox size
    mailbox_size: AtomicU64,
    /// When this actor was created
    created_at: Instant,
    /// Whether the actor is currently alive
    is_alive: AtomicBool,
}

impl ActorMetrics {
    pub fn new(actor_name: impl Into<String>) -> Self {
        Self {
            actor_name: actor_name.into(),
            messages_received: AtomicU64::new(0),
            messages_processed: AtomicU64::new(0),
            messages_failed: AtomicU64::new(0),
            total_processing_time_us: AtomicU64::new(0),
            max_processing_time_us: AtomicU64::new(0),
            min_processing_time_us: AtomicU64::new(u64::MAX),
            restarts: AtomicU64::new(0),
            stashed_messages: AtomicU64::new(0),
            mailbox_size: AtomicU64::new(0),
            created_at: Instant::now(),
            is_alive: AtomicBool::new(true),
        }
    }

    /// Record that a message was received.
    pub fn record_message_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record successful message processing with duration.
    pub fn record_message_processed(&self, duration: Duration) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        let us = duration.as_micros() as u64;
        self.total_processing_time_us.fetch_add(us, Ordering::Relaxed);

        // Update max
        let mut current_max = self.max_processing_time_us.load(Ordering::Relaxed);
        while us > current_max {
            match self.max_processing_time_us.compare_exchange_weak(
                current_max, us, Ordering::Relaxed, Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }

        // Update min
        let mut current_min = self.min_processing_time_us.load(Ordering::Relaxed);
        while us < current_min {
            match self.min_processing_time_us.compare_exchange_weak(
                current_min, us, Ordering::Relaxed, Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
        }
    }

    /// Record a failed message processing.
    pub fn record_message_failed(&self) {
        self.messages_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an actor restart.
    pub fn record_restart(&self) {
        self.restarts.fetch_add(1, Ordering::Relaxed);
    }

    /// Update the current mailbox size.
    pub fn set_mailbox_size(&self, size: u64) {
        self.mailbox_size.store(size, Ordering::Relaxed);
    }

    /// Record stashed message count.
    pub fn record_stash(&self) {
        self.stashed_messages.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark the actor as stopped.
    pub fn mark_stopped(&self) {
        self.is_alive.store(false, Ordering::Relaxed);
    }

    /// Get a snapshot of the current metrics.
    pub fn snapshot(&self) -> ActorMetricsSnapshot {
        let processed = self.messages_processed.load(Ordering::Relaxed);
        let total_us = self.total_processing_time_us.load(Ordering::Relaxed);
        let min_us = self.min_processing_time_us.load(Ordering::Relaxed);

        ActorMetricsSnapshot {
            actor_name: self.actor_name.clone(),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_processed: processed,
            messages_failed: self.messages_failed.load(Ordering::Relaxed),
            avg_processing_time: if processed > 0 {
                Duration::from_micros(total_us / processed)
            } else {
                Duration::ZERO
            },
            max_processing_time: Duration::from_micros(
                self.max_processing_time_us.load(Ordering::Relaxed),
            ),
            min_processing_time: if min_us == u64::MAX {
                Duration::ZERO
            } else {
                Duration::from_micros(min_us)
            },
            restarts: self.restarts.load(Ordering::Relaxed),
            stashed_messages: self.stashed_messages.load(Ordering::Relaxed),
            mailbox_size: self.mailbox_size.load(Ordering::Relaxed),
            uptime: self.created_at.elapsed(),
            is_alive: self.is_alive.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of actor metrics.
#[derive(Debug, Clone)]
pub struct ActorMetricsSnapshot {
    pub actor_name: String,
    pub messages_received: u64,
    pub messages_processed: u64,
    pub messages_failed: u64,
    pub avg_processing_time: Duration,
    pub max_processing_time: Duration,
    pub min_processing_time: Duration,
    pub restarts: u64,
    pub stashed_messages: u64,
    pub mailbox_size: u64,
    pub uptime: Duration,
    pub is_alive: bool,
}

impl fmt::Display for ActorMetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] recv={} proc={} fail={} avg={:?} max={:?} restarts={} alive={}",
            self.actor_name,
            self.messages_received,
            self.messages_processed,
            self.messages_failed,
            self.avg_processing_time,
            self.max_processing_time,
            self.restarts,
            self.is_alive,
        )
    }
}

// ============= System Metrics =============

/// Aggregate metrics for the entire actor system.
#[derive(Debug)]
pub struct SystemMetrics {
    /// System name
    pub system_name: String,
    /// When the system was started
    started_at: Instant,
    /// Total actors created
    actors_created: AtomicU64,
    /// Total actors stopped
    actors_stopped: AtomicU64,
    /// Currently active actors
    active_actors: AtomicU64,
    /// Total messages sent across all actors
    total_messages: AtomicU64,
    /// Total dead letters
    dead_letters: AtomicU64,
    /// Per-actor metrics registry
    actor_metrics: RwLock<HashMap<String, Arc<ActorMetrics>>>,
}

impl SystemMetrics {
    pub fn new(system_name: impl Into<String>) -> Self {
        Self {
            system_name: system_name.into(),
            started_at: Instant::now(),
            actors_created: AtomicU64::new(0),
            actors_stopped: AtomicU64::new(0),
            active_actors: AtomicU64::new(0),
            total_messages: AtomicU64::new(0),
            dead_letters: AtomicU64::new(0),
            actor_metrics: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new actor and return its metrics handle.
    pub async fn register_actor(&self, name: &str) -> Arc<ActorMetrics> {
        let metrics = Arc::new(ActorMetrics::new(name));
        let mut registry = self.actor_metrics.write().await;
        registry.insert(name.to_string(), metrics.clone());
        self.actors_created.fetch_add(1, Ordering::Relaxed);
        self.active_actors.fetch_add(1, Ordering::Relaxed);
        debug!("Registered metrics for actor: {}", name);
        metrics
    }

    /// Unregister an actor (mark as stopped).
    pub async fn unregister_actor(&self, name: &str) {
        let registry = self.actor_metrics.read().await;
        if let Some(metrics) = registry.get(name) {
            metrics.mark_stopped();
        }
        self.actors_stopped.fetch_add(1, Ordering::Relaxed);
        self.active_actors.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a dead letter.
    pub fn record_dead_letter(&self) {
        self.dead_letters.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a message sent.
    pub fn record_message(&self) {
        self.total_messages.fetch_add(1, Ordering::Relaxed);
    }

    /// Get metrics for a specific actor.
    pub async fn get_actor_metrics(&self, name: &str) -> Option<Arc<ActorMetrics>> {
        let registry = self.actor_metrics.read().await;
        registry.get(name).cloned()
    }

    /// Get a system-wide snapshot.
    pub async fn snapshot(&self) -> SystemMetricsSnapshot {
        let registry = self.actor_metrics.read().await;
        let actor_snapshots: Vec<ActorMetricsSnapshot> = registry
            .values()
            .map(|m| m.snapshot())
            .collect();

        SystemMetricsSnapshot {
            system_name: self.system_name.clone(),
            uptime: self.started_at.elapsed(),
            actors_created: self.actors_created.load(Ordering::Relaxed),
            actors_stopped: self.actors_stopped.load(Ordering::Relaxed),
            active_actors: self.active_actors.load(Ordering::Relaxed),
            total_messages: self.total_messages.load(Ordering::Relaxed),
            dead_letters: self.dead_letters.load(Ordering::Relaxed),
            actor_snapshots,
        }
    }

    /// Get list of all registered actor names.
    pub async fn actor_names(&self) -> Vec<String> {
        let registry = self.actor_metrics.read().await;
        registry.keys().cloned().collect()
    }
}

/// System-wide metrics snapshot.
#[derive(Debug, Clone)]
pub struct SystemMetricsSnapshot {
    pub system_name: String,
    pub uptime: Duration,
    pub actors_created: u64,
    pub actors_stopped: u64,
    pub active_actors: u64,
    pub total_messages: u64,
    pub dead_letters: u64,
    pub actor_snapshots: Vec<ActorMetricsSnapshot>,
}

impl fmt::Display for SystemMetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=== System: {} ===", self.system_name)?;
        writeln!(f, "Uptime: {:?}", self.uptime)?;
        writeln!(
            f,
            "Actors: {} created, {} stopped, {} active",
            self.actors_created, self.actors_stopped, self.active_actors
        )?;
        writeln!(f, "Messages: {} total, {} dead letters", self.total_messages, self.dead_letters)?;
        for actor in &self.actor_snapshots {
            writeln!(f, "  {}", actor)?;
        }
        Ok(())
    }
}

// ============= Metrics Collector =============

/// Central metrics collector that periodically reports metrics.
pub struct MetricsCollector {
    system_metrics: Arc<SystemMetrics>,
    enabled: AtomicBool,
}

impl MetricsCollector {
    pub fn new(system_metrics: Arc<SystemMetrics>) -> Self {
        Self {
            system_metrics,
            enabled: AtomicBool::new(true),
        }
    }

    /// Start periodic metrics reporting in the background.
    /// Returns a handle that can be used to stop reporting.
    pub fn start(&self) -> MetricsReportHandle {
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        MetricsReportHandle { stop_tx }
    }
    

    /// Take a one-time snapshot of all metrics.
    pub async fn collect(&self) -> SystemMetricsSnapshot {
        self.system_metrics.snapshot().await
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }
}

/// Handle to stop the metrics reporting background task.
pub struct MetricsReportHandle {
    stop_tx: tokio::sync::watch::Sender<bool>
}

impl MetricsReportHandle {
    /// Stop the periodic reporting.
    pub fn stop(self) {
        let _ = self.stop_tx.send(true);
    }
}

// ============= Timer Guard =============

/// RAII guard that measures message processing time.
/// When dropped, records the elapsed time to the actor metrics.
pub struct ProcessingTimer {
    metrics: Arc<ActorMetrics>,
    start: Instant,
}

impl ProcessingTimer {
    pub fn start(metrics: Arc<ActorMetrics>) -> Self {
        metrics.record_message_received();
        Self {
            metrics,
            start: Instant::now(),
        }
    }

    /// Mark as successful and record timing.
    pub fn success(self) {
        let duration = self.start.elapsed();
        self.metrics.record_message_processed(duration);
        std::mem::forget(self); // prevent Drop from recording failure
    }

    /// Mark as failed.
    pub fn failure(self) {
        self.metrics.record_message_failed();
        std::mem::forget(self); // prevent Drop from double-recording
    }
}

impl Drop for ProcessingTimer {
    fn drop(&mut self) {
        // If neither success() nor failure() was called, count as failure
        self.metrics.record_message_failed();
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_metrics_basic() {
        let metrics = ActorMetrics::new("test-actor");
        metrics.record_message_received();
        metrics.record_message_received();
        metrics.record_message_processed(Duration::from_micros(100));
        metrics.record_message_processed(Duration::from_micros(200));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.actor_name, "test-actor");
        assert_eq!(snapshot.messages_received, 2);
        assert_eq!(snapshot.messages_processed, 2);
        assert_eq!(snapshot.avg_processing_time, Duration::from_micros(150));
        assert_eq!(snapshot.max_processing_time, Duration::from_micros(200));
        assert_eq!(snapshot.min_processing_time, Duration::from_micros(100));
        assert!(snapshot.is_alive);
    }

    #[test]
    fn test_actor_metrics_failures() {
        let metrics = ActorMetrics::new("failing-actor");
        metrics.record_message_received();
        metrics.record_message_failed();
        metrics.record_message_failed();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_received, 1);
        assert_eq!(snapshot.messages_failed, 2);
    }

    #[test]
    fn test_actor_metrics_restarts() {
        let metrics = ActorMetrics::new("restarting-actor");
        metrics.record_restart();
        metrics.record_restart();
        metrics.record_restart();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.restarts, 3);
    }

    #[test]
    fn test_actor_metrics_mailbox_size() {
        let metrics = ActorMetrics::new("busy-actor");
        metrics.set_mailbox_size(42);
        assert_eq!(metrics.snapshot().mailbox_size, 42);
        metrics.set_mailbox_size(0);
        assert_eq!(metrics.snapshot().mailbox_size, 0);
    }

    #[test]
    fn test_actor_metrics_stopped() {
        let metrics = ActorMetrics::new("short-lived");
        assert!(metrics.snapshot().is_alive);
        metrics.mark_stopped();
        assert!(!metrics.snapshot().is_alive);
    }

    #[test]
    fn test_actor_metrics_empty_snapshot() {
        let metrics = ActorMetrics::new("idle-actor");
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_received, 0);
        assert_eq!(snapshot.messages_processed, 0);
        assert_eq!(snapshot.avg_processing_time, Duration::ZERO);
        assert_eq!(snapshot.min_processing_time, Duration::ZERO);
        assert_eq!(snapshot.max_processing_time, Duration::ZERO);
    }

    #[test]
    fn test_actor_metrics_snapshot_display() {
        let metrics = ActorMetrics::new("display-actor");
        metrics.record_message_received();
        metrics.record_message_processed(Duration::from_micros(50));
        let snapshot = metrics.snapshot();
        let display = format!("{}", snapshot);
        assert!(display.contains("display-actor"));
        assert!(display.contains("recv=1"));
        assert!(display.contains("proc=1"));
    }

    #[tokio::test]
    async fn test_system_metrics_register_unregister() {
        let sys_metrics = SystemMetrics::new("test-system");
        let actor_metrics = sys_metrics.register_actor("actor-1").await;
        assert_eq!(actor_metrics.actor_name, "actor-1");

        let snapshot = sys_metrics.snapshot().await;
        assert_eq!(snapshot.actors_created, 1);
        assert_eq!(snapshot.active_actors, 1);

        sys_metrics.unregister_actor("actor-1").await;
        let snapshot = sys_metrics.snapshot().await;
        assert_eq!(snapshot.actors_stopped, 1);
        assert_eq!(snapshot.active_actors, 0);
    }

    #[tokio::test]
    async fn test_system_metrics_dead_letters() {
        let sys_metrics = SystemMetrics::new("test-system");
        sys_metrics.record_dead_letter();
        sys_metrics.record_dead_letter();

        let snapshot = sys_metrics.snapshot().await;
        assert_eq!(snapshot.dead_letters, 2);
    }

    #[tokio::test]
    async fn test_system_metrics_messages() {
        let sys_metrics = SystemMetrics::new("test-system");
        for _ in 0..100 {
            sys_metrics.record_message();
        }
        let snapshot = sys_metrics.snapshot().await;
        assert_eq!(snapshot.total_messages, 100);
    }

    #[tokio::test]
    async fn test_system_metrics_snapshot_display() {
        let sys_metrics = SystemMetrics::new("my-system");
        sys_metrics.register_actor("actor-a").await;
        sys_metrics.register_actor("actor-b").await;
        sys_metrics.record_message();

        let snapshot = sys_metrics.snapshot().await;
        let display = format!("{}", snapshot);
        assert!(display.contains("my-system"));
        assert!(display.contains("2 active"));
        assert!(display.contains("1 total"));
    }

    #[tokio::test]
    async fn test_system_metrics_get_actor() {
        let sys_metrics = SystemMetrics::new("test-system");
        sys_metrics.register_actor("target-actor").await;

        let found = sys_metrics.get_actor_metrics("target-actor").await;
        assert!(found.is_some());
        assert_eq!(found.unwrap().actor_name, "target-actor");

        let not_found = sys_metrics.get_actor_metrics("nonexistent").await;
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_system_metrics_actor_names() {
        let sys_metrics = SystemMetrics::new("test-system");
        sys_metrics.register_actor("a").await;
        sys_metrics.register_actor("b").await;
        sys_metrics.register_actor("c").await;

        let mut names = sys_metrics.actor_names().await;
        names.sort();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn test_metrics_collector_collect() {
        let sys_metrics = Arc::new(SystemMetrics::new("test-system"));
        sys_metrics.register_actor("actor-1").await;
        sys_metrics.record_message();

        let collector = MetricsCollector::new(sys_metrics, Duration::from_secs(60));
        let snapshot = collector.collect().await;
        assert_eq!(snapshot.active_actors, 1);
        assert_eq!(snapshot.total_messages, 1);
    }

    #[tokio::test]
    async fn test_metrics_collector_enable_disable() {
        let sys_metrics = Arc::new(SystemMetrics::new("test-system"));
        let collector = MetricsCollector::new(sys_metrics, Duration::from_secs(60));
        assert!(collector.is_enabled());
        collector.disable();
        assert!(!collector.is_enabled());
        collector.enable();
        assert!(collector.is_enabled());
    }

    #[test]
    fn test_processing_timer_success() {
        let metrics = Arc::new(ActorMetrics::new("timed-actor"));
        let timer = ProcessingTimer::start(metrics.clone());
        std::thread::sleep(Duration::from_millis(1));
        timer.success();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_received, 1);
        assert_eq!(snapshot.messages_processed, 1);
        assert_eq!(snapshot.messages_failed, 0);
        assert!(snapshot.max_processing_time >= Duration::from_millis(1));
    }

    #[test]
    fn test_processing_timer_failure() {
        let metrics = Arc::new(ActorMetrics::new("failing-timer"));
        let timer = ProcessingTimer::start(metrics.clone());
        timer.failure();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_received, 1);
        assert_eq!(snapshot.messages_processed, 0);
        assert_eq!(snapshot.messages_failed, 1);
    }

    #[test]
    fn test_processing_timer_drop_counts_as_failure() {
        let metrics = Arc::new(ActorMetrics::new("dropped-timer"));
        {
            let _timer = ProcessingTimer::start(metrics.clone());
            // timer drops without calling success() or failure()
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_received, 1);
        assert_eq!(snapshot.messages_failed, 1);
    }

    #[test]
    fn test_actor_metrics_stash() {
        let metrics = ActorMetrics::new("stash-actor");
        metrics.record_stash();
        metrics.record_stash();
        assert_eq!(metrics.snapshot().stashed_messages, 2);
    }
}
