//! Persistence Metrics — Observability for persistence operations
//!
//! Tracks timing, throughput, and error rates for journal and snapshot operations:
//!
//! - **JournalMetrics** — Write/replay latency, event counts, batch sizes
//! - **SnapshotMetrics** — Save/load latency, snapshot sizes
//! - **RecoveryMetrics** — Recovery duration, events replayed
//! - **PersistenceMetricsCollector** — Central collector for all metrics

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ============= Histogram =============

/// Simple histogram for tracking distributions.
#[derive(Debug)]
pub struct Histogram {
    count: AtomicU64,
    sum: AtomicU64,
    min: AtomicU64,
    max: AtomicU64,
}

impl Histogram {
    pub fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            min: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
        }
    }

    /// Record a value in microseconds.
    pub fn record(&self, value_us: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value_us, Ordering::Relaxed);
        self.min.fetch_min(value_us, Ordering::Relaxed);
        self.max.fetch_max(value_us, Ordering::Relaxed);
    }

    /// Record a duration.
    pub fn record_duration(&self, d: Duration) {
        self.record(d.as_micros() as u64);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn sum_us(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    pub fn min_us(&self) -> Option<u64> {
        let v = self.min.load(Ordering::Relaxed);
        if v == u64::MAX { None } else { Some(v) }
    }

    pub fn max_us(&self) -> Option<u64> {
        let v = self.max.load(Ordering::Relaxed);
        if v == 0 && self.count() == 0 { None } else { Some(v) }
    }

    pub fn mean_us(&self) -> Option<f64> {
        let c = self.count();
        if c == 0 { None } else { Some(self.sum_us() as f64 / c as f64) }
    }

    /// Get a snapshot of the histogram.
    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            count: self.count(),
            sum_us: self.sum_us(),
            min_us: self.min_us(),
            max_us: self.max_us(),
            mean_us: self.mean_us(),
        }
    }
}

/// Immutable snapshot of histogram state.
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub sum_us: u64,
    pub min_us: Option<u64>,
    pub max_us: Option<u64>,
    pub mean_us: Option<f64>,
}

// ============= Timer =============

/// RAII timer that records duration on drop.
pub struct Timer {
    start: Instant,
    histogram: Arc<Histogram>,
}

impl Timer {
    pub fn new(histogram: Arc<Histogram>) -> Self {
        Self {
            start: Instant::now(),
            histogram,
        }
    }

    /// Manually stop and record.
    pub fn stop(self) -> Duration {
        let elapsed = self.start.elapsed();
        self.histogram.record_duration(elapsed);
        std::mem::forget(self); // prevent double recording in drop
        elapsed
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.histogram.record_duration(self.start.elapsed());
    }
}

// ============= JournalMetrics =============

/// Metrics for journal operations.
pub struct JournalMetrics {
    pub write_latency: Arc<Histogram>,
    pub replay_latency: Arc<Histogram>,
    pub delete_latency: Arc<Histogram>,
    pub events_written: AtomicU64,
    pub events_replayed: AtomicU64,
    pub events_deleted: AtomicU64,
    pub write_failures: AtomicU64,
    pub replay_failures: AtomicU64,
    pub batch_sizes: Arc<Histogram>,
}

impl JournalMetrics {
    pub fn new() -> Self {
        Self {
            write_latency: Arc::new(Histogram::new()),
            replay_latency: Arc::new(Histogram::new()),
            delete_latency: Arc::new(Histogram::new()),
            events_written: AtomicU64::new(0),
            events_replayed: AtomicU64::new(0),
            events_deleted: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            replay_failures: AtomicU64::new(0),
            batch_sizes: Arc::new(Histogram::new()),
        }
    }

    /// Start a write timer.
    pub fn time_write(&self) -> Timer {
        Timer::new(self.write_latency.clone())
    }

    /// Start a replay timer.
    pub fn time_replay(&self) -> Timer {
        Timer::new(self.replay_latency.clone())
    }

    /// Record events written.
    pub fn record_write(&self, count: u64) {
        self.events_written.fetch_add(count, Ordering::Relaxed);
        self.batch_sizes.record(count);
    }

    /// Record events replayed.
    pub fn record_replay(&self, count: u64) {
        self.events_replayed.fetch_add(count, Ordering::Relaxed);
    }

    /// Record write failure.
    pub fn record_write_failure(&self) {
        self.write_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Get snapshot.
    pub fn snapshot(&self) -> JournalMetricsSnapshot {
        JournalMetricsSnapshot {
            write_latency: self.write_latency.snapshot(),
            replay_latency: self.replay_latency.snapshot(),
            events_written: self.events_written.load(Ordering::Relaxed),
            events_replayed: self.events_replayed.load(Ordering::Relaxed),
            events_deleted: self.events_deleted.load(Ordering::Relaxed),
            write_failures: self.write_failures.load(Ordering::Relaxed),
            batch_sizes: self.batch_sizes.snapshot(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct JournalMetricsSnapshot {
    pub write_latency: HistogramSnapshot,
    pub replay_latency: HistogramSnapshot,
    pub events_written: u64,
    pub events_replayed: u64,
    pub events_deleted: u64,
    pub write_failures: u64,
    pub batch_sizes: HistogramSnapshot,
}

// ============= SnapshotMetrics =============

/// Metrics for snapshot operations.
pub struct SnapshotMetrics {
    pub save_latency: Arc<Histogram>,
    pub load_latency: Arc<Histogram>,
    pub delete_latency: Arc<Histogram>,
    pub snapshots_saved: AtomicU64,
    pub snapshots_loaded: AtomicU64,
    pub snapshots_deleted: AtomicU64,
    pub save_failures: AtomicU64,
    pub load_failures: AtomicU64,
    pub snapshot_sizes: Arc<Histogram>,
}

impl SnapshotMetrics {
    pub fn new() -> Self {
        Self {
            save_latency: Arc::new(Histogram::new()),
            load_latency: Arc::new(Histogram::new()),
            delete_latency: Arc::new(Histogram::new()),
            snapshots_saved: AtomicU64::new(0),
            snapshots_loaded: AtomicU64::new(0),
            snapshots_deleted: AtomicU64::new(0),
            save_failures: AtomicU64::new(0),
            load_failures: AtomicU64::new(0),
            snapshot_sizes: Arc::new(Histogram::new()),
        }
    }

    pub fn time_save(&self) -> Timer {
        Timer::new(self.save_latency.clone())
    }

    pub fn time_load(&self) -> Timer {
        Timer::new(self.load_latency.clone())
    }

    pub fn record_save(&self, size_bytes: u64) {
        self.snapshots_saved.fetch_add(1, Ordering::Relaxed);
        self.snapshot_sizes.record(size_bytes);
    }

    pub fn record_load(&self) {
        self.snapshots_loaded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_save_failure(&self) {
        self.save_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> SnapshotMetricsSnapshot {
        SnapshotMetricsSnapshot {
            save_latency: self.save_latency.snapshot(),
            load_latency: self.load_latency.snapshot(),
            snapshots_saved: self.snapshots_saved.load(Ordering::Relaxed),
            snapshots_loaded: self.snapshots_loaded.load(Ordering::Relaxed),
            snapshots_deleted: self.snapshots_deleted.load(Ordering::Relaxed),
            save_failures: self.save_failures.load(Ordering::Relaxed),
            snapshot_sizes: self.snapshot_sizes.snapshot(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotMetricsSnapshot {
    pub save_latency: HistogramSnapshot,
    pub load_latency: HistogramSnapshot,
    pub snapshots_saved: u64,
    pub snapshots_loaded: u64,
    pub snapshots_deleted: u64,
    pub save_failures: u64,
    pub snapshot_sizes: HistogramSnapshot,
}

// ============= RecoveryMetrics =============

/// Metrics for actor recovery operations.
pub struct RecoveryMetrics {
    pub recovery_latency: Arc<Histogram>,
    pub recoveries_completed: AtomicU64,
    pub recoveries_failed: AtomicU64,
    pub events_replayed_total: AtomicU64,
    pub snapshots_loaded_total: AtomicU64,
}

impl RecoveryMetrics {
    pub fn new() -> Self {
        Self {
            recovery_latency: Arc::new(Histogram::new()),
            recoveries_completed: AtomicU64::new(0),
            recoveries_failed: AtomicU64::new(0),
            events_replayed_total: AtomicU64::new(0),
            snapshots_loaded_total: AtomicU64::new(0),
        }
    }

    pub fn time_recovery(&self) -> Timer {
        Timer::new(self.recovery_latency.clone())
    }

    pub fn record_recovery(&self, events_replayed: u64, snapshot_loaded: bool) {
        self.recoveries_completed.fetch_add(1, Ordering::Relaxed);
        self.events_replayed_total.fetch_add(events_replayed, Ordering::Relaxed);
        if snapshot_loaded {
            self.snapshots_loaded_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_failure(&self) {
        self.recoveries_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RecoveryMetricsSnapshot {
        RecoveryMetricsSnapshot {
            recovery_latency: self.recovery_latency.snapshot(),
            recoveries_completed: self.recoveries_completed.load(Ordering::Relaxed),
            recoveries_failed: self.recoveries_failed.load(Ordering::Relaxed),
            events_replayed_total: self.events_replayed_total.load(Ordering::Relaxed),
            snapshots_loaded_total: self.snapshots_loaded_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryMetricsSnapshot {
    pub recovery_latency: HistogramSnapshot,
    pub recoveries_completed: u64,
    pub recoveries_failed: u64,
    pub events_replayed_total: u64,
    pub snapshots_loaded_total: u64,
}

// ============= PersistenceMetricsCollector =============

/// Central collector for all persistence metrics.
pub struct PersistenceMetricsCollector {
    pub journal: Arc<JournalMetrics>,
    pub snapshots: Arc<SnapshotMetrics>,
    pub recovery: Arc<RecoveryMetrics>,
}

impl PersistenceMetricsCollector {
    pub fn new() -> Self {
        Self {
            journal: Arc::new(JournalMetrics::new()),
            snapshots: Arc::new(SnapshotMetrics::new()),
            recovery: Arc::new(RecoveryMetrics::new()),
        }
    }

    /// Get a full metrics snapshot.
    pub fn snapshot(&self) -> PersistenceMetricsSnapshot {
        PersistenceMetricsSnapshot {
            journal: self.journal.snapshot(),
            snapshots: self.snapshots.snapshot(),
            recovery: self.recovery.snapshot(),
        }
    }
}

impl Clone for PersistenceMetricsCollector {
    fn clone(&self) -> Self {
        Self {
            journal: self.journal.clone(),
            snapshots: self.snapshots.clone(),
            recovery: self.recovery.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistenceMetricsSnapshot {
    pub journal: JournalMetricsSnapshot,
    pub snapshots: SnapshotMetricsSnapshot,
    pub recovery: RecoveryMetricsSnapshot,
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Histogram ----

    #[test]
    fn test_histogram_basic() {
        let h = Histogram::new();
        h.record(100);
        h.record(200);
        h.record(300);

        assert_eq!(h.count(), 3);
        assert_eq!(h.sum_us(), 600);
        assert_eq!(h.min_us(), Some(100));
        assert_eq!(h.max_us(), Some(300));
        assert!((h.mean_us().unwrap() - 200.0).abs() < 0.01);
    }

    #[test]
    fn test_histogram_empty() {
        let h = Histogram::new();
        assert_eq!(h.count(), 0);
        assert_eq!(h.min_us(), None);
        assert_eq!(h.max_us(), None);
        assert_eq!(h.mean_us(), None);
    }

    #[test]
    fn test_histogram_duration() {
        let h = Histogram::new();
        h.record_duration(Duration::from_millis(10));
        assert_eq!(h.count(), 1);
        assert!(h.sum_us() >= 10_000); // 10ms = 10000us
    }

    #[test]
    fn test_histogram_snapshot() {
        let h = Histogram::new();
        h.record(50);
        h.record(150);

        let snap = h.snapshot();
        assert_eq!(snap.count, 2);
        assert_eq!(snap.min_us, Some(50));
        assert_eq!(snap.max_us, Some(150));
    }

    // ---- Timer ----

    #[test]
    fn test_timer_records_on_stop() {
        let h = Arc::new(Histogram::new());
        let timer = Timer::new(h.clone());
        std::thread::sleep(Duration::from_millis(5));
        let elapsed = timer.stop();
        assert!(elapsed >= Duration::from_millis(4));
        assert_eq!(h.count(), 1);
    }

    // ---- JournalMetrics ----

    #[test]
    fn test_journal_metrics_write() {
        let m = JournalMetrics::new();
        m.record_write(10);
        m.record_write(20);
        m.record_write_failure();

        assert_eq!(m.events_written.load(Ordering::Relaxed), 30);
        assert_eq!(m.write_failures.load(Ordering::Relaxed), 1);
        assert_eq!(m.batch_sizes.count(), 2);
    }

    #[test]
    fn test_journal_metrics_replay() {
        let m = JournalMetrics::new();
        m.record_replay(100);
        assert_eq!(m.events_replayed.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_journal_metrics_snapshot() {
        let m = JournalMetrics::new();
        m.record_write(5);
        m.record_replay(10);

        let snap = m.snapshot();
        assert_eq!(snap.events_written, 5);
        assert_eq!(snap.events_replayed, 10);
    }

    // ---- SnapshotMetrics ----

    #[test]
    fn test_snapshot_metrics() {
        let m = SnapshotMetrics::new();
        m.record_save(1024);
        m.record_save(2048);
        m.record_load();
        m.record_save_failure();

        assert_eq!(m.snapshots_saved.load(Ordering::Relaxed), 2);
        assert_eq!(m.snapshots_loaded.load(Ordering::Relaxed), 1);
        assert_eq!(m.save_failures.load(Ordering::Relaxed), 1);
        assert_eq!(m.snapshot_sizes.count(), 2);
    }

    #[test]
    fn test_snapshot_metrics_snapshot() {
        let m = SnapshotMetrics::new();
        m.record_save(512);

        let snap = m.snapshot();
        assert_eq!(snap.snapshots_saved, 1);
        assert_eq!(snap.snapshot_sizes.count, 1);
    }

    // ---- RecoveryMetrics ----

    #[test]
    fn test_recovery_metrics() {
        let m = RecoveryMetrics::new();
        m.record_recovery(500, true);
        m.record_recovery(300, false);
        m.record_failure();

        assert_eq!(m.recoveries_completed.load(Ordering::Relaxed), 2);
        assert_eq!(m.recoveries_failed.load(Ordering::Relaxed), 1);
        assert_eq!(m.events_replayed_total.load(Ordering::Relaxed), 800);
        assert_eq!(m.snapshots_loaded_total.load(Ordering::Relaxed), 1);
    }

    // ---- PersistenceMetricsCollector ----

    #[test]
    fn test_collector() {
        let collector = PersistenceMetricsCollector::new();
        collector.journal.record_write(10);
        collector.snapshots.record_save(2048);
        collector.recovery.record_recovery(100, true);

        let snap = collector.snapshot();
        assert_eq!(snap.journal.events_written, 10);
        assert_eq!(snap.snapshots.snapshots_saved, 1);
        assert_eq!(snap.recovery.recoveries_completed, 1);
    }

    #[test]
    fn test_collector_clone_shares_state() {
        let c1 = PersistenceMetricsCollector::new();
        let c2 = c1.clone();

        c1.journal.record_write(5);
        assert_eq!(c2.journal.events_written.load(Ordering::Relaxed), 5);
    }
}
