// bus_metrics.rs - EventBus Metrics & Monitoring
// Consumer lag, throughput, partition health, alerting

use std::collections::HashMap;
use std::time::{Duration, Instant};

// ============================================================================
// Metric Counter
// ============================================================================

#[derive(Debug, Clone)]
pub struct Counter {
    value: u64,
    name: String,
}

impl Counter {
    pub fn new(name: &str) -> Self {
        Self {
            value: 0,
            name: name.to_string(),
        }
    }

    pub fn increment(&mut self) {
        self.value += 1;
    }

    pub fn increment_by(&mut self, n: u64) {
        self.value += n;
    }

    pub fn value(&self) -> u64 {
        self.value
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn reset(&mut self) {
        self.value = 0;
    }
}

// ============================================================================
// Metric Gauge
// ============================================================================

#[derive(Debug, Clone)]
pub struct Gauge {
    value: f64,
    name: String,
}

impl Gauge {
    pub fn new(name: &str) -> Self {
        Self {
            value: 0.0,
            name: name.to_string(),
        }
    }

    pub fn set(&mut self, v: f64) {
        self.value = v;
    }

    pub fn value(&self) -> f64 {
        self.value
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

// ============================================================================
// Rate Meter
// ============================================================================

#[derive(Debug, Clone)]
pub struct RateMeter {
    name: String,
    window_counts: Vec<(Instant, u64)>,
    window_size: Duration,
}

impl RateMeter {
    pub fn new(name: &str, window_size: Duration) -> Self {
        Self {
            name: name.to_string(),
            window_counts: Vec::new(),
            window_size,
        }
    }

    pub fn mark(&mut self, count: u64) {
        self.window_counts.push((Instant::now(), count));
        self.prune();
    }

    /// Calculate rate (events per second) over the window
    pub fn rate(&self) -> f64 {
        let cutoff = Instant::now() - self.window_size;
        let total: u64 = self.window_counts.iter()
            .filter(|(t, _)| *t >= cutoff)
            .map(|(_, c)| c)
            .sum();
        let secs = self.window_size.as_secs_f64();
        if secs > 0.0 { total as f64 / secs } else { 0.0 }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn prune(&mut self) {
        let cutoff = Instant::now() - self.window_size;
        self.window_counts.retain(|(t, _)| *t >= cutoff);
    }
}

// ============================================================================
// Consumer Lag
// ============================================================================

/// Tracks consumer lag: difference between latest partition offset and committed offset
#[derive(Debug, Clone)]
pub struct ConsumerLag {
    pub group_id: String,
    pub topic: String,
    pub partition_id: u32,
    pub latest_offset: u64,
    pub committed_offset: u64,
}

impl ConsumerLag {
    pub fn new(
        group_id: &str,
        topic: &str,
        partition_id: u32,
        latest_offset: u64,
        committed_offset: u64,
    ) -> Self {
        Self {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition_id,
            latest_offset,
            committed_offset,
        }
    }

    /// The lag (number of unconsumed events)
    pub fn lag(&self) -> u64 {
        self.latest_offset.saturating_sub(self.committed_offset)
    }

    /// Whether the consumer is caught up (lag == 0)
    pub fn is_caught_up(&self) -> bool {
        self.lag() == 0
    }
}

// ============================================================================
// Partition Health
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum PartitionHealth {
    Healthy,
    Warning { reason: String },
    Critical { reason: String },
}

#[derive(Debug, Clone)]
pub struct PartitionHealthCheck {
    pub topic: String,
    pub partition_id: u32,
    pub health: PartitionHealth,
    pub buffer_fill_ratio: f64,
    pub events_per_second: f64,
    pub consumer_lag: u64,
}

// ============================================================================
// Alert
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone)]
pub struct Alert {
    pub severity: AlertSeverity,
    pub source: String,
    pub message: String,
    pub timestamp: Instant,
}

impl Alert {
    pub fn info(source: &str, message: &str) -> Self {
        Self {
            severity: AlertSeverity::Info,
            source: source.to_string(),
            message: message.to_string(),
            timestamp: Instant::now(),
        }
    }

    pub fn warning(source: &str, message: &str) -> Self {
        Self {
            severity: AlertSeverity::Warning,
            source: source.to_string(),
            message: message.to_string(),
            timestamp: Instant::now(),
        }
    }

    pub fn critical(source: &str, message: &str) -> Self {
        Self {
            severity: AlertSeverity::Critical,
            source: source.to_string(),
            message: message.to_string(),
            timestamp: Instant::now(),
        }
    }
}

// ============================================================================
// Alert Rules
// ============================================================================

pub struct AlertRule {
    pub name: String,
    pub check: Box<dyn Fn(&BusMetricsSnapshot) -> Option<Alert> + Send + Sync>,
}

impl AlertRule {
    /// Create a consumer lag alert rule
    pub fn consumer_lag_threshold(group_id: &str, threshold: u64) -> Self {
        let group = group_id.to_string();
        Self {
            name: format!("consumer-lag-{}", group_id),
            check: Box::new(move |snapshot: &BusMetricsSnapshot| {
                for lag in &snapshot.consumer_lags {
                    if lag.group_id == group && lag.lag() > threshold {
                        return Some(Alert::warning(
                            &format!("consumer-lag/{}", group),
                            &format!(
                                "Consumer lag for {}/{}/{} is {} (threshold: {})",
                                lag.group_id, lag.topic, lag.partition_id,
                                lag.lag(), threshold
                            ),
                        ));
                    }
                }
                None
            }),
        }
    }

    /// Create a buffer fill alert rule
    pub fn buffer_fill_threshold(threshold: f64) -> Self {
        Self {
            name: "buffer-fill".to_string(),
            check: Box::new(move |snapshot: &BusMetricsSnapshot| {
                for health in &snapshot.partition_health {
                    if health.buffer_fill_ratio > threshold {
                        return Some(Alert::warning(
                            &format!("buffer-fill/{}/{}", health.topic, health.partition_id),
                            &format!(
                                "Buffer fill for {}/{} at {:.1}% (threshold: {:.1}%)",
                                health.topic, health.partition_id,
                                health.buffer_fill_ratio * 100.0,
                                threshold * 100.0
                            ),
                        ));
                    }
                }
                None
            }),
        }
    }
}

// ============================================================================
// Bus Metrics Collector
// ============================================================================

/// Central metrics collector for the EventBus
pub struct BusMetricsCollector {
    // Counters
    pub events_published: Counter,
    pub events_consumed: Counter,
    pub publish_errors: Counter,
    pub rebalance_count: Counter,

    // Gauges
    pub active_topics: Gauge,
    pub active_partitions: Gauge,
    pub active_consumer_groups: Gauge,

    // Rate meters
    pub publish_rate: RateMeter,
    pub consume_rate: RateMeter,

    // Per-topic counters
    pub topic_publish_counts: HashMap<String, Counter>,

    // Alert rules
    alert_rules: Vec<AlertRule>,
    alerts: Vec<Alert>,
    max_alerts: usize,
}

impl BusMetricsCollector {
    pub fn new() -> Self {
        Self {
            events_published: Counter::new("events_published"),
            events_consumed: Counter::new("events_consumed"),
            publish_errors: Counter::new("publish_errors"),
            rebalance_count: Counter::new("rebalance_count"),
            active_topics: Gauge::new("active_topics"),
            active_partitions: Gauge::new("active_partitions"),
            active_consumer_groups: Gauge::new("active_consumer_groups"),
            publish_rate: RateMeter::new("publish_rate", Duration::from_secs(60)),
            consume_rate: RateMeter::new("consume_rate", Duration::from_secs(60)),
            topic_publish_counts: HashMap::new(),
            alert_rules: Vec::new(),
            alerts: Vec::new(),
            max_alerts: 1000,
        }
    }

    /// Record a successful publish
    pub fn record_publish(&mut self, topic: &str) {
        self.events_published.increment();
        self.publish_rate.mark(1);

        self.topic_publish_counts
            .entry(topic.to_string())
            .or_insert_with(|| Counter::new(&format!("topic_{}_published", topic)))
            .increment();
    }

    /// Record a publish error
    pub fn record_publish_error(&mut self) {
        self.publish_errors.increment();
    }

    /// Record events consumed
    pub fn record_consume(&mut self, count: u64) {
        self.events_consumed.increment_by(count);
        self.consume_rate.mark(count);
    }

    /// Record a rebalance event
    pub fn record_rebalance(&mut self) {
        self.rebalance_count.increment();
    }

    /// Update gauge values
    pub fn update_gauges(&mut self, topics: u32, partitions: u32, consumer_groups: u32) {
        self.active_topics.set(topics as f64);
        self.active_partitions.set(partitions as f64);
        self.active_consumer_groups.set(consumer_groups as f64);
    }

    /// Add an alert rule
    pub fn add_alert_rule(&mut self, rule: AlertRule) {
        self.alert_rules.push(rule);
    }

    /// Evaluate all alert rules against a snapshot
    pub fn evaluate_alerts(&mut self, snapshot: &BusMetricsSnapshot) -> Vec<Alert> {
        let mut new_alerts = Vec::new();
        for rule in &self.alert_rules {
            if let Some(alert) = (rule.check)(snapshot) {
                new_alerts.push(alert.clone());
                self.alerts.push(alert);
            }
        }

        // Trim old alerts
        if self.alerts.len() > self.max_alerts {
            let drain_count = self.alerts.len() - self.max_alerts;
            self.alerts.drain(0..drain_count);
        }

        new_alerts
    }

    /// Get all recorded alerts
    pub fn alerts(&self) -> &[Alert] {
        &self.alerts
    }

    /// Create a snapshot of current metrics
    pub fn snapshot(&self) -> BusMetricsSnapshot {
        BusMetricsSnapshot {
            events_published: self.events_published.value(),
            events_consumed: self.events_consumed.value(),
            publish_errors: self.publish_errors.value(),
            rebalance_count: self.rebalance_count.value(),
            active_topics: self.active_topics.value() as u32,
            active_partitions: self.active_partitions.value() as u32,
            active_consumer_groups: self.active_consumer_groups.value() as u32,
            publish_rate: self.publish_rate.rate(),
            consume_rate: self.consume_rate.rate(),
            consumer_lags: Vec::new(),    // populated externally
            partition_health: Vec::new(), // populated externally
        }
    }

    /// Get topic-level publish count
    pub fn topic_publish_count(&self, topic: &str) -> u64 {
        self.topic_publish_counts.get(topic).map(|c| c.value()).unwrap_or(0)
    }
}

impl Default for BusMetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Metrics Snapshot
// ============================================================================

#[derive(Debug, Clone)]
pub struct BusMetricsSnapshot {
    pub events_published: u64,
    pub events_consumed: u64,
    pub publish_errors: u64,
    pub rebalance_count: u64,
    pub active_topics: u32,
    pub active_partitions: u32,
    pub active_consumer_groups: u32,
    pub publish_rate: f64,
    pub consume_rate: f64,
    pub consumer_lags: Vec<ConsumerLag>,
    pub partition_health: Vec<PartitionHealthCheck>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- Counter tests ---

    #[test]
    fn test_counter_increment() {
        let mut c = Counter::new("test");
        assert_eq!(c.value(), 0);
        c.increment();
        assert_eq!(c.value(), 1);
        c.increment_by(5);
        assert_eq!(c.value(), 6);
    }

    #[test]
    fn test_counter_reset() {
        let mut c = Counter::new("test");
        c.increment_by(100);
        c.reset();
        assert_eq!(c.value(), 0);
    }

    #[test]
    fn test_counter_name() {
        let c = Counter::new("my_counter");
        assert_eq!(c.name(), "my_counter");
    }

    // --- Gauge tests ---

    #[test]
    fn test_gauge_set_and_get() {
        let mut g = Gauge::new("cpu");
        assert_eq!(g.value(), 0.0);
        g.set(42.5);
        assert_eq!(g.value(), 42.5);
    }

    #[test]
    fn test_gauge_name() {
        let g = Gauge::new("memory");
        assert_eq!(g.name(), "memory");
    }

    // --- RateMeter tests ---

    #[test]
    fn test_rate_meter_basic() {
        let mut rm = RateMeter::new("events", Duration::from_secs(1));
        rm.mark(100);
        let rate = rm.rate();
        assert!(rate > 0.0); // should be ~100/s
    }

    #[test]
    fn test_rate_meter_name() {
        let rm = RateMeter::new("publish", Duration::from_secs(60));
        assert_eq!(rm.name(), "publish");
    }

    #[test]
    fn test_rate_meter_empty() {
        let rm = RateMeter::new("empty", Duration::from_secs(60));
        assert_eq!(rm.rate(), 0.0);
    }

    // --- ConsumerLag tests ---

    #[test]
    fn test_consumer_lag_calculation() {
        let lag = ConsumerLag::new("billing", "orders", 0, 100, 80);
        assert_eq!(lag.lag(), 20);
        assert!(!lag.is_caught_up());
    }

    #[test]
    fn test_consumer_lag_caught_up() {
        let lag = ConsumerLag::new("billing", "orders", 0, 50, 50);
        assert_eq!(lag.lag(), 0);
        assert!(lag.is_caught_up());
    }

    #[test]
    fn test_consumer_lag_underflow_safety() {
        // committed ahead of latest (shouldn't happen, but safe)
        let lag = ConsumerLag::new("billing", "orders", 0, 10, 20);
        assert_eq!(lag.lag(), 0); // saturating_sub
    }

    // --- PartitionHealth tests ---

    #[test]
    fn test_partition_health_variants() {
        assert_eq!(PartitionHealth::Healthy, PartitionHealth::Healthy);
        let w = PartitionHealth::Warning { reason: "high lag".into() };
        assert_ne!(w, PartitionHealth::Healthy);
    }

    // --- Alert tests ---

    #[test]
    fn test_alert_creation() {
        let a = Alert::info("test", "hello");
        assert_eq!(a.severity, AlertSeverity::Info);
        assert_eq!(a.source, "test");

        let a = Alert::warning("test", "warn");
        assert_eq!(a.severity, AlertSeverity::Warning);

        let a = Alert::critical("test", "crit");
        assert_eq!(a.severity, AlertSeverity::Critical);
    }

    // --- BusMetricsCollector tests ---

    #[test]
    fn test_metrics_collector_publish() {
        let mut metrics = BusMetricsCollector::new();
        metrics.record_publish("orders");
        metrics.record_publish("orders");
        metrics.record_publish("payments");

        assert_eq!(metrics.events_published.value(), 3);
        assert_eq!(metrics.topic_publish_count("orders"), 2);
        assert_eq!(metrics.topic_publish_count("payments"), 1);
        assert_eq!(metrics.topic_publish_count("unknown"), 0);
    }

    #[test]
    fn test_metrics_collector_consume() {
        let mut metrics = BusMetricsCollector::new();
        metrics.record_consume(10);
        metrics.record_consume(5);
        assert_eq!(metrics.events_consumed.value(), 15);
    }

    #[test]
    fn test_metrics_collector_errors() {
        let mut metrics = BusMetricsCollector::new();
        metrics.record_publish_error();
        metrics.record_publish_error();
        assert_eq!(metrics.publish_errors.value(), 2);
    }

    #[test]
    fn test_metrics_collector_rebalance() {
        let mut metrics = BusMetricsCollector::new();
        metrics.record_rebalance();
        assert_eq!(metrics.rebalance_count.value(), 1);
    }

    #[test]
    fn test_metrics_collector_gauges() {
        let mut metrics = BusMetricsCollector::new();
        metrics.update_gauges(5, 30, 3);
        assert_eq!(metrics.active_topics.value(), 5.0);
        assert_eq!(metrics.active_partitions.value(), 30.0);
        assert_eq!(metrics.active_consumer_groups.value(), 3.0);
    }

    #[test]
    fn test_metrics_snapshot() {
        let mut metrics = BusMetricsCollector::new();
        metrics.record_publish("orders");
        metrics.record_consume(5);
        metrics.update_gauges(2, 12, 1);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.events_published, 1);
        assert_eq!(snapshot.events_consumed, 5);
        assert_eq!(snapshot.active_topics, 2);
    }

    // --- Alert rule tests ---

    #[test]
    fn test_alert_rule_consumer_lag() {
        let mut metrics = BusMetricsCollector::new();
        metrics.add_alert_rule(AlertRule::consumer_lag_threshold("billing", 100));

        let mut snapshot = metrics.snapshot();
        snapshot.consumer_lags.push(ConsumerLag::new("billing", "orders", 0, 200, 50));

        let alerts = metrics.evaluate_alerts(&snapshot);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].severity, AlertSeverity::Warning);
    }

    #[test]
    fn test_alert_rule_no_trigger() {
        let mut metrics = BusMetricsCollector::new();
        metrics.add_alert_rule(AlertRule::consumer_lag_threshold("billing", 100));

        let mut snapshot = metrics.snapshot();
        snapshot.consumer_lags.push(ConsumerLag::new("billing", "orders", 0, 50, 50));

        let alerts = metrics.evaluate_alerts(&snapshot);
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_alert_rule_buffer_fill() {
        let mut metrics = BusMetricsCollector::new();
        metrics.add_alert_rule(AlertRule::buffer_fill_threshold(0.80));

        let mut snapshot = metrics.snapshot();
        snapshot.partition_health.push(PartitionHealthCheck {
            topic: "orders".into(),
            partition_id: 0,
            health: PartitionHealth::Healthy,
            buffer_fill_ratio: 0.90,
            events_per_second: 100.0,
            consumer_lag: 0,
        });

        let alerts = metrics.evaluate_alerts(&snapshot);
        assert_eq!(alerts.len(), 1);
    }

    #[test]
    fn test_alert_history() {
        let mut metrics = BusMetricsCollector::new();
        metrics.add_alert_rule(AlertRule::consumer_lag_threshold("billing", 10));

        let mut snapshot = metrics.snapshot();
        snapshot.consumer_lags.push(ConsumerLag::new("billing", "orders", 0, 100, 0));

        metrics.evaluate_alerts(&snapshot);
        metrics.evaluate_alerts(&snapshot);
        assert_eq!(metrics.alerts().len(), 2);
    }
}
