// quota_manager.rs - Producer/Consumer Rate Limiting & Bandwidth Quotas
// Kafka-like quotas for controlling resource usage per client/group

use std::collections::HashMap;
use std::time::{Duration, Instant};

// ============================================================================
// Quota Types
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum QuotaType {
    ProduceRate,
    FetchRate,
    RequestRate,
    Bandwidth,
}

impl std::fmt::Display for QuotaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaType::ProduceRate => write!(f, "ProduceRate"),
            QuotaType::FetchRate => write!(f, "FetchRate"),
            QuotaType::RequestRate => write!(f, "RequestRate"),
            QuotaType::Bandwidth => write!(f, "Bandwidth"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QuotaEntity {
    ClientId(String),
    ConsumerGroup(String),
    User(String),
    IpAddress(String),
    Default,
}

impl std::fmt::Display for QuotaEntity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaEntity::ClientId(id) => write!(f, "client:{}", id),
            QuotaEntity::ConsumerGroup(g) => write!(f, "group:{}", g),
            QuotaEntity::User(u) => write!(f, "user:{}", u),
            QuotaEntity::IpAddress(ip) => write!(f, "ip:{}", ip),
            QuotaEntity::Default => write!(f, "default"),
        }
    }
}

// ============================================================================
// Quota Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct QuotaLimit {
    pub quota_type: QuotaType,
    pub entity: QuotaEntity,
    pub limit_per_second: f64,
    pub burst_limit: Option<f64>,
    pub window: Duration,
}

impl QuotaLimit {
    pub fn new(quota_type: QuotaType, entity: QuotaEntity, limit_per_second: f64) -> Self {
        Self {
            quota_type,
            entity,
            limit_per_second,
            burst_limit: None,
            window: Duration::from_secs(1),
        }
    }

    pub fn with_burst(mut self, burst: f64) -> Self {
        self.burst_limit = Some(burst);
        self
    }

    pub fn with_window(mut self, window: Duration) -> Self {
        self.window = window;
        self
    }

    pub fn effective_burst(&self) -> f64 {
        self.burst_limit.unwrap_or(self.limit_per_second * 2.0)
    }
}

// ============================================================================
// Token Bucket Rate Limiter
// ============================================================================

#[derive(Debug, Clone)]
pub struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_rate: f64,
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            capacity,
            tokens: capacity,
            refill_rate,
            last_refill: Instant::now(),
        }
    }

    pub fn try_consume(&mut self, amount: f64) -> bool {
        self.refill();
        if self.tokens >= amount {
            self.tokens -= amount;
            true
        } else {
            false
        }
    }

    pub fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        self.last_refill = now;
    }

    pub fn available_tokens(&self) -> f64 {
        self.tokens
    }

    pub fn capacity(&self) -> f64 {
        self.capacity
    }

    pub fn refill_rate(&self) -> f64 {
        self.refill_rate
    }

    pub fn utilization(&self) -> f64 {
        if self.capacity == 0.0 {
            0.0
        } else {
            1.0 - (self.tokens / self.capacity)
        }
    }
}

// ============================================================================
// Sliding Window Rate Tracker
// ============================================================================

#[derive(Debug, Clone)]
pub struct SlidingWindowTracker {
    window: Duration,
    events: Vec<(Instant, f64)>,
    total_in_window: f64,
}

impl SlidingWindowTracker {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            events: Vec::new(),
            total_in_window: 0.0,
        }
    }

    pub fn record(&mut self, amount: f64) {
        let now = Instant::now();
        self.events.push((now, amount));
        self.total_in_window += amount;
        self.cleanup(now);
    }

    pub fn current_rate(&mut self) -> f64 {
        let now = Instant::now();
        self.cleanup(now);
        let window_secs = self.window.as_secs_f64();
        if window_secs == 0.0 {
            0.0
        } else {
            self.total_in_window / window_secs
        }
    }

    pub fn total_in_window(&mut self) -> f64 {
        let now = Instant::now();
        self.cleanup(now);
        self.total_in_window
    }

    pub fn event_count(&mut self) -> usize {
        let now = Instant::now();
        self.cleanup(now);
        self.events.len()
    }

    fn cleanup(&mut self, now: Instant) {
        let cutoff = now - self.window;
        let mut removed = 0.0;
        self.events.retain(|(t, amt)| {
            if *t < cutoff {
                removed += amt;
                false
            } else {
                true
            }
        });
        self.total_in_window -= removed;
    }
}

// ============================================================================
// Throttle Result
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum ThrottleResult {
    Allowed,
    Throttled {
        quota_type: QuotaType,
        entity: String,
        delay_ms: u64,
    },
    Rejected {
        quota_type: QuotaType,
        entity: String,
        reason: String,
    },
}

impl ThrottleResult {
    pub fn is_allowed(&self) -> bool {
        matches!(self, ThrottleResult::Allowed)
    }

    pub fn is_throttled(&self) -> bool {
        matches!(self, ThrottleResult::Throttled { .. })
    }

    pub fn is_rejected(&self) -> bool {
        matches!(self, ThrottleResult::Rejected { .. })
    }

    pub fn delay(&self) -> Option<Duration> {
        match self {
            ThrottleResult::Throttled { delay_ms, .. } => Some(Duration::from_millis(*delay_ms)),
            _ => None,
        }
    }
}

// ============================================================================
// Quota Violation
// ============================================================================

#[derive(Debug, Clone)]
pub struct QuotaViolation {
    pub quota_type: QuotaType,
    pub entity: QuotaEntity,
    pub limit: f64,
    pub actual: f64,
    pub timestamp: Instant,
}

impl QuotaViolation {
    pub fn overage(&self) -> f64 {
        (self.actual - self.limit).max(0.0)
    }

    pub fn overage_percent(&self) -> f64 {
        if self.limit == 0.0 {
            if self.actual > 0.0 { f64::INFINITY } else { 0.0 }
        } else {
            (self.actual / self.limit - 1.0).max(0.0) * 100.0
        }
    }
}

// ============================================================================
// Quota Manager
// ============================================================================

#[derive(Debug)]
pub struct QuotaManager {
    quotas: HashMap<String, QuotaLimit>,
    buckets: HashMap<String, TokenBucket>,
    trackers: HashMap<String, SlidingWindowTracker>,
    violations: Vec<QuotaViolation>,
    max_violations_history: usize,
    enabled: bool,
}

impl QuotaManager {
    pub fn new() -> Self {
        Self {
            quotas: HashMap::new(),
            buckets: HashMap::new(),
            trackers: HashMap::new(),
            violations: Vec::new(),
            max_violations_history: 1000,
            enabled: true,
        }
    }

    pub fn with_violations_history(mut self, max: usize) -> Self {
        self.max_violations_history = max;
        self
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn quota_key(quota_type: &QuotaType, entity: &QuotaEntity) -> String {
        format!("{}:{}", quota_type, entity)
    }

    pub fn set_quota(&mut self, limit: QuotaLimit) {
        let key = Self::quota_key(&limit.quota_type, &limit.entity);
        let bucket = TokenBucket::new(limit.effective_burst(), limit.limit_per_second);
        let tracker = SlidingWindowTracker::new(limit.window);
        self.buckets.insert(key.clone(), bucket);
        self.trackers.insert(key.clone(), tracker);
        self.quotas.insert(key, limit);
    }

    pub fn remove_quota(&mut self, quota_type: &QuotaType, entity: &QuotaEntity) -> bool {
        let key = Self::quota_key(quota_type, entity);
        self.buckets.remove(&key);
        self.trackers.remove(&key);
        self.quotas.remove(&key).is_some()
    }

    pub fn get_quota(&self, quota_type: &QuotaType, entity: &QuotaEntity) -> Option<&QuotaLimit> {
        let key = Self::quota_key(quota_type, entity);
        self.quotas.get(&key)
    }

    pub fn check_quota(
        &mut self,
        quota_type: &QuotaType,
        entity: &QuotaEntity,
        amount: f64,
    ) -> ThrottleResult {
        if !self.enabled {
            return ThrottleResult::Allowed;
        }

        let key = Self::quota_key(quota_type, entity);

        // Try specific entity first, then default
        let effective_key = if self.quotas.contains_key(&key) {
            key.clone()
        } else {
            let default_key = Self::quota_key(quota_type, &QuotaEntity::Default);
            if self.quotas.contains_key(&default_key) {
                default_key
            } else {
                return ThrottleResult::Allowed;
            }
        };

        let bucket = match self.buckets.get_mut(&effective_key) {
            Some(b) => b,
            None => return ThrottleResult::Allowed,
        };

        if bucket.try_consume(amount) {
            if let Some(tracker) = self.trackers.get_mut(&effective_key) {
                tracker.record(amount);
            }
            ThrottleResult::Allowed
        } else {
            // Extract values from bucket before dropping the mutable borrow
            let available = bucket.available_tokens();
            let refill = bucket.refill_rate();

            let limit_per_sec = self.quotas.get(&effective_key)
                .map(|l| l.limit_per_second)
                .unwrap_or(0.0);

            let violation = QuotaViolation {
                quota_type: quota_type.clone(),
                entity: entity.clone(),
                limit: limit_per_sec,
                actual: amount,
                timestamp: Instant::now(),
            };
            self.record_violation(violation);

            let tokens_needed = amount - available;
            let delay_secs = if refill > 0.0 { tokens_needed / refill } else { 1.0 };
            let delay_ms = (delay_secs * 1000.0).ceil() as u64;

            ThrottleResult::Throttled {
                quota_type: quota_type.clone(),
                entity: entity.to_string(),
                delay_ms,
            }
        }
    }

    fn record_violation(&mut self, violation: QuotaViolation) {
        self.violations.push(violation);
        if self.violations.len() > self.max_violations_history {
            self.violations.remove(0);
        }
    }

    pub fn violations(&self) -> &[QuotaViolation] {
        &self.violations
    }

    pub fn violation_count(&self) -> usize {
        self.violations.len()
    }

    pub fn clear_violations(&mut self) {
        self.violations.clear();
    }

    pub fn quota_count(&self) -> usize {
        self.quotas.len()
    }

    pub fn all_quotas(&self) -> Vec<&QuotaLimit> {
        self.quotas.values().collect()
    }

    pub fn stats(&self) -> QuotaManagerStats {
        let mut entity_stats: HashMap<String, EntityQuotaStats> = HashMap::new();

        for (key, quota) in &self.quotas {
            let bucket = self.buckets.get(key);
            let utilization = bucket.map(|b| b.utilization()).unwrap_or(0.0);
            let available = bucket.map(|b| b.available_tokens()).unwrap_or(0.0);

            entity_stats.insert(
                key.clone(),
                EntityQuotaStats {
                    quota_type: quota.quota_type.clone(),
                    entity: quota.entity.clone(),
                    limit_per_second: quota.limit_per_second,
                    utilization,
                    available_tokens: available,
                },
            );
        }

        QuotaManagerStats {
            total_quotas: self.quotas.len(),
            total_violations: self.violations.len(),
            enabled: self.enabled,
            entity_stats,
        }
    }
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Quota Manager Stats
// ============================================================================

#[derive(Debug, Clone)]
pub struct EntityQuotaStats {
    pub quota_type: QuotaType,
    pub entity: QuotaEntity,
    pub limit_per_second: f64,
    pub utilization: f64,
    pub available_tokens: f64,
}

#[derive(Debug, Clone)]
pub struct QuotaManagerStats {
    pub total_quotas: usize,
    pub total_violations: usize,
    pub enabled: bool,
    pub entity_stats: HashMap<String, EntityQuotaStats>,
}

// ============================================================================
// Quota Manager Builder
// ============================================================================

pub struct QuotaManagerBuilder {
    quotas: Vec<QuotaLimit>,
    max_violations: usize,
    enabled: bool,
}

impl QuotaManagerBuilder {
    pub fn new() -> Self {
        Self {
            quotas: Vec::new(),
            max_violations: 1000,
            enabled: true,
        }
    }

    pub fn add_quota(mut self, limit: QuotaLimit) -> Self {
        self.quotas.push(limit);
        self
    }

    pub fn produce_rate(self, entity: QuotaEntity, rate: f64) -> Self {
        self.add_quota(QuotaLimit::new(QuotaType::ProduceRate, entity, rate))
    }

    pub fn fetch_rate(self, entity: QuotaEntity, rate: f64) -> Self {
        self.add_quota(QuotaLimit::new(QuotaType::FetchRate, entity, rate))
    }

    pub fn request_rate(self, entity: QuotaEntity, rate: f64) -> Self {
        self.add_quota(QuotaLimit::new(QuotaType::RequestRate, entity, rate))
    }

    pub fn bandwidth(self, entity: QuotaEntity, bytes_per_second: f64) -> Self {
        self.add_quota(QuotaLimit::new(QuotaType::Bandwidth, entity, bytes_per_second))
    }

    pub fn max_violations(mut self, max: usize) -> Self {
        self.max_violations = max;
        self
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn build(self) -> QuotaManager {
        let mut manager = QuotaManager::new()
            .with_violations_history(self.max_violations);
        manager.set_enabled(self.enabled);
        for quota in self.quotas {
            manager.set_quota(quota);
        }
        manager
    }
}

impl Default for QuotaManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_type_display() {
        assert_eq!(QuotaType::ProduceRate.to_string(), "ProduceRate");
        assert_eq!(QuotaType::FetchRate.to_string(), "FetchRate");
        assert_eq!(QuotaType::RequestRate.to_string(), "RequestRate");
        assert_eq!(QuotaType::Bandwidth.to_string(), "Bandwidth");
    }

    #[test]
    fn test_quota_entity_display() {
        assert_eq!(QuotaEntity::ClientId("c1".into()).to_string(), "client:c1");
        assert_eq!(QuotaEntity::ConsumerGroup("g1".into()).to_string(), "group:g1");
        assert_eq!(QuotaEntity::User("u1".into()).to_string(), "user:u1");
        assert_eq!(QuotaEntity::IpAddress("127.0.0.1".into()).to_string(), "ip:127.0.0.1");
        assert_eq!(QuotaEntity::Default.to_string(), "default");
    }

    #[test]
    fn test_quota_limit_creation() {
        let limit = QuotaLimit::new(
            QuotaType::ProduceRate,
            QuotaEntity::ClientId("producer-1".into()),
            1000.0,
        );
        assert_eq!(limit.limit_per_second, 1000.0);
        assert!(limit.burst_limit.is_none());
        assert_eq!(limit.effective_burst(), 2000.0);
    }

    #[test]
    fn test_quota_limit_with_burst() {
        let limit = QuotaLimit::new(
            QuotaType::ProduceRate,
            QuotaEntity::Default,
            500.0,
        ).with_burst(1500.0);
        assert_eq!(limit.burst_limit, Some(1500.0));
        assert_eq!(limit.effective_burst(), 1500.0);
    }

    #[test]
    fn test_quota_limit_with_window() {
        let limit = QuotaLimit::new(
            QuotaType::Bandwidth,
            QuotaEntity::Default,
            1_000_000.0,
        ).with_window(Duration::from_secs(10));
        assert_eq!(limit.window, Duration::from_secs(10));
    }

    #[test]
    fn test_token_bucket_basic() {
        let mut bucket = TokenBucket::new(10.0, 5.0);
        assert_eq!(bucket.capacity(), 10.0);
        assert_eq!(bucket.refill_rate(), 5.0);
        assert!(bucket.try_consume(5.0));
        assert!(bucket.try_consume(5.0));
        assert!(!bucket.try_consume(1.0));
    }

    #[test]
    fn test_token_bucket_utilization() {
        let mut bucket = TokenBucket::new(100.0, 10.0);
        assert_eq!(bucket.utilization(), 0.0);
        bucket.try_consume(50.0);
        let util = bucket.utilization();
        assert!(util > 0.4 && util < 0.6, "utilization was {}", util);
    }

    #[test]
    fn test_token_bucket_partial_consume() {
        let mut bucket = TokenBucket::new(10.0, 1.0);
        assert!(bucket.try_consume(7.0));
        assert!(!bucket.try_consume(5.0));
        assert!(bucket.try_consume(3.0));
    }

    #[test]
    fn test_sliding_window_tracker() {
        let mut tracker = SlidingWindowTracker::new(Duration::from_secs(60));
        tracker.record(100.0);
        tracker.record(200.0);
        tracker.record(300.0);
        assert_eq!(tracker.event_count(), 3);
        let total = tracker.total_in_window();
        assert_eq!(total, 600.0);
    }

    #[test]
    fn test_sliding_window_rate() {
        let mut tracker = SlidingWindowTracker::new(Duration::from_secs(1));
        tracker.record(100.0);
        let rate = tracker.current_rate();
        assert!(rate >= 90.0, "rate was {}", rate);
    }

    #[test]
    fn test_throttle_result_allowed() {
        let result = ThrottleResult::Allowed;
        assert!(result.is_allowed());
        assert!(!result.is_throttled());
        assert!(!result.is_rejected());
        assert!(result.delay().is_none());
    }

    #[test]
    fn test_throttle_result_throttled() {
        let result = ThrottleResult::Throttled {
            quota_type: QuotaType::ProduceRate,
            entity: "client:p1".into(),
            delay_ms: 500,
        };
        assert!(!result.is_allowed());
        assert!(result.is_throttled());
        assert_eq!(result.delay(), Some(Duration::from_millis(500)));
    }

    #[test]
    fn test_throttle_result_rejected() {
        let result = ThrottleResult::Rejected {
            quota_type: QuotaType::Bandwidth,
            entity: "default".into(),
            reason: "hard limit exceeded".into(),
        };
        assert!(result.is_rejected());
        assert!(result.delay().is_none());
    }

    #[test]
    fn test_quota_violation_overage() {
        let violation = QuotaViolation {
            quota_type: QuotaType::ProduceRate,
            entity: QuotaEntity::ClientId("p1".into()),
            limit: 100.0,
            actual: 150.0,
            timestamp: Instant::now(),
        };
        assert_eq!(violation.overage(), 50.0);
        assert_eq!(violation.overage_percent(), 50.0);
    }

    #[test]
    fn test_quota_violation_no_overage() {
        let violation = QuotaViolation {
            quota_type: QuotaType::FetchRate,
            entity: QuotaEntity::Default,
            limit: 200.0,
            actual: 100.0,
            timestamp: Instant::now(),
        };
        assert_eq!(violation.overage(), 0.0);
        assert_eq!(violation.overage_percent(), 0.0);
    }

    #[test]
    fn test_quota_manager_new() {
        let manager = QuotaManager::new();
        assert_eq!(manager.quota_count(), 0);
        assert!(manager.is_enabled());
        assert_eq!(manager.violation_count(), 0);
    }

    #[test]
    fn test_quota_manager_set_and_get() {
        let mut manager = QuotaManager::new();
        let limit = QuotaLimit::new(
            QuotaType::ProduceRate,
            QuotaEntity::ClientId("p1".into()),
            1000.0,
        );
        manager.set_quota(limit);
        assert_eq!(manager.quota_count(), 1);
        let q = manager.get_quota(
            &QuotaType::ProduceRate,
            &QuotaEntity::ClientId("p1".into()),
        );
        assert!(q.is_some());
        assert_eq!(q.unwrap().limit_per_second, 1000.0);
    }

    #[test]
    fn test_quota_manager_remove() {
        let mut manager = QuotaManager::new();
        manager.set_quota(QuotaLimit::new(
            QuotaType::FetchRate,
            QuotaEntity::Default,
            500.0,
        ));
        assert_eq!(manager.quota_count(), 1);
        assert!(manager.remove_quota(&QuotaType::FetchRate, &QuotaEntity::Default));
        assert_eq!(manager.quota_count(), 0);
        assert!(!manager.remove_quota(&QuotaType::FetchRate, &QuotaEntity::Default));
    }

    #[test]
    fn test_quota_manager_check_allowed() {
        let mut manager = QuotaManager::new();
        manager.set_quota(QuotaLimit::new(
            QuotaType::ProduceRate,
            QuotaEntity::ClientId("p1".into()),
            1000.0,
        ));
        let result = manager.check_quota(
            &QuotaType::ProduceRate,
            &QuotaEntity::ClientId("p1".into()),
            100.0,
        );
        assert!(result.is_allowed());
    }

    #[test]
    fn test_quota_manager_check_throttled() {
        let mut manager = QuotaManager::new();
        manager.set_quota(QuotaLimit::new(
            QuotaType::ProduceRate,
            QuotaEntity::ClientId("p1".into()),
            10.0,
        ).with_burst(20.0));

        // Consume all tokens
        let _ = manager.check_quota(
            &QuotaType::ProduceRate,
            &QuotaEntity::ClientId("p1".into()),
            20.0,
        );
        // Should be throttled now
        let result = manager.check_quota(
            &QuotaType::ProduceRate,
            &QuotaEntity::ClientId("p1".into()),
            5.0,
        );
        assert!(result.is_throttled());
        assert!(manager.violation_count() > 0);
    }

    #[test]
    fn test_quota_manager_no_quota_allows() {
        let mut manager = QuotaManager::new();
        let result = manager.check_quota(
            &QuotaType::ProduceRate,
            &QuotaEntity::ClientId("unknown".into()),
            9999.0,
        );
        assert!(result.is_allowed());
    }

    #[test]
    fn test_quota_manager_default_fallback() {
        let mut manager = QuotaManager::new();
        manager.set_quota(QuotaLimit::new(
            QuotaType::ProduceRate,
            QuotaEntity::Default,
            100.0,
        ).with_burst(100.0));

        // Unknown client should fall back to default
        let result = manager.check_quota(
            &QuotaType::ProduceRate,
            &QuotaEntity::ClientId("unknown".into()),
            50.0,
        );
        assert!(result.is_allowed());
    }

    #[test]
    fn test_quota_manager_disabled() {
        let mut manager = QuotaManager::new();
        manager.set_quota(QuotaLimit::new(
            QuotaType::ProduceRate,
            QuotaEntity::Default,
            1.0,
        ).with_burst(1.0));
        manager.set_enabled(false);

        let result = manager.check_quota(
            &QuotaType::ProduceRate,
            &QuotaEntity::Default,
            99999.0,
        );
        assert!(result.is_allowed());
    }

    #[test]
    fn test_quota_manager_violations() {
        let mut manager = QuotaManager::new();
        manager.set_quota(QuotaLimit::new(
            QuotaType::ProduceRate,
            QuotaEntity::Default,
            10.0,
        ).with_burst(10.0));

        // Exhaust tokens
        let _ = manager.check_quota(&QuotaType::ProduceRate, &QuotaEntity::Default, 10.0);
        let _ = manager.check_quota(&QuotaType::ProduceRate, &QuotaEntity::Default, 5.0);

        assert!(manager.violation_count() > 0);
        manager.clear_violations();
        assert_eq!(manager.violation_count(), 0);
    }

    #[test]
    fn test_quota_manager_stats() {
        let mut manager = QuotaManager::new();
        manager.set_quota(QuotaLimit::new(QuotaType::ProduceRate, QuotaEntity::Default, 100.0));
        manager.set_quota(QuotaLimit::new(QuotaType::FetchRate, QuotaEntity::Default, 200.0));
        let stats = manager.stats();
        assert_eq!(stats.total_quotas, 2);
        assert_eq!(stats.total_violations, 0);
        assert!(stats.enabled);
    }

    #[test]
    fn test_quota_manager_all_quotas() {
        let mut manager = QuotaManager::new();
        manager.set_quota(QuotaLimit::new(QuotaType::ProduceRate, QuotaEntity::Default, 100.0));
        manager.set_quota(QuotaLimit::new(QuotaType::Bandwidth, QuotaEntity::Default, 1_000_000.0));
        assert_eq!(manager.all_quotas().len(), 2);
    }

    #[test]
    fn test_quota_manager_builder() {
        let manager = QuotaManagerBuilder::new()
            .produce_rate(QuotaEntity::Default, 1000.0)
            .fetch_rate(QuotaEntity::Default, 2000.0)
            .bandwidth(QuotaEntity::ClientId("big-producer".into()), 10_000_000.0)
            .max_violations(500)
            .build();
        assert_eq!(manager.quota_count(), 3);
        assert!(manager.is_enabled());
    }

    #[test]
    fn test_quota_manager_builder_disabled() {
        let manager = QuotaManagerBuilder::new()
            .produce_rate(QuotaEntity::Default, 100.0)
            .enabled(false)
            .build();
        assert!(!manager.is_enabled());
    }

    #[test]
    fn test_quota_manager_builder_request_rate() {
        let manager = QuotaManagerBuilder::new()
            .request_rate(QuotaEntity::IpAddress("10.0.0.1".into()), 50.0)
            .build();
        let q = manager.get_quota(
            &QuotaType::RequestRate,
            &QuotaEntity::IpAddress("10.0.0.1".into()),
        );
        assert!(q.is_some());
        assert_eq!(q.unwrap().limit_per_second, 50.0);
    }

    #[test]
    fn test_token_bucket_zero_capacity() {
        let mut bucket = TokenBucket::new(0.0, 0.0);
        assert!(!bucket.try_consume(1.0));
        assert_eq!(bucket.utilization(), 0.0);
    }

    #[test]
    fn test_quota_violation_zero_limit() {
        let violation = QuotaViolation {
            quota_type: QuotaType::ProduceRate,
            entity: QuotaEntity::Default,
            limit: 0.0,
            actual: 10.0,
            timestamp: Instant::now(),
        };
        assert_eq!(violation.overage(), 10.0);
        assert!(violation.overage_percent().is_infinite());
    }

    #[test]
    fn test_sliding_window_empty() {
        let mut tracker = SlidingWindowTracker::new(Duration::from_secs(60));
        assert_eq!(tracker.event_count(), 0);
        assert_eq!(tracker.total_in_window(), 0.0);
        assert_eq!(tracker.current_rate(), 0.0);
    }

    #[test]
    fn test_quota_manager_max_violations_history() {
        let mut manager = QuotaManager::new().with_violations_history(3);
        manager.set_quota(QuotaLimit::new(
            QuotaType::ProduceRate,
            QuotaEntity::Default,
            1.0,
        ).with_burst(1.0));

        // Exhaust tokens
        let _ = manager.check_quota(&QuotaType::ProduceRate, &QuotaEntity::Default, 1.0);
        // Generate 5 violations
        for _ in 0..5 {
            let _ = manager.check_quota(&QuotaType::ProduceRate, &QuotaEntity::Default, 1.0);
        }
        assert!(manager.violation_count() <= 3);
    }
}
