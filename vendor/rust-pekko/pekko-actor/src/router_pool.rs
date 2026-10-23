use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// RoutingStrategy
// ============================================================================

/// Strategy for selecting routees from the pool
#[derive(Debug, Clone, PartialEq)]
pub enum PoolRoutingStrategy {
    RoundRobin,
    Random,
    SmallestMailbox,
    ConsistentHash,
    BroadcastPool,
    ScatterGatherFirst { timeout: Duration },
    TailChopping { interval: Duration, within: Duration },
}

// ============================================================================
// PoolRoutee
// ============================================================================

/// A routee (worker) in the pool
#[derive(Debug, Clone)]
pub struct PoolRoutee {
    pub id: u64,
    pub path: String,
    pub state: RouteeState,
    pub messages_processed: u64,
    pub messages_pending: u64,
    pub created_at: Instant,
    pub last_active: Instant,
    pub avg_processing_nanos: u64,
    pub errors: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteeState {
    Active,
    Idle,
    Busy,
    Draining,
    Stopped,
    Failed,
}

impl PoolRoutee {
    pub fn new(id: u64, path: &str) -> Self {
        let now = Instant::now();
        Self {
            id,
            path: path.to_string(),
            state: RouteeState::Idle,
            messages_processed: 0,
            messages_pending: 0,
            created_at: now,
            last_active: now,
            avg_processing_nanos: 0,
            errors: 0,
        }
    }

    pub fn is_available(&self) -> bool {
        matches!(self.state, RouteeState::Active | RouteeState::Idle)
    }

    pub fn utilization(&self) -> f64 {
        if self.messages_processed == 0 && self.messages_pending == 0 {
            return 0.0;
        }
        self.messages_pending as f64 / (self.messages_processed + self.messages_pending).max(1) as f64
    }

    pub fn uptime(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub fn idle_time(&self) -> Duration {
        self.last_active.elapsed()
    }

    pub fn record_message_sent(&mut self) {
        self.messages_pending += 1;
        self.state = RouteeState::Busy;
    }

    pub fn record_message_processed(&mut self, processing_nanos: u64) {
        if self.messages_pending > 0 {
            self.messages_pending -= 1;
        }
        self.messages_processed += 1;
        self.last_active = Instant::now();
        // Running average
        let total = self.avg_processing_nanos * (self.messages_processed - 1) + processing_nanos;
        self.avg_processing_nanos = total / self.messages_processed;
        if self.messages_pending == 0 {
            self.state = RouteeState::Active;
        }
    }

    pub fn record_error(&mut self) {
        self.errors += 1;
        if self.messages_pending > 0 {
            self.messages_pending -= 1;
        }
    }
}

// ============================================================================
// ScalingPolicy
// ============================================================================

/// Auto-scaling configuration
#[derive(Debug, Clone)]
pub struct ScalingPolicy {
    pub min_routees: usize,
    pub max_routees: usize,
    pub scale_up_threshold: f64,    // utilization above this → add routees
    pub scale_down_threshold: f64,  // utilization below this → remove routees
    pub cooldown: Duration,         // minimum time between scaling events
    pub scale_up_step: usize,       // how many to add at once
    pub scale_down_step: usize,     // how many to remove at once
    pub idle_timeout: Duration,     // remove routee if idle longer than this
}

impl Default for ScalingPolicy {
    fn default() -> Self {
        Self {
            min_routees: 1,
            max_routees: 10,
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.2,
            cooldown: Duration::from_secs(30),
            scale_up_step: 1,
            scale_down_step: 1,
            idle_timeout: Duration::from_secs(300),
        }
    }
}

impl ScalingPolicy {
    pub fn fixed(size: usize) -> Self {
        Self {
            min_routees: size,
            max_routees: size,
            ..Default::default()
        }
    }

    pub fn elastic(min: usize, max: usize) -> Self {
        Self {
            min_routees: min,
            max_routees: max,
            ..Default::default()
        }
    }

    pub fn with_thresholds(mut self, up: f64, down: f64) -> Self {
        self.scale_up_threshold = up;
        self.scale_down_threshold = down;
        self
    }

    pub fn with_cooldown(mut self, cooldown: Duration) -> Self {
        self.cooldown = cooldown;
        self
    }

    pub fn with_steps(mut self, up: usize, down: usize) -> Self {
        self.scale_up_step = up;
        self.scale_down_step = down;
        self
    }
}

// ============================================================================
// ScalingDecision
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum ScalingDecision {
    ScaleUp(usize),
    ScaleDown(usize),
    NoChange,
    Cooldown,
}

// ============================================================================
// HealthCheck
// ============================================================================

#[derive(Debug, Clone)]
pub struct HealthCheck {
    pub max_errors: u64,
    pub max_idle: Duration,
    pub max_pending: u64,
    pub max_processing_nanos: u64,
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self {
            max_errors: 10,
            max_idle: Duration::from_secs(600),
            max_pending: 1000,
            max_processing_nanos: 5_000_000_000, // 5s
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}

// ============================================================================
// RouterPool
// ============================================================================

/// Dynamic router pool with auto-scaling and health monitoring
pub struct RouterPool {
    name: String,
    strategy: PoolRoutingStrategy,
    routees: Vec<PoolRoutee>,
    scaling: ScalingPolicy,
    health_check: HealthCheck,
    next_id: u64,
    round_robin_idx: usize,
    last_scale_event: Option<Instant>,
    scale_history: Vec<ScaleEvent>,
    stats: RouterPoolStats,
}

#[derive(Debug, Clone)]
pub struct ScaleEvent {
    pub decision: ScalingDecision,
    pub pool_size_before: usize,
    pub pool_size_after: usize,
    pub avg_utilization: f64,
}

#[derive(Debug, Clone, Default)]
pub struct RouterPoolStats {
    pub messages_routed: u64,
    pub scale_ups: u64,
    pub scale_downs: u64,
    pub routees_created: u64,
    pub routees_removed: u64,
    pub health_checks_run: u64,
    pub unhealthy_removed: u64,
}

impl RouterPool {
    pub fn new(name: &str, strategy: PoolRoutingStrategy, initial_size: usize) -> Self {
        let mut pool = Self {
            name: name.to_string(),
            strategy,
            routees: Vec::new(),
            scaling: ScalingPolicy::default(),
            health_check: HealthCheck::default(),
            next_id: 0,
            round_robin_idx: 0,
            last_scale_event: None,
            scale_history: Vec::new(),
            stats: RouterPoolStats::default(),
        };
        for _ in 0..initial_size {
            pool.add_routee();
        }
        pool
    }

    pub fn with_scaling(mut self, policy: ScalingPolicy) -> Self {
        self.scaling = policy;
        self
    }

    pub fn with_health_check(mut self, check: HealthCheck) -> Self {
        self.health_check = check;
        self
    }

    /// Add a new routee to the pool
    pub fn add_routee(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        let path = format!("/pool/{}/routee-{}", self.name, id);
        self.routees.push(PoolRoutee::new(id, &path));
        self.stats.routees_created += 1;
        id
    }

    /// Remove a routee by id
    pub fn remove_routee(&mut self, id: u64) -> bool {
        if let Some(pos) = self.routees.iter().position(|r| r.id == id) {
            self.routees.remove(pos);
            self.stats.routees_removed += 1;
            true
        } else {
            false
        }
    }

    /// Select next routee based on strategy
    pub fn select_routee(&mut self) -> Option<u64> {
        let available: Vec<usize> = self.routees.iter()
            .enumerate()
            .filter(|(_, r)| r.is_available())
            .map(|(i, _)| i)
            .collect();

        if available.is_empty() {
            return None;
        }

        self.stats.messages_routed += 1;

        let idx = match &self.strategy {
            PoolRoutingStrategy::RoundRobin => {
                let pos = self.round_robin_idx % available.len();
                self.round_robin_idx = self.round_robin_idx.wrapping_add(1);
                available[pos]
            }
            PoolRoutingStrategy::Random => {
                // Deterministic for testing: use messages_routed as seed
                let pos = (self.stats.messages_routed as usize) % available.len();
                available[pos]
            }
            PoolRoutingStrategy::SmallestMailbox => {
                let min_idx = available.iter()
                    .min_by_key(|&&i| self.routees[i].messages_pending)
                    .copied()
                    .unwrap();
                min_idx
            }
            PoolRoutingStrategy::ConsistentHash => {
                // Simple hash based on message count
                let pos = (self.stats.messages_routed as usize) % available.len();
                available[pos]
            }
            PoolRoutingStrategy::BroadcastPool => {
                // Return first; broadcast handled externally
                available[0]
            }
            PoolRoutingStrategy::ScatterGatherFirst { .. } => {
                available[0]
            }
            PoolRoutingStrategy::TailChopping { .. } => {
                available[0]
            }
        };

        let id = self.routees[idx].id;
        self.routees[idx].record_message_sent();
        Some(id)
    }

    /// Get all routee ids for broadcast
    pub fn all_routee_ids(&self) -> Vec<u64> {
        self.routees.iter()
            .filter(|r| r.is_available())
            .map(|r| r.id)
            .collect()
    }

    /// Record successful processing for a routee
    pub fn record_processed(&mut self, routee_id: u64, processing_nanos: u64) {
        if let Some(r) = self.routees.iter_mut().find(|r| r.id == routee_id) {
            r.record_message_processed(processing_nanos);
        }
    }

    /// Record error for a routee
    pub fn record_error(&mut self, routee_id: u64) {
        if let Some(r) = self.routees.iter_mut().find(|r| r.id == routee_id) {
            r.record_error();
        }
    }

    // ── Scaling ──────────────────────────────────────────

    /// Evaluate whether to scale up or down
    pub fn evaluate_scaling(&self) -> ScalingDecision {
        let current = self.routees.len();

        // Check cooldown
        if let Some(last) = self.last_scale_event {
            if last.elapsed() < self.scaling.cooldown {
                return ScalingDecision::Cooldown;
            }
        }

        let avg_util = self.average_utilization();

        if avg_util > self.scaling.scale_up_threshold && current < self.scaling.max_routees {
            let add = self.scaling.scale_up_step.min(self.scaling.max_routees - current);
            ScalingDecision::ScaleUp(add)
        } else if avg_util < self.scaling.scale_down_threshold && current > self.scaling.min_routees {
            let remove = self.scaling.scale_down_step.min(current - self.scaling.min_routees);
            ScalingDecision::ScaleDown(remove)
        } else {
            ScalingDecision::NoChange
        }
    }

    /// Apply a scaling decision
    pub fn apply_scaling(&mut self, decision: ScalingDecision) {
        let before = self.routees.len();
        match &decision {
            ScalingDecision::ScaleUp(n) => {
                for _ in 0..*n {
                    self.add_routee();
                }
                self.stats.scale_ups += 1;
            }
            ScalingDecision::ScaleDown(n) => {
                // Remove least utilized routees
                let mut to_remove = Vec::new();
                let mut sorted_indices: Vec<usize> = (0..self.routees.len()).collect();
                sorted_indices.sort_by(|&a, &b| {
                    self.routees[a].utilization()
                        .partial_cmp(&self.routees[b].utilization())
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                for &idx in sorted_indices.iter().take(*n) {
                    to_remove.push(self.routees[idx].id);
                }
                for id in to_remove {
                    self.remove_routee(id);
                }
                self.stats.scale_downs += 1;
            }
            ScalingDecision::NoChange | ScalingDecision::Cooldown => {}
        }
        let after = self.routees.len();
        if before != after {
            self.last_scale_event = Some(Instant::now());
            self.scale_history.push(ScaleEvent {
                decision,
                pool_size_before: before,
                pool_size_after: after,
                avg_utilization: self.average_utilization(),
            });
        }
    }

    /// Auto-scale: evaluate + apply
    pub fn auto_scale(&mut self) -> ScalingDecision {
        let decision = self.evaluate_scaling();
        self.apply_scaling(decision.clone());
        decision
    }

    // ── Health ───────────────────────────────────────────

    /// Check health of a single routee
    pub fn check_routee_health(&self, routee: &PoolRoutee) -> HealthStatus {
        if routee.errors > self.health_check.max_errors {
            return HealthStatus::Unhealthy(format!("too many errors: {}", routee.errors));
        }
        if routee.messages_pending > self.health_check.max_pending {
            return HealthStatus::Unhealthy(format!("too many pending: {}", routee.messages_pending));
        }
        if routee.avg_processing_nanos > self.health_check.max_processing_nanos {
            return HealthStatus::Degraded(format!("slow processing: {}ns avg", routee.avg_processing_nanos));
        }
        if routee.state == RouteeState::Failed {
            return HealthStatus::Unhealthy("routee failed".to_string());
        }
        HealthStatus::Healthy
    }

    /// Run health check on all routees, remove unhealthy ones
    pub fn run_health_check(&mut self) -> Vec<(u64, HealthStatus)> {
        self.stats.health_checks_run += 1;
        let mut results = Vec::new();
        let mut to_remove = Vec::new();

        for routee in &self.routees {
            let status = self.check_routee_health(routee);
            if matches!(status, HealthStatus::Unhealthy(_)) {
                to_remove.push(routee.id);
            }
            results.push((routee.id, status));
        }

        for id in to_remove {
            if self.routees.len() > self.scaling.min_routees {
                self.remove_routee(id);
                self.stats.unhealthy_removed += 1;
            }
        }

        results
    }

    /// Remove idle routees exceeding idle_timeout
    pub fn remove_idle_routees(&mut self) -> usize {
        let timeout = self.scaling.idle_timeout;
        let min = self.scaling.min_routees;
        let mut removed = 0;
        let idle_ids: Vec<u64> = self.routees.iter()
            .filter(|r| r.state == RouteeState::Idle && r.idle_time() > timeout)
            .map(|r| r.id)
            .collect();

        for id in idle_ids {
            if self.routees.len() > min {
                self.remove_routee(id);
                removed += 1;
            }
        }
        removed
    }

    // ── Metrics ──────────────────────────────────────────

    pub fn average_utilization(&self) -> f64 {
        if self.routees.is_empty() {
            return 0.0;
        }
        let total: f64 = self.routees.iter().map(|r| r.utilization()).sum();
        total / self.routees.len() as f64
    }

    pub fn total_pending(&self) -> u64 {
        self.routees.iter().map(|r| r.messages_pending).sum()
    }

    pub fn total_processed(&self) -> u64 {
        self.routees.iter().map(|r| r.messages_processed).sum()
    }

    pub fn pool_size(&self) -> usize {
        self.routees.len()
    }

    pub fn active_routees(&self) -> usize {
        self.routees.iter().filter(|r| r.is_available()).count()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn strategy(&self) -> &PoolRoutingStrategy {
        &self.strategy
    }

    pub fn routees(&self) -> &[PoolRoutee] {
        &self.routees
    }

    pub fn routee(&self, id: u64) -> Option<&PoolRoutee> {
        self.routees.iter().find(|r| r.id == id)
    }

    pub fn stats(&self) -> &RouterPoolStats {
        &self.stats
    }

    pub fn scale_history(&self) -> &[ScaleEvent] {
        &self.scale_history
    }

    /// Drain a routee (stop sending new messages)
    pub fn drain_routee(&mut self, id: u64) -> bool {
        if let Some(r) = self.routees.iter_mut().find(|r| r.id == id) {
            r.state = RouteeState::Draining;
            true
        } else {
            false
        }
    }

    /// Stop a routee
    pub fn stop_routee(&mut self, id: u64) -> bool {
        if let Some(r) = self.routees.iter_mut().find(|r| r.id == id) {
            r.state = RouteeState::Stopped;
            true
        } else {
            false
        }
    }

    /// Mark a routee as failed
    pub fn fail_routee(&mut self, id: u64) -> bool {
        if let Some(r) = self.routees.iter_mut().find(|r| r.id == id) {
            r.state = RouteeState::Failed;
            true
        } else {
            false
        }
    }

    /// Restart a stopped/failed routee
    pub fn restart_routee(&mut self, id: u64) -> bool {
        if let Some(r) = self.routees.iter_mut().find(|r| r.id == id) {
            if matches!(r.state, RouteeState::Stopped | RouteeState::Failed) {
                r.state = RouteeState::Idle;
                r.errors = 0;
                r.messages_pending = 0;
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}

impl fmt::Debug for RouterPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouterPool")
            .field("name", &self.name)
            .field("strategy", &self.strategy)
            .field("pool_size", &self.routees.len())
            .field("active", &self.active_routees())
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pool(size: usize) -> RouterPool {
        RouterPool::new("test", PoolRoutingStrategy::RoundRobin, size)
    }

    #[test]
    fn test_create_pool() {
        let pool = make_pool(3);
        assert_eq!(pool.pool_size(), 3);
        assert_eq!(pool.active_routees(), 3);
        assert_eq!(pool.name(), "test");
    }

    #[test]
    fn test_round_robin_routing() {
        let mut pool = make_pool(3);
        let r1 = pool.select_routee().unwrap();
        let r2 = pool.select_routee().unwrap();
        let r3 = pool.select_routee().unwrap();
        // Should cycle through routees
        assert_ne!(r1, r2);
        assert_ne!(r2, r3);
        // Fourth should wrap around (but routees might be Busy now)
        pool.record_processed(r1, 1000);
        pool.record_processed(r2, 1000);
        pool.record_processed(r3, 1000);
        let r4 = pool.select_routee().unwrap();
        assert_eq!(r4, r1);
    }

    #[test]
    fn test_smallest_mailbox() {
        let mut pool = RouterPool::new("test", PoolRoutingStrategy::SmallestMailbox, 3);
        // Send 2 messages to first routee
        pool.select_routee(); // goes to smallest (any)
        pool.select_routee();
        // Third should go to one with fewer pending
        let _r = pool.select_routee();
        // At least one routee should have fewer messages
        let min_pending = pool.routees().iter().map(|r| r.messages_pending).min().unwrap();
        assert!(min_pending <= 2);
    }

    #[test]
    fn test_add_remove_routee() {
        let mut pool = make_pool(2);
        assert_eq!(pool.pool_size(), 2);
        let id = pool.add_routee();
        assert_eq!(pool.pool_size(), 3);
        assert!(pool.remove_routee(id));
        assert_eq!(pool.pool_size(), 2);
        assert!(!pool.remove_routee(999));
    }

    #[test]
    fn test_routee_lifecycle() {
        let mut routee = PoolRoutee::new(0, "/test");
        assert_eq!(routee.state, RouteeState::Idle);
        assert!(routee.is_available());

        routee.record_message_sent();
        assert_eq!(routee.state, RouteeState::Busy);
        assert_eq!(routee.messages_pending, 1);

        routee.record_message_processed(1000);
        assert_eq!(routee.state, RouteeState::Active);
        assert_eq!(routee.messages_processed, 1);
        assert_eq!(routee.messages_pending, 0);
    }

    #[test]
    fn test_routee_error() {
        let mut routee = PoolRoutee::new(0, "/test");
        routee.record_message_sent();
        routee.record_error();
        assert_eq!(routee.errors, 1);
        assert_eq!(routee.messages_pending, 0);
    }

    #[test]
    fn test_routee_utilization() {
        let routee = PoolRoutee::new(0, "/test");
        assert_eq!(routee.utilization(), 0.0);

        let mut r2 = PoolRoutee::new(1, "/test2");
        r2.record_message_sent();
        r2.record_message_sent();
        assert!(r2.utilization() > 0.0);
    }

    #[test]
    fn test_scaling_policy_fixed() {
        let policy = ScalingPolicy::fixed(5);
        assert_eq!(policy.min_routees, 5);
        assert_eq!(policy.max_routees, 5);
    }

    #[test]
    fn test_scaling_policy_elastic() {
        let policy = ScalingPolicy::elastic(2, 8)
            .with_thresholds(0.7, 0.3)
            .with_cooldown(Duration::from_secs(10))
            .with_steps(2, 1);
        assert_eq!(policy.min_routees, 2);
        assert_eq!(policy.max_routees, 8);
        assert_eq!(policy.scale_up_threshold, 0.7);
        assert_eq!(policy.scale_down_threshold, 0.3);
        assert_eq!(policy.scale_up_step, 2);
    }

    #[test]
    fn test_evaluate_scale_up() {
        let mut pool = make_pool(2);
        pool.scaling = ScalingPolicy::elastic(1, 5)
            .with_thresholds(0.3, 0.1);
        pool.last_scale_event = None;
        // Make routees busy
        for r in pool.routees.iter_mut() {
            r.messages_pending = 10;
        }
        let decision = pool.evaluate_scaling();
        assert!(matches!(decision, ScalingDecision::ScaleUp(_)));
    }

    #[test]
    fn test_evaluate_scale_down() {
        let mut pool = make_pool(5);
        pool.scaling = ScalingPolicy::elastic(1, 10)
            .with_thresholds(0.8, 0.5);
        pool.last_scale_event = None;
        // All routees idle (utilization = 0)
        let decision = pool.evaluate_scaling();
        assert!(matches!(decision, ScalingDecision::ScaleDown(_)));
    }

    #[test]
    fn test_evaluate_no_change() {
        let mut pool = make_pool(3);
        pool.scaling = ScalingPolicy::elastic(1, 10)
            .with_thresholds(0.9, 0.0);
        pool.last_scale_event = None;
        let decision = pool.evaluate_scaling();
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_evaluate_cooldown() {
        let mut pool = make_pool(2);
        pool.last_scale_event = Some(Instant::now());
        pool.scaling.cooldown = Duration::from_secs(60);
        let decision = pool.evaluate_scaling();
        assert_eq!(decision, ScalingDecision::Cooldown);
    }

    #[test]
    fn test_apply_scale_up() {
        let mut pool = make_pool(2);
        pool.apply_scaling(ScalingDecision::ScaleUp(2));
        assert_eq!(pool.pool_size(), 4);
        assert_eq!(pool.stats.scale_ups, 1);
    }

    #[test]
    fn test_apply_scale_down() {
        let mut pool = make_pool(4);
        pool.apply_scaling(ScalingDecision::ScaleDown(2));
        assert_eq!(pool.pool_size(), 2);
        assert_eq!(pool.stats.scale_downs, 1);
    }

    #[test]
    fn test_auto_scale() {
        let mut pool = make_pool(2);
        pool.scaling = ScalingPolicy::elastic(1, 5)
            .with_thresholds(0.3, 0.1);
        pool.last_scale_event = None;
        for r in pool.routees.iter_mut() {
            r.messages_pending = 10;
        }
        let decision = pool.auto_scale();
        assert!(matches!(decision, ScalingDecision::ScaleUp(_)));
        assert!(pool.pool_size() > 2);
    }

    #[test]
    fn test_health_check_healthy() {
        let pool = make_pool(2);
        let routee = &pool.routees()[0];
        assert_eq!(pool.check_routee_health(routee), HealthStatus::Healthy);
    }

    #[test]
    fn test_health_check_unhealthy_errors() {
        let mut pool = make_pool(1);
        pool.health_check.max_errors = 5;
        pool.routees[0].errors = 10;
        let status = pool.check_routee_health(&pool.routees[0].clone());
        assert!(matches!(status, HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_health_check_unhealthy_pending() {
        let mut pool = make_pool(1);
        pool.health_check.max_pending = 50;
        pool.routees[0].messages_pending = 100;
        let status = pool.check_routee_health(&pool.routees[0].clone());
        assert!(matches!(status, HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_health_check_degraded() {
        let mut pool = make_pool(1);
        pool.health_check.max_processing_nanos = 1000;
        pool.routees[0].avg_processing_nanos = 5000;
        let status = pool.check_routee_health(&pool.routees[0].clone());
        assert!(matches!(status, HealthStatus::Degraded(_)));
    }

    #[test]
    fn test_run_health_check_removes_unhealthy() {
        let mut pool = make_pool(3);
        pool.scaling.min_routees = 1;
        pool.health_check.max_errors = 5;
        pool.routees[0].errors = 10;
        pool.routees[1].errors = 10;
        let results = pool.run_health_check();
        assert!(results.len() >= 2);
        assert!(pool.pool_size() <= 3);
        assert!(pool.stats.unhealthy_removed > 0);
    }

    #[test]
    fn test_drain_routee() {
        let mut pool = make_pool(2);
        let id = pool.routees[0].id;
        assert!(pool.drain_routee(id));
        assert_eq!(pool.routee(id).unwrap().state, RouteeState::Draining);
        assert!(!pool.routee(id).unwrap().is_available());
    }

    #[test]
    fn test_stop_routee() {
        let mut pool = make_pool(2);
        let id = pool.routees[0].id;
        assert!(pool.stop_routee(id));
        assert_eq!(pool.routee(id).unwrap().state, RouteeState::Stopped);
    }

    #[test]
    fn test_fail_and_restart() {
        let mut pool = make_pool(1);
        let id = pool.routees[0].id;
        pool.fail_routee(id);
        assert_eq!(pool.routee(id).unwrap().state, RouteeState::Failed);
        assert!(pool.restart_routee(id));
        assert_eq!(pool.routee(id).unwrap().state, RouteeState::Idle);
        assert_eq!(pool.routee(id).unwrap().errors, 0);
    }

    #[test]
    fn test_restart_active_fails() {
        let mut pool = make_pool(1);
        let id = pool.routees[0].id;
        assert!(!pool.restart_routee(id)); // Idle, not stopped/failed
    }

    #[test]
    fn test_all_routee_ids() {
        let mut pool = make_pool(3);
        let id = pool.routees[0].id;
        pool.stop_routee(id);
        let ids = pool.all_routee_ids();
        assert_eq!(ids.len(), 2);
        assert!(!ids.contains(&id));
    }

    #[test]
    fn test_total_metrics() {
        let mut pool = make_pool(2);
        let r1 = pool.select_routee().unwrap();
        let r2 = pool.select_routee().unwrap();
        pool.record_processed(r1, 1000);
        pool.record_processed(r2, 2000);
        assert_eq!(pool.total_processed(), 2);
        assert_eq!(pool.stats.messages_routed, 2);
    }

    #[test]
    fn test_select_no_available() {
        let mut pool = make_pool(1);
        let id = pool.routees[0].id;
        pool.stop_routee(id);
        assert!(pool.select_routee().is_none());
    }

    #[test]
    fn test_scale_history() {
        let mut pool = make_pool(2);
        pool.apply_scaling(ScalingDecision::ScaleUp(1));
        assert_eq!(pool.scale_history().len(), 1);
        assert_eq!(pool.scale_history()[0].pool_size_before, 2);
        assert_eq!(pool.scale_history()[0].pool_size_after, 3);
    }

    #[test]
    fn test_pool_debug() {
        let pool = make_pool(2);
        let debug = format!("{:?}", pool);
        assert!(debug.contains("RouterPool"));
        assert!(debug.contains("test"));
    }

    #[test]
    fn test_broadcast_routing() {
        let mut pool = RouterPool::new("bc", PoolRoutingStrategy::BroadcastPool, 3);
        let ids = pool.all_routee_ids();
        assert_eq!(ids.len(), 3);
        let selected = pool.select_routee().unwrap();
        assert!(ids.contains(&selected));
    }

    #[test]
    fn test_routee_avg_processing() {
        let mut r = PoolRoutee::new(0, "/test");
        r.record_message_sent();
        r.record_message_processed(1000);
        r.record_message_sent();
        r.record_message_processed(3000);
        assert_eq!(r.avg_processing_nanos, 2000);
        assert_eq!(r.messages_processed, 2);
    }

    #[test]
    fn test_pool_with_config() {
        let pool = RouterPool::new("test", PoolRoutingStrategy::Random, 3)
            .with_scaling(ScalingPolicy::elastic(2, 8))
            .with_health_check(HealthCheck {
                max_errors: 5,
                ..Default::default()
            });
        assert_eq!(pool.scaling.min_routees, 2);
        assert_eq!(pool.health_check.max_errors, 5);
    }

    #[test]
    fn test_health_failed_routee() {
        let mut pool = make_pool(1);
        pool.routees[0].state = RouteeState::Failed;
        let status = pool.check_routee_health(&pool.routees[0].clone());
        assert!(matches!(status, HealthStatus::Unhealthy(_)));
    }
}
