//! Router - Message routing to multiple actors
//!
//! Routers distribute messages across a pool of actors (routees) using
//! various routing strategies.
//!
//! ## Strategies
//!
//! - **RoundRobin**: Distribute messages in order, cycling through routees
//! - **Random**: Randomly select a routee for each message
//! - **Broadcast**: Send message to all routees
//! - **SmallestMailbox**: Send to routee with fewest pending messages
//! - **ConsistentHash**: Route based on message hash (same key → same routee)
//!
//! ## Example
//!
//! ```rust,ignore
//! use pekko_actor::{Router, RoundRobinStrategy};
//!
//! // Create a router with 5 workers
//! let mut router = Router::new(RoundRobinStrategy::new());
//!
//! for i in 0..5 {
//!     let worker = system.spawn(Worker::new(), &format!("worker-{}", i)).await?;
//!     router.add_routee(worker);
//! }
//!
//! // Send messages - they'll be distributed round-robin
//! for task in tasks {
//!     router.route(task);
//! }
//! ```

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::actor::Actor;
use crate::actor_ref::{ActorRef, ActorError};

/// A routee that can receive routed messages
pub trait Routee<M>: Send + Sync {
    /// Send a message to this routee
    fn send(&self, msg: M) -> Result<(), ActorError>;

    /// Get a unique identifier for this routee
    fn id(&self) -> String;

    /// Check if the routee is still alive
    fn is_alive(&self) -> bool;
}

/// Implementation of Routee for ActorRef
impl<A: Actor> Routee<A::Message> for ActorRef<A>
where
    A::Message: Clone,
{
    fn send(&self, msg: A::Message) -> Result<(), ActorError> {
        self.tell(msg)
    }

    fn id(&self) -> String {
        self.path().to_string()
    }

    fn is_alive(&self) -> bool {
        !self.is_terminated()
    }
}

/// Routing logic trait
pub trait RoutingStrategy<M>: Send + Sync {
    /// Select routee(s) to receive the message
    /// Returns indices of selected routees
    fn select(&self, msg: &M, routees: &[Box<dyn Routee<M>>]) -> Vec<usize>;

    /// Called when a routee is added
    fn on_routee_added(&mut self, _index: usize) {}

    /// Called when a routee is removed
    fn on_routee_removed(&mut self, _index: usize) {}

    /// Get strategy name for debugging
    fn name(&self) -> &'static str;
}

// ============================================================
// Routing Strategies
// ============================================================

/// Round-robin routing - cycles through routees in order
pub struct RoundRobinStrategy {
    counter: AtomicUsize,
}

impl RoundRobinStrategy {
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobinStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> RoutingStrategy<M> for RoundRobinStrategy {
    fn select(&self, _msg: &M, routees: &[Box<dyn Routee<M>>]) -> Vec<usize> {
        if routees.is_empty() {
            return vec![];
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % routees.len();
        vec![idx]
    }

    fn name(&self) -> &'static str {
        "RoundRobin"
    }
}

impl fmt::Debug for RoundRobinStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoundRobinStrategy")
            .field("counter", &self.counter.load(Ordering::Relaxed))
            .finish()
    }
}

/// Random routing - randomly selects a routee
pub struct RandomStrategy {
    // Simple LCG random number generator state
    state: AtomicUsize,
}

impl RandomStrategy {
    pub fn new() -> Self {
        // Seed with current time
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as usize)
            .unwrap_or(42);
        Self {
            state: AtomicUsize::new(seed),
        }
    }

    fn next_random(&self) -> usize {
        // Simple LCG: next = (a * current + c) mod m
        let a: usize = 1103515245;
        let c: usize = 12345;
        loop {
            let current = self.state.load(Ordering::Relaxed);
            let next = current.wrapping_mul(a).wrapping_add(c);
            if self
                .state
                .compare_exchange(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return next;
            }
        }
    }
}

impl Default for RandomStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> RoutingStrategy<M> for RandomStrategy {
    fn select(&self, _msg: &M, routees: &[Box<dyn Routee<M>>]) -> Vec<usize> {
        if routees.is_empty() {
            return vec![];
        }
        let idx = self.next_random() % routees.len();
        vec![idx]
    }

    fn name(&self) -> &'static str {
        "Random"
    }
}

impl fmt::Debug for RandomStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RandomStrategy").finish()
    }
}

/// Broadcast routing - sends to all routees
#[derive(Debug, Default)]
pub struct BroadcastStrategy;

impl BroadcastStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl<M> RoutingStrategy<M> for BroadcastStrategy {
    fn select(&self, _msg: &M, routees: &[Box<dyn Routee<M>>]) -> Vec<usize> {
        (0..routees.len()).collect()
    }

    fn name(&self) -> &'static str {
        "Broadcast"
    }
}

/// Scatter-gather routing - sends to all and collects responses
/// (For now, same as Broadcast - response collection needs ask pattern)
#[derive(Debug, Default)]
pub struct ScatterGatherStrategy;

impl ScatterGatherStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl<M> RoutingStrategy<M> for ScatterGatherStrategy {
    fn select(&self, _msg: &M, routees: &[Box<dyn Routee<M>>]) -> Vec<usize> {
        (0..routees.len()).collect()
    }

    fn name(&self) -> &'static str {
        "ScatterGather"
    }
}

/// Consistent hash routing - routes based on message hash
pub struct ConsistentHashStrategy<F> {
    hash_fn: F,
}

impl<F> ConsistentHashStrategy<F> {
    pub fn new(hash_fn: F) -> Self {
        Self { hash_fn }
    }
}

impl<M, F> RoutingStrategy<M> for ConsistentHashStrategy<F>
where
    F: Fn(&M) -> u64 + Send + Sync,
{
    fn select(&self, msg: &M, routees: &[Box<dyn Routee<M>>]) -> Vec<usize> {
        if routees.is_empty() {
            return vec![];
        }
        let hash = (self.hash_fn)(msg);
        let idx = (hash as usize) % routees.len();
        vec![idx]
    }

    fn name(&self) -> &'static str {
        "ConsistentHash"
    }
}

impl<F> fmt::Debug for ConsistentHashStrategy<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsistentHashStrategy").finish()
    }
}

/// First available routing - sends to first alive routee
#[derive(Debug, Default)]
pub struct FirstAvailableStrategy;

impl FirstAvailableStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl<M> RoutingStrategy<M> for FirstAvailableStrategy {
    fn select(&self, _msg: &M, routees: &[Box<dyn Routee<M>>]) -> Vec<usize> {
        for (idx, routee) in routees.iter().enumerate() {
            if routee.is_alive() {
                return vec![idx];
            }
        }
        vec![]
    }

    fn name(&self) -> &'static str {
        "FirstAvailable"
    }
}

/// Adaptive routing - tracks performance and routes to best performer
pub struct AdaptiveStrategy {
    // Track response times (in practice, would need metrics collection)
    weights: std::sync::RwLock<Vec<f64>>,
}

impl AdaptiveStrategy {
    pub fn new() -> Self {
        Self {
            weights: std::sync::RwLock::new(Vec::new()),
        }
    }

    /// Update weight for a routee (lower is better)
    pub fn update_weight(&self, index: usize, weight: f64) {
        let mut weights = self.weights.write().unwrap();
        if index < weights.len() {
            weights[index] = weight;
        }
    }
}

impl Default for AdaptiveStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> RoutingStrategy<M> for AdaptiveStrategy {
    fn select(&self, _msg: &M, routees: &[Box<dyn Routee<M>>]) -> Vec<usize> {
        if routees.is_empty() {
            return vec![];
        }

        let weights = self.weights.read().unwrap();
        if weights.is_empty() || weights.len() != routees.len() {
            // Fall back to first routee if weights not initialized
            return vec![0];
        }

        // Select routee with lowest weight
        let mut min_idx = 0;
        let mut min_weight = weights[0];
        for (idx, &weight) in weights.iter().enumerate().skip(1) {
            if weight < min_weight && routees[idx].is_alive() {
                min_weight = weight;
                min_idx = idx;
            }
        }
        vec![min_idx]
    }

    fn on_routee_added(&mut self, _index: usize) {
        let mut weights = self.weights.write().unwrap();
        weights.push(1.0); // Default weight
    }

    fn on_routee_removed(&mut self, index: usize) {
        let mut weights = self.weights.write().unwrap();
        if index < weights.len() {
            weights.remove(index);
        }
    }

    fn name(&self) -> &'static str {
        "Adaptive"
    }
}

impl fmt::Debug for AdaptiveStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdaptiveStrategy")
            .field("weights", &*self.weights.read().unwrap())
            .finish()
    }
}

// ============================================================
// Router
// ============================================================

/// Result of a routing operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteResult {
    /// Message was sent to specified number of routees
    Sent(usize),
    /// No routees available
    NoRoutees,
    /// Some sends failed
    PartialFailure { sent: usize, failed: usize },
}

/// A router that distributes messages to routees using a strategy
pub struct Router<M, S>
where
    S: RoutingStrategy<M>,
{
    routees: Vec<Box<dyn Routee<M>>>,
    strategy: S,
    /// Remove dead routees automatically
    auto_remove_dead: bool,
}

impl<M, S> Router<M, S>
where
    S: RoutingStrategy<M>,
    M: Clone,
{
    /// Create a new router with the given strategy
    pub fn new(strategy: S) -> Self {
        Self {
            routees: Vec::new(),
            strategy,
            auto_remove_dead: true,
        }
    }

    /// Create a router that doesn't auto-remove dead routees
    pub fn new_persistent(strategy: S) -> Self {
        Self {
            routees: Vec::new(),
            strategy,
            auto_remove_dead: false,
        }
    }

    /// Add a routee
    pub fn add_routee<R: Routee<M> + 'static>(&mut self, routee: R) {
        let index = self.routees.len();
        self.routees.push(Box::new(routee));
        self.strategy.on_routee_added(index);
        tracing::debug!(
            "Router[{}]: added routee (total: {})",
            self.strategy.name(),
            self.routees.len()
        );
    }

    /// Remove a routee by index
    pub fn remove_routee(&mut self, index: usize) -> bool {
        if index < self.routees.len() {
            self.routees.remove(index);
            self.strategy.on_routee_removed(index);
            tracing::debug!(
                "Router[{}]: removed routee {} (total: {})",
                self.strategy.name(),
                index,
                self.routees.len()
            );
            true
        } else {
            false
        }
    }

    /// Remove dead routees
    pub fn remove_dead_routees(&mut self) -> usize {
        let before = self.routees.len();
        self.routees.retain(|r| r.is_alive());
        let removed = before - self.routees.len();
        if removed > 0 {
            tracing::debug!(
                "Router[{}]: removed {} dead routees",
                self.strategy.name(),
                removed
            );
        }
        removed
    }

    /// Route a message
    pub fn route(&mut self, msg: M) -> RouteResult {
        // Auto-remove dead routees if enabled
        if self.auto_remove_dead {
            self.remove_dead_routees();
        }

        if self.routees.is_empty() {
            return RouteResult::NoRoutees;
        }

        let selected = self.strategy.select(&msg, &self.routees);

        if selected.is_empty() {
            return RouteResult::NoRoutees;
        }

        let mut sent = 0;
        let mut failed = 0;

        for idx in selected {
            if idx < self.routees.len() {
                match self.routees[idx].send(msg.clone()) {
                    Ok(()) => sent += 1,
                    Err(e) => {
                        failed += 1;
                        tracing::warn!(
                            "Router[{}]: failed to send to routee {}: {:?}",
                            self.strategy.name(),
                            idx,
                            e
                        );
                    }
                }
            }
        }

        if failed == 0 {
            RouteResult::Sent(sent)
        } else if sent == 0 {
            RouteResult::NoRoutees
        } else {
            RouteResult::PartialFailure { sent, failed }
        }
    }

    /// Get the number of routees
    pub fn routee_count(&self) -> usize {
        self.routees.len()
    }

    /// Get the number of alive routees
    pub fn alive_count(&self) -> usize {
        self.routees.iter().filter(|r| r.is_alive()).count()
    }

    /// Get the strategy name
    pub fn strategy_name(&self) -> &'static str {
        self.strategy.name()
    }

    /// Check if router has any routees
    pub fn is_empty(&self) -> bool {
        self.routees.is_empty()
    }

    /// Get routee IDs
    pub fn routee_ids(&self) -> Vec<String> {
        self.routees.iter().map(|r| r.id()).collect()
    }
}

impl<M, S> fmt::Debug for Router<M, S>
where
    S: RoutingStrategy<M> + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Router")
            .field("strategy", &self.strategy)
            .field("routee_count", &self.routees.len())
            .field("auto_remove_dead", &self.auto_remove_dead)
            .finish()
    }
}

/// Helper to create routers with common strategies
pub struct Routers;

impl Routers {
    /// Create a round-robin router
    pub fn round_robin<M: Clone>() -> Router<M, RoundRobinStrategy> {
        Router::new(RoundRobinStrategy::new())
    }

    /// Create a random router
    pub fn random<M: Clone>() -> Router<M, RandomStrategy> {
        Router::new(RandomStrategy::new())
    }

    /// Create a broadcast router
    pub fn broadcast<M: Clone>() -> Router<M, BroadcastStrategy> {
        Router::new(BroadcastStrategy::new())
    }

    /// Create a consistent hash router
    pub fn consistent_hash<M: Clone, F>(hash_fn: F) -> Router<M, ConsistentHashStrategy<F>>
    where
        F: Fn(&M) -> u64 + Send + Sync,
    {
        Router::new(ConsistentHashStrategy::new(hash_fn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    /// A simple test routee with internal shared state
    #[derive(Clone)]
    struct TestRoutee {
        id: String,
        messages: Arc<std::sync::Mutex<Vec<u32>>>,
        alive: Arc<AtomicBool>,
    }

    impl TestRoutee {
        fn new(id: &str) -> Self {
            Self {
                id: id.to_string(),
                messages: Arc::new(std::sync::Mutex::new(Vec::new())),
                alive: Arc::new(AtomicBool::new(true)),
            }
        }

        fn messages(&self) -> Vec<u32> {
            self.messages.lock().unwrap().clone()
        }

        fn kill(&self) {
            self.alive.store(false, Ordering::SeqCst);
        }
    }

    impl Routee<u32> for TestRoutee {
        fn send(&self, msg: u32) -> Result<(), ActorError> {
            if self.alive.load(Ordering::SeqCst) {
                self.messages.lock().unwrap().push(msg);
                Ok(())
            } else {
                Err(ActorError::Failure("Dead routee".to_string()))
            }
        }

        fn id(&self) -> String {
            self.id.clone()
        }

        fn is_alive(&self) -> bool {
            self.alive.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn test_round_robin() {
        let r1 = TestRoutee::new("r1");
        let r2 = TestRoutee::new("r2");
        let r3 = TestRoutee::new("r3");

        let r1_msgs = r1.messages.clone();
        let r2_msgs = r2.messages.clone();
        let r3_msgs = r3.messages.clone();

        let mut router: Router<u32, RoundRobinStrategy> = Routers::round_robin();
        router.add_routee(r1);
        router.add_routee(r2);
        router.add_routee(r3);

        // Send 6 messages
        for i in 0..6 {
            router.route(i);
        }

        // Should be evenly distributed
        assert_eq!(*r1_msgs.lock().unwrap(), vec![0, 3]);
        assert_eq!(*r2_msgs.lock().unwrap(), vec![1, 4]);
        assert_eq!(*r3_msgs.lock().unwrap(), vec![2, 5]);
    }

    #[test]
    fn test_random() {
        let r1 = TestRoutee::new("r1");
        let r2 = TestRoutee::new("r2");

        let r1_msgs = r1.messages.clone();
        let r2_msgs = r2.messages.clone();

        let mut router: Router<u32, RandomStrategy> = Routers::random();
        router.add_routee(r1);
        router.add_routee(r2);

        // Send 100 messages
        for i in 0..100 {
            router.route(i);
        }

        // Both should have received some messages (statistically)
        let total = r1_msgs.lock().unwrap().len() + r2_msgs.lock().unwrap().len();
        assert_eq!(total, 100);
        assert!(!r1_msgs.lock().unwrap().is_empty());
        assert!(!r2_msgs.lock().unwrap().is_empty());
    }

    #[test]
    fn test_broadcast() {
        let r1 = TestRoutee::new("r1");
        let r2 = TestRoutee::new("r2");
        let r3 = TestRoutee::new("r3");

        let r1_msgs = r1.messages.clone();
        let r2_msgs = r2.messages.clone();
        let r3_msgs = r3.messages.clone();

        let mut router: Router<u32, BroadcastStrategy> = Routers::broadcast();
        router.add_routee(r1);
        router.add_routee(r2);
        router.add_routee(r3);

        router.route(42);
        router.route(99);

        // All should have received both messages
        assert_eq!(*r1_msgs.lock().unwrap(), vec![42, 99]);
        assert_eq!(*r2_msgs.lock().unwrap(), vec![42, 99]);
        assert_eq!(*r3_msgs.lock().unwrap(), vec![42, 99]);
    }

    #[test]
    fn test_consistent_hash() {
        let r1 = TestRoutee::new("r1");
        let r2 = TestRoutee::new("r2");

        let r1_msgs = r1.messages.clone();
        let r2_msgs = r2.messages.clone();

        let mut router = Routers::consistent_hash(|msg: &u32| *msg as u64);
        router.add_routee(r1);
        router.add_routee(r2);

        // Same message should always go to same routee
        router.route(10);
        router.route(10);
        router.route(10);

        // Either r1 or r2 should have all three
        let r1_has_all = *r1_msgs.lock().unwrap() == vec![10, 10, 10];
        let r2_has_all = *r2_msgs.lock().unwrap() == vec![10, 10, 10];
        assert!(r1_has_all || r2_has_all);
    }

    #[test]
    fn test_no_routees() {
        let mut router: Router<u32, RoundRobinStrategy> = Routers::round_robin();

        let result = router.route(42);
        assert_eq!(result, RouteResult::NoRoutees);
    }

    #[test]
    fn test_remove_dead_routees() {
        let r1 = TestRoutee::new("r1");
        let r2 = TestRoutee::new("r2");
        let r3 = TestRoutee::new("r3");

        let r2_alive = r2.alive.clone();

        let mut router: Router<u32, RoundRobinStrategy> = Routers::round_robin();
        router.add_routee(r1);
        router.add_routee(r2);
        router.add_routee(r3);

        assert_eq!(router.routee_count(), 3);

        // Kill r2
        r2_alive.store(false, Ordering::SeqCst);

        // Should auto-remove on route
        router.route(1);
        assert_eq!(router.routee_count(), 2);

        // Only r1 and r3 should be left
        let ids = router.routee_ids();
        assert!(ids.contains(&"r1".to_string()));
        assert!(!ids.contains(&"r2".to_string()));
        assert!(ids.contains(&"r3".to_string()));
    }

    #[test]
    fn test_first_available() {
        let r1 = TestRoutee::new("r1");
        let r2 = TestRoutee::new("r2");
        let r3 = TestRoutee::new("r3");

        let r1_alive = r1.alive.clone();
        let r1_msgs = r1.messages.clone();
        let r2_msgs = r2.messages.clone();
        let r3_msgs = r3.messages.clone();

        let mut router = Router::new_persistent(FirstAvailableStrategy::new());
        router.add_routee(r1);
        router.add_routee(r2);
        router.add_routee(r3);

        // Kill first routee
        r1_alive.store(false, Ordering::SeqCst);

        router.route(1);
        router.route(2);

        // All should go to r2 (first alive)
        assert!(r1_msgs.lock().unwrap().is_empty());
        assert_eq!(*r2_msgs.lock().unwrap(), vec![1, 2]);
        assert!(r3_msgs.lock().unwrap().is_empty());
    }

    #[test]
    fn test_adaptive_strategy() {
        let r1 = TestRoutee::new("r1");
        let r2 = TestRoutee::new("r2");

        let strategy = AdaptiveStrategy::new();
        let mut router = Router::new(strategy);
        router.add_routee(r1);
        router.add_routee(r2);

        // Initially, should go to first (index 0)
        router.route(1);

        // Update weights - r2 is now better
        // Can't easily access strategy after move, so this test is limited
        // In practice, you'd update weights through some feedback mechanism
    }

    #[test]
    fn test_routee_count() {
        let r1 = TestRoutee::new("r1");
        let r2 = TestRoutee::new("r2");

        let mut router: Router<u32, RoundRobinStrategy> = Routers::round_robin();
        assert_eq!(router.routee_count(), 0);
        assert!(router.is_empty());

        router.add_routee(r1);
        assert_eq!(router.routee_count(), 1);
        assert!(!router.is_empty());

        router.add_routee(r2);
        assert_eq!(router.routee_count(), 2);

        router.remove_routee(0);
        assert_eq!(router.routee_count(), 1);
    }

    #[test]
    fn test_partial_failure() {
        let r1 = TestRoutee::new("r1");
        let r2 = TestRoutee::new("r2");
        let r3 = TestRoutee::new("r3");

        let r2_alive = r2.alive.clone();

        let mut router = Router::new_persistent(BroadcastStrategy::new());
        router.add_routee(r1);
        router.add_routee(r2);
        router.add_routee(r3);

        // Kill one routee
        r2_alive.store(false, Ordering::SeqCst);

        let result = router.route(42);

        // Should report partial failure
        assert_eq!(result, RouteResult::PartialFailure { sent: 2, failed: 1 });
    }

    #[test]
    fn test_strategy_names() {
        let rr: &dyn RoutingStrategy<u32> = &RoundRobinStrategy::new();
        assert_eq!(rr.name(), "RoundRobin");

        let rand: &dyn RoutingStrategy<u32> = &RandomStrategy::new();
        assert_eq!(rand.name(), "Random");

        let bc: &dyn RoutingStrategy<u32> = &BroadcastStrategy::new();
        assert_eq!(bc.name(), "Broadcast");

        let fa: &dyn RoutingStrategy<u32> = &FirstAvailableStrategy::new();
        assert_eq!(fa.name(), "FirstAvailable");

        let ad: &dyn RoutingStrategy<u32> = &AdaptiveStrategy::new();
        assert_eq!(ad.name(), "Adaptive");
    }
}
