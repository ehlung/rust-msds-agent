//! Circuit Breaker — Protection pattern for external calls
//!
//! Implements the Circuit Breaker pattern from Scala Pekko:
//! - **Closed**: Calls pass through normally; failures counted
//! - **Open**: Calls fail fast without execution
//! - **HalfOpen**: Limited trial calls to check if service recovered
//!
//! ## Example
//! ```ignore
//! use pekko_actor::circuit_breaker::CircuitBreaker;
//! use std::time::Duration;
//!
//! let cb = CircuitBreaker::new()
//!     .max_failures(5)
//!     .call_timeout(Duration::from_secs(2))
//!     .reset_timeout(Duration::from_secs(30))
//!     .build();
//!
//! let result = cb.call(|| async { expensive_external_call().await }).await;
//! ```

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, RwLock};

// ============= State =============

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerState {
    /// Normal operation; failures are counted.
    Closed,
    /// Calls fail fast; waiting for reset timeout.
    Open,
    /// One trial call allowed to test recovery.
    HalfOpen,
}

impl fmt::Display for CircuitBreakerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "Closed"),
            Self::Open => write!(f, "Open"),
            Self::HalfOpen => write!(f, "HalfOpen"),
        }
    }
}

// ============= Error =============

/// Circuit breaker error types.
#[derive(Debug, Clone)]
pub enum CircuitBreakerError<E> {
    /// The circuit is open; call rejected.
    Open,
    /// The call timed out.
    Timeout,
    /// The underlying call failed.
    CallFailed(E),
}

impl<E: fmt::Display> fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open => write!(f, "Circuit breaker is open"),
            Self::Timeout => write!(f, "Circuit breaker call timed out"),
            Self::CallFailed(e) => write!(f, "Call failed: {}", e),
        }
    }
}

// ============= Listeners =============

type StateChangeCallback = Box<dyn Fn(CircuitBreakerState, CircuitBreakerState) + Send + Sync>;

// ============= CircuitBreaker =============

/// Internal state for the circuit breaker.
struct CircuitBreakerInner {
    state: RwLock<CircuitBreakerState>,
    failure_count: AtomicU64,
    success_count: AtomicU64,
    half_open_attempts: AtomicU64,
    last_failure_time: AtomicI64,
    opened_at: AtomicI64,
    total_calls: AtomicU64,
    total_failures: AtomicU64,
    total_successes: AtomicU64,
    total_rejections: AtomicU64,
    total_timeouts: AtomicU64,
    listeners: Mutex<Vec<StateChangeCallback>>,
    notify: Notify,
}

/// Circuit breaker for protecting external calls.
#[derive(Clone)]
pub struct CircuitBreaker {
    max_failures: u64,
    call_timeout: Duration,
    reset_timeout: Duration,
    max_half_open_calls: u64,
    exponential_backoff_factor: f64,
    inner: Arc<CircuitBreakerInner>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker builder.
    pub fn new() -> CircuitBreakerBuilder {
        CircuitBreakerBuilder::default()
    }

    /// Get current state.
    pub async fn state(&self) -> CircuitBreakerState {
        let state = *self.inner.state.read().await;
        // Check if open state should transition to half-open
        if state == CircuitBreakerState::Open {
            let opened_at = self.inner.opened_at.load(Ordering::Relaxed);
            if opened_at > 0 {
                let elapsed = Instant::now()
                    .duration_since(Instant::now() - Duration::from_millis(
                        (self.now_millis() - opened_at) as u64
                    ));
                if elapsed >= self.effective_reset_timeout() {
                    return CircuitBreakerState::HalfOpen;
                }
            }
        }
        state
    }

    /// Execute a call through the circuit breaker.
    pub async fn call<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        // Check state
        let current_state = self.check_and_transition().await;

        match current_state {
            CircuitBreakerState::Closed => self.call_in_closed(f).await,
            CircuitBreakerState::Open => {
                self.inner.total_rejections.fetch_add(1, Ordering::Relaxed);
                Err(CircuitBreakerError::Open)
            }
            CircuitBreakerState::HalfOpen => self.call_in_half_open(f).await,
        }
    }

    /// Execute a call, falling back to a default on circuit open.
    pub async fn call_with_fallback<F, Fut, G, GFut, T, E>(
        &self,
        f: F,
        fallback: G,
    ) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        G: FnOnce() -> GFut,
        GFut: Future<Output = Result<T, E>>,
    {
        match self.call(f).await {
            Err(CircuitBreakerError::Open) => {
                fallback().await.map_err(CircuitBreakerError::CallFailed)
            }
            other => other,
        }
    }

    /// Force the circuit to open.
    pub async fn force_open(&self) {
        self.transition_to(CircuitBreakerState::Open).await;
    }

    /// Force the circuit to close (reset).
    pub async fn force_close(&self) {
        self.inner.failure_count.store(0, Ordering::Relaxed);
        self.transition_to(CircuitBreakerState::Closed).await;
    }

    /// Register a state change listener.
    pub async fn on_state_change<F>(&self, callback: F)
    where
        F: Fn(CircuitBreakerState, CircuitBreakerState) + Send + Sync + 'static,
    {
        self.inner.listeners.lock().await.push(Box::new(callback));
    }

    /// Get statistics snapshot.
    pub fn stats(&self) -> CircuitBreakerStats {
        CircuitBreakerStats {
            total_calls: self.inner.total_calls.load(Ordering::Relaxed),
            total_successes: self.inner.total_successes.load(Ordering::Relaxed),
            total_failures: self.inner.total_failures.load(Ordering::Relaxed),
            total_rejections: self.inner.total_rejections.load(Ordering::Relaxed),
            total_timeouts: self.inner.total_timeouts.load(Ordering::Relaxed),
            current_failure_count: self.inner.failure_count.load(Ordering::Relaxed),
        }
    }

    // -- Internal methods --

    async fn check_and_transition(&self) -> CircuitBreakerState {
        let current = *self.inner.state.read().await;
        if current == CircuitBreakerState::Open {
            let opened_at = self.inner.opened_at.load(Ordering::Relaxed);
            if opened_at > 0 {
                let elapsed_ms = self.now_millis() - opened_at;
                let reset_ms = self.effective_reset_timeout().as_millis() as i64;
                if elapsed_ms >= reset_ms {
                    self.transition_to(CircuitBreakerState::HalfOpen).await;
                    return CircuitBreakerState::HalfOpen;
                }
            }
        }
        current
    }

    async fn call_in_closed<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.inner.total_calls.fetch_add(1, Ordering::Relaxed);

        let result = if self.call_timeout > Duration::ZERO {
            match tokio::time::timeout(self.call_timeout, f()).await {
                Ok(r) => r.map_err(CircuitBreakerError::CallFailed),
                Err(_) => {
                    self.inner.total_timeouts.fetch_add(1, Ordering::Relaxed);
                    self.on_failure().await;
                    return Err(CircuitBreakerError::Timeout);
                }
            }
        } else {
            f().await.map_err(CircuitBreakerError::CallFailed)
        };

        match &result {
            Ok(_) => self.on_success().await,
            Err(_) => self.on_failure().await,
        }

        result
    }

    async fn call_in_half_open<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let attempts = self.inner.half_open_attempts.fetch_add(1, Ordering::Relaxed);
        if attempts >= self.max_half_open_calls {
            self.inner.total_rejections.fetch_add(1, Ordering::Relaxed);
            return Err(CircuitBreakerError::Open);
        }

        self.inner.total_calls.fetch_add(1, Ordering::Relaxed);

        let result = if self.call_timeout > Duration::ZERO {
            match tokio::time::timeout(self.call_timeout, f()).await {
                Ok(r) => r.map_err(CircuitBreakerError::CallFailed),
                Err(_) => {
                    self.inner.total_timeouts.fetch_add(1, Ordering::Relaxed);
                    self.transition_to(CircuitBreakerState::Open).await;
                    return Err(CircuitBreakerError::Timeout);
                }
            }
        } else {
            f().await.map_err(CircuitBreakerError::CallFailed)
        };

        match &result {
            Ok(_) => {
                self.inner.total_successes.fetch_add(1, Ordering::Relaxed);
                self.inner.failure_count.store(0, Ordering::Relaxed);
                self.inner.half_open_attempts.store(0, Ordering::Relaxed);
                self.transition_to(CircuitBreakerState::Closed).await;
            }
            Err(_) => {
                self.inner.total_failures.fetch_add(1, Ordering::Relaxed);
                self.inner.half_open_attempts.store(0, Ordering::Relaxed);
                self.transition_to(CircuitBreakerState::Open).await;
            }
        }

        result
    }

    async fn on_success(&self) {
        self.inner.total_successes.fetch_add(1, Ordering::Relaxed);
        self.inner.success_count.fetch_add(1, Ordering::Relaxed);
        self.inner.failure_count.store(0, Ordering::Relaxed);
    }

    async fn on_failure(&self) {
        self.inner.total_failures.fetch_add(1, Ordering::Relaxed);
        self.inner.last_failure_time.store(self.now_millis(), Ordering::Relaxed);
        let failures = self.inner.failure_count.fetch_add(1, Ordering::Relaxed) + 1;

        if failures >= self.max_failures {
            self.transition_to(CircuitBreakerState::Open).await;
        }
    }

    async fn transition_to(&self, new_state: CircuitBreakerState) {
        let mut state = self.inner.state.write().await;
        let old_state = *state;
        if old_state == new_state {
            return;
        }

        *state = new_state;

        if new_state == CircuitBreakerState::Open {
            self.inner.opened_at.store(self.now_millis(), Ordering::Relaxed);
            self.inner.half_open_attempts.store(0, Ordering::Relaxed);
        }

        drop(state); // Release write lock before calling listeners

        // Notify listeners
        let listeners = self.inner.listeners.lock().await;
        for listener in listeners.iter() {
            listener(old_state, new_state);
        }

        self.inner.notify.notify_waiters();
    }

    fn effective_reset_timeout(&self) -> Duration {
        if self.exponential_backoff_factor <= 1.0 {
            return self.reset_timeout;
        }
        let failure_count = self.inner.failure_count.load(Ordering::Relaxed);
        if failure_count == 0 {
            return self.reset_timeout;
        }
        let factor = self.exponential_backoff_factor.powi(failure_count.min(10) as i32);
        Duration::from_millis((self.reset_timeout.as_millis() as f64 * factor) as u64)
    }

    fn now_millis(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}

// ============= Builder =============

/// Builder for CircuitBreaker.
pub struct CircuitBreakerBuilder {
    max_failures: u64,
    call_timeout: Duration,
    reset_timeout: Duration,
    max_half_open_calls: u64,
    exponential_backoff_factor: f64,
}

impl Default for CircuitBreakerBuilder {
    fn default() -> Self {
        Self {
            max_failures: 5,
            call_timeout: Duration::from_secs(10),
            reset_timeout: Duration::from_secs(30),
            max_half_open_calls: 1,
            exponential_backoff_factor: 1.0,
        }
    }
}

impl CircuitBreakerBuilder {
    /// Set max consecutive failures before opening.
    pub fn max_failures(mut self, n: u64) -> Self {
        self.max_failures = n;
        self
    }

    /// Set timeout for each call.
    pub fn call_timeout(mut self, d: Duration) -> Self {
        self.call_timeout = d;
        self
    }

    /// Set time to wait before trying half-open.
    pub fn reset_timeout(mut self, d: Duration) -> Self {
        self.reset_timeout = d;
        self
    }

    /// Set max concurrent calls in half-open state.
    pub fn max_half_open_calls(mut self, n: u64) -> Self {
        self.max_half_open_calls = n;
        self
    }

    /// Set exponential backoff factor for reset timeout.
    pub fn exponential_backoff(mut self, factor: f64) -> Self {
        self.exponential_backoff_factor = factor;
        self
    }

    /// Build the circuit breaker.
    pub fn build(self) -> CircuitBreaker {
        CircuitBreaker {
            max_failures: self.max_failures,
            call_timeout: self.call_timeout,
            reset_timeout: self.reset_timeout,
            max_half_open_calls: self.max_half_open_calls,
            exponential_backoff_factor: self.exponential_backoff_factor,
            inner: Arc::new(CircuitBreakerInner {
                state: RwLock::new(CircuitBreakerState::Closed),
                failure_count: AtomicU64::new(0),
                success_count: AtomicU64::new(0),
                half_open_attempts: AtomicU64::new(0),
                last_failure_time: AtomicI64::new(0),
                opened_at: AtomicI64::new(0),
                total_calls: AtomicU64::new(0),
                total_failures: AtomicU64::new(0),
                total_successes: AtomicU64::new(0),
                total_rejections: AtomicU64::new(0),
                total_timeouts: AtomicU64::new(0),
                listeners: Mutex::new(Vec::new()),
                notify: Notify::new(),
            }),
        }
    }
}

// ============= Stats =============

/// Statistics snapshot for the circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerStats {
    pub total_calls: u64,
    pub total_successes: u64,
    pub total_failures: u64,
    pub total_rejections: u64,
    pub total_timeouts: u64,
    pub current_failure_count: u64,
}

impl CircuitBreakerStats {
    /// Success rate as a percentage (0.0 - 100.0).
    pub fn success_rate(&self) -> f64 {
        if self.total_calls == 0 {
            return 100.0;
        }
        (self.total_successes as f64 / self.total_calls as f64) * 100.0
    }

    /// Failure rate as a percentage (0.0 - 100.0).
    pub fn failure_rate(&self) -> f64 {
        if self.total_calls == 0 {
            return 0.0;
        }
        (self.total_failures as f64 / self.total_calls as f64) * 100.0
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI32;

    fn make_cb(max_failures: u64) -> CircuitBreaker {
        CircuitBreaker::new()
            .max_failures(max_failures)
            .call_timeout(Duration::from_secs(5))
            .reset_timeout(Duration::from_millis(100))
            .build()
    }

    #[tokio::test]
    async fn test_closed_state_success() {
        let cb = make_cb(3);
        let result: Result<i32, CircuitBreakerError<String>> =
            cb.call(|| async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);

        let stats = cb.stats();
        assert_eq!(stats.total_calls, 1);
        assert_eq!(stats.total_successes, 1);
    }

    #[tokio::test]
    async fn test_closed_state_failure() {
        let cb = make_cb(3);
        let result: Result<i32, CircuitBreakerError<String>> =
            cb.call(|| async { Err("boom".to_string()) }).await;
        assert!(matches!(result, Err(CircuitBreakerError::CallFailed(_))));

        let stats = cb.stats();
        assert_eq!(stats.total_failures, 1);
        assert_eq!(stats.current_failure_count, 1);
    }

    #[tokio::test]
    async fn test_opens_after_max_failures() {
        let cb = make_cb(3);

        for _ in 0..3 {
            let _: Result<i32, _> = cb.call(|| async { Err::<i32, _>("fail") }).await;
        }

        let state = cb.check_and_transition().await;
        assert_eq!(state, CircuitBreakerState::Open);

        // Next call should be rejected
        let result: Result<i32, CircuitBreakerError<&str>> =
            cb.call(|| async { Ok(1) }).await;
        assert!(matches!(result, Err(CircuitBreakerError::Open)));

        let stats = cb.stats();
        assert_eq!(stats.total_rejections, 1);
    }

    #[tokio::test]
    async fn test_success_resets_failure_count() {
        let cb = make_cb(3);

        // 2 failures
        for _ in 0..2 {
            let _: Result<i32, _> = cb.call(|| async { Err::<i32, _>("fail") }).await;
        }
        assert_eq!(cb.stats().current_failure_count, 2);

        // 1 success resets
        let _: Result<i32, CircuitBreakerError<&str>> = cb.call(|| async { Ok(1) }).await;
        assert_eq!(cb.stats().current_failure_count, 0);
    }

    #[tokio::test]
    async fn test_half_open_transition() {
        let cb = make_cb(2);

        // Trip to open
        for _ in 0..2 {
            let _: Result<i32, _> = cb.call(|| async { Err::<i32, _>("fail") }).await;
        }

        // Wait for reset timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should transition to half-open
        let state = cb.check_and_transition().await;
        assert_eq!(state, CircuitBreakerState::HalfOpen);
    }

    #[tokio::test]
    async fn test_half_open_success_closes() {
        let cb = make_cb(2);

        // Trip to open
        for _ in 0..2 {
            let _: Result<i32, _> = cb.call(|| async { Err::<i32, _>("fail") }).await;
        }

        // Wait for reset timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Successful call in half-open should close
        let result: Result<i32, CircuitBreakerError<&str>> =
            cb.call(|| async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);

        let state = *cb.inner.state.read().await;
        assert_eq!(state, CircuitBreakerState::Closed);
    }

    #[tokio::test]
    async fn test_half_open_failure_reopens() {
        let cb = make_cb(2);

        // Trip to open
        for _ in 0..2 {
            let _: Result<i32, _> = cb.call(|| async { Err::<i32, _>("fail") }).await;
        }

        // Wait for reset timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Failed call in half-open should reopen
        let _: Result<i32, _> = cb.call(|| async { Err::<i32, _>("still broken") }).await;

        let state = *cb.inner.state.read().await;
        assert_eq!(state, CircuitBreakerState::Open);
    }

    #[tokio::test]
    async fn test_call_timeout() {
        let cb = CircuitBreaker::new()
            .max_failures(3)
            .call_timeout(Duration::from_millis(50))
            .reset_timeout(Duration::from_secs(30))
            .build();

        let result: Result<i32, CircuitBreakerError<String>> = cb
            .call(|| async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(1)
            })
            .await;

        assert!(matches!(result, Err(CircuitBreakerError::Timeout)));
        assert_eq!(cb.stats().total_timeouts, 1);
    }

    #[tokio::test]
    async fn test_force_open() {
        let cb = make_cb(3);
        cb.force_open().await;

        let result: Result<i32, CircuitBreakerError<&str>> =
            cb.call(|| async { Ok(1) }).await;
        assert!(matches!(result, Err(CircuitBreakerError::Open)));
    }

    #[tokio::test]
    async fn test_force_close() {
        let cb = make_cb(2);

        // Trip to open
        for _ in 0..2 {
            let _: Result<i32, _> = cb.call(|| async { Err::<i32, _>("fail") }).await;
        }

        cb.force_close().await;

        let result: Result<i32, CircuitBreakerError<&str>> =
            cb.call(|| async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_call_with_fallback() {
        let cb = make_cb(1);

        // Trip to open
        let _: Result<i32, _> = cb.call(|| async { Err::<i32, _>("fail") }).await;

        // Fallback should be called
        let result: Result<i32, CircuitBreakerError<&str>> = cb
            .call_with_fallback(|| async { Ok(1) }, || async { Ok(-1) })
            .await;
        assert_eq!(result.unwrap(), -1);
    }

    #[tokio::test]
    async fn test_state_change_listener() {
        let cb = make_cb(2);
        let transitions = Arc::new(Mutex::new(Vec::new()));
        let t = transitions.clone();

        cb.on_state_change(move |from, to| {
            let t2 = t.clone();
            tokio::spawn(async move {
                t2.lock().await.push((from, to));
            });
        })
        .await;

        // Trip to open
        for _ in 0..2 {
            let _: Result<i32, _> = cb.call(|| async { Err::<i32, _>("fail") }).await;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        let t = transitions.lock().await;
        assert!(!t.is_empty());
        assert_eq!(
            t[0],
            (CircuitBreakerState::Closed, CircuitBreakerState::Open)
        );
    }

    #[tokio::test]
    async fn test_stats_success_rate() {
        let cb = make_cb(10);

        for _ in 0..3 {
            let _: Result<i32, CircuitBreakerError<&str>> = cb.call(|| async { Ok(1) }).await;
        }
        for _ in 0..1 {
            let _: Result<i32, _> = cb.call(|| async { Err::<i32, _>("fail") }).await;
        }

        let stats = cb.stats();
        assert_eq!(stats.total_calls, 4);
        assert!((stats.success_rate() - 75.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let cb1 = make_cb(3);
        let cb2 = cb1.clone();

        for _ in 0..3 {
            let _: Result<i32, _> = cb1.call(|| async { Err::<i32, _>("fail") }).await;
        }

        // cb2 should also see the open state
        let result: Result<i32, CircuitBreakerError<&str>> =
            cb2.call(|| async { Ok(1) }).await;
        assert!(matches!(result, Err(CircuitBreakerError::Open)));
    }

    #[tokio::test]
    async fn test_concurrent_calls() {
        let cb = make_cb(100);
        let counter = Arc::new(AtomicI32::new(0));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let cb = cb.clone();
            let c = counter.clone();
            handles.push(tokio::spawn(async move {
                let _: Result<i32, CircuitBreakerError<String>> = cb
                    .call(|| async {
                        c.fetch_add(1, Ordering::Relaxed);
                        Ok(1)
                    })
                    .await;
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(counter.load(Ordering::Relaxed), 10);
        assert_eq!(cb.stats().total_calls, 10);
    }

    #[test]
    fn test_builder_defaults() {
        let cb = CircuitBreaker::new().build();
        assert_eq!(cb.max_failures, 5);
        assert_eq!(cb.call_timeout, Duration::from_secs(10));
        assert_eq!(cb.reset_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_state_display() {
        assert_eq!(CircuitBreakerState::Closed.to_string(), "Closed");
        assert_eq!(CircuitBreakerState::Open.to_string(), "Open");
        assert_eq!(CircuitBreakerState::HalfOpen.to_string(), "HalfOpen");
    }

    #[test]
    fn test_error_display() {
        let e: CircuitBreakerError<String> = CircuitBreakerError::Open;
        assert_eq!(e.to_string(), "Circuit breaker is open");
        let e: CircuitBreakerError<String> = CircuitBreakerError::Timeout;
        assert_eq!(e.to_string(), "Circuit breaker call timed out");
        let e = CircuitBreakerError::CallFailed("boom".to_string());
        assert_eq!(e.to_string(), "Call failed: boom");
    }
}
