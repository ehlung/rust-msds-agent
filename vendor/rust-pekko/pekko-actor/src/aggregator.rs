//! # Aggregator Pattern
//!
//! Collects responses from multiple actors with timeout and quorum support.
//! Based on Akka/Pekko's Aggregator pattern for scatter-gather workflows.
//!
//! ## Features
//! - Collect responses from N actors with configurable timeout
//! - Quorum-based completion (e.g., 3 out of 5 responses)
//! - First-response-wins mode
//! - Custom aggregation functions
//! - Retry support for failed requests
//! - Partial result handling on timeout

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Strategy for determining when aggregation is complete
#[derive(Debug, Clone, PartialEq)]
pub enum CompletionStrategy {
    /// Wait for all expected responses
    All,
    /// Wait for at least N responses (quorum)
    Quorum(usize),
    /// Return after first response
    First,
    /// Custom: wait for at least N responses within percentage threshold
    Threshold {
        min_count: usize,
        min_percentage: f64,
    },
}

impl CompletionStrategy {
    /// Check if the given count meets the completion criteria
    pub fn is_satisfied(&self, received: usize, expected: usize) -> bool {
        match self {
            CompletionStrategy::All => received >= expected,
            CompletionStrategy::Quorum(n) => received >= *n,
            CompletionStrategy::First => received >= 1,
            CompletionStrategy::Threshold { min_count, min_percentage } => {
                if expected == 0 {
                    return true;
                }
                let pct = received as f64 / expected as f64;
                received >= *min_count && pct >= *min_percentage
            }
        }
    }

    /// Validate the strategy against expected count
    pub fn validate(&self, expected: usize) -> Result<(), AggregatorError> {
        match self {
            CompletionStrategy::Quorum(n) if *n > expected => {
                Err(AggregatorError::InvalidConfig(format!(
                    "Quorum {} exceeds expected count {}", n, expected
                )))
            }
            CompletionStrategy::Quorum(0) => {
                Err(AggregatorError::InvalidConfig(
                    "Quorum must be at least 1".to_string()
                ))
            }
            CompletionStrategy::Threshold { min_count, min_percentage } => {
                if *min_percentage <= 0.0 || *min_percentage > 1.0 {
                    return Err(AggregatorError::InvalidConfig(format!(
                        "Threshold percentage must be in (0.0, 1.0], got {}", min_percentage
                    )));
                }
                if *min_count > expected {
                    return Err(AggregatorError::InvalidConfig(format!(
                        "Threshold min_count {} exceeds expected count {}", min_count, expected
                    )));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Error types for aggregator operations
#[derive(Debug, thiserror::Error, Clone)]
pub enum AggregatorError {
    #[error("Aggregation timed out: received {received} of {expected} responses")]
    Timeout { received: usize, expected: usize },
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("Aggregator already completed")]
    AlreadyCompleted,
    #[error("Duplicate response from source: {0}")]
    DuplicateResponse(String),
    #[error("Unknown source: {0}")]
    UnknownSource(String),
    #[error("Response rejected: {0}")]
    Rejected(String),
}

/// Status of an individual response source
#[derive(Debug, Clone, PartialEq)]
pub enum ResponseStatus<T: Clone> {
    /// Waiting for response
    Pending,
    /// Response received
    Received(T),
    /// Source reported an error
    Failed(String),
    /// Response timed out
    TimedOut,
    /// Response was rejected by filter
    Rejected,
}

impl<T: Clone> ResponseStatus<T> {
    pub fn is_pending(&self) -> bool {
        matches!(self, ResponseStatus::Pending)
    }

    pub fn is_received(&self) -> bool {
        matches!(self, ResponseStatus::Received(_))
    }

    pub fn is_terminal(&self) -> bool {
        !self.is_pending()
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            ResponseStatus::Received(v) => Some(v),
            _ => None,
        }
    }
}

/// Configuration for the aggregator
#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    /// How long to wait for responses
    pub timeout: Duration,
    /// Strategy for completion
    pub completion: CompletionStrategy,
    /// Whether to allow duplicate responses (update existing)
    pub allow_duplicates: bool,
    /// Maximum number of retries for failed sources
    pub max_retries: usize,
    /// Retry delay
    pub retry_delay: Duration,
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            completion: CompletionStrategy::All,
            allow_duplicates: false,
            max_retries: 0,
            retry_delay: Duration::from_millis(500),
        }
    }
}

impl AggregatorConfig {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout, ..Default::default() }
    }

    pub fn with_completion(mut self, strategy: CompletionStrategy) -> Self {
        self.completion = strategy;
        self
    }

    pub fn with_allow_duplicates(mut self, allow: bool) -> Self {
        self.allow_duplicates = allow;
        self
    }

    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Builder: quorum-based aggregation
    pub fn quorum(_expected: usize, quorum: usize, timeout: Duration) -> Self {
        Self {
            timeout,
            completion: CompletionStrategy::Quorum(quorum),
            ..Default::default()
        }
    }

    /// Builder: first-response-wins
    pub fn first_response(timeout: Duration) -> Self {
        Self {
            timeout,
            completion: CompletionStrategy::First,
            ..Default::default()
        }
    }
}

/// Tracks retry state for a source
#[derive(Debug, Clone)]
pub struct RetryState {
    pub attempts: usize,
    pub last_attempt: Option<Instant>,
    pub max_retries: usize,
    pub delay: Duration,
}

impl RetryState {
    pub fn new(max_retries: usize, delay: Duration) -> Self {
        Self {
            attempts: 0,
            last_attempt: None,
            max_retries,
            delay,
        }
    }

    pub fn can_retry(&self) -> bool {
        self.attempts < self.max_retries
    }

    pub fn should_retry_now(&self) -> bool {
        if !self.can_retry() {
            return false;
        }
        match self.last_attempt {
            None => true,
            Some(last) => last.elapsed() >= self.delay,
        }
    }

    pub fn record_attempt(&mut self) {
        self.attempts += 1;
        self.last_attempt = Some(Instant::now());
    }
}

/// Result of an aggregation operation
#[derive(Debug, Clone)]
pub struct AggregateResult<T: Clone> {
    /// Successfully collected responses
    pub responses: HashMap<String, T>,
    /// Sources that failed or timed out
    pub failures: HashMap<String, ResponseStatus<T>>,
    /// Total time taken
    pub elapsed: Duration,
    /// Whether the completion strategy was satisfied
    pub is_complete: bool,
    /// Number of retries that occurred
    pub total_retries: usize,
}

impl<T: Clone> AggregateResult<T> {
    pub fn success_count(&self) -> usize {
        self.responses.len()
    }

    pub fn failure_count(&self) -> usize {
        self.failures.len()
    }

    pub fn total_sources(&self) -> usize {
        self.responses.len() + self.failures.len()
    }

    pub fn success_rate(&self) -> f64 {
        let total = self.total_sources();
        if total == 0 { return 0.0; }
        self.responses.len() as f64 / total as f64
    }

    pub fn values(&self) -> Vec<&T> {
        self.responses.values().collect()
    }
}

/// The core aggregator that collects responses from multiple sources
pub struct Aggregator<T: Clone> {
    config: AggregatorConfig,
    sources: HashMap<String, ResponseStatus<T>>,
    retries: HashMap<String, RetryState>,
    started_at: Option<Instant>,
    completed: bool,
    total_retries: usize,
    response_filter: Option<Box<dyn Fn(&str, &T) -> bool + Send + Sync>>,
}

impl<T: Clone + std::fmt::Debug> std::fmt::Debug for Aggregator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Aggregator")
            .field("config", &self.config)
            .field("sources", &self.sources)
            .field("retries", &self.retries)
            .field("started_at", &self.started_at)
            .field("completed", &self.completed)
            .field("total_retries", &self.total_retries)
            .field("response_filter", &self.response_filter.as_ref().map(|_| "..."))
            .finish()
    }
}

impl<T: Clone + std::fmt::Debug> Aggregator<T> {
    /// Create a new aggregator with the given config and source names
    pub fn new(config: AggregatorConfig, sources: Vec<String>) -> Result<Self, AggregatorError> {
        if sources.is_empty() && !matches!(config.completion, CompletionStrategy::All) {
            return Err(AggregatorError::InvalidConfig(
                "No sources provided".to_string()
            ));
        }
        config.completion.validate(sources.len())?;

        let mut source_map = HashMap::new();
        let mut retry_map = HashMap::new();
        for name in &sources {
            source_map.insert(name.clone(), ResponseStatus::Pending);
            if config.max_retries > 0 {
                retry_map.insert(
                    name.clone(),
                    RetryState::new(config.max_retries, config.retry_delay),
                );
            }
        }

        Ok(Self {
            config,
            sources: source_map,
            retries: retry_map,
            started_at: None,
            completed: false,
            total_retries: 0,
            response_filter: None,
        })
    }

    /// Set a response filter that can reject responses
    pub fn with_filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&str, &T) -> bool + Send + Sync + 'static,
    {
        self.response_filter = Some(Box::new(filter));
        self
    }

    /// Start the aggregation timer
    pub fn start(&mut self) {
        if self.started_at.is_none() {
            self.started_at = Some(Instant::now());
        }
    }

    /// Record a response from a source
    pub fn add_response(&mut self, source: &str, value: T) -> Result<AggregateStatus, AggregatorError> {
        if self.completed {
            return Err(AggregatorError::AlreadyCompleted);
        }

        // Auto-start on first response
        if self.started_at.is_none() {
            self.start();
        }

        let status = self.sources.get(source)
            .ok_or_else(|| AggregatorError::UnknownSource(source.to_string()))?;

        if status.is_received() && !self.config.allow_duplicates {
            return Err(AggregatorError::DuplicateResponse(source.to_string()));
        }

        // Apply filter
        if let Some(ref filter) = self.response_filter {
            if !filter(source, &value) {
                self.sources.insert(source.to_string(), ResponseStatus::Rejected);
                return Err(AggregatorError::Rejected(source.to_string()));
            }
        }

        self.sources.insert(source.to_string(), ResponseStatus::Received(value));

        // Check completion
        let received = self.received_count();
        let expected = self.sources.len();
        if self.config.completion.is_satisfied(received, expected) {
            self.completed = true;
            Ok(AggregateStatus::Complete)
        } else {
            Ok(AggregateStatus::Pending {
                received,
                remaining: expected - received,
            })
        }
    }

    /// Record a failure from a source
    pub fn add_failure(&mut self, source: &str, reason: String) -> Result<AggregateStatus, AggregatorError> {
        if self.completed {
            return Err(AggregatorError::AlreadyCompleted);
        }

        if !self.sources.contains_key(source) {
            return Err(AggregatorError::UnknownSource(source.to_string()));
        }

        // Check if retry is possible
        if let Some(retry) = self.retries.get_mut(source) {
            if retry.can_retry() {
                retry.record_attempt();
                self.total_retries += 1;
                return Ok(AggregateStatus::RetryScheduled {
                    source: source.to_string(),
                    attempt: retry.attempts,
                });
            }
        }

        self.sources.insert(source.to_string(), ResponseStatus::Failed(reason));

        // Check if we can still meet the completion criteria
        let received = self.received_count();
        let pending = self.pending_count();
        let expected = self.sources.len();

        if self.config.completion.is_satisfied(received, expected) {
            self.completed = true;
            Ok(AggregateStatus::Complete)
        } else if received + pending < self.min_required() {
            // Can't possibly meet criteria
            self.completed = true;
            Ok(AggregateStatus::Failed)
        } else {
            Ok(AggregateStatus::Pending {
                received,
                remaining: pending,
            })
        }
    }

    /// Check if the aggregation has timed out
    pub fn check_timeout(&mut self) -> bool {
        if self.completed {
            return false;
        }
        if let Some(started) = self.started_at {
            if started.elapsed() >= self.config.timeout {
                // Mark remaining pending as timed out
                let pending_keys: Vec<String> = self.sources.iter()
                    .filter(|(_, s)| s.is_pending())
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in pending_keys {
                    self.sources.insert(key, ResponseStatus::TimedOut);
                }
                self.completed = true;
                return true;
            }
        }
        false
    }

    /// Finalize and get the aggregate result
    pub fn finalize(mut self) -> AggregateResult<T> {
        if !self.completed {
            self.check_timeout();
        }

        let elapsed = self.started_at
            .map(|s| s.elapsed())
            .unwrap_or_default();

        let received = self.received_count();
        let expected = self.sources.len();
        let is_complete = self.config.completion.is_satisfied(received, expected);

        let mut responses = HashMap::new();
        let mut failures = HashMap::new();

        for (key, status) in self.sources {
            match status {
                ResponseStatus::Received(v) => { responses.insert(key, v); }
                other => { failures.insert(key, other); }
            }
        }

        AggregateResult {
            responses,
            failures,
            elapsed,
            is_complete,
            total_retries: self.total_retries,
        }
    }

    /// Get the number of received responses
    pub fn received_count(&self) -> usize {
        self.sources.values().filter(|s| s.is_received()).count()
    }

    /// Get the number of pending responses
    pub fn pending_count(&self) -> usize {
        self.sources.values().filter(|s| s.is_pending()).count()
    }

    /// Get the number of failed responses
    pub fn failed_count(&self) -> usize {
        self.sources.values().filter(|s| matches!(s, ResponseStatus::Failed(_) | ResponseStatus::TimedOut | ResponseStatus::Rejected)).count()
    }

    /// Check if aggregation is complete
    pub fn is_complete(&self) -> bool {
        self.completed
    }

    /// Get remaining time before timeout
    pub fn remaining_time(&self) -> Option<Duration> {
        self.started_at.and_then(|started| {
            let elapsed = started.elapsed();
            self.config.timeout.checked_sub(elapsed)
        })
    }

    /// Get status of a specific source
    pub fn source_status(&self, source: &str) -> Option<&ResponseStatus<T>> {
        self.sources.get(source)
    }

    /// Get sources that need retrying
    pub fn sources_needing_retry(&self) -> Vec<String> {
        self.retries.iter()
            .filter(|(_, retry)| retry.should_retry_now())
            .filter(|(key, _)| {
                matches!(self.sources.get(*key), Some(ResponseStatus::Failed(_)))
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Minimum number of responses needed to satisfy completion
    fn min_required(&self) -> usize {
        match &self.config.completion {
            CompletionStrategy::All => self.sources.len(),
            CompletionStrategy::Quorum(n) => *n,
            CompletionStrategy::First => 1,
            CompletionStrategy::Threshold { min_count, .. } => *min_count,
        }
    }

    /// Get a snapshot of current aggregation progress
    pub fn progress(&self) -> AggregationProgress {
        AggregationProgress {
            received: self.received_count(),
            pending: self.pending_count(),
            failed: self.failed_count(),
            total: self.sources.len(),
            elapsed: self.started_at.map(|s| s.elapsed()).unwrap_or_default(),
            is_complete: self.completed,
            min_required: self.min_required(),
        }
    }
}

/// Status returned from adding a response or failure
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateStatus {
    /// Aggregation is now complete
    Complete,
    /// Still waiting for more responses
    Pending {
        received: usize,
        remaining: usize,
    },
    /// A retry has been scheduled for the source
    RetryScheduled {
        source: String,
        attempt: usize,
    },
    /// Can no longer satisfy completion criteria
    Failed,
}

/// Snapshot of aggregation progress
#[derive(Debug, Clone)]
pub struct AggregationProgress {
    pub received: usize,
    pub pending: usize,
    pub failed: usize,
    pub total: usize,
    pub elapsed: Duration,
    pub is_complete: bool,
    pub min_required: usize,
}

impl AggregationProgress {
    pub fn completion_percentage(&self) -> f64 {
        if self.min_required == 0 { return 100.0; }
        (self.received as f64 / self.min_required as f64 * 100.0).min(100.0)
    }
}

/// Typed aggregation function that transforms collected values into a final result
pub struct AggregationReducer<T: Clone, R> {
    reduce_fn: Box<dyn Fn(HashMap<String, T>) -> R + Send + Sync>,
}

impl<T: Clone + std::fmt::Debug, R> AggregationReducer<T, R> {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(HashMap<String, T>) -> R + Send + Sync + 'static,
    {
        Self { reduce_fn: Box::new(f) }
    }

    /// Apply the reducer to an aggregate result
    pub fn apply(&self, result: AggregateResult<T>) -> ReducedResult<R> {
        let is_complete = result.is_complete;
        let elapsed = result.elapsed;
        let success_count = result.success_count();
        let failure_count = result.failure_count();
        let value = (self.reduce_fn)(result.responses);

        ReducedResult {
            value,
            is_complete,
            elapsed,
            success_count,
            failure_count,
        }
    }
}

/// Result after applying a reducer
#[derive(Debug)]
pub struct ReducedResult<R> {
    pub value: R,
    pub is_complete: bool,
    pub elapsed: Duration,
    pub success_count: usize,
    pub failure_count: usize,
}

/// Builder for creating aggregators with fluent API
pub struct AggregatorBuilder<T: Clone + std::fmt::Debug> {
    sources: Vec<String>,
    timeout: Duration,
    completion: CompletionStrategy,
    allow_duplicates: bool,
    max_retries: usize,
    retry_delay: Duration,
    filter: Option<Box<dyn Fn(&str, &T) -> bool + Send + Sync>>,
}

impl<T: Clone + std::fmt::Debug> AggregatorBuilder<T> {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            timeout: Duration::from_secs(5),
            completion: CompletionStrategy::All,
            allow_duplicates: false,
            max_retries: 0,
            retry_delay: Duration::from_millis(500),
            filter: None,
        }
    }

    pub fn source(mut self, name: impl Into<String>) -> Self {
        self.sources.push(name.into());
        self
    }

    pub fn sources(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.sources.extend(names.into_iter().map(|n| n.into()));
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn completion(mut self, strategy: CompletionStrategy) -> Self {
        self.completion = strategy;
        self
    }

    pub fn quorum(mut self, n: usize) -> Self {
        self.completion = CompletionStrategy::Quorum(n);
        self
    }

    pub fn first_wins(mut self) -> Self {
        self.completion = CompletionStrategy::First;
        self
    }

    pub fn allow_duplicates(mut self, allow: bool) -> Self {
        self.allow_duplicates = allow;
        self
    }

    pub fn retries(mut self, max: usize, delay: Duration) -> Self {
        self.max_retries = max;
        self.retry_delay = delay;
        self
    }

    pub fn filter<F>(mut self, f: F) -> Self
    where
        F: Fn(&str, &T) -> bool + Send + Sync + 'static,
    {
        self.filter = Some(Box::new(f));
        self
    }

    pub fn build(self) -> Result<Aggregator<T>, AggregatorError> {
        let config = AggregatorConfig {
            timeout: self.timeout,
            completion: self.completion,
            allow_duplicates: self.allow_duplicates,
            max_retries: self.max_retries,
            retry_delay: self.retry_delay,
        };

        let mut agg = Aggregator::new(config, self.sources)?;
        if let Some(f) = self.filter {
            agg.response_filter = Some(f);
        }
        Ok(agg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // === CompletionStrategy Tests ===

    #[test]
    fn test_completion_all() {
        let s = CompletionStrategy::All;
        assert!(!s.is_satisfied(2, 3));
        assert!(s.is_satisfied(3, 3));
        assert!(s.is_satisfied(4, 3));
    }

    #[test]
    fn test_completion_quorum() {
        let s = CompletionStrategy::Quorum(3);
        assert!(!s.is_satisfied(2, 5));
        assert!(s.is_satisfied(3, 5));
        assert!(s.is_satisfied(5, 5));
    }

    #[test]
    fn test_completion_first() {
        let s = CompletionStrategy::First;
        assert!(!s.is_satisfied(0, 5));
        assert!(s.is_satisfied(1, 5));
    }

    #[test]
    fn test_completion_threshold() {
        let s = CompletionStrategy::Threshold { min_count: 2, min_percentage: 0.5 };
        assert!(!s.is_satisfied(1, 4)); // count not met
        assert!(s.is_satisfied(2, 4));  // 50% and ≥2
        assert!(s.is_satisfied(3, 4));
    }

    #[test]
    fn test_completion_validate() {
        assert!(CompletionStrategy::Quorum(3).validate(5).is_ok());
        assert!(CompletionStrategy::Quorum(6).validate(5).is_err());
        assert!(CompletionStrategy::Quorum(0).validate(5).is_err());
        assert!(CompletionStrategy::Threshold { min_count: 2, min_percentage: 0.5 }.validate(5).is_ok());
        assert!(CompletionStrategy::Threshold { min_count: 2, min_percentage: 1.5 }.validate(5).is_err());
        assert!(CompletionStrategy::Threshold { min_count: 6, min_percentage: 0.5 }.validate(5).is_err());
    }

    // === ResponseStatus Tests ===

    #[test]
    fn test_response_status() {
        let pending: ResponseStatus<i32> = ResponseStatus::Pending;
        assert!(pending.is_pending());
        assert!(!pending.is_received());
        assert!(!pending.is_terminal());
        assert!(pending.value().is_none());

        let received = ResponseStatus::Received(42);
        assert!(!received.is_pending());
        assert!(received.is_received());
        assert!(received.is_terminal());
        assert_eq!(received.value(), Some(&42));

        let failed: ResponseStatus<i32> = ResponseStatus::Failed("err".into());
        assert!(!failed.is_pending());
        assert!(!failed.is_received());
        assert!(failed.is_terminal());
    }

    // === AggregatorConfig Tests ===

    #[test]
    fn test_config_default() {
        let cfg = AggregatorConfig::default();
        assert_eq!(cfg.timeout, Duration::from_secs(5));
        assert_eq!(cfg.completion, CompletionStrategy::All);
        assert!(!cfg.allow_duplicates);
        assert_eq!(cfg.max_retries, 0);
    }

    #[test]
    fn test_config_builders() {
        let cfg = AggregatorConfig::quorum(5, 3, Duration::from_secs(10));
        assert_eq!(cfg.timeout, Duration::from_secs(10));
        assert_eq!(cfg.completion, CompletionStrategy::Quorum(3));

        let cfg = AggregatorConfig::first_response(Duration::from_secs(2));
        assert_eq!(cfg.completion, CompletionStrategy::First);
    }

    #[test]
    fn test_config_chaining() {
        let cfg = AggregatorConfig::new(Duration::from_secs(3))
            .with_completion(CompletionStrategy::Quorum(2))
            .with_allow_duplicates(true)
            .with_max_retries(3)
            .with_retry_delay(Duration::from_millis(100));
        assert_eq!(cfg.completion, CompletionStrategy::Quorum(2));
        assert!(cfg.allow_duplicates);
        assert_eq!(cfg.max_retries, 3);
    }

    // === RetryState Tests ===

    #[test]
    fn test_retry_state() {
        let mut retry = RetryState::new(3, Duration::from_millis(10));
        assert!(retry.can_retry());
        assert!(retry.should_retry_now());

        retry.record_attempt();
        assert_eq!(retry.attempts, 1);
        assert!(retry.can_retry()); // 1 < 3

        retry.record_attempt();
        retry.record_attempt();
        assert!(!retry.can_retry()); // 3 >= 3
    }

    // === Aggregator Core Tests ===

    #[test]
    fn test_aggregator_all_responses() {
        let sources = vec!["a".into(), "b".into(), "c".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5));
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();

        let s1 = agg.add_response("a", 1).unwrap();
        assert!(matches!(s1, AggregateStatus::Pending { received: 1, remaining: 2 }));

        let s2 = agg.add_response("b", 2).unwrap();
        assert!(matches!(s2, AggregateStatus::Pending { received: 2, remaining: 1 }));

        let s3 = agg.add_response("c", 3).unwrap();
        assert_eq!(s3, AggregateStatus::Complete);
        assert!(agg.is_complete());

        let result = agg.finalize();
        assert_eq!(result.success_count(), 3);
        assert_eq!(result.failure_count(), 0);
        assert!(result.is_complete);
    }

    #[test]
    fn test_aggregator_quorum() {
        let sources = vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5))
            .with_completion(CompletionStrategy::Quorum(3));
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();

        agg.add_response("a", 10).unwrap();
        agg.add_response("c", 30).unwrap();
        let status = agg.add_response("e", 50).unwrap();
        assert_eq!(status, AggregateStatus::Complete);

        let result = agg.finalize();
        assert_eq!(result.success_count(), 3);
        assert!(result.is_complete);
    }

    #[test]
    fn test_aggregator_first_wins() {
        let sources = vec!["a".into(), "b".into(), "c".into()];
        let config = AggregatorConfig::first_response(Duration::from_secs(5));
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();

        let status = agg.add_response("b", 42).unwrap();
        assert_eq!(status, AggregateStatus::Complete);

        let result = agg.finalize();
        assert_eq!(result.success_count(), 1);
        assert_eq!(*result.responses.get("b").unwrap(), 42);
    }

    #[test]
    fn test_aggregator_duplicate_rejection() {
        let sources = vec!["a".into(), "b".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5));
        let mut agg = Aggregator::new(config, sources).unwrap();

        agg.add_response("a", 1).unwrap();
        let err = agg.add_response("a", 2).unwrap_err();
        assert!(matches!(err, AggregatorError::DuplicateResponse(_)));
    }

    #[test]
    fn test_aggregator_allow_duplicates() {
        let sources = vec!["a".into(), "b".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5))
            .with_allow_duplicates(true);
        let mut agg = Aggregator::new(config, sources).unwrap();

        agg.add_response("a", 1).unwrap();
        // Should be able to update
        agg.add_response("a", 99).unwrap();

        let result = agg.finalize();
        assert_eq!(*result.responses.get("a").unwrap(), 99);
    }

    #[test]
    fn test_aggregator_unknown_source() {
        let sources = vec!["a".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5));
        let mut agg = Aggregator::new(config, sources).unwrap();

        let err = agg.add_response("z", 1).unwrap_err();
        assert!(matches!(err, AggregatorError::UnknownSource(_)));
    }

    #[test]
    fn test_aggregator_failure_handling() {
        let sources = vec!["a".into(), "b".into(), "c".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5))
            .with_completion(CompletionStrategy::Quorum(2));
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();

        agg.add_failure("a", "network error".into()).unwrap();
        agg.add_response("b", 1).unwrap();
        let status = agg.add_response("c", 2).unwrap();
        assert_eq!(status, AggregateStatus::Complete);

        let result = agg.finalize();
        assert_eq!(result.success_count(), 2);
        assert_eq!(result.failure_count(), 1);
        assert!(result.is_complete);
    }

    #[test]
    fn test_aggregator_impossible_completion() {
        let sources = vec!["a".into(), "b".into(), "c".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5)); // needs All
        let mut agg: Aggregator<i32> = Aggregator::new(config, sources).unwrap();
        agg.start();

        agg.add_failure("a", "fail".into()).unwrap();
        let status = agg.add_failure("b", "fail".into()).unwrap();
        // With 2 failures, can't satisfy All with 3 sources
        assert_eq!(status, AggregateStatus::Failed);
    }

    #[test]
    fn test_aggregator_timeout() {
        let sources = vec!["a".into(), "b".into()];
        let config = AggregatorConfig::new(Duration::from_millis(1));
        let mut agg: Aggregator<i32> = Aggregator::new(config, sources).unwrap();
        agg.start();

        std::thread::sleep(Duration::from_millis(5));

        assert!(agg.check_timeout());
        assert!(agg.is_complete());

        let result = agg.finalize();
        assert_eq!(result.success_count(), 0);
        assert_eq!(result.failure_count(), 2);
        assert!(!result.is_complete);
    }

    #[test]
    fn test_aggregator_partial_timeout() {
        let sources = vec!["a".into(), "b".into()];
        let config = AggregatorConfig::new(Duration::from_millis(1))
            .with_completion(CompletionStrategy::Quorum(1));
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();

        agg.add_response("a", 42).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        agg.check_timeout();

        let result = agg.finalize();
        assert_eq!(result.success_count(), 1);
        assert!(result.is_complete); // quorum 1 satisfied
    }

    #[test]
    fn test_aggregator_already_completed() {
        let sources = vec!["a".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5));
        let mut agg = Aggregator::new(config, sources).unwrap();

        agg.add_response("a", 1).unwrap();
        let err = agg.add_response("a", 2).unwrap_err();
        assert!(matches!(err, AggregatorError::AlreadyCompleted));
    }

    #[test]
    fn test_aggregator_with_filter() {
        let sources = vec!["a".into(), "b".into(), "c".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5))
            .with_completion(CompletionStrategy::Quorum(2));
        let mut agg = Aggregator::new(config, sources).unwrap();
        // Only accept positive values
        agg.response_filter = Some(Box::new(|_, v: &i32| *v > 0));
        agg.start();

        let err = agg.add_response("a", -1).unwrap_err();
        assert!(matches!(err, AggregatorError::Rejected(_)));

        agg.add_response("b", 10).unwrap();
        let status = agg.add_response("c", 20).unwrap();
        assert_eq!(status, AggregateStatus::Complete);
    }

    #[test]
    fn test_aggregator_retry() {
        let sources = vec!["a".into(), "b".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5))
            .with_max_retries(2)
            .with_retry_delay(Duration::from_millis(1));
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();

        let status = agg.add_failure("a", "err".into()).unwrap();
        assert!(matches!(status, AggregateStatus::RetryScheduled { .. }));

        // After first retry-scheduled failure, "a" is still "Failed"
        // The source should still be retryable
        // Simulate the retry succeeding
        agg.sources.insert("a".to_string(), ResponseStatus::Pending); // Reset for retry
        agg.add_response("a", 1).unwrap();
        agg.add_response("b", 2).unwrap();

        assert!(agg.is_complete());
    }

    // === AggregateResult Tests ===

    #[test]
    fn test_aggregate_result_stats() {
        let sources = vec!["a".into(), "b".into(), "c".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5));
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();

        agg.add_response("a", 1).unwrap();
        agg.add_response("b", 2).unwrap();
        agg.add_response("c", 3).unwrap();

        let result = agg.finalize();
        assert_eq!(result.success_count(), 3);
        assert_eq!(result.failure_count(), 0);
        assert_eq!(result.total_sources(), 3);
        assert!((result.success_rate() - 1.0).abs() < f64::EPSILON);
        assert_eq!(result.values().len(), 3);
    }

    #[test]
    fn test_aggregate_result_mixed() {
        let sources = vec!["a".into(), "b".into(), "c".into()];
        let config = AggregatorConfig::new(Duration::from_millis(1))
            .with_completion(CompletionStrategy::Quorum(1));
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();

        agg.add_response("a", 10).unwrap();
        agg.add_failure("b", "error".into()).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        agg.check_timeout();

        let result = agg.finalize();
        assert_eq!(result.success_count(), 1);
        assert_eq!(result.failure_count(), 2);
        assert!(result.is_complete); // quorum met
    }

    // === Progress Tests ===

    #[test]
    fn test_aggregation_progress() {
        let sources = vec!["a".into(), "b".into(), "c".into(), "d".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5))
            .with_completion(CompletionStrategy::Quorum(2));
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();

        agg.add_response("a", 1).unwrap();

        let progress = agg.progress();
        assert_eq!(progress.received, 1);
        assert_eq!(progress.pending, 3);
        assert_eq!(progress.failed, 0);
        assert_eq!(progress.total, 4);
        assert!(!progress.is_complete);
        assert_eq!(progress.min_required, 2);
        assert!((progress.completion_percentage() - 50.0).abs() < f64::EPSILON);
    }

    // === AggregationReducer Tests ===

    #[test]
    fn test_reducer_sum() {
        let reducer = AggregationReducer::<i32, i32>::new(|responses| {
            responses.values().sum()
        });

        let sources = vec!["a".into(), "b".into(), "c".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5));
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();
        agg.add_response("a", 10).unwrap();
        agg.add_response("b", 20).unwrap();
        agg.add_response("c", 30).unwrap();

        let result = reducer.apply(agg.finalize());
        assert_eq!(result.value, 60);
        assert!(result.is_complete);
        assert_eq!(result.success_count, 3);
    }

    #[test]
    fn test_reducer_max() {
        let reducer = AggregationReducer::<i32, Option<i32>>::new(|responses| {
            responses.values().max().copied()
        });

        let sources = vec!["x".into(), "y".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5));
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();
        agg.add_response("x", 5).unwrap();
        agg.add_response("y", 15).unwrap();

        let result = reducer.apply(agg.finalize());
        assert_eq!(result.value, Some(15));
    }

    // === Builder Tests ===

    #[test]
    fn test_aggregator_builder() {
        let agg = AggregatorBuilder::<i32>::new()
            .source("a")
            .source("b")
            .source("c")
            .timeout(Duration::from_secs(10))
            .quorum(2)
            .allow_duplicates(true)
            .retries(3, Duration::from_millis(100))
            .build()
            .unwrap();

        assert_eq!(agg.sources.len(), 3);
        assert_eq!(agg.config.timeout, Duration::from_secs(10));
        assert_eq!(agg.config.completion, CompletionStrategy::Quorum(2));
    }

    #[test]
    fn test_aggregator_builder_sources_iter() {
        let agg = AggregatorBuilder::<String>::new()
            .sources(vec!["s1", "s2", "s3"])
            .first_wins()
            .build()
            .unwrap();

        assert_eq!(agg.sources.len(), 3);
        assert_eq!(agg.config.completion, CompletionStrategy::First);
    }

    #[test]
    fn test_builder_with_filter() {
        let mut agg = AggregatorBuilder::<i32>::new()
            .sources(vec!["a", "b"])
            .filter(|_, v| *v > 0)
            .build()
            .unwrap();

        agg.start();
        let err = agg.add_response("a", -5).unwrap_err();
        assert!(matches!(err, AggregatorError::Rejected(_)));
    }

    #[test]
    fn test_invalid_config_quorum_exceeds() {
        let err = AggregatorBuilder::<i32>::new()
            .sources(vec!["a", "b"])
            .quorum(5)
            .build()
            .unwrap_err();
        assert!(matches!(err, AggregatorError::InvalidConfig(_)));
    }

    // === Source Status Tests ===

    #[test]
    fn test_source_status_tracking() {
        let sources = vec!["a".into(), "b".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5));
        let mut agg = Aggregator::new(config, sources).unwrap();

        assert!(agg.source_status("a").unwrap().is_pending());
        agg.add_response("a", 42).unwrap();
        assert!(agg.source_status("a").unwrap().is_received());
        assert_eq!(agg.source_status("a").unwrap().value(), Some(&42));
        assert!(agg.source_status("nonexistent").is_none());
    }

    #[test]
    fn test_remaining_time() {
        let sources = vec!["a".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5));
        let mut agg: Aggregator<i32> = Aggregator::new(config, sources).unwrap();

        assert!(agg.remaining_time().is_none()); // not started

        agg.start();
        let remaining = agg.remaining_time().unwrap();
        assert!(remaining <= Duration::from_secs(5));
        assert!(remaining > Duration::from_secs(4));
    }

    #[test]
    fn test_auto_start_on_response() {
        let sources = vec!["a".into(), "b".into()];
        let config = AggregatorConfig::new(Duration::from_secs(5));
        let mut agg = Aggregator::new(config, sources).unwrap();

        assert!(agg.remaining_time().is_none());
        agg.add_response("a", 1).unwrap();
        assert!(agg.remaining_time().is_some()); // auto-started
    }

    // === Threshold Completion Tests ===

    #[test]
    fn test_threshold_completion() {
        let sources: Vec<String> = (0..10).map(|i| format!("s{}", i)).collect();
        let config = AggregatorConfig::new(Duration::from_secs(5))
            .with_completion(CompletionStrategy::Threshold {
                min_count: 5,
                min_percentage: 0.5,
            });
        let mut agg = Aggregator::new(config, sources).unwrap();
        agg.start();

        for i in 0..4 {
            agg.add_response(&format!("s{}", i), i as i32).unwrap();
        }
        assert!(!agg.is_complete());

        // 5th response meets min_count=5 and 50%
        let status = agg.add_response("s4", 4).unwrap();
        assert_eq!(status, AggregateStatus::Complete);
    }

    #[test]
    fn test_threshold_zero_expected() {
        let s = CompletionStrategy::Threshold { min_count: 0, min_percentage: 0.5 };
        assert!(s.is_satisfied(0, 0));
    }
}
