//! # Message Adapter
//!
//! Provides message transformation and adaptation between actor types.
//! Based on Akka/Pekko's MessageAdapter for protocol bridging.
//!
//! ## Features
//! - Type-safe message transformation between incompatible actor protocols
//! - Adapter chains for multi-step transformations
//! - MessageWrapper for envelope-based forwarding
//! - SpawnProtocol for typed actor spawning requests
//! - Backpressure-aware message buffering

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// A message adapter that transforms messages from one type to another
pub struct MessageAdapter<From, To> {
    /// The transformation function
    transform: Box<dyn Fn(From) -> Option<To> + Send + Sync>,
    /// Name for debugging
    name: String,
    /// Statistics
    stats: AdapterStats,
}

impl<From, To> MessageAdapter<From, To>
where
    From: Send + 'static,
    To: Send + 'static,
{
    /// Create a new message adapter with a transformation function
    pub fn new<F>(name: impl Into<String>, transform: F) -> Self
    where
        F: Fn(From) -> Option<To> + Send + Sync + 'static,
    {
        Self {
            transform: Box::new(transform),
            name: name.into(),
            stats: AdapterStats::new(),
        }
    }

    /// Create an adapter that always succeeds (infallible transform)
    pub fn infallible<F>(name: impl Into<String>, transform: F) -> Self
    where
        F: Fn(From) -> To + Send + Sync + 'static,
    {
        Self {
            transform: Box::new(move |msg| Some(transform(msg))),
            name: name.into(),
            stats: AdapterStats::new(),
        }
    }

    /// Apply the transformation
    pub fn adapt(&mut self, msg: From) -> Option<To> {
        self.stats.total_received += 1;
        let result = (self.transform)(msg);
        if result.is_some() {
            self.stats.total_transformed += 1;
        } else {
            self.stats.total_dropped += 1;
        }
        result
    }

    /// Get the adapter name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get adapter statistics
    pub fn stats(&self) -> &AdapterStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = AdapterStats::new();
    }
}

/// Statistics for a message adapter
#[derive(Debug, Clone)]
pub struct AdapterStats {
    pub total_received: u64,
    pub total_transformed: u64,
    pub total_dropped: u64,
}

impl AdapterStats {
    pub fn new() -> Self {
        Self {
            total_received: 0,
            total_transformed: 0,
            total_dropped: 0,
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_received == 0 { return 0.0; }
        self.total_transformed as f64 / self.total_received as f64
    }

    pub fn drop_rate(&self) -> f64 {
        if self.total_received == 0 { return 0.0; }
        self.total_dropped as f64 / self.total_received as f64
    }
}

impl Default for AdapterStats {
    fn default() -> Self {
        Self::new()
    }
}

/// A chain of adapters that transforms A → B → C
pub struct AdapterChain<A, C> {
    /// The composed transformation
    transform: Box<dyn Fn(A) -> Option<C> + Send + Sync>,
    name: String,
    stats: AdapterStats,
}

impl<A, C> AdapterChain<A, C>
where
    A: Send + 'static,
    C: Send + 'static,
{
    /// Create a chain from two transformations
    pub fn new<B, F1, F2>(
        name: impl Into<String>,
        first: F1,
        second: F2,
    ) -> Self
    where
        B: Send + 'static,
        F1: Fn(A) -> Option<B> + Send + Sync + 'static,
        F2: Fn(B) -> Option<C> + Send + Sync + 'static,
    {
        Self {
            transform: Box::new(move |a| {
                first(a).and_then(|b| second(b))
            }),
            name: name.into(),
            stats: AdapterStats::new(),
        }
    }

    /// Apply the chain transformation
    pub fn adapt(&mut self, msg: A) -> Option<C> {
        self.stats.total_received += 1;
        let result = (self.transform)(msg);
        if result.is_some() {
            self.stats.total_transformed += 1;
        } else {
            self.stats.total_dropped += 1;
        }
        result
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn stats(&self) -> &AdapterStats {
        &self.stats
    }
}

/// A message envelope that wraps a message with metadata
#[derive(Debug, Clone)]
pub struct MessageEnvelope<M> {
    /// The actual message
    pub message: M,
    /// Source identifier
    pub sender: String,
    /// Target identifier
    pub recipient: String,
    /// Correlation ID for request-response tracking
    pub correlation_id: Option<String>,
    /// Timestamp when the envelope was created
    pub timestamp: Instant,
    /// Number of hops this message has taken
    pub hop_count: u32,
    /// Custom headers
    pub headers: Vec<(String, String)>,
}

impl<M> MessageEnvelope<M> {
    pub fn new(message: M, sender: impl Into<String>, recipient: impl Into<String>) -> Self {
        Self {
            message,
            sender: sender.into(),
            recipient: recipient.into(),
            correlation_id: None,
            timestamp: Instant::now(),
            hop_count: 0,
            headers: Vec::new(),
        }
    }

    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    pub fn get_header(&self, key: &str) -> Option<&str> {
        self.headers.iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Forward this envelope to a new recipient, incrementing hop count
    pub fn forward(mut self, new_recipient: impl Into<String>) -> Self {
        self.recipient = new_recipient.into();
        self.hop_count += 1;
        self
    }

    /// Transform the inner message while preserving envelope metadata
    pub fn map<N, F>(self, f: F) -> MessageEnvelope<N>
    where
        F: FnOnce(M) -> N,
    {
        MessageEnvelope {
            message: f(self.message),
            sender: self.sender,
            recipient: self.recipient,
            correlation_id: self.correlation_id,
            timestamp: self.timestamp,
            hop_count: self.hop_count,
            headers: self.headers,
        }
    }

    /// Extract the inner message, consuming the envelope
    pub fn unwrap(self) -> M {
        self.message
    }

    pub fn age(&self) -> Duration {
        self.timestamp.elapsed()
    }
}

/// Request to spawn a new actor (SpawnProtocol)
#[derive(Debug, Clone)]
pub struct SpawnRequest {
    /// Name for the new actor
    pub name: String,
    /// Actor type identifier
    pub actor_type: String,
    /// Optional configuration
    pub config: SpawnConfig,
    /// Requester identifier for response routing
    pub requester: String,
    /// Correlation ID
    pub correlation_id: String,
}

impl SpawnRequest {
    pub fn new(
        name: impl Into<String>,
        actor_type: impl Into<String>,
        requester: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            actor_type: actor_type.into(),
            config: SpawnConfig::default(),
            requester: requester.into(),
            correlation_id: format!("spawn-{}", uuid_v4()),
        }
    }

    pub fn with_config(mut self, config: SpawnConfig) -> Self {
        self.config = config;
        self
    }
}

/// Configuration for spawning an actor
#[derive(Debug, Clone)]
pub struct SpawnConfig {
    pub dispatcher: Option<String>,
    pub mailbox_size: Option<usize>,
    pub supervision: Option<String>,
    pub tags: Vec<String>,
}

impl Default for SpawnConfig {
    fn default() -> Self {
        Self {
            dispatcher: None,
            mailbox_size: None,
            supervision: None,
            tags: Vec::new(),
        }
    }
}

impl SpawnConfig {
    pub fn with_dispatcher(mut self, dispatcher: impl Into<String>) -> Self {
        self.dispatcher = Some(dispatcher.into());
        self
    }

    pub fn with_mailbox_size(mut self, size: usize) -> Self {
        self.mailbox_size = Some(size);
        self
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }
}

/// Response to a spawn request
#[derive(Debug, Clone)]
pub enum SpawnResponse {
    /// Actor was successfully spawned
    Spawned {
        name: String,
        actor_path: String,
        correlation_id: String,
    },
    /// Spawn failed
    Failed {
        name: String,
        reason: String,
        correlation_id: String,
    },
}

impl SpawnResponse {
    pub fn is_success(&self) -> bool {
        matches!(self, SpawnResponse::Spawned { .. })
    }

    pub fn correlation_id(&self) -> &str {
        match self {
            SpawnResponse::Spawned { correlation_id, .. } => correlation_id,
            SpawnResponse::Failed { correlation_id, .. } => correlation_id,
        }
    }
}

/// A buffered adapter that queues transformed messages
pub struct BufferedAdapter<From, To> {
    adapter: MessageAdapter<From, To>,
    buffer: VecDeque<To>,
    max_buffer_size: usize,
    overflow_strategy: BufferOverflow,
    overflow_count: u64,
}

/// What to do when buffer is full
#[derive(Debug, Clone, PartialEq)]
pub enum BufferOverflow {
    /// Drop the newest message
    DropNewest,
    /// Drop the oldest message
    DropOldest,
    /// Reject the message (return error)
    Reject,
}

impl<From, To> BufferedAdapter<From, To>
where
    From: Send + 'static,
    To: Send + 'static,
{
    pub fn new(adapter: MessageAdapter<From, To>, max_size: usize) -> Self {
        Self {
            adapter,
            buffer: VecDeque::with_capacity(max_size.min(1024)),
            max_buffer_size: max_size,
            overflow_strategy: BufferOverflow::DropOldest,
            overflow_count: 0,
        }
    }

    pub fn with_overflow_strategy(mut self, strategy: BufferOverflow) -> Self {
        self.overflow_strategy = strategy;
        self
    }

    /// Transform and buffer a message
    pub fn push(&mut self, msg: From) -> Result<(), AdapterError> {
        if let Some(transformed) = self.adapter.adapt(msg) {
            if self.buffer.len() >= self.max_buffer_size {
                match self.overflow_strategy {
                    BufferOverflow::DropOldest => {
                        self.buffer.pop_front();
                        self.overflow_count += 1;
                    }
                    BufferOverflow::DropNewest => {
                        self.overflow_count += 1;
                        return Ok(()); // drop the new one
                    }
                    BufferOverflow::Reject => {
                        return Err(AdapterError::BufferFull);
                    }
                }
            }
            self.buffer.push_back(transformed);
        }
        Ok(())
    }

    /// Take the next buffered message
    pub fn pop(&mut self) -> Option<To> {
        self.buffer.pop_front()
    }

    /// Drain all buffered messages
    pub fn drain(&mut self) -> Vec<To> {
        self.buffer.drain(..).collect()
    }

    /// Number of buffered messages
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    /// Whether buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Whether buffer is full
    pub fn is_full(&self) -> bool {
        self.buffer.len() >= self.max_buffer_size
    }

    pub fn overflow_count(&self) -> u64 {
        self.overflow_count
    }

    pub fn adapter_stats(&self) -> &AdapterStats {
        self.adapter.stats()
    }
}

/// Error types for adapter operations
#[derive(Debug, thiserror::Error, Clone)]
pub enum AdapterError {
    #[error("Buffer is full")]
    BufferFull,
    #[error("Transformation failed: {0}")]
    TransformFailed(String),
    #[error("Adapter chain broken at step: {0}")]
    ChainBroken(String),
}

/// Simple UUID v4 generator (for correlation IDs)
fn uuid_v4() -> String {
    use std::time::SystemTime;
    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:016x}{:016x}", t, t.wrapping_mul(6364136223846793005))
}

/// Protocol bridge for connecting two actor protocols
pub struct ProtocolBridge<A, B> {
    /// A → B adapter
    forward: MessageAdapter<A, B>,
    /// B → A adapter (optional, for bidirectional)
    reverse: Option<MessageAdapter<B, A>>,
    name: String,
}

impl<A, B> ProtocolBridge<A, B>
where
    A: Send + 'static,
    B: Send + 'static,
{
    /// Create a unidirectional bridge (A → B)
    pub fn unidirectional<F>(name: impl Into<String>, forward: F) -> Self
    where
        F: Fn(A) -> Option<B> + Send + Sync + 'static,
    {
        let name = name.into();
        Self {
            forward: MessageAdapter::new(format!("{}-fwd", name), forward),
            reverse: None,
            name,
        }
    }

    /// Create a bidirectional bridge (A ↔ B)
    pub fn bidirectional<F1, F2>(
        name: impl Into<String>,
        forward: F1,
        reverse: F2,
    ) -> Self
    where
        F1: Fn(A) -> Option<B> + Send + Sync + 'static,
        F2: Fn(B) -> Option<A> + Send + Sync + 'static,
    {
        let name = name.into();
        Self {
            forward: MessageAdapter::new(format!("{}-fwd", name), forward),
            reverse: Some(MessageAdapter::new(format!("{}-rev", name), reverse)),
            name,
        }
    }

    /// Transform A → B
    pub fn forward(&mut self, msg: A) -> Option<B> {
        self.forward.adapt(msg)
    }

    /// Transform B → A (only for bidirectional bridges)
    pub fn reverse(&mut self, msg: B) -> Option<A> {
        self.reverse.as_mut().and_then(|r| r.adapt(msg))
    }

    /// Check if this bridge is bidirectional
    pub fn is_bidirectional(&self) -> bool {
        self.reverse.is_some()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn forward_stats(&self) -> &AdapterStats {
        self.forward.stats()
    }

    pub fn reverse_stats(&self) -> Option<&AdapterStats> {
        self.reverse.as_ref().map(|r| r.stats())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // === MessageAdapter Tests ===

    #[test]
    fn test_adapter_basic() {
        let mut adapter = MessageAdapter::infallible("int-to-string", |n: i32| format!("val:{}", n));
        let result = adapter.adapt(42);
        assert_eq!(result, Some("val:42".to_string()));
        assert_eq!(adapter.stats().total_received, 1);
        assert_eq!(adapter.stats().total_transformed, 1);
    }

    #[test]
    fn test_adapter_filtering() {
        let mut adapter = MessageAdapter::new("positive-only", |n: i32| {
            if n > 0 { Some(n as u32) } else { None }
        });

        assert_eq!(adapter.adapt(5), Some(5u32));
        assert_eq!(adapter.adapt(-3), None);
        assert_eq!(adapter.adapt(10), Some(10u32));

        assert_eq!(adapter.stats().total_received, 3);
        assert_eq!(adapter.stats().total_transformed, 2);
        assert_eq!(adapter.stats().total_dropped, 1);
    }

    #[test]
    fn test_adapter_name_and_reset() {
        let mut adapter = MessageAdapter::infallible("test", |n: i32| n * 2);
        assert_eq!(adapter.name(), "test");

        adapter.adapt(1);
        adapter.adapt(2);
        assert_eq!(adapter.stats().total_received, 2);

        adapter.reset_stats();
        assert_eq!(adapter.stats().total_received, 0);
    }

    #[test]
    fn test_adapter_stats_rates() {
        let mut adapter = MessageAdapter::new("half", |n: i32| {
            if n % 2 == 0 { Some(n) } else { None }
        });
        for i in 0..10 {
            adapter.adapt(i);
        }
        assert!((adapter.stats().success_rate() - 0.5).abs() < f64::EPSILON);
        assert!((adapter.stats().drop_rate() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_adapter_stats_empty() {
        let stats = AdapterStats::new();
        assert!((stats.success_rate() - 0.0).abs() < f64::EPSILON);
        assert!((stats.drop_rate() - 0.0).abs() < f64::EPSILON);
    }

    // === AdapterChain Tests ===

    #[test]
    fn test_chain_basic() {
        let mut chain = AdapterChain::new(
            "int-to-float-to-string",
            |n: i32| Some(n as f64),
            |f: f64| Some(format!("{:.1}", f)),
        );

        let result = chain.adapt(42);
        assert_eq!(result, Some("42.0".to_string()));
        assert_eq!(chain.stats().total_transformed, 1);
    }

    #[test]
    fn test_chain_first_fails() {
        let mut chain = AdapterChain::new(
            "filter-then-map",
            |n: i32| if n > 0 { Some(n) } else { None },
            |n: i32| Some(n * 10),
        );

        assert_eq!(chain.adapt(5), Some(50));
        assert_eq!(chain.adapt(-1), None);
        assert_eq!(chain.stats().total_dropped, 1);
    }

    #[test]
    fn test_chain_second_fails() {
        let mut chain = AdapterChain::new(
            "map-then-filter",
            |n: i32| Some(n * 2),
            |n: i32| if n < 100 { Some(n) } else { None },
        );

        assert_eq!(chain.adapt(10), Some(20));
        assert_eq!(chain.adapt(60), None); // 120 > 100
    }

    // === MessageEnvelope Tests ===

    #[test]
    fn test_envelope_basic() {
        let env = MessageEnvelope::new("hello", "sender-1", "recipient-1");
        assert_eq!(env.message, "hello");
        assert_eq!(env.sender, "sender-1");
        assert_eq!(env.recipient, "recipient-1");
        assert!(env.correlation_id.is_none());
        assert_eq!(env.hop_count, 0);
    }

    #[test]
    fn test_envelope_with_metadata() {
        let env = MessageEnvelope::new(42, "s", "r")
            .with_correlation_id("corr-123")
            .with_header("priority", "high")
            .with_header("source", "api");

        assert_eq!(env.correlation_id, Some("corr-123".to_string()));
        assert_eq!(env.get_header("priority"), Some("high"));
        assert_eq!(env.get_header("source"), Some("api"));
        assert_eq!(env.get_header("missing"), None);
    }

    #[test]
    fn test_envelope_forward() {
        let env = MessageEnvelope::new("msg", "sender", "recv-1");
        let forwarded = env.forward("recv-2");
        assert_eq!(forwarded.recipient, "recv-2");
        assert_eq!(forwarded.sender, "sender"); // original sender preserved
        assert_eq!(forwarded.hop_count, 1);

        let twice = forwarded.forward("recv-3");
        assert_eq!(twice.hop_count, 2);
    }

    #[test]
    fn test_envelope_map() {
        let env = MessageEnvelope::new(42, "s", "r")
            .with_correlation_id("c1");

        let mapped = env.map(|n| format!("value:{}", n));
        assert_eq!(mapped.message, "value:42");
        assert_eq!(mapped.sender, "s");
        assert_eq!(mapped.correlation_id, Some("c1".to_string()));
    }

    #[test]
    fn test_envelope_unwrap() {
        let env = MessageEnvelope::new(99, "s", "r");
        assert_eq!(env.unwrap(), 99);
    }

    #[test]
    fn test_envelope_age() {
        let env = MessageEnvelope::new(1, "s", "r");
        std::thread::sleep(Duration::from_millis(5));
        assert!(env.age() >= Duration::from_millis(5));
    }

    // === SpawnRequest/Response Tests ===

    #[test]
    fn test_spawn_request() {
        let req = SpawnRequest::new("worker-1", "Worker", "supervisor");
        assert_eq!(req.name, "worker-1");
        assert_eq!(req.actor_type, "Worker");
        assert_eq!(req.requester, "supervisor");
        assert!(!req.correlation_id.is_empty());
    }

    #[test]
    fn test_spawn_request_with_config() {
        let config = SpawnConfig::default()
            .with_dispatcher("custom")
            .with_mailbox_size(100)
            .with_tag("worker");

        let req = SpawnRequest::new("w1", "Worker", "sup")
            .with_config(config);

        assert_eq!(req.config.dispatcher, Some("custom".to_string()));
        assert_eq!(req.config.mailbox_size, Some(100));
        assert_eq!(req.config.tags, vec!["worker".to_string()]);
    }

    #[test]
    fn test_spawn_response_success() {
        let resp = SpawnResponse::Spawned {
            name: "w1".into(),
            actor_path: "/user/w1".into(),
            correlation_id: "c1".into(),
        };
        assert!(resp.is_success());
        assert_eq!(resp.correlation_id(), "c1");
    }

    #[test]
    fn test_spawn_response_failure() {
        let resp = SpawnResponse::Failed {
            name: "w1".into(),
            reason: "out of memory".into(),
            correlation_id: "c2".into(),
        };
        assert!(!resp.is_success());
        assert_eq!(resp.correlation_id(), "c2");
    }

    // === BufferedAdapter Tests ===

    #[test]
    fn test_buffered_basic() {
        let adapter = MessageAdapter::infallible("double", |n: i32| n * 2);
        let mut buf = BufferedAdapter::new(adapter, 10);

        buf.push(5).unwrap();
        buf.push(10).unwrap();

        assert_eq!(buf.buffered_count(), 2);
        assert_eq!(buf.pop(), Some(10));
        assert_eq!(buf.pop(), Some(20));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_buffered_overflow_drop_oldest() {
        let adapter = MessageAdapter::infallible("id", |n: i32| n);
        let mut buf = BufferedAdapter::new(adapter, 3)
            .with_overflow_strategy(BufferOverflow::DropOldest);

        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();
        assert!(buf.is_full());

        buf.push(4).unwrap(); // drops 1
        assert_eq!(buf.overflow_count(), 1);
        assert_eq!(buf.pop(), Some(2)); // 1 was dropped
    }

    #[test]
    fn test_buffered_overflow_drop_newest() {
        let adapter = MessageAdapter::infallible("id", |n: i32| n);
        let mut buf = BufferedAdapter::new(adapter, 2)
            .with_overflow_strategy(BufferOverflow::DropNewest);

        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap(); // dropped (newest)

        assert_eq!(buf.overflow_count(), 1);
        assert_eq!(buf.pop(), Some(1));
        assert_eq!(buf.pop(), Some(2));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_buffered_overflow_reject() {
        let adapter = MessageAdapter::infallible("id", |n: i32| n);
        let mut buf = BufferedAdapter::new(adapter, 2)
            .with_overflow_strategy(BufferOverflow::Reject);

        buf.push(1).unwrap();
        buf.push(2).unwrap();
        let err = buf.push(3).unwrap_err();
        assert!(matches!(err, AdapterError::BufferFull));
    }

    #[test]
    fn test_buffered_drain() {
        let adapter = MessageAdapter::infallible("id", |n: i32| n);
        let mut buf = BufferedAdapter::new(adapter, 10);

        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();

        let all = buf.drain();
        assert_eq!(all, vec![1, 2, 3]);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_buffered_with_filter() {
        let adapter = MessageAdapter::new("evens", |n: i32| {
            if n % 2 == 0 { Some(n) } else { None }
        });
        let mut buf = BufferedAdapter::new(adapter, 10);

        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();
        buf.push(4).unwrap();

        assert_eq!(buf.buffered_count(), 2);
        assert_eq!(buf.drain(), vec![2, 4]);
    }

    // === ProtocolBridge Tests ===

    #[test]
    fn test_unidirectional_bridge() {
        let mut bridge = ProtocolBridge::unidirectional(
            "int-to-str",
            |n: i32| Some(format!("{}", n)),
        );

        assert!(!bridge.is_bidirectional());
        assert_eq!(bridge.forward(42), Some("42".to_string()));
        assert_eq!(bridge.reverse("hello".to_string()), None);
    }

    #[test]
    fn test_bidirectional_bridge() {
        let mut bridge = ProtocolBridge::bidirectional(
            "int-str",
            |n: i32| Some(format!("{}", n)),
            |s: String| s.parse::<i32>().ok(),
        );

        assert!(bridge.is_bidirectional());
        assert_eq!(bridge.forward(42), Some("42".to_string()));
        assert_eq!(bridge.reverse("100".to_string()), Some(100));
        assert_eq!(bridge.reverse("not_a_number".to_string()), None);
    }

    #[test]
    fn test_bridge_stats() {
        let mut bridge = ProtocolBridge::bidirectional(
            "test",
            |n: i32| Some(n as f64),
            |f: f64| Some(f as i32),
        );

        bridge.forward(1);
        bridge.forward(2);
        bridge.reverse(3.0);

        assert_eq!(bridge.forward_stats().total_received, 2);
        assert_eq!(bridge.reverse_stats().unwrap().total_received, 1);
    }

    #[test]
    fn test_bridge_name() {
        let bridge = ProtocolBridge::<i32, String>::unidirectional("my-bridge", |_| None);
        assert_eq!(bridge.name(), "my-bridge");
    }

    // === Integration-style Tests ===

    #[test]
    fn test_envelope_through_adapter() {
        // Simulate: receive envelope with i32, transform to envelope with String
        let mut adapter = MessageAdapter::infallible(
            "envelope-transform",
            |env: MessageEnvelope<i32>| env.map(|n| format!("processed:{}", n)),
        );

        let input = MessageEnvelope::new(42, "source", "target")
            .with_correlation_id("req-1");

        let output = adapter.adapt(input).unwrap();
        assert_eq!(output.message, "processed:42");
        assert_eq!(output.correlation_id, Some("req-1".to_string()));
    }

    #[test]
    fn test_multi_hop_envelope() {
        let env = MessageEnvelope::new("data", "origin", "hop-1")
            .with_header("trace", "t1");

        let hop1 = env.forward("hop-2");
        let hop2 = hop1.forward("final");

        assert_eq!(hop2.sender, "origin");
        assert_eq!(hop2.recipient, "final");
        assert_eq!(hop2.hop_count, 2);
        assert_eq!(hop2.get_header("trace"), Some("t1"));
    }

    #[test]
    fn test_spawn_protocol_flow() {
        let req = SpawnRequest::new("worker-1", "DataProcessor", "supervisor");
        let corr_id = req.correlation_id.clone();

        // Simulate successful spawn
        let resp = SpawnResponse::Spawned {
            name: req.name.clone(),
            actor_path: format!("/user/{}", req.name),
            correlation_id: corr_id.clone(),
        };

        assert!(resp.is_success());
        assert_eq!(resp.correlation_id(), &corr_id);
    }
}
