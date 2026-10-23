use std::collections::VecDeque;
use std::fmt;

/// Configuration for stash behavior
#[derive(Debug, Clone)]
pub struct StashConfig {
    /// Maximum number of messages to stash (None = unlimited)
    pub capacity: Option<usize>,
    /// What to do when capacity is exceeded
    pub overflow_strategy: StashOverflowStrategy,
    /// Whether to preserve stash across restarts
    pub preserve_on_restart: bool,
}

impl Default for StashConfig {
    fn default() -> Self {
        Self {
            capacity: Some(1000), // Reasonable default limit
            overflow_strategy: StashOverflowStrategy::DropOldest,
            preserve_on_restart: false,
        }
    }
}

impl StashConfig {
    pub fn unlimited() -> Self {
        Self {
            capacity: None,
            ..Default::default()
        }
    }

    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }

    pub fn with_overflow_strategy(mut self, strategy: StashOverflowStrategy) -> Self {
        self.overflow_strategy = strategy;
        self
    }

    pub fn preserve_on_restart(mut self, preserve: bool) -> Self {
        self.preserve_on_restart = preserve;
        self
    }
}

/// Strategy for handling stash overflow
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StashOverflowStrategy {
    /// Drop the oldest stashed message to make room
    DropOldest,
    /// Drop the newest message (don't stash it)
    DropNewest,
    /// Fail the actor (panic)
    Fail,
}

/// Result of a stash operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StashResult {
    /// Message was successfully stashed
    Stashed,
    /// Message was dropped due to overflow (oldest was removed)
    DroppedOldest,
    /// Message was not stashed due to overflow (newest dropped)
    DroppedNewest,
    /// Stash is at capacity and Fail strategy is set
    OverflowError,
}

/// A type-safe message stash buffer
pub struct Stash<M> {
    buffer: VecDeque<M>,
    config: StashConfig,
    dropped_count: u64,
}

impl<M> Stash<M> {
    /// Create a new stash with default configuration
    pub fn new() -> Self {
        Self {
            buffer: VecDeque::new(),
            config: StashConfig::default(),
            dropped_count: 0,
        }
    }

    /// Create a new stash with custom configuration
    pub fn with_config(config: StashConfig) -> Self {
        let capacity = config.capacity.unwrap_or(1000);
        Self {
            buffer: VecDeque::with_capacity(capacity.min(1000)),
            config,
            dropped_count: 0,
        }
    }

    /// Create an unlimited stash
    pub fn unlimited() -> Self {
        Self::with_config(StashConfig::unlimited())
    }

    /// Stash a message for later processing
    pub fn stash(&mut self, msg: M) -> StashResult {
        // Check capacity
        if let Some(capacity) = self.config.capacity {
            if self.buffer.len() >= capacity {
                match self.config.overflow_strategy {
                    StashOverflowStrategy::DropOldest => {
                        self.buffer.pop_front();
                        self.buffer.push_back(msg);
                        self.dropped_count += 1;
                        tracing::warn!(
                            "Stash overflow: dropped oldest message (total dropped: {})",
                            self.dropped_count
                        );
                        return StashResult::DroppedOldest;
                    }
                    StashOverflowStrategy::DropNewest => {
                        self.dropped_count += 1;
                        tracing::warn!(
                            "Stash overflow: dropped newest message (total dropped: {})",
                            self.dropped_count
                        );
                        return StashResult::DroppedNewest;
                    }
                    StashOverflowStrategy::Fail => {
                        tracing::error!(
                            "Stash overflow: capacity {} exceeded",
                            capacity
                        );
                        return StashResult::OverflowError;
                    }
                }
            }
        }

        self.buffer.push_back(msg);
        StashResult::Stashed
    }

    /// Take one message from the front of the stash
    pub fn unstash_one(&mut self) -> Option<M> {
        self.buffer.pop_front()
    }

    /// Take all messages from the stash
    pub fn unstash_all(&mut self) -> Vec<M> {
        self.buffer.drain(..).collect()
    }

    /// Peek at the next message without removing it
    pub fn peek(&self) -> Option<&M> {
        self.buffer.front()
    }

    /// Check if stash is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Get the number of stashed messages
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Get the total number of dropped messages due to overflow
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }

    /// Clear all stashed messages
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Get the capacity (None if unlimited)
    pub fn capacity(&self) -> Option<usize> {
        self.config.capacity
    }

    /// Check if stash should be preserved on restart
    pub fn preserve_on_restart(&self) -> bool {
        self.config.preserve_on_restart
    }

    /// Prepend messages to the stash (used for unstashing to mailbox front)
    pub fn prepend(&mut self, messages: impl IntoIterator<Item = M>) {
        let msgs: Vec<M> = messages.into_iter().collect();
        for msg in msgs.into_iter().rev() {
            self.buffer.push_front(msg);
        }
    }

    /// Get iterator over stashed messages (for inspection)
    pub fn iter(&self) -> impl Iterator<Item = &M> {
        self.buffer.iter()
    }
}

impl<M> Default for Stash<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: fmt::Debug> fmt::Debug for Stash<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stash")
            .field("len", &self.buffer.len())
            .field("config", &self.config)
            .field("dropped_count", &self.dropped_count)
            .finish()
    }
}

/// Trait for actors that support stashing
pub trait StashSupport {
    type Message;

    /// Get a reference to the stash
    fn stash(&self) -> &Stash<Self::Message>;

    /// Get a mutable reference to the stash
    fn stash_mut(&mut self) -> &mut Stash<Self::Message>;
}

/// A bounded stash that enforces a strict capacity limit
pub struct BoundedStash<M> {
    inner: Stash<M>,
}

impl<M> BoundedStash<M> {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Stash::with_config(
                StashConfig::default()
                    .with_capacity(capacity)
                    .with_overflow_strategy(StashOverflowStrategy::Fail),
            ),
        }
    }

    /// Try to stash a message, returns false if at capacity
    pub fn try_stash(&mut self, msg: M) -> bool {
        match self.inner.stash(msg) {
            StashResult::Stashed => true,
            StashResult::OverflowError => false,
            _ => false,
        }
    }

    pub fn unstash_one(&mut self) -> Option<M> {
        self.inner.unstash_one()
    }

    pub fn unstash_all(&mut self) -> Vec<M> {
        self.inner.unstash_all()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_full(&self) -> bool {
        self.inner.capacity()
            .map(|cap| self.inner.len() >= cap)
            .unwrap_or(false)
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<M: fmt::Debug> fmt::Debug for BoundedStash<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedStash")
            .field("inner", &self.inner)
            .finish()
    }
}

/// Type-erased stash for storing in actor context
pub struct AnyStash {
    buffer: VecDeque<Box<dyn std::any::Any + Send>>,
    config: StashConfig,
    dropped_count: u64,
}

impl AnyStash {
    pub fn new() -> Self {
        Self {
            buffer: VecDeque::new(),
            config: StashConfig::default(),
            dropped_count: 0,
        }
    }

    pub fn with_config(config: StashConfig) -> Self {
        Self {
            buffer: VecDeque::new(),
            config,
            dropped_count: 0,
        }
    }

    pub fn stash<M: Send + 'static>(&mut self, msg: M) -> StashResult {
        if let Some(capacity) = self.config.capacity {
            if self.buffer.len() >= capacity {
                match self.config.overflow_strategy {
                    StashOverflowStrategy::DropOldest => {
                        self.buffer.pop_front();
                        self.buffer.push_back(Box::new(msg));
                        self.dropped_count += 1;
                        return StashResult::DroppedOldest;
                    }
                    StashOverflowStrategy::DropNewest => {
                        self.dropped_count += 1;
                        return StashResult::DroppedNewest;
                    }
                    StashOverflowStrategy::Fail => {
                        return StashResult::OverflowError;
                    }
                }
            }
        }

        self.buffer.push_back(Box::new(msg));
        StashResult::Stashed
    }

    pub fn unstash_one<M: 'static>(&mut self) -> Option<M> {
        self.buffer.pop_front().and_then(|boxed| {
            boxed.downcast::<M>().ok().map(|b| *b)
        })
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl Default for AnyStash {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for AnyStash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnyStash")
            .field("len", &self.buffer.len())
            .field("config", &self.config)
            .field("dropped_count", &self.dropped_count)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    enum TestMessage {
        Init,
        Work(u32),
        Stop,
    }

    #[test]
    fn test_stash_basic() {
        let mut stash: Stash<TestMessage> = Stash::new();

        assert!(stash.is_empty());
        assert_eq!(stash.len(), 0);

        stash.stash(TestMessage::Work(1));
        stash.stash(TestMessage::Work(2));
        stash.stash(TestMessage::Work(3));

        assert!(!stash.is_empty());
        assert_eq!(stash.len(), 3);
    }

    #[test]
    fn test_unstash_one() {
        let mut stash: Stash<TestMessage> = Stash::new();

        stash.stash(TestMessage::Work(1));
        stash.stash(TestMessage::Work(2));
        stash.stash(TestMessage::Work(3));

        assert_eq!(stash.unstash_one(), Some(TestMessage::Work(1)));
        assert_eq!(stash.unstash_one(), Some(TestMessage::Work(2)));
        assert_eq!(stash.unstash_one(), Some(TestMessage::Work(3)));
        assert_eq!(stash.unstash_one(), None);
    }

    #[test]
    fn test_unstash_all() {
        let mut stash: Stash<TestMessage> = Stash::new();

        stash.stash(TestMessage::Work(1));
        stash.stash(TestMessage::Work(2));
        stash.stash(TestMessage::Work(3));

        let all = stash.unstash_all();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0], TestMessage::Work(1));
        assert_eq!(all[1], TestMessage::Work(2));
        assert_eq!(all[2], TestMessage::Work(3));

        assert!(stash.is_empty());
    }

    #[test]
    fn test_peek() {
        let mut stash: Stash<TestMessage> = Stash::new();

        assert_eq!(stash.peek(), None);

        stash.stash(TestMessage::Work(1));
        assert_eq!(stash.peek(), Some(&TestMessage::Work(1)));
        assert_eq!(stash.len(), 1); // peek doesn't remove
    }

    #[test]
    fn test_capacity_drop_oldest() {
        let config = StashConfig::default()
            .with_capacity(3)
            .with_overflow_strategy(StashOverflowStrategy::DropOldest);
        let mut stash: Stash<TestMessage> = Stash::with_config(config);

        stash.stash(TestMessage::Work(1));
        stash.stash(TestMessage::Work(2));
        stash.stash(TestMessage::Work(3));
        
        let result = stash.stash(TestMessage::Work(4));
        assert_eq!(result, StashResult::DroppedOldest);
        assert_eq!(stash.dropped_count(), 1);

        // Should have 2, 3, 4 (1 was dropped)
        let all = stash.unstash_all();
        assert_eq!(all, vec![
            TestMessage::Work(2),
            TestMessage::Work(3),
            TestMessage::Work(4),
        ]);
    }

    #[test]
    fn test_capacity_drop_newest() {
        let config = StashConfig::default()
            .with_capacity(3)
            .with_overflow_strategy(StashOverflowStrategy::DropNewest);
        let mut stash: Stash<TestMessage> = Stash::with_config(config);

        stash.stash(TestMessage::Work(1));
        stash.stash(TestMessage::Work(2));
        stash.stash(TestMessage::Work(3));
        
        let result = stash.stash(TestMessage::Work(4));
        assert_eq!(result, StashResult::DroppedNewest);

        // Should have 1, 2, 3 (4 was not added)
        let all = stash.unstash_all();
        assert_eq!(all, vec![
            TestMessage::Work(1),
            TestMessage::Work(2),
            TestMessage::Work(3),
        ]);
    }

    #[test]
    fn test_capacity_fail() {
        let config = StashConfig::default()
            .with_capacity(3)
            .with_overflow_strategy(StashOverflowStrategy::Fail);
        let mut stash: Stash<TestMessage> = Stash::with_config(config);

        stash.stash(TestMessage::Work(1));
        stash.stash(TestMessage::Work(2));
        stash.stash(TestMessage::Work(3));
        
        let result = stash.stash(TestMessage::Work(4));
        assert_eq!(result, StashResult::OverflowError);
    }

    #[test]
    fn test_unlimited_stash() {
        let mut stash: Stash<u32> = Stash::unlimited();

        for i in 0..10000 {
            stash.stash(i);
        }

        assert_eq!(stash.len(), 10000);
        assert_eq!(stash.dropped_count(), 0);
    }

    #[test]
    fn test_clear() {
        let mut stash: Stash<TestMessage> = Stash::new();

        stash.stash(TestMessage::Work(1));
        stash.stash(TestMessage::Work(2));
        stash.stash(TestMessage::Work(3));

        stash.clear();
        assert!(stash.is_empty());
    }

    #[test]
    fn test_prepend() {
        let mut stash: Stash<TestMessage> = Stash::new();

        stash.stash(TestMessage::Work(3));
        stash.stash(TestMessage::Work(4));

        // Prepend 1 and 2
        stash.prepend(vec![TestMessage::Work(1), TestMessage::Work(2)]);

        // Order should be 1, 2, 3, 4
        let all = stash.unstash_all();
        assert_eq!(all, vec![
            TestMessage::Work(1),
            TestMessage::Work(2),
            TestMessage::Work(3),
            TestMessage::Work(4),
        ]);
    }

    #[test]
    fn test_bounded_stash() {
        let mut stash = BoundedStash::<u32>::new(3);

        assert!(stash.try_stash(1));
        assert!(stash.try_stash(2));
        assert!(stash.try_stash(3));
        assert!(stash.is_full());
        assert!(!stash.try_stash(4)); // Should fail

        assert_eq!(stash.unstash_one(), Some(1));
        assert!(!stash.is_full());
        assert!(stash.try_stash(4)); // Now it works
    }

    #[test]
    fn test_any_stash() {
        let mut stash = AnyStash::new();

        stash.stash(42u32);
        stash.stash("hello".to_string());

        assert_eq!(stash.len(), 2);

        let num: Option<u32> = stash.unstash_one();
        assert_eq!(num, Some(42));

        let string: Option<String> = stash.unstash_one();
        assert_eq!(string, Some("hello".to_string()));
    }

    #[test]
    fn test_stash_config_builder() {
        let config = StashConfig::default()
            .with_capacity(500)
            .with_overflow_strategy(StashOverflowStrategy::DropNewest)
            .preserve_on_restart(true);

        assert_eq!(config.capacity, Some(500));
        assert_eq!(config.overflow_strategy, StashOverflowStrategy::DropNewest);
        assert!(config.preserve_on_restart);
    }

    #[test]
    fn test_iterator() {
        let mut stash: Stash<u32> = Stash::new();
        stash.stash(1);
        stash.stash(2);
        stash.stash(3);

        let sum: u32 = stash.iter().sum();
        assert_eq!(sum, 6);
        
        // Iterator doesn't consume
        assert_eq!(stash.len(), 3);
    }

    #[test]
    fn test_preserve_on_restart_config() {
        let config_preserve = StashConfig::default().preserve_on_restart(true);
        let config_clear = StashConfig::default().preserve_on_restart(false);

        let stash_preserve: Stash<u32> = Stash::with_config(config_preserve);
        let stash_clear: Stash<u32> = Stash::with_config(config_clear);

        assert!(stash_preserve.preserve_on_restart());
        assert!(!stash_clear.preserve_on_restart());
    }
}
