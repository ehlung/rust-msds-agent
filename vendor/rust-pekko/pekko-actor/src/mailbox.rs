//! Mailbox types — Message queuing strategies for actors
//!
//! Modeled after Scala Pekko's Mailbox system:
//!
//! - **UnboundedMailbox**: Default, no capacity limit (backed by tokio mpsc unbounded)
//! - **BoundedMailbox**: Fixed capacity with configurable overflow strategy
//! - **PriorityMailbox**: Messages are dequeued by priority
//! - **ControlAwareMailbox**: System/control messages get priority over user messages
//! - **DeadLetterMailbox**: Sink for messages to stopped actors

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::{debug, warn};

// ============= Mailbox Config =============

/// Configuration for mailbox behavior.
#[derive(Debug, Clone)]
pub struct MailboxConfig {
    /// Type of mailbox to use
    pub mailbox_type: MailboxType,
    /// Capacity for bounded mailboxes (ignored for unbounded)
    pub capacity: usize,
    /// What to do when a bounded mailbox is full
    pub overflow_strategy: MailboxOverflowStrategy,
    /// Throughput: number of messages processed per actor scheduling
    pub throughput: usize,
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self {
            mailbox_type: MailboxType::Unbounded,
            capacity: 1000,
            overflow_strategy: MailboxOverflowStrategy::Block,
            throughput: 10,
        }
    }
}

/// Available mailbox types.
#[derive(Debug, Clone, PartialEq)]
pub enum MailboxType {
    /// Unlimited capacity, non-blocking enqueue
    Unbounded,
    /// Fixed capacity with overflow strategy
    Bounded,
    /// Priority-based dequeuing
    Priority,
    /// System/control messages are processed before user messages
    ControlAware,
}

/// Strategy when a bounded mailbox is full.
#[derive(Debug, Clone, PartialEq)]
pub enum MailboxOverflowStrategy {
    /// Block the sender until space is available
    Block,
    /// Drop the newest message (the one being enqueued)
    DropNewest,
    /// Drop the oldest message in the queue
    DropOldest,
    /// Return an error to the sender
    Fail,
}

// ============= Mailbox Trait =============

/// A mailbox that can enqueue and dequeue messages.
#[async_trait::async_trait]
pub trait Mailbox<M: Send + 'static>: Send + Sync {
    /// Enqueue a message. Returns Err if the mailbox rejects it.
    async fn enqueue(&self, message: M) -> Result<(), MailboxError>;

    /// Dequeue the next message. Returns None if the mailbox is empty and closed.
    async fn dequeue(&self) -> Option<M>;

    /// Number of messages currently in the mailbox.
    fn message_count(&self) -> usize;

    /// Whether the mailbox has been closed.
    fn is_closed(&self) -> bool;

    /// Close the mailbox (no more messages accepted).
    fn close(&self);

    /// Mailbox type identifier.
    fn mailbox_type(&self) -> MailboxType;
}

/// Errors that can occur during mailbox operations.
#[derive(Debug, Clone)]
pub enum MailboxError {
    /// Mailbox is full and overflow strategy is Fail
    Full,
    /// Mailbox has been closed
    Closed,
    /// Message was dropped due to overflow strategy
    Dropped,
}

impl fmt::Display for MailboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MailboxError::Full => write!(f, "Mailbox is full"),
            MailboxError::Closed => write!(f, "Mailbox is closed"),
            MailboxError::Dropped => write!(f, "Message was dropped"),
        }
    }
}

impl std::error::Error for MailboxError {}

// ============= Unbounded Mailbox =============

/// Default mailbox with unlimited capacity.
pub struct UnboundedMailbox<M: Send + 'static> {
    tx: mpsc::UnboundedSender<M>,
    rx: Mutex<mpsc::UnboundedReceiver<M>>,
    count: AtomicU64,
    closed: std::sync::atomic::AtomicBool,
}

impl<M: Send + 'static> UnboundedMailbox<M> {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tx,
            rx: Mutex::new(rx),
            count: AtomicU64::new(0),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl<M: Send + 'static> Mailbox<M> for UnboundedMailbox<M> {
    async fn enqueue(&self, message: M) -> Result<(), MailboxError> {
        if self.closed.load(AtomicOrdering::Relaxed) {
            return Err(MailboxError::Closed);
        }
        self.tx.send(message).map_err(|_| MailboxError::Closed)?;
        self.count.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    async fn dequeue(&self) -> Option<M> {
        let mut rx = self.rx.lock().await;
        let msg = rx.recv().await;
        if msg.is_some() {
            self.count.fetch_sub(1, AtomicOrdering::Relaxed);
        }
        msg
    }

    fn message_count(&self) -> usize {
        self.count.load(AtomicOrdering::Relaxed) as usize
    }

    fn is_closed(&self) -> bool {
        self.closed.load(AtomicOrdering::Relaxed)
    }

    fn close(&self) {
        self.closed.store(true, AtomicOrdering::Relaxed);
    }

    fn mailbox_type(&self) -> MailboxType {
        MailboxType::Unbounded
    }
}

// ============= Bounded Mailbox =============

/// Fixed-capacity mailbox with configurable overflow strategy.
pub struct BoundedMailbox<M: Send + 'static> {
    tx: mpsc::Sender<M>,
    rx: Mutex<mpsc::Receiver<M>>,
    capacity: usize,
    count: AtomicU64,
    overflow_strategy: MailboxOverflowStrategy,
    closed: std::sync::atomic::AtomicBool,
}

impl<M: Send + 'static> BoundedMailbox<M> {
    pub fn new(capacity: usize, overflow_strategy: MailboxOverflowStrategy) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            tx,
            rx: Mutex::new(rx),
            capacity,
            count: AtomicU64::new(0),
            overflow_strategy,
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[async_trait::async_trait]
impl<M: Send + 'static> Mailbox<M> for BoundedMailbox<M> {
    async fn enqueue(&self, message: M) -> Result<(), MailboxError> {
        if self.closed.load(AtomicOrdering::Relaxed) {
            return Err(MailboxError::Closed);
        }

        match &self.overflow_strategy {
            MailboxOverflowStrategy::Block => {
                self.tx.send(message).await.map_err(|_| MailboxError::Closed)?;
                self.count.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(())
            }
            MailboxOverflowStrategy::Fail => {
                match self.tx.try_send(message) {
                    Ok(()) => {
                        self.count.fetch_add(1, AtomicOrdering::Relaxed);
                        Ok(())
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => Err(MailboxError::Full),
                    Err(mpsc::error::TrySendError::Closed(_)) => Err(MailboxError::Closed),
                }
            }
            MailboxOverflowStrategy::DropNewest => {
                match self.tx.try_send(message) {
                    Ok(()) => {
                        self.count.fetch_add(1, AtomicOrdering::Relaxed);
                        Ok(())
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        debug!("BoundedMailbox: dropping newest message (mailbox full)");
                        Err(MailboxError::Dropped)
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => Err(MailboxError::Closed),
                }
            }
            MailboxOverflowStrategy::DropOldest => {
                match self.tx.try_send(message) {
                    Ok(()) => {
                        self.count.fetch_add(1, AtomicOrdering::Relaxed);
                        Ok(())
                    }
                    Err(mpsc::error::TrySendError::Full(msg)) => {
                        // Drain the oldest message to make room
                        let mut rx = self.rx.lock().await;
                        let _ = rx.try_recv(); // drop oldest
                        self.count.fetch_sub(1, AtomicOrdering::Relaxed);
                        drop(rx);
                        debug!("BoundedMailbox: dropped oldest message to make room");
                        // Try again
                        self.tx.try_send(msg).map_err(|_| MailboxError::Closed)?;
                        self.count.fetch_add(1, AtomicOrdering::Relaxed);
                        Ok(())
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => Err(MailboxError::Closed),
                }
            }
        }
    }

    async fn dequeue(&self) -> Option<M> {
        let mut rx = self.rx.lock().await;
        let msg = rx.recv().await;
        if msg.is_some() {
            self.count.fetch_sub(1, AtomicOrdering::Relaxed);
        }
        msg
    }

    fn message_count(&self) -> usize {
        self.count.load(AtomicOrdering::Relaxed) as usize
    }

    fn is_closed(&self) -> bool {
        self.closed.load(AtomicOrdering::Relaxed)
    }

    fn close(&self) {
        self.closed.store(true, AtomicOrdering::Relaxed);
    }

    fn mailbox_type(&self) -> MailboxType {
        MailboxType::Bounded
    }
}

// ============= Priority Mailbox =============

/// A message wrapper with priority for the PriorityMailbox.
struct PriorityMessage<M> {
    priority: i32,
    seq: u64,
    message: M,
}

impl<M> Eq for PriorityMessage<M> {}

impl<M> PartialEq for PriorityMessage<M> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}

impl<M> Ord for PriorityMessage<M> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first (max-heap natural order), then earlier sequence first (FIFO)
        self.priority.cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl<M> PartialOrd for PriorityMessage<M> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A function that assigns priority to messages.
/// Higher values = higher priority. Default priority is 0.
pub type PriorityFn<M> = Arc<dyn Fn(&M) -> i32 + Send + Sync>;

/// Mailbox that dequeues messages based on priority.
pub struct PriorityMailbox<M: Send + 'static> {
    heap: Mutex<BinaryHeap<PriorityMessage<M>>>,
    priority_fn: PriorityFn<M>,
    seq: AtomicU64,
    notify: Notify,
    closed: std::sync::atomic::AtomicBool,
}

impl<M: Send + 'static> PriorityMailbox<M> {
    pub fn new(priority_fn: PriorityFn<M>) -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::new()),
            priority_fn,
            seq: AtomicU64::new(0),
            notify: Notify::new(),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl<M: Send + 'static> Mailbox<M> for PriorityMailbox<M> {
    async fn enqueue(&self, message: M) -> Result<(), MailboxError> {
        if self.closed.load(AtomicOrdering::Relaxed) {
            return Err(MailboxError::Closed);
        }
        let priority = (self.priority_fn)(&message);
        let seq = self.seq.fetch_add(1, AtomicOrdering::Relaxed);
        let mut heap = self.heap.lock().await;
        heap.push(PriorityMessage { priority, seq, message });
        drop(heap);
        self.notify.notify_one();
        Ok(())
    }

    async fn dequeue(&self) -> Option<M> {
        loop {
            {
                let mut heap = self.heap.lock().await;
                if let Some(pm) = heap.pop() {
                    return Some(pm.message);
                }
                if self.closed.load(AtomicOrdering::Relaxed) {
                    return None;
                }
            }
            self.notify.notified().await;
        }
    }

    fn message_count(&self) -> usize {
        // Approximate: we can't lock synchronously, use try_lock
        if let Ok(heap) = self.heap.try_lock() {
            heap.len()
        } else {
            0
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(AtomicOrdering::Relaxed)
    }

    fn close(&self) {
        self.closed.store(true, AtomicOrdering::Relaxed);
        self.notify.notify_waiters();
    }

    fn mailbox_type(&self) -> MailboxType {
        MailboxType::Priority
    }
}

// ============= Control-Aware Mailbox =============

/// Messages in a ControlAwareMailbox are either control or user messages.
pub enum ControlAwareMessage<M> {
    /// High-priority system/control message (processed first)
    Control(M),
    /// Normal user message
    User(M),
}

/// Mailbox that processes control messages before user messages.
/// Modeled after Pekko's UnboundedControlAwareMailbox.
pub struct ControlAwareMailbox<M: Send + 'static> {
    control_queue: Mutex<Vec<M>>,
    user_tx: mpsc::UnboundedSender<M>,
    user_rx: Mutex<mpsc::UnboundedReceiver<M>>,
    control_count: AtomicU64,
    user_count: AtomicU64,
    notify: Notify,
    closed: std::sync::atomic::AtomicBool,
}

impl<M: Send + 'static> ControlAwareMailbox<M> {
    pub fn new() -> Self {
        let (user_tx, user_rx) = mpsc::unbounded_channel();
        Self {
            control_queue: Mutex::new(Vec::new()),
            user_tx,
            user_rx: Mutex::new(user_rx),
            control_count: AtomicU64::new(0),
            user_count: AtomicU64::new(0),
            notify: Notify::new(),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Enqueue a control message (high priority).
    pub async fn enqueue_control(&self, message: M) -> Result<(), MailboxError> {
        if self.closed.load(AtomicOrdering::Relaxed) {
            return Err(MailboxError::Closed);
        }
        let mut queue = self.control_queue.lock().await;
        queue.push(message);
        self.control_count.fetch_add(1, AtomicOrdering::Relaxed);
        drop(queue);
        self.notify.notify_one();
        Ok(())
    }

    /// Enqueue a user message (normal priority).
    pub async fn enqueue_user(&self, message: M) -> Result<(), MailboxError> {
        if self.closed.load(AtomicOrdering::Relaxed) {
            return Err(MailboxError::Closed);
        }
        self.user_tx.send(message).map_err(|_| MailboxError::Closed)?;
        self.user_count.fetch_add(1, AtomicOrdering::Relaxed);
        self.notify.notify_one();
        Ok(())
    }

    pub fn control_count(&self) -> usize {
        self.control_count.load(AtomicOrdering::Relaxed) as usize
    }

    pub fn user_count(&self) -> usize {
        self.user_count.load(AtomicOrdering::Relaxed) as usize
    }
}

#[async_trait::async_trait]
impl<M: Send + 'static> Mailbox<M> for ControlAwareMailbox<M> {
    async fn enqueue(&self, message: M) -> Result<(), MailboxError> {
        // Default enqueue goes to user queue
        self.enqueue_user(message).await
    }

    async fn dequeue(&self) -> Option<M> {
        loop {
            // Control messages first
            {
                let mut control = self.control_queue.lock().await;
                if !control.is_empty() {
                    let msg = control.remove(0);
                    self.control_count.fetch_sub(1, AtomicOrdering::Relaxed);
                    return Some(msg);
                }
            }
            // Then user messages (non-blocking check)
            {
                let mut user_rx = self.user_rx.lock().await;
                match user_rx.try_recv() {
                    Ok(msg) => {
                        self.user_count.fetch_sub(1, AtomicOrdering::Relaxed);
                        return Some(msg);
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {}
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        if self.closed.load(AtomicOrdering::Relaxed) {
                            return None;
                        }
                    }
                }
            }
            if self.closed.load(AtomicOrdering::Relaxed) {
                return None;
            }
            self.notify.notified().await;
        }
    }

    fn message_count(&self) -> usize {
        self.control_count() + self.user_count()
    }

    fn is_closed(&self) -> bool {
        self.closed.load(AtomicOrdering::Relaxed)
    }

    fn close(&self) {
        self.closed.store(true, AtomicOrdering::Relaxed);
        self.notify.notify_waiters();
    }

    fn mailbox_type(&self) -> MailboxType {
        MailboxType::ControlAware
    }
}

// ============= Dead Letter Mailbox =============

/// A mailbox that counts and logs all messages as dead letters.
/// Used for messages sent to stopped actors.
pub struct DeadLetterMailbox {
    count: AtomicU64,
}

impl DeadLetterMailbox {
    pub fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
        }
    }

    pub fn dead_letter_count(&self) -> u64 {
        self.count.load(AtomicOrdering::Relaxed)
    }
}

#[async_trait::async_trait]
impl<M: Send + fmt::Debug + 'static> Mailbox<M> for DeadLetterMailbox {
    async fn enqueue(&self, message: M) -> Result<(), MailboxError> {
        self.count.fetch_add(1, AtomicOrdering::Relaxed);
        warn!("Dead letter received: {:?}", message);
        Ok(()) // Dead letters don't fail, they just log
    }

    async fn dequeue(&self) -> Option<M> {
        None // Dead letter mailbox never delivers
    }

    fn message_count(&self) -> usize {
        0
    }

    fn is_closed(&self) -> bool {
        true
    }

    fn close(&self) {}

    fn mailbox_type(&self) -> MailboxType {
        MailboxType::Unbounded // type doesn't matter for dead letters
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_unbounded_mailbox_basic() {
        let mb = UnboundedMailbox::<i32>::new();
        mb.enqueue(1).await.unwrap();
        mb.enqueue(2).await.unwrap();
        mb.enqueue(3).await.unwrap();
        assert_eq!(mb.message_count(), 3);

        assert_eq!(mb.dequeue().await, Some(1));
        assert_eq!(mb.dequeue().await, Some(2));
        assert_eq!(mb.message_count(), 1);
    }

    #[tokio::test]
    async fn test_unbounded_mailbox_closed() {
        let mb = UnboundedMailbox::<i32>::new();
        mb.enqueue(1).await.unwrap();
        mb.close();
        assert!(mb.is_closed());
        let result = mb.enqueue(2).await;
        assert!(matches!(result, Err(MailboxError::Closed)));
    }

    #[tokio::test]
    async fn test_bounded_mailbox_block() {
        let mb = BoundedMailbox::<i32>::new(2, MailboxOverflowStrategy::Block);
        mb.enqueue(1).await.unwrap();
        mb.enqueue(2).await.unwrap();
        assert_eq!(mb.message_count(), 2);
        assert_eq!(mb.capacity(), 2);
    }

    #[tokio::test]
    async fn test_bounded_mailbox_fail_overflow() {
        let mb = BoundedMailbox::<i32>::new(2, MailboxOverflowStrategy::Fail);
        mb.enqueue(1).await.unwrap();
        mb.enqueue(2).await.unwrap();
        let result = mb.enqueue(3).await;
        assert!(matches!(result, Err(MailboxError::Full)));
    }

    #[tokio::test]
    async fn test_bounded_mailbox_drop_newest() {
        let mb = BoundedMailbox::<i32>::new(2, MailboxOverflowStrategy::DropNewest);
        mb.enqueue(1).await.unwrap();
        mb.enqueue(2).await.unwrap();
        let result = mb.enqueue(3).await;
        assert!(matches!(result, Err(MailboxError::Dropped)));
        // Original messages preserved
        assert_eq!(mb.dequeue().await, Some(1));
        assert_eq!(mb.dequeue().await, Some(2));
    }

    #[tokio::test]
    async fn test_bounded_mailbox_drop_oldest() {
        let mb = BoundedMailbox::<i32>::new(2, MailboxOverflowStrategy::DropOldest);
        mb.enqueue(1).await.unwrap();
        mb.enqueue(2).await.unwrap();
        mb.enqueue(3).await.unwrap(); // Should drop 1
        assert_eq!(mb.dequeue().await, Some(2));
        assert_eq!(mb.dequeue().await, Some(3));
    }

    #[tokio::test]
    async fn test_priority_mailbox() {
        let priority_fn: PriorityFn<(i32, &str)> = Arc::new(|msg: &(i32, &str)| msg.0);
        let mb = PriorityMailbox::new(priority_fn);

        mb.enqueue((1, "low")).await.unwrap();
        mb.enqueue((10, "high")).await.unwrap();
        mb.enqueue((5, "medium")).await.unwrap();

        // Should come out in priority order: high, medium, low
        assert_eq!(mb.dequeue().await.unwrap().1, "high");
        assert_eq!(mb.dequeue().await.unwrap().1, "medium");
        assert_eq!(mb.dequeue().await.unwrap().1, "low");
    }

    #[tokio::test]
    async fn test_priority_mailbox_same_priority_fifo() {
        let priority_fn: PriorityFn<&str> = Arc::new(|_| 0); // all same priority
        let mb = PriorityMailbox::new(priority_fn);

        mb.enqueue("first").await.unwrap();
        mb.enqueue("second").await.unwrap();
        mb.enqueue("third").await.unwrap();

        // FIFO within same priority
        assert_eq!(mb.dequeue().await, Some("first"));
        assert_eq!(mb.dequeue().await, Some("second"));
        assert_eq!(mb.dequeue().await, Some("third"));
    }

    #[tokio::test]
    async fn test_control_aware_mailbox() {
        let mb = ControlAwareMailbox::<String>::new();

        // Enqueue user messages first
        mb.enqueue_user("user1".into()).await.unwrap();
        mb.enqueue_user("user2".into()).await.unwrap();

        // Enqueue control message
        mb.enqueue_control("ctrl1".into()).await.unwrap();

        // Control message should come first
        assert_eq!(mb.dequeue().await.unwrap(), "ctrl1");
        assert_eq!(mb.dequeue().await.unwrap(), "user1");
        assert_eq!(mb.dequeue().await.unwrap(), "user2");
    }

    #[tokio::test]
    async fn test_control_aware_counts() {
        let mb = ControlAwareMailbox::<i32>::new();
        mb.enqueue_control(1).await.unwrap();
        mb.enqueue_user(2).await.unwrap();
        mb.enqueue_user(3).await.unwrap();

        assert_eq!(mb.control_count(), 1);
        assert_eq!(mb.user_count(), 2);
        assert_eq!(mb.message_count(), 3);
    }

    #[tokio::test]
    async fn test_dead_letter_mailbox() {
        let mb = DeadLetterMailbox::new();
        let mb_ref: &dyn Mailbox<i32> = &mb;
        mb_ref.enqueue(42).await.unwrap(); // doesn't fail
        mb_ref.enqueue(99).await.unwrap();
        assert_eq!(mb.dead_letter_count(), 2);
        let result = mb_ref.dequeue().await;
        assert_eq!(result, None); // never delivers
        assert!(mb_ref.is_closed());
    }

    #[tokio::test]
    async fn test_mailbox_config_default() {
        let config = MailboxConfig::default();
        assert_eq!(config.mailbox_type, MailboxType::Unbounded);
        assert_eq!(config.capacity, 1000);
        assert_eq!(config.overflow_strategy, MailboxOverflowStrategy::Block);
        assert_eq!(config.throughput, 10);
    }

    #[tokio::test]
    async fn test_mailbox_type_identification() {
        let ub = UnboundedMailbox::<i32>::new();
        assert_eq!(ub.mailbox_type(), MailboxType::Unbounded);

        let bb = BoundedMailbox::<i32>::new(10, MailboxOverflowStrategy::Fail);
        assert_eq!(bb.mailbox_type(), MailboxType::Bounded);

        let pf: PriorityFn<i32> = Arc::new(|_| 0);
        let pb = PriorityMailbox::new(pf);
        assert_eq!(pb.mailbox_type(), MailboxType::Priority);

        let ca = ControlAwareMailbox::<i32>::new();
        assert_eq!(ca.mailbox_type(), MailboxType::ControlAware);
    }
}
