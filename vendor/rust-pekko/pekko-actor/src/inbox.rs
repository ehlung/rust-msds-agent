//! # Inbox — Synchronous Actor Interaction
//!
//! Provides a synchronous interface for sending messages to and receiving
//! responses from actors, primarily for testing.
//!
//! Inspired by Pekko's Inbox pattern.
//!
//! Provides:
//! - Blocking message receive with timeout
//! - Message collection and assertions
//! - Pattern-based message matching
//! - Dead letter tracking

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};

// ---------------------------------------------------------------------------
// InboxMessage
// ---------------------------------------------------------------------------

/// A message received by the inbox
#[derive(Debug, Clone)]
pub struct InboxMessage<T> {
    pub payload: T,
    pub received_at: Instant,
}

impl<T> InboxMessage<T> {
    pub fn new(payload: T) -> Self {
        Self {
            payload,
            received_at: Instant::now(),
        }
    }
}

/// Error types for inbox operations
#[derive(Debug, Clone, PartialEq)]
pub enum InboxError {
    /// Receive timed out
    Timeout,
    /// Inbox is closed
    Closed,
    /// No messages available
    Empty,
    /// Message didn't match expected predicate
    NoMatch,
}

impl fmt::Display for InboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InboxError::Timeout => write!(f, "Inbox receive timed out"),
            InboxError::Closed => write!(f, "Inbox is closed"),
            InboxError::Empty => write!(f, "Inbox is empty"),
            InboxError::NoMatch => write!(f, "No matching message found"),
        }
    }
}

// ---------------------------------------------------------------------------
// Inbox
// ---------------------------------------------------------------------------

/// Synchronous message inbox for actor interaction
pub struct Inbox<T: Send + 'static> {
    messages: Arc<Mutex<VecDeque<InboxMessage<T>>>>,
    sender: mpsc::UnboundedSender<T>,
    receiver: Arc<Mutex<mpsc::UnboundedReceiver<T>>>,
    notify: Arc<Notify>,
    max_capacity: usize,
    total_received: Arc<Mutex<u64>>,
    is_closed: Arc<Mutex<bool>>,
}

impl<T: Send + Clone + 'static> Inbox<T> {
    /// Create a new inbox
    pub fn new() -> Self {
        Self::with_capacity(1000)
    }

    /// Create a new inbox with max capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            messages: Arc::new(Mutex::new(VecDeque::new())),
            sender: tx,
            receiver: Arc::new(Mutex::new(rx)),
            notify: Arc::new(Notify::new()),
            max_capacity: capacity,
            total_received: Arc::new(Mutex::new(0)),
            is_closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Get a sender that can be used to send messages to this inbox
    pub fn sender(&self) -> InboxSender<T> {
        InboxSender {
            tx: self.sender.clone(),
            notify: self.notify.clone(),
        }
    }

    /// Send a message directly
    pub fn send(&self, message: T) -> Result<(), InboxError> {
        if *self.is_closed.lock().unwrap() {
            return Err(InboxError::Closed);
        }

        let mut msgs = self.messages.lock().unwrap();
        if msgs.len() >= self.max_capacity {
            msgs.pop_front(); // Drop oldest
        }
        msgs.push_back(InboxMessage::new(message));
        *self.total_received.lock().unwrap() += 1;
        self.notify.notify_one();
        Ok(())
    }

    /// Receive the next message (non-blocking)
    pub fn try_receive(&self) -> Result<T, InboxError> {
        // First drain from channel
        self.drain_channel();

        let mut msgs = self.messages.lock().unwrap();
        msgs.pop_front()
            .map(|m| m.payload)
            .ok_or(InboxError::Empty)
    }

    /// Receive the next message with timeout (blocking)
    pub fn receive(&self, timeout: Duration) -> Result<T, InboxError> {
        let deadline = Instant::now() + timeout;

        loop {
            self.drain_channel();

            {
                let mut msgs = self.messages.lock().unwrap();
                if let Some(msg) = msgs.pop_front() {
                    return Ok(msg.payload);
                }
            }

            if Instant::now() >= deadline {
                return Err(InboxError::Timeout);
            }

            // Brief sleep to avoid busy-waiting
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Receive a message matching a predicate
    pub fn receive_matching<F>(&self, predicate: F, timeout: Duration) -> Result<T, InboxError>
    where
        F: Fn(&T) -> bool,
    {
        let deadline = Instant::now() + timeout;

        loop {
            self.drain_channel();

            {
                let mut msgs = self.messages.lock().unwrap();
                if let Some(pos) = msgs.iter().position(|m| predicate(&m.payload)) {
                    return Ok(msgs.remove(pos).unwrap().payload);
                }
            }

            if Instant::now() >= deadline {
                return Err(InboxError::Timeout);
            }

            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Collect all available messages
    pub fn collect_all(&self) -> Vec<T> {
        self.drain_channel();
        let mut msgs = self.messages.lock().unwrap();
        msgs.drain(..).map(|m| m.payload).collect()
    }

    /// Peek at the next message without consuming it
    pub fn peek(&self) -> Option<T> {
        self.drain_channel();
        let msgs = self.messages.lock().unwrap();
        msgs.front().map(|m| m.payload.clone())
    }

    /// Check if inbox has messages
    pub fn has_messages(&self) -> bool {
        self.drain_channel();
        !self.messages.lock().unwrap().is_empty()
    }

    /// Get number of pending messages
    pub fn size(&self) -> usize {
        self.drain_channel();
        self.messages.lock().unwrap().len()
    }

    /// Get total messages received
    pub fn total_received(&self) -> u64 {
        *self.total_received.lock().unwrap()
    }

    /// Assert that no messages arrive within duration
    pub fn expect_no_message(&self, duration: Duration) -> bool {
        std::thread::sleep(duration);
        self.drain_channel();
        self.messages.lock().unwrap().is_empty()
    }

    /// Clear all messages
    pub fn clear(&self) {
        self.drain_channel();
        self.messages.lock().unwrap().clear();
    }

    /// Close the inbox
    pub fn close(&self) {
        *self.is_closed.lock().unwrap() = true;
    }

    /// Check if closed
    pub fn is_closed(&self) -> bool {
        *self.is_closed.lock().unwrap()
    }

    fn drain_channel(&self) {
        let mut rx = self.receiver.lock().unwrap();
        let mut msgs = self.messages.lock().unwrap();
        let mut total = self.total_received.lock().unwrap();

        while let Ok(msg) = rx.try_recv() {
            if msgs.len() >= self.max_capacity {
                msgs.pop_front();
            }
            msgs.push_back(InboxMessage::new(msg));
            *total += 1;
        }
    }
}

impl<T: Send + Clone + 'static> Default for Inbox<T> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// InboxSender
// ---------------------------------------------------------------------------

/// Sender half of an inbox, can be cloned and shared
#[derive(Clone)]
pub struct InboxSender<T> {
    tx: mpsc::UnboundedSender<T>,
    notify: Arc<Notify>,
}

impl<T> InboxSender<T> {
    pub fn send(&self, message: T) -> Result<(), InboxError> {
        self.tx.send(message).map_err(|_| InboxError::Closed)?;
        self.notify.notify_one();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// InboxAssertion — fluent assertion API
// ---------------------------------------------------------------------------

/// Fluent assertions for inbox contents
pub struct InboxAssertion<T> {
    messages: Vec<T>,
}

impl<T: fmt::Debug + PartialEq + Clone> InboxAssertion<T> {
    pub fn new(messages: Vec<T>) -> Self {
        Self { messages }
    }

    pub fn has_count(self, expected: usize) -> Self {
        assert_eq!(self.messages.len(), expected, "Expected {} messages, got {}", expected, self.messages.len());
        self
    }

    pub fn first_is(self, expected: &T) -> Self {
        assert!(!self.messages.is_empty(), "No messages to check");
        assert_eq!(&self.messages[0], expected, "First message mismatch");
        self
    }

    pub fn last_is(self, expected: &T) -> Self {
        assert!(!self.messages.is_empty(), "No messages to check");
        assert_eq!(self.messages.last().unwrap(), expected, "Last message mismatch");
        self
    }

    pub fn contains(self, expected: &T) -> Self {
        assert!(self.messages.contains(expected), "Messages do not contain {:?}", expected);
        self
    }

    pub fn is_empty(self) -> Self {
        assert!(self.messages.is_empty(), "Expected empty, got {} messages", self.messages.len());
        self
    }

    pub fn all_match<F: Fn(&T) -> bool>(self, predicate: F) -> Self {
        assert!(self.messages.iter().all(&predicate), "Not all messages match predicate");
        self
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_and_receive() {
        let inbox: Inbox<String> = Inbox::new();
        inbox.send("hello".into()).unwrap();

        let msg = inbox.try_receive().unwrap();
        assert_eq!(msg, "hello");
    }

    #[test]
    fn test_receive_empty() {
        let inbox: Inbox<i32> = Inbox::new();
        assert_eq!(inbox.try_receive(), Err(InboxError::Empty));
    }

    #[test]
    fn test_receive_with_timeout() {
        let inbox: Inbox<i32> = Inbox::new();
        inbox.send(42).unwrap();

        let result = inbox.receive(Duration::from_millis(100));
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_receive_timeout_expires() {
        let inbox: Inbox<i32> = Inbox::new();
        let result = inbox.receive(Duration::from_millis(10));
        assert_eq!(result, Err(InboxError::Timeout));
    }

    #[test]
    fn test_sender_channel() {
        let inbox: Inbox<String> = Inbox::new();
        let sender = inbox.sender();

        sender.send("via channel".into()).unwrap();
        let msg = inbox.receive(Duration::from_millis(100)).unwrap();
        assert_eq!(msg, "via channel");
    }

    #[test]
    fn test_collect_all() {
        let inbox: Inbox<i32> = Inbox::new();
        inbox.send(1).unwrap();
        inbox.send(2).unwrap();
        inbox.send(3).unwrap();

        let all = inbox.collect_all();
        assert_eq!(all, vec![1, 2, 3]);
        assert_eq!(inbox.size(), 0);
    }

    #[test]
    fn test_peek() {
        let inbox: Inbox<i32> = Inbox::new();
        inbox.send(99).unwrap();

        assert_eq!(inbox.peek(), Some(99));
        assert_eq!(inbox.peek(), Some(99)); // Still there
        assert_eq!(inbox.size(), 1);
    }

    #[test]
    fn test_receive_matching() {
        let inbox: Inbox<i32> = Inbox::new();
        inbox.send(1).unwrap();
        inbox.send(2).unwrap();
        inbox.send(3).unwrap();

        let result = inbox.receive_matching(|&x| x == 2, Duration::from_millis(100));
        assert_eq!(result, Ok(2));

        // 1 and 3 should remain
        assert_eq!(inbox.size(), 2);
    }

    #[test]
    fn test_capacity_limit() {
        let inbox: Inbox<i32> = Inbox::with_capacity(3);
        inbox.send(1).unwrap();
        inbox.send(2).unwrap();
        inbox.send(3).unwrap();
        inbox.send(4).unwrap(); // Drops oldest (1)

        let all = inbox.collect_all();
        assert_eq!(all, vec![2, 3, 4]);
    }

    #[test]
    fn test_clear() {
        let inbox: Inbox<i32> = Inbox::new();
        inbox.send(1).unwrap();
        inbox.send(2).unwrap();

        inbox.clear();
        assert_eq!(inbox.size(), 0);
    }

    #[test]
    fn test_close() {
        let inbox: Inbox<i32> = Inbox::new();
        inbox.close();
        assert!(inbox.is_closed());
        assert_eq!(inbox.send(1), Err(InboxError::Closed));
    }

    #[test]
    fn test_total_received() {
        let inbox: Inbox<i32> = Inbox::new();
        inbox.send(1).unwrap();
        inbox.send(2).unwrap();
        inbox.send(3).unwrap();

        assert_eq!(inbox.total_received(), 3);
    }

    #[test]
    fn test_has_messages() {
        let inbox: Inbox<i32> = Inbox::new();
        assert!(!inbox.has_messages());

        inbox.send(1).unwrap();
        assert!(inbox.has_messages());
    }

    #[test]
    fn test_expect_no_message() {
        let inbox: Inbox<i32> = Inbox::new();
        assert!(inbox.expect_no_message(Duration::from_millis(10)));
    }

    #[test]
    fn test_inbox_assertion() {
        let inbox: Inbox<i32> = Inbox::new();
        inbox.send(1).unwrap();
        inbox.send(2).unwrap();
        inbox.send(3).unwrap();

        InboxAssertion::new(inbox.collect_all())
            .has_count(3)
            .first_is(&1)
            .last_is(&3)
            .contains(&2);
    }

    #[test]
    fn test_inbox_assertion_all_match() {
        let inbox: Inbox<i32> = Inbox::new();
        inbox.send(2).unwrap();
        inbox.send(4).unwrap();
        inbox.send(6).unwrap();

        InboxAssertion::new(inbox.collect_all())
            .all_match(|x| x % 2 == 0);
    }

    #[test]
    fn test_fifo_ordering() {
        let inbox: Inbox<i32> = Inbox::new();
        for i in 0..10 {
            inbox.send(i).unwrap();
        }

        for i in 0..10 {
            assert_eq!(inbox.try_receive().unwrap(), i);
        }
    }

    #[test]
    fn test_error_display() {
        assert_eq!(InboxError::Timeout.to_string(), "Inbox receive timed out");
        assert_eq!(InboxError::Closed.to_string(), "Inbox is closed");
        assert_eq!(InboxError::Empty.to_string(), "Inbox is empty");
        assert_eq!(InboxError::NoMatch.to_string(), "No matching message found");
    }
}
