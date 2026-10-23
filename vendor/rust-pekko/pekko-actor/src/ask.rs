//! Ask Pattern Implementation
//! 
//! The Ask pattern allows sending a message to an actor and waiting for a response.
//! This is similar to Akka's `ask` pattern (also known as `?` operator).
//! 
//! # Example
//! 
//! ```rust,ignore
//! // Define a message that can be asked
//! enum MyMessage {
//!     GetValue { reply_to: ReplyTo<i32> },
//!     SetValue(i32),
//! }
//! 
//! // In the actor's receive method:
//! fn receive(&mut self, msg: MyMessage, ctx: &mut ActorContext<Self>) {
//!     match msg {
//!         MyMessage::GetValue { reply_to } => {
//!             reply_to.reply(self.value);
//!         }
//!         MyMessage::SetValue(v) => {
//!             self.value = v;
//!         }
//!     }
//! }
//! 
//! // Asking the actor:
//! let result = actor_ref.ask(|reply_to| MyMessage::GetValue { reply_to }).await?;
//! ```

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::{timeout, Timeout};

/// Error types for ask operations
#[derive(Debug, thiserror::Error)]
pub enum AskError {
    /// The actor did not respond within the timeout
    #[error("Ask timed out after {0:?}")]
    Timeout(Duration),
    
    /// The actor stopped before responding
    #[error("Actor stopped before responding")]
    ActorStopped,
    
    /// Failed to send the message
    #[error("Failed to send message: {0}")]
    SendFailed(String),
    
    /// The response channel was dropped
    #[error("Response channel dropped")]
    ChannelDropped,
}

/// A handle for replying to an ask request.
/// 
/// This is passed as part of the message to the actor,
/// allowing the actor to send a response back to the caller.
#[derive(Debug)]
pub struct ReplyTo<T> {
    sender: Option<oneshot::Sender<T>>,
}

impl<T> ReplyTo<T> {
    /// Create a new ReplyTo handle
    pub fn new(sender: oneshot::Sender<T>) -> Self {
        Self { sender: Some(sender) }
    }
    
    /// Send a reply to the caller.
    /// 
    /// This consumes the ReplyTo handle. If the caller has already
    /// timed out or dropped the receiver, this will silently fail.
    pub fn reply(mut self, value: T) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(value);
        }
    }
    
    /// Check if the reply channel is still open.
    /// 
    /// Returns false if the caller has timed out or dropped the receiver.
    pub fn is_open(&self) -> bool {
        self.sender.as_ref().map(|s| !s.is_closed()).unwrap_or(false)
    }
    
    /// Try to send a reply, returning an error if the channel is closed.
    pub fn try_reply(mut self, value: T) -> Result<(), T> {
        if let Some(sender) = self.sender.take() {
            sender.send(value)
        } else {
            Err(value)
        }
    }
}

// ReplyTo should not be cloneable - only one response is allowed
// But we need Send + Sync for actor messages
unsafe impl<T: Send> Send for ReplyTo<T> {}
unsafe impl<T: Send> Sync for ReplyTo<T> {}

/// A future that resolves to the response of an ask request.
/// 
/// This wraps a oneshot receiver with optional timeout.
pub struct AskFuture<T> {
    inner: AskFutureInner<T>,
}

enum AskFutureInner<T> {
    Pending(oneshot::Receiver<T>),
    WithTimeout(Pin<Box<Timeout<oneshot::Receiver<T>>>>),
    Completed,
}

impl<T> AskFuture<T> {
    /// Create a new AskFuture without timeout
    pub fn new(receiver: oneshot::Receiver<T>) -> Self {
        Self {
            inner: AskFutureInner::Pending(receiver),
        }
    }
    
    /// Create a new AskFuture with timeout
    pub fn with_timeout(receiver: oneshot::Receiver<T>, duration: Duration) -> Self {
        Self {
            inner: AskFutureInner::WithTimeout(Box::pin(timeout(duration, receiver))),
        }
    }
}

impl<T> Future for AskFuture<T> {
    type Output = Result<T, AskError>;
    
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.inner {
            AskFutureInner::Pending(receiver) => {
                let pinned = Pin::new(receiver);
                match pinned.poll(cx) {
                    Poll::Ready(Ok(value)) => {
                        self.inner = AskFutureInner::Completed;
                        Poll::Ready(Ok(value))
                    }
                    Poll::Ready(Err(_)) => {
                        self.inner = AskFutureInner::Completed;
                        Poll::Ready(Err(AskError::ChannelDropped))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            AskFutureInner::WithTimeout(timeout_fut) => {
                match timeout_fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(Ok(value))) => {
                        self.inner = AskFutureInner::Completed;
                        Poll::Ready(Ok(value))
                    }
                    Poll::Ready(Ok(Err(_))) => {
                        self.inner = AskFutureInner::Completed;
                        Poll::Ready(Err(AskError::ChannelDropped))
                    }
                    Poll::Ready(Err(_elapsed)) => {
                        self.inner = AskFutureInner::Completed;
                        // We don't have the duration here, use a default message
                        Poll::Ready(Err(AskError::Timeout(Duration::from_secs(0))))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            AskFutureInner::Completed => {
                panic!("AskFuture polled after completion")
            }
        }
    }
}

/// Helper function to create an ask channel pair
/// 
/// Returns a tuple of (ReplyTo, AskFuture) that can be used to
/// implement the ask pattern.
pub fn ask_channel<T>() -> (ReplyTo<T>, AskFuture<T>) {
    let (tx, rx) = oneshot::channel();
    (ReplyTo::new(tx), AskFuture::new(rx))
}

/// Helper function to create an ask channel pair with timeout
pub fn ask_channel_with_timeout<T>(duration: Duration) -> (ReplyTo<T>, AskFuture<T>) {
    let (tx, rx) = oneshot::channel();
    (ReplyTo::new(tx), AskFuture::with_timeout(rx, duration))
}

/// Default timeout for ask operations
pub const DEFAULT_ASK_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_reply_to_success() {
        let (reply_to, future) = ask_channel::<i32>();
        
        // Simulate actor responding
        tokio::spawn(async move {
            reply_to.reply(42);
        });
        
        let result = future.await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }
    
    #[tokio::test]
    async fn test_reply_to_dropped() {
        let (reply_to, future) = ask_channel::<i32>();
        
        // Drop the reply_to without responding
        drop(reply_to);
        
        let result = future.await;
        assert!(matches!(result, Err(AskError::ChannelDropped)));
    }
    
    #[tokio::test]
    async fn test_ask_timeout() {
        let (_reply_to, future) = ask_channel_with_timeout::<i32>(Duration::from_millis(50));
        
        // Don't respond, let it timeout
        let result = future.await;
        assert!(matches!(result, Err(AskError::Timeout(_))));
    }
    
    #[tokio::test]
    async fn test_is_open() {
        let (reply_to, _future) = ask_channel::<i32>();
        assert!(reply_to.is_open());
    }
    
    #[tokio::test]
    async fn test_try_reply() {
        let (reply_to, future) = ask_channel::<i32>();
        
        let result = reply_to.try_reply(42);
        assert!(result.is_ok());
        
        let response = future.await;
        assert_eq!(response.unwrap(), 42);
    }
}
