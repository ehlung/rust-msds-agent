//! Actor reference implementation

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use crate::actor::Actor;
use crate::actor_path::ActorPath;
use crate::ask::{ReplyTo, AskFuture, ask_channel, ask_channel_with_timeout, DEFAULT_ASK_TIMEOUT};

/// A reference to an actor that can be used to send messages
pub struct ActorRef<A: Actor + ?Sized> {
    path: ActorPath,
    sender: mpsc::UnboundedSender<A::Message>,
    is_terminated: Arc<AtomicBool>,
}

impl<A: Actor + ?Sized> ActorRef<A> {
    /// Create a new actor reference
    pub fn new(
        path: ActorPath,
        sender: mpsc::UnboundedSender<A::Message>,
    ) -> Self {
        Self {
            path,
            sender,
            is_terminated: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Send a message to the actor (fire-and-forget)
    /// 
    /// This is the basic "tell" pattern - send a message without expecting a response.
    pub fn tell(&self, msg: A::Message) -> Result<(), ActorError> {
        if self.is_terminated.load(Ordering::Relaxed) {
            return Err(ActorError::Stopped);
        }
        
        self.sender.send(msg)
            .map_err(|_| ActorError::Stopped)
    }
    
    /// Send a message and wait for a response (request-response pattern)
    /// 
    /// This is the "ask" pattern - send a message and get a Future that will
    /// resolve to the response. Uses the default timeout.
    /// 
    /// # Example
    /// 
    /// ```rust,ignore
    /// // Message type with ReplyTo
    /// enum MyMessage {
    ///     GetValue { reply_to: ReplyTo<i32> },
    /// }
    /// 
    /// // Ask the actor
    /// let result = actor_ref.ask(|reply_to| MyMessage::GetValue { reply_to }).await?;
    /// ```
    pub fn ask<R, F>(&self, message_fn: F) -> AskFuture<R>
    where
        R: Send + 'static,
        F: FnOnce(ReplyTo<R>) -> A::Message,
    {
        self.ask_with_timeout(message_fn, DEFAULT_ASK_TIMEOUT)
    }
    
    /// Send a message and wait for a response with custom timeout
    /// 
    /// # Example
    /// 
    /// ```rust,ignore
    /// use std::time::Duration;
    /// 
    /// let result = actor_ref
    ///     .ask_with_timeout(
    ///         |reply_to| MyMessage::GetValue { reply_to },
    ///         Duration::from_secs(10)
    ///     )
    ///     .await?;
    /// ```
    pub fn ask_with_timeout<R, F>(&self, message_fn: F, timeout: Duration) -> AskFuture<R>
    where
        R: Send + 'static,
        F: FnOnce(ReplyTo<R>) -> A::Message,
    {
        let (reply_to, future) = ask_channel_with_timeout(timeout);
        let message = message_fn(reply_to);
        
        // Try to send the message
        if let Err(_) = self.tell(message) {
            // If send failed, return a future that immediately resolves to error
            // We need to create a dummy future that returns ActorStopped
            return AskFuture::with_timeout(
                tokio::sync::oneshot::channel().1,
                Duration::from_millis(0)
            );
        }
        
        future
    }
    
    /// Send a message and wait for a response without timeout
    /// 
    /// ⚠️ Warning: This will wait forever if the actor never responds.
    /// Prefer using `ask` or `ask_with_timeout` instead.
    pub fn ask_no_timeout<R, F>(&self, message_fn: F) -> AskFuture<R>
    where
        R: Send + 'static,
        F: FnOnce(ReplyTo<R>) -> A::Message,
    {
        let (reply_to, future) = ask_channel();
        let message = message_fn(reply_to);
        
        if let Err(_) = self.tell(message) {
            return AskFuture::new(tokio::sync::oneshot::channel().1);
        }
        
        future
    }

    /// Get the actor path
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Check if the actor is terminated
    pub fn is_terminated(&self) -> bool {
        self.is_terminated.load(Ordering::Relaxed)
    }

    /// Mark the actor as terminated
    pub fn terminate(&self) {
        self.is_terminated.store(true, Ordering::Relaxed);
    }
}

impl<A: Actor + ?Sized> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            sender: self.sender.clone(),
            is_terminated: self.is_terminated.clone(),
        }
    }
}

impl<A: Actor + ?Sized> std::fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRef")
            .field("path", &self.path)
            .field("is_terminated", &self.is_terminated.load(Ordering::Relaxed))
            .finish()
    }
}

/// Error type for actor operations
#[derive(Debug, thiserror::Error)]
pub enum ActorError {
    #[error("Actor failed: {0}")]
    Failure(String),
    #[error("Actor stopped")]
    Stopped,
    #[error("Actor not found")]
    NotFound,
    #[error("Actor already exists: {0}")]
    ActorAlreadyExists(String),
}
