//! PipeTo — Pipe future results to actors
//!
//! The pipe pattern allows sending the result of an async operation
//! to an actor, similar to Scala Pekko's `pipeTo` pattern.
//!
//! ## Example
//! ```ignore
//! use pekko_actor::pipe_to::PipeToExt;
//!
//! // Pipe a future result to an actor
//! async_operation()
//!     .pipe_to(&actor_ref)
//!     .await;
//!
//! // With error mapping
//! async_operation()
//!     .pipe_to_with_status(&actor_ref, |r| match r {
//!         Ok(v) => StatusMessage::Success(v),
//!         Err(e) => StatusMessage::Failure(e.to_string()),
//!     })
//!     .await;
//! ```

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;

// ============= PipeTarget =============

/// Trait for targets that can receive piped messages.
pub trait PipeTarget<T: Send + 'static>: Send + Sync {
    /// Send a message to the target.
    fn pipe_send(&self, msg: T) -> Result<(), PipeError>;
}

/// Error when piping fails.
#[derive(Debug, Clone)]
pub enum PipeError {
    /// Target channel is closed.
    TargetClosed,
    /// Send operation timed out.
    Timeout,
    /// Custom error.
    Custom(String),
}

impl std::fmt::Display for PipeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetClosed => write!(f, "Pipe target is closed"),
            Self::Timeout => write!(f, "Pipe send timed out"),
            Self::Custom(s) => write!(f, "Pipe error: {}", s),
        }
    }
}

/// An unbounded channel-based pipe target.
pub struct ChannelTarget<T: Send + 'static> {
    sender: mpsc::UnboundedSender<T>,
}

impl<T: Send + 'static> ChannelTarget<T> {
    pub fn new(sender: mpsc::UnboundedSender<T>) -> Self {
        Self { sender }
    }
}

impl<T: Send + Debug + 'static> PipeTarget<T> for ChannelTarget<T> {
    fn pipe_send(&self, msg: T) -> Result<(), PipeError> {
        self.sender.send(msg).map_err(|_| PipeError::TargetClosed)
    }
}

// ============= PipeTo =============

/// Pipe the result of a future to a target.
pub async fn pipe_to<F, T, Target>(future: F, target: &Target) -> Result<(), PipeError>
where
    F: Future<Output = T>,
    T: Send + Debug + 'static,
    Target: PipeTarget<T>,
{
    let result = future.await;
    target.pipe_send(result)
}

/// Pipe with transformation of the result.
pub async fn pipe_to_mapped<F, T, U, M, Target>(
    future: F,
    target: &Target,
    mapper: M,
) -> Result<(), PipeError>
where
    F: Future<Output = T>,
    U: Send + Debug + 'static,
    M: FnOnce(T) -> U,
    Target: PipeTarget<U>,
{
    let result = future.await;
    let mapped = mapper(result);
    target.pipe_send(mapped)
}

/// Pipe with timeout — if the future doesn't complete in time, send a timeout message.
pub async fn pipe_to_with_timeout<F, T, Target>(
    future: F,
    target: &Target,
    timeout_duration: Duration,
    timeout_msg: T,
) -> Result<(), PipeError>
where
    F: Future<Output = T>,
    T: Send + Debug + 'static,
    Target: PipeTarget<T>,
{
    match tokio::time::timeout(timeout_duration, future).await {
        Ok(result) => target.pipe_send(result),
        Err(_) => target.pipe_send(timeout_msg),
    }
}

// ============= PipeToExt trait =============

/// Extension trait for futures to enable `.pipe_to()` syntax.
pub trait PipeToExt: Sized {
    type Output;

    /// Pipe the future's result to a target.
    fn pipe_to<Target>(
        self,
        target: &Target,
    ) -> Pin<Box<dyn Future<Output = Result<(), PipeError>> + Send + '_>>
    where
        Target: PipeTarget<Self::Output> + Sync,
        Self::Output: Send + Debug + 'static;

    /// Pipe with a mapper function.
    fn pipe_to_mapped<U, M, Target>(
        self,
        target: &Target,
        mapper: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), PipeError>> + Send + '_>>
    where
        U: Send + Debug + 'static,
        M: FnOnce(Self::Output) -> U + Send + 'static,
        Target: PipeTarget<U> + Sync,
        Self::Output: Send + 'static;
}

impl<F, T> PipeToExt for F
where
    F: Future<Output = T> + Send + 'static,
    T: Send + Debug + 'static,
{
    type Output = T;

    fn pipe_to<Target>(
        self,
        target: &Target,
    ) -> Pin<Box<dyn Future<Output = Result<(), PipeError>> + Send + '_>>
    where
        Target: PipeTarget<T> + Sync,
    {
        Box::pin(pipe_to(self, target))
    }

    fn pipe_to_mapped<U, M, Target>(
        self,
        target: &Target,
        mapper: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), PipeError>> + Send + '_>>
    where
        U: Send + Debug + 'static,
        M: FnOnce(T) -> U + Send + 'static,
        Target: PipeTarget<U> + Sync,
    {
        Box::pin(pipe_to_mapped(self, target, mapper))
    }
}

// ============= PipeBuilder =============

/// Builder for constructing complex pipe operations.
pub struct PipeBuilder<T: Send + 'static> {
    _phantom: std::marker::PhantomData<T>,
    timeout: Option<Duration>,
}

impl<T: Send + Debug + 'static> PipeBuilder<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
            timeout: None,
        }
    }

    /// Set a timeout for the pipe operation.
    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    /// Execute the pipe operation.
    pub async fn pipe<F, Target>(
        self,
        future: F,
        target: &Target,
    ) -> Result<(), PipeError>
    where
        F: Future<Output = T>,
        Target: PipeTarget<T>,
    {
        if let Some(timeout_d) = self.timeout {
            match tokio::time::timeout(timeout_d, future).await {
                Ok(result) => target.pipe_send(result),
                Err(_) => Err(PipeError::Timeout),
            }
        } else {
            let result = future.await;
            target.pipe_send(result)
        }
    }
}

// ============= StatusPipe =============

/// Status wrapper for piped results (Success/Failure).
#[derive(Debug, Clone, PartialEq)]
pub enum PipeStatus<T, E> {
    Success(T),
    Failure(E),
}

/// Pipe a Result future, wrapping it in PipeStatus.
pub async fn pipe_status<F, T, E, Target>(
    future: F,
    target: &Target,
) -> Result<(), PipeError>
where
    F: Future<Output = Result<T, E>>,
    T: Send + Debug + 'static,
    E: Send + Debug + 'static,
    Target: PipeTarget<PipeStatus<T, E>>,
{
    let result = future.await;
    let status = match result {
        Ok(v) => PipeStatus::Success(v),
        Err(e) => PipeStatus::Failure(e),
    };
    target.pipe_send(status)
}

// ============= Multi-pipe =============

/// Pipe multiple futures and collect results.
pub async fn pipe_all<T, Target>(
    futures: Vec<Pin<Box<dyn Future<Output = T> + Send>>>,
    target: &Target,
) -> Result<(), PipeError>
where
    T: Send + Debug + 'static,
    Target: PipeTarget<Vec<T>>,
{
    let results = futures::future::join_all(futures).await;
    target.pipe_send(results)
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    fn make_target<T: Send + Debug + 'static>() -> (ChannelTarget<T>, mpsc::UnboundedReceiver<T>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (ChannelTarget::new(tx), rx)
    }

    #[tokio::test]
    async fn test_pipe_to_basic() {
        let (target, mut rx) = make_target::<i32>();

        pipe_to(async { 42 }, &target).await.unwrap();

        assert_eq!(rx.recv().await, Some(42));
    }

    #[tokio::test]
    async fn test_pipe_to_string() {
        let (target, mut rx) = make_target::<String>();

        pipe_to(async { "hello".to_string() }, &target)
            .await
            .unwrap();

        assert_eq!(rx.recv().await, Some("hello".to_string()));
    }

    #[tokio::test]
    async fn test_pipe_to_mapped() {
        let (target, mut rx) = make_target::<String>();

        pipe_to_mapped(async { 42 }, &target, |n| format!("value: {}", n))
            .await
            .unwrap();

        assert_eq!(rx.recv().await, Some("value: 42".to_string()));
    }

    #[tokio::test]
    async fn test_pipe_to_closed_target() {
        let (tx, rx) = mpsc::unbounded_channel::<i32>();
        let target = ChannelTarget::new(tx);
        drop(rx);

        let result = pipe_to(async { 42 }, &target).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pipe_to_with_timeout_success() {
        let (target, mut rx) = make_target::<i32>();

        pipe_to_with_timeout(
            async { 42 },
            &target,
            Duration::from_secs(1),
            -1,
        )
        .await
        .unwrap();

        assert_eq!(rx.recv().await, Some(42));
    }

    #[tokio::test]
    async fn test_pipe_to_with_timeout_expired() {
        let (target, mut rx) = make_target::<i32>();

        pipe_to_with_timeout(
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                42
            },
            &target,
            Duration::from_millis(50),
            -1, // timeout message
        )
        .await
        .unwrap();

        assert_eq!(rx.recv().await, Some(-1)); // Got timeout message
    }

    #[tokio::test]
    async fn test_pipe_ext_trait() {
        let (target, mut rx) = make_target::<i32>();

        async { 99 }.pipe_to(&target).await.unwrap();

        assert_eq!(rx.recv().await, Some(99));
    }

    #[tokio::test]
    async fn test_pipe_ext_mapped() {
        let (target, mut rx) = make_target::<String>();

        async { 42 }
            .pipe_to_mapped(&target, |n| format!("got {}", n))
            .await
            .unwrap();

        assert_eq!(rx.recv().await, Some("got 42".to_string()));
    }

    #[tokio::test]
    async fn test_pipe_builder_basic() {
        let (target, mut rx) = make_target::<i32>();

        PipeBuilder::new()
            .pipe(async { 10 }, &target)
            .await
            .unwrap();

        assert_eq!(rx.recv().await, Some(10));
    }

    #[tokio::test]
    async fn test_pipe_builder_timeout() {
        let (target, _rx) = make_target::<i32>();

        let result = PipeBuilder::new()
            .with_timeout(Duration::from_millis(50))
            .pipe(
                async {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    42
                },
                &target,
            )
            .await;

        assert!(matches!(result, Err(PipeError::Timeout)));
    }

    #[tokio::test]
    async fn test_pipe_status_success() {
        let (target, mut rx) = make_target::<PipeStatus<i32, String>>();

        pipe_status(async { Ok::<_, String>(42) }, &target)
            .await
            .unwrap();

        assert_eq!(rx.recv().await, Some(PipeStatus::Success(42)));
    }

    #[tokio::test]
    async fn test_pipe_status_failure() {
        let (target, mut rx) = make_target::<PipeStatus<i32, String>>();

        pipe_status(async { Err::<i32, _>("error".to_string()) }, &target)
            .await
            .unwrap();

        assert_eq!(
            rx.recv().await,
            Some(PipeStatus::Failure("error".to_string()))
        );
    }

    #[tokio::test]
    async fn test_pipe_all() {
        let (target, mut rx) = make_target::<Vec<i32>>();

        let futures: Vec<Pin<Box<dyn Future<Output = i32> + Send>>> = vec![
            Box::pin(async { 1 }),
            Box::pin(async { 2 }),
            Box::pin(async { 3 }),
        ];

        pipe_all(futures, &target).await.unwrap();

        let result = rx.recv().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_pipe_error_display() {
        assert_eq!(PipeError::TargetClosed.to_string(), "Pipe target is closed");
        assert_eq!(PipeError::Timeout.to_string(), "Pipe send timed out");
        assert_eq!(
            PipeError::Custom("test".into()).to_string(),
            "Pipe error: test"
        );
    }
}
