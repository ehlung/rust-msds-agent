//! Scheduler - Delayed and periodic message delivery
//!
//! The Scheduler provides functionality for sending messages to actors
//! after a delay or at regular intervals.
//!
//! ## Features
//!
//! - **schedule_once**: Send a message after a delay
//! - **schedule_at_fixed_rate**: Send messages at regular intervals
//! - **Cancellable**: Cancel scheduled tasks
//!
//! ## Example
//!
//! ```rust,ignore
//! use pekko_actor::Scheduler;
//! use std::time::Duration;
//!
//! // Schedule a one-time message
//! let cancellable = scheduler.schedule_once(
//!     Duration::from_secs(5),
//!     actor_ref.clone(),
//!     MyMessage::Timeout,
//! );
//!
//! // Schedule periodic messages
//! let cancellable = scheduler.schedule_at_fixed_rate(
//!     Duration::from_secs(0),   // initial delay
//!     Duration::from_secs(10),  // interval
//!     actor_ref.clone(),
//!     MyMessage::Heartbeat,
//! );
//!
//! // Cancel if needed
//! cancellable.cancel();
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, sleep, Instant};

/// A handle to cancel a scheduled task
#[derive(Clone)]
pub struct Cancellable {
    cancelled: Arc<AtomicBool>,
    task_id: u64,
}

impl Cancellable {
    fn new(task_id: u64) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            task_id,
        }
    }

    /// Cancel the scheduled task
    /// Returns true if the task was successfully cancelled
    pub fn cancel(&self) -> bool {
        let was_cancelled = self.cancelled.swap(true, Ordering::SeqCst);
        if !was_cancelled {
            tracing::debug!("Cancelled scheduled task {}", self.task_id);
        }
        !was_cancelled
    }

    /// Check if the task has been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Get the task ID
    pub fn task_id(&self) -> u64 {
        self.task_id
    }
}

impl std::fmt::Debug for Cancellable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cancellable")
            .field("task_id", &self.task_id)
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Task sender function type
pub type TaskFn = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// The Scheduler manages delayed and periodic message delivery
pub struct Scheduler {
    next_task_id: AtomicU64,
}

impl Scheduler {
    /// Create a new scheduler
    pub fn new() -> Self {
        Self {
            next_task_id: AtomicU64::new(1),
        }
    }

    fn next_id(&self) -> u64 {
        self.next_task_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Schedule a one-time task to run after a delay
    pub fn schedule_once<F, Fut>(&self, delay: Duration, task: F) -> Cancellable
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let task_id = self.next_id();
        let cancellable = Cancellable::new(task_id);
        let cancelled = cancellable.cancelled.clone();

        tokio::spawn(async move {
            sleep(delay).await;

            if !cancelled.load(Ordering::SeqCst) {
                task().await;
                tracing::trace!("Scheduled task {} executed", task_id);
            } else {
                tracing::trace!("Scheduled task {} was cancelled", task_id);
            }
        });

        tracing::debug!(
            "Scheduled one-time task {} to run after {:?}",
            task_id,
            delay
        );

        cancellable
    }

    /// Schedule a task to run at fixed intervals
    pub fn schedule_at_fixed_rate<F, Fut>(
        &self,
        initial_delay: Duration,
        period: Duration,
        mut task: F,
    ) -> Cancellable
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let task_id = self.next_id();
        let cancellable = Cancellable::new(task_id);
        let cancelled = cancellable.cancelled.clone();

        tokio::spawn(async move {
            // Initial delay
            if initial_delay > Duration::ZERO {
                sleep(initial_delay).await;
            }

            let mut interval_timer = interval(period);

            loop {
                interval_timer.tick().await;

                if cancelled.load(Ordering::SeqCst) {
                    tracing::debug!("Periodic task {} cancelled", task_id);
                    break;
                }

                task().await;
                tracing::trace!("Periodic task {} tick", task_id);
            }
        });

        tracing::debug!(
            "Scheduled periodic task {} (initial: {:?}, period: {:?})",
            task_id,
            initial_delay,
            period
        );

        cancellable
    }

    /// Schedule a task to run at fixed delay (delay between end of one execution and start of next)
    pub fn schedule_with_fixed_delay<F, Fut>(
        &self,
        initial_delay: Duration,
        delay: Duration,
        mut task: F,
    ) -> Cancellable
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let task_id = self.next_id();
        let cancellable = Cancellable::new(task_id);
        let cancelled = cancellable.cancelled.clone();

        tokio::spawn(async move {
            // Initial delay
            if initial_delay > Duration::ZERO {
                sleep(initial_delay).await;
            }

            loop {
                if cancelled.load(Ordering::SeqCst) {
                    tracing::debug!("Fixed-delay task {} cancelled", task_id);
                    break;
                }

                task().await;
                tracing::trace!("Fixed-delay task {} executed", task_id);

                sleep(delay).await;
            }
        });

        tracing::debug!(
            "Scheduled fixed-delay task {} (initial: {:?}, delay: {:?})",
            task_id,
            initial_delay,
            delay
        );

        cancellable
    }

    /// Schedule a task to run at a specific instant
    pub fn schedule_at<F, Fut>(&self, instant: Instant, task: F) -> Cancellable
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let now = Instant::now();
        let delay = if instant > now {
            instant - now
        } else {
            Duration::ZERO
        };
        self.schedule_once(delay, task)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("next_task_id", &self.next_task_id.load(Ordering::SeqCst))
            .finish()
    }
}

/// Extension trait for ActorRef to enable scheduled message sending
pub trait ScheduleExt<M: Clone + Send + 'static> {
    /// Schedule a message to be sent after a delay
    fn schedule_tell(&self, scheduler: &Scheduler, delay: Duration, msg: M) -> Cancellable;

    /// Schedule a message to be sent periodically
    fn schedule_tell_repeatedly(
        &self,
        scheduler: &Scheduler,
        initial_delay: Duration,
        interval: Duration,
        msg: M,
    ) -> Cancellable;
}

/// A timer that can be used within actors
pub struct ActorTimer {
    scheduler: Arc<Scheduler>,
    active_timers: std::sync::Mutex<Vec<Cancellable>>,
}

impl ActorTimer {
    pub fn new(scheduler: Arc<Scheduler>) -> Self {
        Self {
            scheduler,
            active_timers: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Schedule a one-time callback
    pub fn schedule_once<F, Fut>(&self, delay: Duration, task: F) -> Cancellable
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cancellable = self.scheduler.schedule_once(delay, task);
        self.active_timers.lock().unwrap().push(cancellable.clone());
        cancellable
    }

    /// Schedule a periodic callback
    pub fn schedule_at_fixed_rate<F, Fut>(
        &self,
        initial_delay: Duration,
        period: Duration,
        task: F,
    ) -> Cancellable
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cancellable = self.scheduler.schedule_at_fixed_rate(initial_delay, period, task);
        self.active_timers.lock().unwrap().push(cancellable.clone());
        cancellable
    }

    /// Cancel all active timers
    pub fn cancel_all(&self) {
        let timers = self.active_timers.lock().unwrap();
        for timer in timers.iter() {
            timer.cancel();
        }
        tracing::debug!("Cancelled {} active timers", timers.len());
    }

    /// Get the number of active (non-cancelled) timers
    pub fn active_count(&self) -> usize {
        let timers = self.active_timers.lock().unwrap();
        timers.iter().filter(|t| !t.is_cancelled()).count()
    }

    /// Clean up completed/cancelled timers
    pub fn cleanup(&self) {
        let mut timers = self.active_timers.lock().unwrap();
        timers.retain(|t| !t.is_cancelled());
    }
}

impl Drop for ActorTimer {
    fn drop(&mut self) {
        self.cancel_all();
    }
}

impl std::fmt::Debug for ActorTimer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorTimer")
            .field("active_count", &self.active_count())
            .finish()
    }
}

/// Builder for creating scheduled tasks with more options
pub struct ScheduleBuilder<'a> {
    scheduler: &'a Scheduler,
    initial_delay: Duration,
    period: Option<Duration>,
    max_executions: Option<u64>,
}

impl<'a> ScheduleBuilder<'a> {
    pub fn new(scheduler: &'a Scheduler) -> Self {
        Self {
            scheduler,
            initial_delay: Duration::ZERO,
            period: None,
            max_executions: None,
        }
    }

    /// Set initial delay
    pub fn after(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set to repeat at fixed rate
    pub fn every(mut self, period: Duration) -> Self {
        self.period = Some(period);
        self
    }

    /// Limit the number of executions
    pub fn times(mut self, count: u64) -> Self {
        self.max_executions = Some(count);
        self
    }

    /// Run once
    pub fn once(self) -> Self {
        Self {
            max_executions: Some(1),
            period: None,
            ..self
        }
    }

    /// Execute the task
    pub fn run<F, Fut>(self, task: F) -> Cancellable
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        match (self.period, self.max_executions) {
            (None, _) | (_, Some(1)) => {
                // One-time execution
                let mut task = task;
                self.scheduler.schedule_once(self.initial_delay, move || task())
            }
            (Some(period), max) => {
                // Periodic execution
                let task_id = self.scheduler.next_id();
                let cancellable = Cancellable::new(task_id);
                let cancelled = cancellable.cancelled.clone();
                let mut task = task;
                let initial_delay = self.initial_delay;

                tokio::spawn(async move {
                    if initial_delay > Duration::ZERO {
                        sleep(initial_delay).await;
                    }

                    let mut execution_count = 0u64;
                    let mut interval_timer = interval(period);

                    loop {
                        interval_timer.tick().await;

                        if cancelled.load(Ordering::SeqCst) {
                            break;
                        }

                        task().await;
                        execution_count += 1;

                        if let Some(max) = max {
                            if execution_count >= max {
                                tracing::debug!(
                                    "Task {} completed after {} executions",
                                    task_id,
                                    execution_count
                                );
                                break;
                            }
                        }
                    }
                });

                cancellable
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_schedule_once() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        let _cancellable = scheduler.schedule_once(Duration::from_millis(50), move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        // Should not have run yet
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        // Wait for it to run
        sleep(Duration::from_millis(100)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Should not run again
        sleep(Duration::from_millis(100)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_schedule_once_cancel() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        let cancellable = scheduler.schedule_once(Duration::from_millis(100), move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        // Cancel before it runs
        sleep(Duration::from_millis(30)).await;
        assert!(cancellable.cancel());
        assert!(cancellable.is_cancelled());

        // Wait past when it would have run
        sleep(Duration::from_millis(150)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_schedule_at_fixed_rate() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        let cancellable = scheduler.schedule_at_fixed_rate(
            Duration::ZERO,
            Duration::from_millis(30),
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            },
        );

        // Wait for a few ticks
        sleep(Duration::from_millis(100)).await;
        
        let count = counter.load(Ordering::SeqCst);
        assert!(count >= 2, "Expected at least 2 executions, got {}", count);

        // Cancel
        cancellable.cancel();
        let count_at_cancel = counter.load(Ordering::SeqCst);

        // Wait more and verify no more executions
        sleep(Duration::from_millis(100)).await;
        let final_count = counter.load(Ordering::SeqCst);
        
        // Allow for at most one more execution (race condition)
        assert!(
            final_count <= count_at_cancel + 1,
            "Expected no more than {} executions after cancel, got {}",
            count_at_cancel + 1,
            final_count
        );
    }

    #[tokio::test]
    async fn test_schedule_with_fixed_delay() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        let cancellable = scheduler.schedule_with_fixed_delay(
            Duration::ZERO,
            Duration::from_millis(30),
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    // Simulate some work
                    sleep(Duration::from_millis(10)).await;
                }
            },
        );

        sleep(Duration::from_millis(120)).await;
        cancellable.cancel();

        let count = counter.load(Ordering::SeqCst);
        assert!(count >= 2, "Expected at least 2 executions, got {}", count);
    }

    #[tokio::test]
    async fn test_cancellable_double_cancel() {
        let scheduler = Scheduler::new();

        let cancellable = scheduler.schedule_once(Duration::from_secs(10), || async {});

        assert!(cancellable.cancel()); // First cancel returns true
        assert!(!cancellable.cancel()); // Second cancel returns false
    }

    #[tokio::test]
    async fn test_schedule_at() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        let target_instant = Instant::now() + Duration::from_millis(50);
        
        let _cancellable = scheduler.schedule_at(target_instant, move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        sleep(Duration::from_millis(100)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_actor_timer() {
        let scheduler = Arc::new(Scheduler::new());
        let timer = ActorTimer::new(scheduler);

        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        timer.schedule_once(Duration::from_millis(30), move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        assert_eq!(timer.active_count(), 1);

        sleep(Duration::from_millis(100)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_actor_timer_cancel_all() {
        let scheduler = Arc::new(Scheduler::new());
        let timer = ActorTimer::new(scheduler);

        let counter = Arc::new(AtomicU32::new(0));

        for _ in 0..3 {
            let c = counter.clone();
            timer.schedule_once(Duration::from_millis(100), move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            });
        }

        assert_eq!(timer.active_count(), 3);

        // Cancel all before they run
        timer.cancel_all();

        sleep(Duration::from_millis(200)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_schedule_builder_once() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        let _cancellable = ScheduleBuilder::new(&scheduler)
            .after(Duration::from_millis(30))
            .once()
            .run(move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            });

        sleep(Duration::from_millis(100)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_schedule_builder_times() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        let _cancellable = ScheduleBuilder::new(&scheduler)
            .after(Duration::ZERO)
            .every(Duration::from_millis(20))
            .times(3)
            .run(move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            });

        sleep(Duration::from_millis(200)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_multiple_tasks() {
        let scheduler = Scheduler::new();
        let results = Arc::new(std::sync::Mutex::new(Vec::new()));

        for i in 0..5 {
            let r = results.clone();
            let delay = Duration::from_millis((5 - i) * 20); // Reverse order
            scheduler.schedule_once(delay, move || async move {
                r.lock().unwrap().push(i);
            });
        }

        sleep(Duration::from_millis(200)).await;

        let final_results = results.lock().unwrap();
        // Should be in reverse order (shorter delays first)
        assert_eq!(*final_results, vec![4, 3, 2, 1, 0]);
    }

    #[tokio::test]
    async fn test_zero_delay() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        scheduler.schedule_once(Duration::ZERO, move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        // Give it a moment to execute
        sleep(Duration::from_millis(10)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_task_id_increment() {
        let scheduler = Scheduler::new();

        let c1 = scheduler.schedule_once(Duration::from_secs(100), || async {});
        let c2 = scheduler.schedule_once(Duration::from_secs(100), || async {});
        let c3 = scheduler.schedule_once(Duration::from_secs(100), || async {});

        assert_eq!(c1.task_id(), 1);
        assert_eq!(c2.task_id(), 2);
        assert_eq!(c3.task_id(), 3);

        // Cancel to cleanup
        c1.cancel();
        c2.cancel();
        c3.cancel();
    }
}
