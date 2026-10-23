//! Coordinated Shutdown — Orderly shutdown of actor system
//!
//! Provides a phased shutdown mechanism similar to Scala Pekko's CoordinatedShutdown.
//! Tasks are registered in ordered phases and executed sequentially during shutdown.
//!
//! ## Phases (default order):
//! 1. BeforeServiceUnbind — Prepare for shutdown
//! 2. ServiceUnbind — Stop accepting new connections
//! 3. ServiceRequestsDone — Wait for in-flight requests
//! 4. ServiceStop — Stop services
//! 5. BeforeClusterShutdown — Cluster pre-shutdown
//! 6. ClusterShardingShutdown — Sharding handoff
//! 7. ClusterLeave — Leave cluster
//! 8. ClusterExiting — Exiting cluster
//! 9. ClusterExitingDone — Cluster exit complete
//! 10. BeforeActorSystemTerminate — Pre-terminate hooks
//! 11. ActorSystemTerminate — Final termination

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify, RwLock};

// ============= Phase =============

/// Shutdown phases in execution order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ShutdownPhase {
    BeforeServiceUnbind = 0,
    ServiceUnbind = 1,
    ServiceRequestsDone = 2,
    ServiceStop = 3,
    BeforeClusterShutdown = 4,
    ClusterShardingShutdown = 5,
    ClusterLeave = 6,
    ClusterExiting = 7,
    ClusterExitingDone = 8,
    BeforeActorSystemTerminate = 9,
    ActorSystemTerminate = 10,
}

impl fmt::Display for ShutdownPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeServiceUnbind => write!(f, "before-service-unbind"),
            Self::ServiceUnbind => write!(f, "service-unbind"),
            Self::ServiceRequestsDone => write!(f, "service-requests-done"),
            Self::ServiceStop => write!(f, "service-stop"),
            Self::BeforeClusterShutdown => write!(f, "before-cluster-shutdown"),
            Self::ClusterShardingShutdown => write!(f, "cluster-sharding-shutdown"),
            Self::ClusterLeave => write!(f, "cluster-leave"),
            Self::ClusterExiting => write!(f, "cluster-exiting"),
            Self::ClusterExitingDone => write!(f, "cluster-exiting-done"),
            Self::BeforeActorSystemTerminate => write!(f, "before-actor-system-terminate"),
            Self::ActorSystemTerminate => write!(f, "actor-system-terminate"),
        }
    }
}

impl ShutdownPhase {
    /// Get all phases in order.
    pub fn all() -> Vec<ShutdownPhase> {
        vec![
            Self::BeforeServiceUnbind,
            Self::ServiceUnbind,
            Self::ServiceRequestsDone,
            Self::ServiceStop,
            Self::BeforeClusterShutdown,
            Self::ClusterShardingShutdown,
            Self::ClusterLeave,
            Self::ClusterExiting,
            Self::ClusterExitingDone,
            Self::BeforeActorSystemTerminate,
            Self::ActorSystemTerminate,
        ]
    }
}

// ============= Task =============

type ShutdownTask = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

struct PhaseTask {
    name: String,
    task: ShutdownTask,
}

// ============= Reason =============

/// Reason for initiating shutdown.
#[derive(Debug, Clone, PartialEq)]
pub enum ShutdownReason {
    /// Unknown reason.
    Unknown,
    /// JVM/Process shutdown hook triggered.
    JvmExitReason,
    /// Cluster downining.
    ClusterDowning,
    /// Cluster leaving.
    ClusterLeaving,
    /// Actor system terminated.
    ActorSystemTerminateReason,
    /// Custom reason.
    Custom(String),
}

impl fmt::Display for ShutdownReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => write!(f, "unknown"),
            Self::JvmExitReason => write!(f, "jvm-exit"),
            Self::ClusterDowning => write!(f, "cluster-downing"),
            Self::ClusterLeaving => write!(f, "cluster-leaving"),
            Self::ActorSystemTerminateReason => write!(f, "actor-system-terminate"),
            Self::Custom(s) => write!(f, "{}", s),
        }
    }
}

// ============= ShutdownResult =============

/// Result of a shutdown execution.
#[derive(Debug, Clone)]
pub struct ShutdownResult {
    /// Whether shutdown completed successfully.
    pub success: bool,
    /// Reason for shutdown.
    pub reason: ShutdownReason,
    /// Per-phase results.
    pub phase_results: Vec<PhaseResult>,
    /// Total duration.
    pub duration: Duration,
}

/// Result of executing a single phase.
#[derive(Debug, Clone)]
pub struct PhaseResult {
    pub phase: ShutdownPhase,
    pub success: bool,
    pub duration: Duration,
    pub tasks_completed: usize,
    pub error: Option<String>,
}

// ============= CoordinatedShutdown =============

/// Coordinated shutdown controller.
pub struct CoordinatedShutdown {
    tasks: Mutex<BTreeMap<ShutdownPhase, Vec<PhaseTask>>>,
    phase_timeouts: RwLock<HashMap<ShutdownPhase, Duration>>,
    running: AtomicBool,
    shutdown_done: AtomicBool,
    default_timeout: Duration,
    reason: Mutex<Option<ShutdownReason>>,
    notify: Notify,
    result: Mutex<Option<ShutdownResult>>,
}

impl CoordinatedShutdown {
    /// Create a new coordinated shutdown with default timeout per phase.
    pub fn new(default_phase_timeout: Duration) -> Self {
        Self {
            tasks: Mutex::new(BTreeMap::new()),
            phase_timeouts: RwLock::new(HashMap::new()),
            running: AtomicBool::new(false),
            shutdown_done: AtomicBool::new(false),
            default_timeout: default_phase_timeout,
            reason: Mutex::new(None),
            notify: Notify::new(),
            result: Mutex::new(None),
        }
    }

    /// Create with a 10-second default timeout.
    pub fn with_defaults() -> Self {
        Self::new(Duration::from_secs(10))
    }

    /// Register a task for a specific phase.
    pub async fn add_task<F, Fut>(&self, phase: ShutdownPhase, name: impl Into<String>, task: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if self.shutdown_done.load(Ordering::SeqCst) {
            return; // Already shut down, ignore
        }

        let name = name.into();
        let boxed_task: ShutdownTask = Box::new(move || Box::pin(task()));
        let phase_task = PhaseTask {
            name,
            task: boxed_task,
        };

        let mut tasks = self.tasks.lock().await;
        tasks.entry(phase).or_insert_with(Vec::new).push(phase_task);
    }

    /// Set timeout for a specific phase.
    pub async fn set_phase_timeout(&self, phase: ShutdownPhase, timeout: Duration) {
        self.phase_timeouts.write().await.insert(phase, timeout);
    }

    /// Check if shutdown is in progress.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Check if shutdown is complete.
    pub fn is_done(&self) -> bool {
        self.shutdown_done.load(Ordering::SeqCst)
    }

    /// Run the coordinated shutdown.
    pub async fn run_all(&self, reason: ShutdownReason) -> ShutdownResult {
        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            self.notify.notified().await;
            return self.result.lock().await.clone().unwrap_or(ShutdownResult {
                success: false,
                reason: reason.clone(),
                phase_results: vec![],
                duration: Duration::ZERO,
            });
        }

        *self.reason.lock().await = Some(reason.clone());
        let start = std::time::Instant::now();
        let mut phase_results = Vec::new();
        let mut all_success = true;

        // Take all tasks (BTreeMap is already sorted by phase order)
        let all_tasks = {
            let mut tasks = self.tasks.lock().await;
            std::mem::take(&mut *tasks)
        };

        for (phase, tasks) in all_tasks {
            let result = self.run_phase(phase, tasks).await;
            if !result.success {
                all_success = false;
            }
            phase_results.push(result);
        }

        let result = ShutdownResult {
            success: all_success,
            reason,
            phase_results,
            duration: start.elapsed(),
        };

        *self.result.lock().await = Some(result.clone());
        self.shutdown_done.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();

        result
    }

    async fn run_phase(&self, phase: ShutdownPhase, tasks: Vec<PhaseTask>) -> PhaseResult {
        let phase_start = std::time::Instant::now();
        let timeout_duration = {
            let timeouts = self.phase_timeouts.read().await;
            timeouts.get(&phase).copied().unwrap_or(self.default_timeout)
        };

        let task_count = tasks.len();
        let mut error = None;

        for task in tasks {
            let fut = (task.task)();
            match tokio::time::timeout(timeout_duration, fut).await {
                Ok(()) => {}
                Err(_) => {
                    error = Some(format!(
                        "Task '{}' in phase {} timed out after {:?}",
                        task.name, phase, timeout_duration
                    ));
                    break;
                }
            }
        }

        PhaseResult {
            phase,
            success: error.is_none(),
            duration: phase_start.elapsed(),
            tasks_completed: if error.is_some() { task_count - 1 } else { task_count },
            error,
        }
    }

    /// Get the shutdown reason (if shutdown has started).
    pub async fn reason(&self) -> Option<ShutdownReason> {
        self.reason.lock().await.clone()
    }

    /// Get the result of shutdown (if completed).
    pub async fn result(&self) -> Option<ShutdownResult> {
        self.result.lock().await.clone()
    }

    /// Get the number of registered tasks across all phases.
    pub async fn task_count(&self) -> usize {
        self.tasks
            .lock()
            .await
            .values()
            .map(|v| v.len())
            .sum()
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI32, AtomicU32};

    #[tokio::test]
    async fn test_basic_shutdown() {
        let cs = CoordinatedShutdown::with_defaults();
        let counter = Arc::new(AtomicI32::new(0));
        let c = counter.clone();

        cs.add_task(ShutdownPhase::ServiceStop, "stop-service", move || async move {
            c.fetch_add(1, Ordering::Relaxed);
        })
        .await;

        let result = cs.run_all(ShutdownReason::Unknown).await;
        assert!(result.success);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_phase_ordering() {
        let cs = CoordinatedShutdown::with_defaults();
        let log = Arc::new(Mutex::new(Vec::new()));

        let l1 = log.clone();
        cs.add_task(ShutdownPhase::ServiceStop, "phase3", move || async move {
            l1.lock().await.push(3);
        })
        .await;

        let l2 = log.clone();
        cs.add_task(ShutdownPhase::BeforeServiceUnbind, "phase1", move || async move {
            l2.lock().await.push(1);
        })
        .await;

        let l3 = log.clone();
        cs.add_task(ShutdownPhase::ServiceUnbind, "phase2", move || async move {
            l3.lock().await.push(2);
        })
        .await;

        let result = cs.run_all(ShutdownReason::Unknown).await;
        assert!(result.success);

        let order = log.lock().await;
        assert_eq!(*order, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_multiple_tasks_per_phase() {
        let cs = CoordinatedShutdown::with_defaults();
        let counter = Arc::new(AtomicI32::new(0));

        for i in 0..3 {
            let c = counter.clone();
            cs.add_task(
                ShutdownPhase::ServiceStop,
                format!("task-{}", i),
                move || async move {
                    c.fetch_add(1, Ordering::Relaxed);
                },
            )
            .await;
        }

        let result = cs.run_all(ShutdownReason::Unknown).await;
        assert!(result.success);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_phase_timeout() {
        let cs = CoordinatedShutdown::new(Duration::from_millis(50));

        cs.add_task(ShutdownPhase::ServiceStop, "slow-task", || async {
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
        .await;

        let result = cs.run_all(ShutdownReason::Unknown).await;
        assert!(!result.success);
        assert!(result.phase_results.iter().any(|r| !r.success));
    }

    #[tokio::test]
    async fn test_custom_phase_timeout() {
        let cs = CoordinatedShutdown::with_defaults();
        cs.set_phase_timeout(ShutdownPhase::ServiceStop, Duration::from_millis(50))
            .await;

        cs.add_task(ShutdownPhase::ServiceStop, "slow", || async {
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
        .await;

        let result = cs.run_all(ShutdownReason::Unknown).await;
        assert!(!result.success);
    }

    #[tokio::test]
    async fn test_shutdown_reason() {
        let cs = CoordinatedShutdown::with_defaults();

        let result = cs
            .run_all(ShutdownReason::Custom("test".into()))
            .await;
        assert_eq!(result.reason, ShutdownReason::Custom("test".into()));

        let reason = cs.reason().await;
        assert_eq!(reason, Some(ShutdownReason::Custom("test".into())));
    }

    #[tokio::test]
    async fn test_is_done() {
        let cs = CoordinatedShutdown::with_defaults();
        assert!(!cs.is_done());

        cs.run_all(ShutdownReason::Unknown).await;
        assert!(cs.is_done());
    }

    #[tokio::test]
    async fn test_add_task_after_shutdown() {
        let cs = CoordinatedShutdown::with_defaults();
        cs.run_all(ShutdownReason::Unknown).await;

        // Task added after shutdown should be ignored
        let called = Arc::new(AtomicBool::new(false));
        let c = called.clone();
        cs.add_task(ShutdownPhase::ServiceStop, "late", move || async move {
            c.store(true, Ordering::SeqCst);
        })
        .await;

        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_empty_shutdown() {
        let cs = CoordinatedShutdown::with_defaults();
        let result = cs.run_all(ShutdownReason::Unknown).await;
        assert!(result.success);
        assert!(result.phase_results.is_empty());
    }

    #[tokio::test]
    async fn test_task_count() {
        let cs = CoordinatedShutdown::with_defaults();

        cs.add_task(ShutdownPhase::ServiceStop, "a", || async {}).await;
        cs.add_task(ShutdownPhase::ServiceStop, "b", || async {}).await;
        cs.add_task(ShutdownPhase::ServiceUnbind, "c", || async {}).await;

        assert_eq!(cs.task_count().await, 3);
    }

    #[tokio::test]
    async fn test_phase_result_details() {
        let cs = CoordinatedShutdown::with_defaults();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        cs.add_task(ShutdownPhase::ServiceStop, "task1", move || async move {
            c.fetch_add(1, Ordering::Relaxed);
        })
        .await;

        let result = cs.run_all(ShutdownReason::Unknown).await;
        let phase = &result.phase_results[0];
        assert_eq!(phase.phase, ShutdownPhase::ServiceStop);
        assert!(phase.success);
        assert_eq!(phase.tasks_completed, 1);
        assert!(phase.error.is_none());
    }

    #[tokio::test]
    async fn test_full_lifecycle() {
        let cs = CoordinatedShutdown::with_defaults();
        let log = Arc::new(Mutex::new(Vec::new()));

        // Register tasks in various phases
        let phases = vec![
            (ShutdownPhase::BeforeServiceUnbind, "prepare"),
            (ShutdownPhase::ServiceUnbind, "unbind"),
            (ShutdownPhase::ServiceRequestsDone, "drain"),
            (ShutdownPhase::ServiceStop, "stop"),
            (ShutdownPhase::ActorSystemTerminate, "terminate"),
        ];

        for (phase, name) in phases {
            let l = log.clone();
            let n = name.to_string();
            cs.add_task(phase, name, move || async move {
                l.lock().await.push(n);
            })
            .await;
        }

        let result = cs
            .run_all(ShutdownReason::ActorSystemTerminateReason)
            .await;
        assert!(result.success);
        assert_eq!(result.phase_results.len(), 5);

        let order = log.lock().await;
        assert_eq!(
            *order,
            vec!["prepare", "unbind", "drain", "stop", "terminate"]
        );
    }

    #[test]
    fn test_phase_display() {
        assert_eq!(ShutdownPhase::ServiceStop.to_string(), "service-stop");
        assert_eq!(
            ShutdownPhase::ActorSystemTerminate.to_string(),
            "actor-system-terminate"
        );
    }

    #[test]
    fn test_reason_display() {
        assert_eq!(ShutdownReason::Unknown.to_string(), "unknown");
        assert_eq!(
            ShutdownReason::Custom("graceful".into()).to_string(),
            "graceful"
        );
    }

    #[test]
    fn test_phase_all_ordered() {
        let phases = ShutdownPhase::all();
        assert_eq!(phases.len(), 11);
        assert_eq!(phases[0], ShutdownPhase::BeforeServiceUnbind);
        assert_eq!(phases[10], ShutdownPhase::ActorSystemTerminate);
    }
}
