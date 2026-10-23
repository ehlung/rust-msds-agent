// work_stealing.rs - Work-Stealing Dispatcher for load balancing across workers
// Implements a work-stealing queue where idle workers steal tasks from busy ones

use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// Task
// ============================================================================

/// A unit of work in the dispatcher
#[derive(Debug, Clone)]
pub struct Task {
    pub id: u64,
    pub actor_path: String,
    pub priority: TaskPriority,
    pub created_at: Instant,
    pub payload_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Urgent = 3,
}

impl Task {
    pub fn new(id: u64, actor_path: &str) -> Self {
        Self {
            id,
            actor_path: actor_path.to_string(),
            priority: TaskPriority::Normal,
            created_at: Instant::now(),
            payload_size: 0,
        }
    }

    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_payload_size(mut self, size: usize) -> Self {
        self.payload_size = size;
        self
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

// ============================================================================
// WorkerQueue
// ============================================================================

/// Per-worker double-ended queue
#[derive(Debug)]
pub struct WorkerQueue {
    pub id: usize,
    queue: VecDeque<Task>,
    processed: u64,
    stolen_from: u64,
    stolen_to: u64,
    total_wait_nanos: u64,
    total_exec_nanos: u64,
    state: WorkerState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Idle,
    Processing,
    Stealing,
    Parked,
}

impl WorkerQueue {
    pub fn new(id: usize) -> Self {
        Self {
            id,
            queue: VecDeque::new(),
            processed: 0,
            stolen_from: 0,
            stolen_to: 0,
            total_wait_nanos: 0,
            total_exec_nanos: 0,
            state: WorkerState::Idle,
        }
    }

    /// Push a task to the back (owner pushes here)
    pub fn push_back(&mut self, task: Task) {
        self.queue.push_back(task);
    }

    /// Pop a task from the front (owner pops here - FIFO for own tasks)
    pub fn pop_front(&mut self) -> Option<Task> {
        self.queue.pop_front()
    }

    /// Steal a task from the back (thieves steal from here - opposite end)
    pub fn steal_back(&mut self) -> Option<Task> {
        if self.queue.len() > 1 {
            // Only steal if worker has more than 1 task
            let task = self.queue.pop_back();
            if task.is_some() {
                self.stolen_from += 1;
            }
            task
        } else {
            None
        }
    }

    /// Steal half of the tasks from this queue
    pub fn steal_half(&mut self) -> Vec<Task> {
        let steal_count = self.queue.len() / 2;
        if steal_count == 0 {
            return Vec::new();
        }
        let mut stolen = Vec::with_capacity(steal_count);
        for _ in 0..steal_count {
            if let Some(task) = self.queue.pop_back() {
                stolen.push(task);
                self.stolen_from += 1;
            }
        }
        stolen
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn record_processed(&mut self, wait_nanos: u64, exec_nanos: u64) {
        self.processed += 1;
        self.total_wait_nanos += wait_nanos;
        self.total_exec_nanos += exec_nanos;
    }

    pub fn record_stolen_to(&mut self) {
        self.stolen_to += 1;
    }

    pub fn processed(&self) -> u64 {
        self.processed
    }

    pub fn avg_wait_nanos(&self) -> u64 {
        if self.processed == 0 { 0 } else { self.total_wait_nanos / self.processed }
    }

    pub fn avg_exec_nanos(&self) -> u64 {
        if self.processed == 0 { 0 } else { self.total_exec_nanos / self.processed }
    }

    pub fn state(&self) -> WorkerState {
        self.state
    }

    pub fn set_state(&mut self, state: WorkerState) {
        self.state = state;
    }

    pub fn stolen_from_count(&self) -> u64 {
        self.stolen_from
    }

    pub fn stolen_to_count(&self) -> u64 {
        self.stolen_to
    }
}

// ============================================================================
// StealingStrategy
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum StealingStrategy {
    /// Steal one task at a time
    StealOne,
    /// Steal half of the victim's queue
    StealHalf,
    /// Steal up to N tasks
    StealN(usize),
}

// ============================================================================
// WorkStealingConfig
// ============================================================================

#[derive(Debug, Clone)]
pub struct WorkStealingConfig {
    pub num_workers: usize,
    pub strategy: StealingStrategy,
    pub max_steal_attempts: usize,
    pub park_after_failed_steals: usize,
    pub task_queue_capacity: usize,
    pub enable_priority: bool,
}

impl Default for WorkStealingConfig {
    fn default() -> Self {
        Self {
            num_workers: 4,
            strategy: StealingStrategy::StealHalf,
            max_steal_attempts: 3,
            park_after_failed_steals: 5,
            task_queue_capacity: 1024,
            enable_priority: false,
        }
    }
}

impl WorkStealingConfig {
    pub fn with_workers(mut self, n: usize) -> Self {
        self.num_workers = n;
        self
    }

    pub fn with_strategy(mut self, strategy: StealingStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn with_priority(mut self, enable: bool) -> Self {
        self.enable_priority = enable;
        self
    }
}

// ============================================================================
// WorkStealingDispatcher
// ============================================================================

/// Work-stealing dispatcher that distributes tasks across worker queues
pub struct WorkStealingDispatcher {
    name: String,
    config: WorkStealingConfig,
    workers: Vec<WorkerQueue>,
    next_worker: usize,
    next_task_id: u64,
    stats: WorkStealingStats,
}

#[derive(Debug, Clone, Default)]
pub struct WorkStealingStats {
    pub tasks_submitted: u64,
    pub tasks_completed: u64,
    pub steal_attempts: u64,
    pub successful_steals: u64,
    pub failed_steals: u64,
    pub workers_parked: u64,
    pub workers_unparked: u64,
}

impl WorkStealingDispatcher {
    pub fn new(name: &str, config: WorkStealingConfig) -> Self {
        let num = config.num_workers;
        let mut workers = Vec::with_capacity(num);
        for i in 0..num {
            workers.push(WorkerQueue::new(i));
        }
        Self {
            name: name.to_string(),
            config,
            workers,
            next_worker: 0,
            next_task_id: 0,
            stats: WorkStealingStats::default(),
        }
    }

    /// Submit a task to the dispatcher (round-robin assignment)
    pub fn submit(&mut self, actor_path: &str) -> u64 {
        let id = self.next_task_id;
        self.next_task_id += 1;
        let task = Task::new(id, actor_path);
        self.submit_task(task);
        id
    }

    /// Submit a pre-built task
    pub fn submit_task(&mut self, task: Task) {
        let worker_idx = self.next_worker % self.workers.len();
        self.next_worker = self.next_worker.wrapping_add(1);
        self.workers[worker_idx].push_back(task);
        self.stats.tasks_submitted += 1;

        // Unpark the worker if it was parked
        if self.workers[worker_idx].state() == WorkerState::Parked {
            self.workers[worker_idx].set_state(WorkerState::Idle);
            self.stats.workers_unparked += 1;
        }
    }

    /// Submit to the least-loaded worker
    pub fn submit_to_least_loaded(&mut self, actor_path: &str) -> u64 {
        let id = self.next_task_id;
        self.next_task_id += 1;
        let task = Task::new(id, actor_path);

        let min_idx = self.workers.iter()
            .enumerate()
            .min_by_key(|(_, w)| w.len())
            .map(|(i, _)| i)
            .unwrap_or(0);

        self.workers[min_idx].push_back(task);
        self.stats.tasks_submitted += 1;

        if self.workers[min_idx].state() == WorkerState::Parked {
            self.workers[min_idx].set_state(WorkerState::Idle);
            self.stats.workers_unparked += 1;
        }

        id
    }

    /// Worker picks its next task (or steals if empty)
    pub fn next_task(&mut self, worker_id: usize) -> Option<Task> {
        if worker_id >= self.workers.len() {
            return None;
        }

        // Try own queue first
        if let Some(task) = self.workers[worker_id].pop_front() {
            self.workers[worker_id].set_state(WorkerState::Processing);
            return Some(task);
        }

        // Own queue empty → try stealing
        self.try_steal(worker_id)
    }

    /// Try to steal work from another worker
    fn try_steal(&mut self, thief_id: usize) -> Option<Task> {
        self.workers[thief_id].set_state(WorkerState::Stealing);
        let num_workers = self.workers.len();

        for attempt in 0..self.config.max_steal_attempts {
            let victim_id = (thief_id + attempt + 1) % num_workers;
            if victim_id == thief_id {
                continue;
            }

            self.stats.steal_attempts += 1;

            match &self.config.strategy {
                StealingStrategy::StealOne => {
                    // Need to split borrow
                    let stolen = if self.workers[victim_id].len() > 1 {
                        self.workers[victim_id].steal_back()
                    } else {
                        None
                    };
                    if let Some(task) = stolen {
                        self.workers[thief_id].record_stolen_to();
                        self.stats.successful_steals += 1;
                        self.workers[thief_id].set_state(WorkerState::Processing);
                        return Some(task);
                    }
                }
                StealingStrategy::StealHalf => {
                    let stolen = self.workers[victim_id].steal_half();
                    if !stolen.is_empty() {
                        let first = stolen[0].clone();
                        for task in stolen.into_iter().skip(1) {
                            self.workers[thief_id].push_back(task);
                        }
                        self.workers[thief_id].record_stolen_to();
                        self.stats.successful_steals += 1;
                        self.workers[thief_id].set_state(WorkerState::Processing);
                        return Some(first);
                    }
                }
                StealingStrategy::StealN(n) => {
                    let n = *n;
                    let mut stolen = Vec::new();
                    for _ in 0..n {
                        if let Some(task) = self.workers[victim_id].steal_back() {
                            stolen.push(task);
                        } else {
                            break;
                        }
                    }
                    if !stolen.is_empty() {
                        let first = stolen[0].clone();
                        for task in stolen.into_iter().skip(1) {
                            self.workers[thief_id].push_back(task);
                        }
                        self.workers[thief_id].record_stolen_to();
                        self.stats.successful_steals += 1;
                        self.workers[thief_id].set_state(WorkerState::Processing);
                        return Some(first);
                    }
                }
            }

            self.stats.failed_steals += 1;
        }

        // All steal attempts failed
        self.workers[thief_id].set_state(WorkerState::Idle);
        None
    }

    /// Record task completion
    pub fn complete_task(&mut self, worker_id: usize, wait_nanos: u64, exec_nanos: u64) {
        if worker_id < self.workers.len() {
            self.workers[worker_id].record_processed(wait_nanos, exec_nanos);
            self.stats.tasks_completed += 1;
            if self.workers[worker_id].is_empty() {
                self.workers[worker_id].set_state(WorkerState::Idle);
            }
        }
    }

    /// Park a worker (no work available)
    pub fn park_worker(&mut self, worker_id: usize) {
        if worker_id < self.workers.len() {
            self.workers[worker_id].set_state(WorkerState::Parked);
            self.stats.workers_parked += 1;
        }
    }

    // ── Metrics ──────────────────────────────────────────

    pub fn total_pending(&self) -> usize {
        self.workers.iter().map(|w| w.len()).sum()
    }

    pub fn worker_loads(&self) -> Vec<(usize, usize)> {
        self.workers.iter().map(|w| (w.id, w.len())).collect()
    }

    pub fn load_imbalance(&self) -> f64 {
        if self.workers.is_empty() {
            return 0.0;
        }
        let loads: Vec<usize> = self.workers.iter().map(|w| w.len()).collect();
        let max = *loads.iter().max().unwrap_or(&0) as f64;
        let min = *loads.iter().min().unwrap_or(&0) as f64;
        let avg = loads.iter().sum::<usize>() as f64 / loads.len() as f64;
        if avg == 0.0 { 0.0 } else { (max - min) / avg }
    }

    pub fn steal_success_rate(&self) -> f64 {
        if self.stats.steal_attempts == 0 {
            return 0.0;
        }
        self.stats.successful_steals as f64 / self.stats.steal_attempts as f64
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }

    pub fn worker(&self, id: usize) -> Option<&WorkerQueue> {
        self.workers.get(id)
    }

    pub fn workers(&self) -> &[WorkerQueue] {
        &self.workers
    }

    pub fn config(&self) -> &WorkStealingConfig {
        &self.config
    }

    pub fn stats(&self) -> &WorkStealingStats {
        &self.stats
    }

    /// Get worker states summary
    pub fn worker_states(&self) -> Vec<(usize, WorkerState)> {
        self.workers.iter().map(|w| (w.id, w.state())).collect()
    }

    /// Check if all workers are idle
    pub fn all_idle(&self) -> bool {
        self.workers.iter().all(|w| {
            matches!(w.state(), WorkerState::Idle | WorkerState::Parked) && w.is_empty()
        })
    }
}

impl fmt::Debug for WorkStealingDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkStealingDispatcher")
            .field("name", &self.name)
            .field("workers", &self.workers.len())
            .field("total_pending", &self.total_pending())
            .field("steal_rate", &format!("{:.1}%", self.steal_success_rate() * 100.0))
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_dispatcher(workers: usize) -> WorkStealingDispatcher {
        WorkStealingDispatcher::new(
            "test",
            WorkStealingConfig::default().with_workers(workers),
        )
    }

    #[test]
    fn test_create_dispatcher() {
        let d = make_dispatcher(4);
        assert_eq!(d.num_workers(), 4);
        assert_eq!(d.name(), "test");
        assert_eq!(d.total_pending(), 0);
    }

    #[test]
    fn test_submit_task() {
        let mut d = make_dispatcher(4);
        d.submit("/user/actor1");
        d.submit("/user/actor2");
        assert_eq!(d.total_pending(), 2);
        assert_eq!(d.stats.tasks_submitted, 2);
    }

    #[test]
    fn test_round_robin_submission() {
        let mut d = make_dispatcher(2);
        d.submit("/a");
        d.submit("/b");
        d.submit("/c");
        d.submit("/d");
        // Round robin: worker0 gets 2, worker1 gets 2
        assert_eq!(d.worker(0).unwrap().len(), 2);
        assert_eq!(d.worker(1).unwrap().len(), 2);
    }

    #[test]
    fn test_least_loaded_submission() {
        let mut d = make_dispatcher(2);
        // Manually load worker 0
        d.submit("/a"); // → worker 0
        d.submit("/b"); // → worker 1
        d.submit("/c"); // → worker 0
        // Now use least-loaded
        d.submit_to_least_loaded("/d"); // → worker 1 (less loaded)
        assert_eq!(d.worker(1).unwrap().len(), 2);
    }

    #[test]
    fn test_next_task_own_queue() {
        let mut d = make_dispatcher(2);
        d.submit("/a"); // → worker 0
        d.submit("/b"); // → worker 1
        let task = d.next_task(0).unwrap();
        assert_eq!(task.actor_path, "/a");
        assert_eq!(d.worker(0).unwrap().state(), WorkerState::Processing);
    }

    #[test]
    fn test_steal_one() {
        let mut d = WorkStealingDispatcher::new(
            "test",
            WorkStealingConfig::default()
                .with_workers(2)
                .with_strategy(StealingStrategy::StealOne),
        );
        // Load worker 1 with 3 tasks
        for i in 0..3 {
            let task = Task::new(i, &format!("/actor{}", i));
            d.workers[1].push_back(task);
        }
        d.stats.tasks_submitted = 3;

        // Worker 0 has nothing → should steal from worker 1
        let stolen = d.next_task(0);
        assert!(stolen.is_some());
        assert_eq!(d.stats.successful_steals, 1);
    }

    #[test]
    fn test_steal_half() {
        let mut d = WorkStealingDispatcher::new(
            "test",
            WorkStealingConfig::default()
                .with_workers(2)
                .with_strategy(StealingStrategy::StealHalf),
        );
        // Load worker 1 with 4 tasks
        for i in 0..4 {
            d.workers[1].push_back(Task::new(i, "/a"));
        }

        // Worker 0 steals half (2)
        let stolen = d.next_task(0);
        assert!(stolen.is_some());
        // Worker 0 got 1 task directly + potentially more in queue
        // Worker 1 should have fewer tasks
        assert!(d.worker(1).unwrap().len() < 4);
    }

    #[test]
    fn test_steal_n() {
        let mut d = WorkStealingDispatcher::new(
            "test",
            WorkStealingConfig::default()
                .with_workers(2)
                .with_strategy(StealingStrategy::StealN(2)),
        );
        for i in 0..5 {
            d.workers[1].push_back(Task::new(i, "/a"));
        }

        let stolen = d.next_task(0);
        assert!(stolen.is_some());
        assert!(d.worker(1).unwrap().len() < 5);
    }

    #[test]
    fn test_no_steal_when_victim_has_one() {
        let mut d = WorkStealingDispatcher::new(
            "test",
            WorkStealingConfig::default()
                .with_workers(2)
                .with_strategy(StealingStrategy::StealOne),
        );
        // Worker 1 has only 1 task → shouldn't be stolen
        d.workers[1].push_back(Task::new(0, "/a"));
        let stolen = d.next_task(0);
        assert!(stolen.is_none());
    }

    #[test]
    fn test_complete_task() {
        let mut d = make_dispatcher(2);
        d.submit("/a");
        let _task = d.next_task(0);
        d.complete_task(0, 100, 500);
        assert_eq!(d.stats.tasks_completed, 1);
        assert_eq!(d.worker(0).unwrap().processed(), 1);
    }

    #[test]
    fn test_park_worker() {
        let mut d = make_dispatcher(2);
        d.park_worker(0);
        assert_eq!(d.worker(0).unwrap().state(), WorkerState::Parked);
        assert_eq!(d.stats.workers_parked, 1);
    }

    #[test]
    fn test_unpark_on_submit() {
        let mut d = make_dispatcher(2);
        d.park_worker(0);
        // Submit to worker 0 (round robin starts at 0)
        d.submit("/a");
        assert_eq!(d.worker(0).unwrap().state(), WorkerState::Idle);
        assert_eq!(d.stats.workers_unparked, 1);
    }

    #[test]
    fn test_worker_queue_steal_back() {
        let mut q = WorkerQueue::new(0);
        q.push_back(Task::new(0, "/a"));
        q.push_back(Task::new(1, "/b"));
        q.push_back(Task::new(2, "/c"));

        let stolen = q.steal_back().unwrap();
        assert_eq!(stolen.id, 2); // Stolen from back
        assert_eq!(q.stolen_from_count(), 1);
    }

    #[test]
    fn test_worker_queue_steal_half() {
        let mut q = WorkerQueue::new(0);
        for i in 0..6 {
            q.push_back(Task::new(i, "/a"));
        }
        let stolen = q.steal_half();
        assert_eq!(stolen.len(), 3);
        assert_eq!(q.len(), 3);
    }

    #[test]
    fn test_worker_queue_fifo() {
        let mut q = WorkerQueue::new(0);
        q.push_back(Task::new(0, "/first"));
        q.push_back(Task::new(1, "/second"));
        let t1 = q.pop_front().unwrap();
        assert_eq!(t1.actor_path, "/first");
    }

    #[test]
    fn test_worker_stats() {
        let mut q = WorkerQueue::new(0);
        q.record_processed(100, 500);
        q.record_processed(200, 700);
        assert_eq!(q.processed(), 2);
        assert_eq!(q.avg_wait_nanos(), 150);
        assert_eq!(q.avg_exec_nanos(), 600);
    }

    #[test]
    fn test_load_imbalance() {
        let mut d = make_dispatcher(2);
        // All tasks to worker 0
        for _ in 0..10 {
            d.workers[0].push_back(Task::new(0, "/a"));
        }
        let imbalance = d.load_imbalance();
        assert!(imbalance > 0.0);
    }

    #[test]
    fn test_load_imbalance_balanced() {
        let d = make_dispatcher(2);
        assert_eq!(d.load_imbalance(), 0.0);
    }

    #[test]
    fn test_steal_success_rate() {
        let mut d = make_dispatcher(2);
        d.stats.steal_attempts = 10;
        d.stats.successful_steals = 7;
        assert!((d.steal_success_rate() - 0.7).abs() < 0.001);
    }

    #[test]
    fn test_steal_success_rate_no_attempts() {
        let d = make_dispatcher(2);
        assert_eq!(d.steal_success_rate(), 0.0);
    }

    #[test]
    fn test_all_idle() {
        let d = make_dispatcher(3);
        assert!(d.all_idle());
    }

    #[test]
    fn test_not_all_idle() {
        let mut d = make_dispatcher(2);
        d.submit("/a");
        assert!(!d.all_idle());
    }

    #[test]
    fn test_worker_states() {
        let mut d = make_dispatcher(3);
        d.park_worker(1);
        let states = d.worker_states();
        assert_eq!(states[0].1, WorkerState::Idle);
        assert_eq!(states[1].1, WorkerState::Parked);
    }

    #[test]
    fn test_task_priority() {
        let task = Task::new(0, "/a")
            .with_priority(TaskPriority::Urgent)
            .with_payload_size(1024);
        assert_eq!(task.priority, TaskPriority::Urgent);
        assert_eq!(task.payload_size, 1024);
    }

    #[test]
    fn test_priority_ordering() {
        assert!(TaskPriority::Low < TaskPriority::Normal);
        assert!(TaskPriority::Normal < TaskPriority::High);
        assert!(TaskPriority::High < TaskPriority::Urgent);
    }

    #[test]
    fn test_config_builder() {
        let config = WorkStealingConfig::default()
            .with_workers(8)
            .with_strategy(StealingStrategy::StealOne)
            .with_priority(true);
        assert_eq!(config.num_workers, 8);
        assert_eq!(config.strategy, StealingStrategy::StealOne);
        assert!(config.enable_priority);
    }

    #[test]
    fn test_worker_loads() {
        let mut d = make_dispatcher(3);
        d.workers[0].push_back(Task::new(0, "/a"));
        d.workers[0].push_back(Task::new(1, "/b"));
        d.workers[2].push_back(Task::new(2, "/c"));
        let loads = d.worker_loads();
        assert_eq!(loads, vec![(0, 2), (1, 0), (2, 1)]);
    }

    #[test]
    fn test_dispatcher_debug() {
        let d = make_dispatcher(2);
        let debug = format!("{:?}", d);
        assert!(debug.contains("WorkStealingDispatcher"));
        assert!(debug.contains("test"));
    }

    #[test]
    fn test_next_task_invalid_worker() {
        let mut d = make_dispatcher(2);
        assert!(d.next_task(99).is_none());
    }

    #[test]
    fn test_complete_task_idle_after() {
        let mut d = make_dispatcher(1);
        d.submit("/a");
        let _task = d.next_task(0);
        d.complete_task(0, 100, 200);
        assert_eq!(d.worker(0).unwrap().state(), WorkerState::Idle);
    }
}
