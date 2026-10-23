// saga.rs - Saga Orchestration Pattern for distributed transactions
// Implements the Saga pattern with compensating actions for failure recovery

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// SagaStep
// ============================================================================

/// A single step in a saga with an action and compensating action
#[derive(Debug, Clone)]
pub struct SagaStep {
    pub name: String,
    pub timeout: Duration,
    pub retry_limit: usize,
    pub compensatable: bool,
}

impl SagaStep {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            timeout: Duration::from_secs(30),
            retry_limit: 3,
            compensatable: true,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_retry_limit(mut self, limit: usize) -> Self {
        self.retry_limit = limit;
        self
    }

    pub fn non_compensatable(mut self) -> Self {
        self.compensatable = false;
        self
    }
}

// ============================================================================
// StepStatus
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum StepStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
    Compensating,
    Compensated,
    CompensationFailed(String),
    Skipped,
}

impl StepStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            StepStatus::Completed
                | StepStatus::Failed(_)
                | StepStatus::Compensated
                | StepStatus::CompensationFailed(_)
                | StepStatus::Skipped
        )
    }

    pub fn is_success(&self) -> bool {
        matches!(self, StepStatus::Completed)
    }

    pub fn is_failure(&self) -> bool {
        matches!(self, StepStatus::Failed(_) | StepStatus::CompensationFailed(_))
    }
}

impl fmt::Display for StepStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StepStatus::Pending => write!(f, "Pending"),
            StepStatus::Running => write!(f, "Running"),
            StepStatus::Completed => write!(f, "Completed"),
            StepStatus::Failed(e) => write!(f, "Failed: {}", e),
            StepStatus::Compensating => write!(f, "Compensating"),
            StepStatus::Compensated => write!(f, "Compensated"),
            StepStatus::CompensationFailed(e) => write!(f, "CompensationFailed: {}", e),
            StepStatus::Skipped => write!(f, "Skipped"),
        }
    }
}

// ============================================================================
// StepExecution
// ============================================================================

#[derive(Debug, Clone)]
pub struct StepExecution {
    pub step_name: String,
    pub status: StepStatus,
    pub attempts: usize,
    pub started_at: Option<Instant>,
    pub completed_at: Option<Instant>,
    pub compensation_started_at: Option<Instant>,
    pub compensation_completed_at: Option<Instant>,
}

impl StepExecution {
    pub fn new(step_name: &str) -> Self {
        Self {
            step_name: step_name.to_string(),
            status: StepStatus::Pending,
            attempts: 0,
            started_at: None,
            completed_at: None,
            compensation_started_at: None,
            compensation_completed_at: None,
        }
    }

    pub fn duration(&self) -> Option<Duration> {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => Some(end.duration_since(start)),
            _ => None,
        }
    }

    pub fn compensation_duration(&self) -> Option<Duration> {
        match (self.compensation_started_at, self.compensation_completed_at) {
            (Some(start), Some(end)) => Some(end.duration_since(start)),
            _ => None,
        }
    }

    pub fn start(&mut self) {
        self.status = StepStatus::Running;
        self.attempts += 1;
        self.started_at = Some(Instant::now());
    }

    pub fn complete(&mut self) {
        self.status = StepStatus::Completed;
        self.completed_at = Some(Instant::now());
    }

    pub fn fail(&mut self, reason: &str) {
        self.status = StepStatus::Failed(reason.to_string());
        self.completed_at = Some(Instant::now());
    }

    pub fn start_compensation(&mut self) {
        self.status = StepStatus::Compensating;
        self.compensation_started_at = Some(Instant::now());
    }

    pub fn complete_compensation(&mut self) {
        self.status = StepStatus::Compensated;
        self.compensation_completed_at = Some(Instant::now());
    }

    pub fn fail_compensation(&mut self, reason: &str) {
        self.status = StepStatus::CompensationFailed(reason.to_string());
        self.compensation_completed_at = Some(Instant::now());
    }

    pub fn skip(&mut self) {
        self.status = StepStatus::Skipped;
    }
}

// ============================================================================
// SagaState
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum SagaState {
    Created,
    Running,
    Completed,
    Failed,
    Compensating,
    Compensated,
    CompensationFailed,
    Aborted,
}

impl fmt::Display for SagaState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SagaState::Created => write!(f, "Created"),
            SagaState::Running => write!(f, "Running"),
            SagaState::Completed => write!(f, "Completed"),
            SagaState::Failed => write!(f, "Failed"),
            SagaState::Compensating => write!(f, "Compensating"),
            SagaState::Compensated => write!(f, "Compensated"),
            SagaState::CompensationFailed => write!(f, "CompensationFailed"),
            SagaState::Aborted => write!(f, "Aborted"),
        }
    }
}

// ============================================================================
// SagaConfig
// ============================================================================

#[derive(Debug, Clone)]
pub struct SagaConfig {
    pub max_retries: usize,
    pub step_timeout: Duration,
    pub saga_timeout: Duration,
    pub parallel_compensation: bool,
    pub continue_on_compensation_failure: bool,
}

impl Default for SagaConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            step_timeout: Duration::from_secs(30),
            saga_timeout: Duration::from_secs(300),
            parallel_compensation: false,
            continue_on_compensation_failure: false,
        }
    }
}

impl SagaConfig {
    pub fn with_max_retries(mut self, n: usize) -> Self {
        self.max_retries = n;
        self
    }

    pub fn with_step_timeout(mut self, timeout: Duration) -> Self {
        self.step_timeout = timeout;
        self
    }

    pub fn with_saga_timeout(mut self, timeout: Duration) -> Self {
        self.saga_timeout = timeout;
        self
    }

    pub fn with_parallel_compensation(mut self, parallel: bool) -> Self {
        self.parallel_compensation = parallel;
        self
    }

    pub fn with_continue_on_compensation_failure(mut self, cont: bool) -> Self {
        self.continue_on_compensation_failure = cont;
        self
    }
}

// ============================================================================
// SagaError
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum SagaError {
    StepFailed { step: String, reason: String },
    CompensationFailed { step: String, reason: String },
    Timeout { step: String },
    SagaTimeout,
    MaxRetriesExceeded { step: String },
    InvalidState { expected: String, actual: String },
    NoSteps,
    AlreadyRunning,
    AlreadyCompleted,
}

impl fmt::Display for SagaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SagaError::StepFailed { step, reason } => write!(f, "Step '{}' failed: {}", step, reason),
            SagaError::CompensationFailed { step, reason } => write!(f, "Compensation for '{}' failed: {}", step, reason),
            SagaError::Timeout { step } => write!(f, "Step '{}' timed out", step),
            SagaError::SagaTimeout => write!(f, "Saga timed out"),
            SagaError::MaxRetriesExceeded { step } => write!(f, "Max retries exceeded for step '{}'", step),
            SagaError::InvalidState { expected, actual } => write!(f, "Invalid state: expected {}, got {}", expected, actual),
            SagaError::NoSteps => write!(f, "Saga has no steps"),
            SagaError::AlreadyRunning => write!(f, "Saga is already running"),
            SagaError::AlreadyCompleted => write!(f, "Saga is already completed"),
        }
    }
}

// ============================================================================
// SagaLog
// ============================================================================

#[derive(Debug, Clone)]
pub struct SagaLogEntry {
    pub step_name: String,
    pub event: SagaLogEvent,
    pub timestamp: Instant,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SagaLogEvent {
    StepStarted,
    StepCompleted,
    StepFailed(String),
    StepRetried(usize),
    CompensationStarted,
    CompensationCompleted,
    CompensationFailed(String),
    StepSkipped,
    SagaStarted,
    SagaCompleted,
    SagaFailed,
    SagaCompensating,
    SagaCompensated,
}

// ============================================================================
// Saga
// ============================================================================

/// Saga orchestrator that manages a sequence of steps with compensating actions
pub struct Saga {
    id: String,
    config: SagaConfig,
    steps: Vec<SagaStep>,
    executions: Vec<StepExecution>,
    state: SagaState,
    current_step: usize,
    started_at: Option<Instant>,
    completed_at: Option<Instant>,
    log: Vec<SagaLogEntry>,
    data: HashMap<String, String>,
}

impl Saga {
    pub fn new(id: &str, steps: Vec<SagaStep>) -> Self {
        let executions = steps.iter().map(|s| StepExecution::new(&s.name)).collect();
        Self {
            id: id.to_string(),
            config: SagaConfig::default(),
            steps,
            executions,
            state: SagaState::Created,
            current_step: 0,
            started_at: None,
            completed_at: None,
            log: Vec::new(),
            data: HashMap::new(),
        }
    }

    pub fn with_config(mut self, config: SagaConfig) -> Self {
        self.config = config;
        self
    }

    // ── Lifecycle ────────────────────────────────────────

    /// Start the saga
    pub fn start(&mut self) -> Result<(), SagaError> {
        if self.steps.is_empty() {
            return Err(SagaError::NoSteps);
        }
        match self.state {
            SagaState::Created => {
                self.state = SagaState::Running;
                self.started_at = Some(Instant::now());
                self.current_step = 0;
                self.add_log("saga", SagaLogEvent::SagaStarted);
                Ok(())
            }
            SagaState::Running => Err(SagaError::AlreadyRunning),
            SagaState::Completed | SagaState::Compensated => Err(SagaError::AlreadyCompleted),
            _ => Err(SagaError::InvalidState {
                expected: "Created".to_string(),
                actual: self.state.to_string(),
            }),
        }
    }

    /// Begin executing the current step
    pub fn begin_step(&mut self) -> Result<&str, SagaError> {
        if self.state != SagaState::Running {
            return Err(SagaError::InvalidState {
                expected: "Running".to_string(),
                actual: self.state.to_string(),
            });
        }
        if self.current_step >= self.steps.len() {
            return Err(SagaError::AlreadyCompleted);
        }

        let step_name = self.steps[self.current_step].name.clone();
        self.executions[self.current_step].start();
        self.add_log(&step_name, SagaLogEvent::StepStarted);
        Ok(&self.steps[self.current_step].name)
    }

    /// Complete the current step successfully
    pub fn complete_step(&mut self) -> Result<Option<&str>, SagaError> {
        if self.current_step >= self.steps.len() {
            return Err(SagaError::AlreadyCompleted);
        }

        let step_name = self.steps[self.current_step].name.clone();
        self.executions[self.current_step].complete();
        self.add_log(&step_name, SagaLogEvent::StepCompleted);

        self.current_step += 1;

        if self.current_step >= self.steps.len() {
            self.state = SagaState::Completed;
            self.completed_at = Some(Instant::now());
            self.add_log("saga", SagaLogEvent::SagaCompleted);
            Ok(None)
        } else {
            Ok(Some(&self.steps[self.current_step].name))
        }
    }

    /// Fail the current step
    pub fn fail_step(&mut self, reason: &str) -> Result<SagaAction, SagaError> {
        if self.current_step >= self.steps.len() {
            return Err(SagaError::AlreadyCompleted);
        }

        let step_name = self.steps[self.current_step].name.clone();
        let retry_limit = self.steps[self.current_step].retry_limit;
        let attempts = self.executions[self.current_step].attempts;

        self.executions[self.current_step].fail(reason);
        self.add_log(&step_name, SagaLogEvent::StepFailed(reason.to_string()));

        if attempts < retry_limit {
            self.add_log(&step_name, SagaLogEvent::StepRetried(attempts + 1));
            Ok(SagaAction::Retry)
        } else {
            self.state = SagaState::Failed;
            self.add_log("saga", SagaLogEvent::SagaFailed);
            Ok(SagaAction::Compensate)
        }
    }

    /// Start compensation (rollback completed steps in reverse)
    pub fn begin_compensation(&mut self) -> Result<Vec<String>, SagaError> {
        if !matches!(self.state, SagaState::Failed | SagaState::Running) {
            return Err(SagaError::InvalidState {
                expected: "Failed or Running".to_string(),
                actual: self.state.to_string(),
            });
        }

        self.state = SagaState::Compensating;
        self.add_log("saga", SagaLogEvent::SagaCompensating);

        // Collect completed steps that need compensation (in reverse order)
        let to_compensate: Vec<String> = self.executions.iter()
            .enumerate()
            .rev()
            .filter(|(i, e)| e.status == StepStatus::Completed && self.steps[*i].compensatable)
            .map(|(_, e)| e.step_name.clone())
            .collect();

        for name in &to_compensate {
            if let Some(exec) = self.executions.iter_mut().find(|e| &e.step_name == name) {
                exec.start_compensation();
                self.log.push(SagaLogEntry {
                    step_name: name.clone(),
                    event: SagaLogEvent::CompensationStarted,
                    timestamp: Instant::now(),
                });
            }
        }

        Ok(to_compensate)
    }

    /// Complete compensation for a specific step
    pub fn complete_compensation(&mut self, step_name: &str) -> Result<(), SagaError> {
        if let Some(exec) = self.executions.iter_mut().find(|e| e.step_name == step_name) {
            exec.complete_compensation();
            self.log.push(SagaLogEntry {
                step_name: step_name.to_string(),
                event: SagaLogEvent::CompensationCompleted,
                timestamp: Instant::now(),
            });
            Ok(())
        } else {
            Err(SagaError::StepFailed {
                step: step_name.to_string(),
                reason: "Step not found".to_string(),
            })
        }
    }

    /// Fail compensation for a specific step
    pub fn fail_compensation(&mut self, step_name: &str, reason: &str) -> Result<(), SagaError> {
        if let Some(exec) = self.executions.iter_mut().find(|e| e.step_name == step_name) {
            exec.fail_compensation(reason);
            self.log.push(SagaLogEntry {
                step_name: step_name.to_string(),
                event: SagaLogEvent::CompensationFailed(reason.to_string()),
                timestamp: Instant::now(),
            });
            Ok(())
        } else {
            Err(SagaError::StepFailed {
                step: step_name.to_string(),
                reason: "Step not found".to_string(),
            })
        }
    }

    /// Finalize the compensation phase
    pub fn finalize_compensation(&mut self) -> SagaState {
        let has_compensation_failure = self.executions.iter().any(|e| {
            matches!(e.status, StepStatus::CompensationFailed(_))
        });

        if has_compensation_failure && !self.config.continue_on_compensation_failure {
            self.state = SagaState::CompensationFailed;
        } else {
            self.state = SagaState::Compensated;
            self.add_log("saga", SagaLogEvent::SagaCompensated);
        }
        self.completed_at = Some(Instant::now());
        self.state.clone()
    }

    /// Abort the saga immediately
    pub fn abort(&mut self) {
        self.state = SagaState::Aborted;
        self.completed_at = Some(Instant::now());
        // Skip remaining steps
        for exec in &mut self.executions {
            if exec.status == StepStatus::Pending {
                exec.skip();
            }
        }
    }

    // ── Data ─────────────────────────────────────────────

    pub fn set_data(&mut self, key: &str, value: &str) {
        self.data.insert(key.to_string(), value.to_string());
    }

    pub fn get_data(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    // ── Queries ──────────────────────────────────────────

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn state(&self) -> &SagaState {
        &self.state
    }

    pub fn config(&self) -> &SagaConfig {
        &self.config
    }

    pub fn steps(&self) -> &[SagaStep] {
        &self.steps
    }

    pub fn executions(&self) -> &[StepExecution] {
        &self.executions
    }

    pub fn current_step_name(&self) -> Option<&str> {
        self.steps.get(self.current_step).map(|s| s.name.as_str())
    }

    pub fn current_step_index(&self) -> usize {
        self.current_step
    }

    pub fn log(&self) -> &[SagaLogEntry] {
        &self.log
    }

    pub fn completed_steps(&self) -> usize {
        self.executions.iter().filter(|e| e.status == StepStatus::Completed).count()
    }

    pub fn failed_steps(&self) -> usize {
        self.executions.iter().filter(|e| e.status.is_failure()).count()
    }

    pub fn compensated_steps(&self) -> usize {
        self.executions.iter().filter(|e| e.status == StepStatus::Compensated).count()
    }

    pub fn total_attempts(&self) -> usize {
        self.executions.iter().map(|e| e.attempts).sum()
    }

    pub fn duration(&self) -> Option<Duration> {
        match (self.started_at, self.completed_at) {
            (Some(s), Some(e)) => Some(e.duration_since(s)),
            (Some(s), None) => Some(s.elapsed()),
            _ => None,
        }
    }

    pub fn progress(&self) -> f64 {
        if self.steps.is_empty() {
            return 0.0;
        }
        self.completed_steps() as f64 / self.steps.len() as f64
    }

    pub fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SagaState::Completed | SagaState::Compensated | SagaState::CompensationFailed | SagaState::Aborted
        )
    }

    pub fn is_success(&self) -> bool {
        self.state == SagaState::Completed
    }

    fn add_log(&mut self, step: &str, event: SagaLogEvent) {
        self.log.push(SagaLogEntry {
            step_name: step.to_string(),
            event,
            timestamp: Instant::now(),
        });
    }
}

/// Action to take after a step failure
#[derive(Debug, Clone, PartialEq)]
pub enum SagaAction {
    Retry,
    Compensate,
}

impl fmt::Debug for Saga {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Saga")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("steps", &self.steps.len())
            .field("completed", &self.completed_steps())
            .field("progress", &format!("{:.0}%", self.progress() * 100.0))
            .finish()
    }
}

// ============================================================================
// SagaBuilder
// ============================================================================

pub struct SagaBuilder {
    id: String,
    steps: Vec<SagaStep>,
    config: SagaConfig,
}

impl SagaBuilder {
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            steps: Vec::new(),
            config: SagaConfig::default(),
        }
    }

    pub fn step(mut self, step: SagaStep) -> Self {
        self.steps.push(step);
        self
    }

    pub fn step_named(mut self, name: &str) -> Self {
        self.steps.push(SagaStep::new(name));
        self
    }

    pub fn config(mut self, config: SagaConfig) -> Self {
        self.config = config;
        self
    }

    pub fn max_retries(mut self, n: usize) -> Self {
        self.config.max_retries = n;
        self
    }

    pub fn build(self) -> Result<Saga, SagaError> {
        if self.steps.is_empty() {
            return Err(SagaError::NoSteps);
        }
        Ok(Saga::new(&self.id, self.steps).with_config(self.config))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn three_step_saga() -> Saga {
        SagaBuilder::new("test-saga")
            .step_named("reserve-inventory")
            .step_named("process-payment")
            .step_named("ship-order")
            .build()
            .unwrap()
    }

    #[test]
    fn test_create_saga() {
        let saga = three_step_saga();
        assert_eq!(saga.id(), "test-saga");
        assert_eq!(saga.state(), &SagaState::Created);
        assert_eq!(saga.steps().len(), 3);
    }

    #[test]
    fn test_saga_builder() {
        let saga = SagaBuilder::new("builder-test")
            .step(SagaStep::new("step1").with_timeout(Duration::from_secs(10)))
            .step(SagaStep::new("step2").with_retry_limit(5))
            .step(SagaStep::new("step3").non_compensatable())
            .max_retries(2)
            .build()
            .unwrap();
        assert_eq!(saga.steps().len(), 3);
        assert_eq!(saga.steps()[0].timeout, Duration::from_secs(10));
        assert_eq!(saga.steps()[1].retry_limit, 5);
        assert!(!saga.steps()[2].compensatable);
    }

    #[test]
    fn test_builder_no_steps_error() {
        let err = SagaBuilder::new("empty").build().unwrap_err();
        assert_eq!(err, SagaError::NoSteps);
    }

    #[test]
    fn test_start_saga() {
        let mut saga = three_step_saga();
        assert!(saga.start().is_ok());
        assert_eq!(saga.state(), &SagaState::Running);
    }

    #[test]
    fn test_start_already_running() {
        let mut saga = three_step_saga();
        saga.start().unwrap();
        let err = saga.start().unwrap_err();
        assert_eq!(err, SagaError::AlreadyRunning);
    }

    #[test]
    fn test_happy_path() {
        let mut saga = three_step_saga();
        saga.start().unwrap();

        saga.begin_step().unwrap();
        let next = saga.complete_step().unwrap();
        assert!(next.is_some());
        assert_eq!(next.unwrap(), "process-payment");

        saga.begin_step().unwrap();
        let next = saga.complete_step().unwrap();
        assert!(next.is_some());

        saga.begin_step().unwrap();
        let next = saga.complete_step().unwrap();
        assert!(next.is_none()); // saga complete

        assert_eq!(saga.state(), &SagaState::Completed);
        assert!(saga.is_success());
        assert_eq!(saga.completed_steps(), 3);
        assert_eq!(saga.progress(), 1.0);
    }

    #[test]
    fn test_step_failure_retry() {
        let mut saga = three_step_saga();
        saga.start().unwrap();

        saga.begin_step().unwrap();
        let action = saga.fail_step("connection error").unwrap();
        assert_eq!(action, SagaAction::Retry); // attempts=1 < retry_limit=3
    }

    #[test]
    fn test_step_failure_compensate() {
        let mut saga = SagaBuilder::new("test")
            .step(SagaStep::new("s1").with_retry_limit(1))
            .step(SagaStep::new("s2").with_retry_limit(1))
            .build()
            .unwrap();

        saga.start().unwrap();
        saga.begin_step().unwrap();
        saga.complete_step().unwrap();

        saga.begin_step().unwrap();
        let action = saga.fail_step("error").unwrap();
        assert_eq!(action, SagaAction::Compensate);
        assert_eq!(saga.state(), &SagaState::Failed);
    }

    #[test]
    fn test_compensation_flow() {
        let mut saga = three_step_saga();
        saga.start().unwrap();

        // Complete first 2 steps
        saga.begin_step().unwrap();
        saga.complete_step().unwrap();
        saga.begin_step().unwrap();
        saga.complete_step().unwrap();

        // Fail third step
        saga.begin_step().unwrap();
        saga.steps[2].retry_limit = 1; // Force immediate compensate
        saga.fail_step("shipping failed").unwrap();

        // Begin compensation
        let to_compensate = saga.begin_compensation().unwrap();
        assert_eq!(to_compensate.len(), 2); // 2 completed steps
        assert_eq!(to_compensate[0], "process-payment"); // reverse order
        assert_eq!(to_compensate[1], "reserve-inventory");

        // Complete compensations
        saga.complete_compensation("process-payment").unwrap();
        saga.complete_compensation("reserve-inventory").unwrap();

        let final_state = saga.finalize_compensation();
        assert_eq!(final_state, SagaState::Compensated);
        assert_eq!(saga.compensated_steps(), 2);
    }

    #[test]
    fn test_compensation_failure() {
        let mut saga = SagaBuilder::new("test")
            .step(SagaStep::new("s1").with_retry_limit(0))
            .build()
            .unwrap();

        saga.start().unwrap();
        saga.begin_step().unwrap();
        saga.complete_step().unwrap();

        // Force failed state
        saga.state = SagaState::Failed;
        let to_compensate = saga.begin_compensation().unwrap();
        assert_eq!(to_compensate.len(), 1);

        saga.fail_compensation("s1", "compensation error").unwrap();
        let state = saga.finalize_compensation();
        assert_eq!(state, SagaState::CompensationFailed);
    }

    #[test]
    fn test_non_compensatable_step_skipped() {
        let mut saga = SagaBuilder::new("test")
            .step(SagaStep::new("s1"))
            .step(SagaStep::new("s2").non_compensatable())
            .build()
            .unwrap();

        saga.start().unwrap();
        saga.begin_step().unwrap();
        saga.complete_step().unwrap();
        saga.begin_step().unwrap();
        saga.complete_step().unwrap();

        saga.state = SagaState::Failed;
        let to_compensate = saga.begin_compensation().unwrap();
        // s2 is non-compensatable, only s1 should be compensated
        assert_eq!(to_compensate.len(), 1);
        assert_eq!(to_compensate[0], "s1");
    }

    #[test]
    fn test_abort() {
        let mut saga = three_step_saga();
        saga.start().unwrap();
        saga.begin_step().unwrap();
        saga.complete_step().unwrap();

        saga.abort();
        assert_eq!(saga.state(), &SagaState::Aborted);
        assert!(saga.is_complete());
        // Remaining steps should be skipped
        assert_eq!(saga.executions()[1].status, StepStatus::Skipped);
        assert_eq!(saga.executions()[2].status, StepStatus::Skipped);
    }

    #[test]
    fn test_saga_data() {
        let mut saga = three_step_saga();
        saga.set_data("order_id", "12345");
        saga.set_data("amount", "99.99");
        assert_eq!(saga.get_data("order_id"), Some(&"12345".to_string()));
        assert_eq!(saga.get_data("amount"), Some(&"99.99".to_string()));
        assert_eq!(saga.get_data("missing"), None);
    }

    #[test]
    fn test_saga_log() {
        let mut saga = three_step_saga();
        saga.start().unwrap();
        saga.begin_step().unwrap();
        saga.complete_step().unwrap();

        let log = saga.log();
        assert!(log.len() >= 3); // SagaStarted, StepStarted, StepCompleted
        assert_eq!(log[0].event, SagaLogEvent::SagaStarted);
        assert_eq!(log[1].event, SagaLogEvent::StepStarted);
        assert_eq!(log[2].event, SagaLogEvent::StepCompleted);
    }

    #[test]
    fn test_step_execution_duration() {
        let mut exec = StepExecution::new("test");
        assert!(exec.duration().is_none());
        exec.start();
        std::thread::sleep(Duration::from_millis(1));
        exec.complete();
        assert!(exec.duration().is_some());
        assert!(exec.duration().unwrap() >= Duration::from_millis(1));
    }

    #[test]
    fn test_step_status_display() {
        assert_eq!(StepStatus::Pending.to_string(), "Pending");
        assert_eq!(StepStatus::Failed("err".into()).to_string(), "Failed: err");
        assert!(StepStatus::Completed.is_success());
        assert!(StepStatus::Failed("x".into()).is_failure());
        assert!(StepStatus::Completed.is_terminal());
    }

    #[test]
    fn test_saga_state_display() {
        assert_eq!(SagaState::Running.to_string(), "Running");
        assert_eq!(SagaState::Compensating.to_string(), "Compensating");
    }

    #[test]
    fn test_saga_error_display() {
        let err = SagaError::StepFailed { step: "s1".into(), reason: "timeout".into() };
        assert!(err.to_string().contains("s1"));
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn test_saga_debug() {
        let saga = three_step_saga();
        let debug = format!("{:?}", saga);
        assert!(debug.contains("Saga"));
        assert!(debug.contains("test-saga"));
    }

    #[test]
    fn test_progress() {
        let mut saga = three_step_saga();
        assert_eq!(saga.progress(), 0.0);
        saga.start().unwrap();
        saga.begin_step().unwrap();
        saga.complete_step().unwrap();
        assert!((saga.progress() - 1.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn test_total_attempts() {
        let mut saga = three_step_saga();
        saga.start().unwrap();
        saga.begin_step().unwrap(); // attempt 1
        saga.fail_step("err").unwrap(); // retry
        saga.begin_step().unwrap(); // attempt 2
        saga.complete_step().unwrap();
        assert_eq!(saga.total_attempts(), 2);
    }

    #[test]
    fn test_config_builder() {
        let config = SagaConfig::default()
            .with_max_retries(5)
            .with_step_timeout(Duration::from_secs(10))
            .with_saga_timeout(Duration::from_secs(120))
            .with_parallel_compensation(true)
            .with_continue_on_compensation_failure(true);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.step_timeout, Duration::from_secs(10));
        assert!(config.parallel_compensation);
        assert!(config.continue_on_compensation_failure);
    }

    #[test]
    fn test_continue_on_compensation_failure() {
        let mut saga = SagaBuilder::new("test")
            .step(SagaStep::new("s1").with_retry_limit(0))
            .config(SagaConfig::default().with_continue_on_compensation_failure(true))
            .build()
            .unwrap();

        saga.start().unwrap();
        saga.begin_step().unwrap();
        saga.complete_step().unwrap();

        saga.state = SagaState::Failed;
        saga.begin_compensation().unwrap();
        saga.fail_compensation("s1", "fail").unwrap();

        let state = saga.finalize_compensation();
        assert_eq!(state, SagaState::Compensated); // continues despite failure
    }

    #[test]
    fn test_current_step_name() {
        let mut saga = three_step_saga();
        assert_eq!(saga.current_step_name(), Some("reserve-inventory"));
        saga.start().unwrap();
        saga.begin_step().unwrap();
        saga.complete_step().unwrap();
        assert_eq!(saga.current_step_name(), Some("process-payment"));
    }
}
