//! Integration tests for supervision strategies
//!
//! These tests verify that supervision works correctly in real actor scenarios.

#[cfg(test)]
mod tests {
    use crate::actor::{Actor, Props};
    use crate::actor_context::ActorContext;
    use crate::actor_system::ActorSystem;
    use crate::supervision::{
        ChildFailure, Directive, OneForAllStrategy, OneForOneStrategy, RestForOneStrategy,
        SupervisorState,
    };
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::sleep;

    // ============================================================
    // Test Actors
    // ============================================================

    /// A simple worker that can be told to fail
    #[derive(Clone)]
    struct TestWorker {
        name: String,
        /// Counts how many times this worker has been started (including restarts)
        start_count: Arc<AtomicU32>,
        started_tx: Option<mpsc::Sender<String>>,
    }

    #[derive(Debug, Clone)]
    enum WorkerMessage {
        DoWork,
        Fail(String),
        GetRestartCount,
    }

    impl TestWorker {
        fn new(name: &str, start_count: Arc<AtomicU32>) -> Self {
            Self {
                name: name.to_string(),
                start_count,
                started_tx: None,
            }
        }

        fn with_started_notification(
            name: &str,
            start_count: Arc<AtomicU32>,
            started_tx: mpsc::Sender<String>,
        ) -> Self {
            Self {
                name: name.to_string(),
                start_count,
                started_tx: Some(started_tx),
            }
        }
    }

    #[async_trait]
    impl Actor for TestWorker {
        type Message = WorkerMessage;

        async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
            match msg {
                WorkerMessage::DoWork => {
                }
                WorkerMessage::Fail(reason) => {
                    tracing::error!("Worker '{}' failing: {}", self.name, reason);
                    panic!("Intentional failure: {}", reason);
                }
                WorkerMessage::GetRestartCount => {
                    let count = self.start_count.load(Ordering::SeqCst);
                }
            }
        }

        async fn pre_start(&mut self) {
            let count = self.start_count.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some(ref tx) = self.started_tx {
                let _ = tx.send(self.name.clone()).await;
            }
        }

        async fn post_stop(&mut self) {
        }
    }

    // ============================================================
    // Supervision Strategy Unit Tests
    // ============================================================

    #[tokio::test]
    async fn test_one_for_one_only_restarts_failed_child() {
        let strategy = Arc::new(OneForOneStrategy::with_defaults(10, Duration::from_secs(60)));
        let supervisor_state = SupervisorState::new(strategy);

        // Register children
        supervisor_state.register_child("child1").await;
        supervisor_state.register_child("child2").await;
        supervisor_state.register_child("child3").await;

        // Simulate child2 failure
        let failure = ChildFailure {
            child_name: "child2".to_string(),
            error_message: "test error".to_string(),
            timestamp: std::time::Instant::now(),
        };

        let result = supervisor_state.handle_child_failure(&failure).await;

        assert_eq!(result.directive, Directive::Restart);
        assert_eq!(result.children_to_restart, vec!["child2".to_string()]);
        assert!(!result.retry_limit_exceeded);
    }

    #[tokio::test]
    async fn test_one_for_all_restarts_all_children() {
        let strategy = Arc::new(OneForAllStrategy::with_defaults(10, Duration::from_secs(60)));
        let supervisor_state = SupervisorState::new(strategy);

        supervisor_state.register_child("child1").await;
        supervisor_state.register_child("child2").await;
        supervisor_state.register_child("child3").await;

        let failure = ChildFailure {
            child_name: "child2".to_string(),
            error_message: "test error".to_string(),
            timestamp: std::time::Instant::now(),
        };

        let result = supervisor_state.handle_child_failure(&failure).await;

        assert_eq!(result.directive, Directive::Restart);
        assert_eq!(
            result.children_to_restart,
            vec![
                "child1".to_string(),
                "child2".to_string(),
                "child3".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn test_rest_for_one_restarts_failed_and_subsequent() {
        let strategy = Arc::new(RestForOneStrategy::with_defaults(10, Duration::from_secs(60)));
        let supervisor_state = SupervisorState::new(strategy);

        // Order matters for RestForOne
        supervisor_state.register_child("child1").await;
        supervisor_state.register_child("child2").await;
        supervisor_state.register_child("child3").await;

        // child2 fails - should restart child2 and child3, but not child1
        let failure = ChildFailure {
            child_name: "child2".to_string(),
            error_message: "test error".to_string(),
            timestamp: std::time::Instant::now(),
        };

        let result = supervisor_state.handle_child_failure(&failure).await;

        assert_eq!(result.directive, Directive::Restart);
        assert_eq!(
            result.children_to_restart,
            vec!["child2".to_string(), "child3".to_string()]
        );
    }

    #[tokio::test]
    async fn test_restart_limit_exceeded_stops_child() {
        // Only allow 2 restarts within 60 seconds
        let strategy = Arc::new(OneForOneStrategy::with_defaults(2, Duration::from_secs(60)));
        let supervisor_state = SupervisorState::new(strategy);

        supervisor_state.register_child("unstable_child").await;

        let failure = ChildFailure {
            child_name: "unstable_child".to_string(),
            error_message: "recurring error".to_string(),
            timestamp: std::time::Instant::now(),
        };

        // First failure - should restart
        let result1 = supervisor_state.handle_child_failure(&failure).await;
        assert_eq!(result1.directive, Directive::Restart);
        assert!(!result1.retry_limit_exceeded);

        // Second failure - should restart
        let result2 = supervisor_state.handle_child_failure(&failure).await;
        assert_eq!(result2.directive, Directive::Restart);
        assert!(!result2.retry_limit_exceeded);

        // Third failure - should stop (exceeded limit of 2)
        let result3 = supervisor_state.handle_child_failure(&failure).await;
        assert_eq!(result3.directive, Directive::Stop);
        assert!(result3.retry_limit_exceeded);
    }

    #[tokio::test]
    async fn test_custom_decider_resume() {
        let strategy = Arc::new(OneForOneStrategy::new(
            10,
            Duration::from_secs(60),
            |error| {
                if error.to_string().contains("transient") {
                    Directive::Resume
                } else {
                    Directive::Restart
                }
            },
        ));
        let supervisor_state = SupervisorState::new(strategy);

        supervisor_state.register_child("child1").await;

        // Transient error - should resume
        let transient_failure = ChildFailure {
            child_name: "child1".to_string(),
            error_message: "transient network error".to_string(),
            timestamp: std::time::Instant::now(),
        };

        let result = supervisor_state.handle_child_failure(&transient_failure).await;
        assert_eq!(result.directive, Directive::Resume);
        assert!(result.children_to_restart.is_empty());

        // Fatal error - should restart
        let fatal_failure = ChildFailure {
            child_name: "child1".to_string(),
            error_message: "fatal error".to_string(),
            timestamp: std::time::Instant::now(),
        };

        let result = supervisor_state.handle_child_failure(&fatal_failure).await;
        assert_eq!(result.directive, Directive::Restart);
    }

    #[tokio::test]
    async fn test_custom_decider_escalate() {
        let strategy = Arc::new(OneForOneStrategy::new(
            10,
            Duration::from_secs(60),
            |error| {
                if error.to_string().contains("critical") {
                    Directive::Escalate
                } else {
                    Directive::Restart
                }
            },
        ));
        let supervisor_state = SupervisorState::new(strategy);

        supervisor_state.register_child("child1").await;

        let critical_failure = ChildFailure {
            child_name: "child1".to_string(),
            error_message: "critical system error".to_string(),
            timestamp: std::time::Instant::now(),
        };

        let result = supervisor_state.handle_child_failure(&critical_failure).await;
        assert_eq!(result.directive, Directive::Escalate);
    }

    #[tokio::test]
    async fn test_reset_stats_allows_more_restarts() {
        let strategy = Arc::new(OneForOneStrategy::with_defaults(2, Duration::from_secs(60)));
        let supervisor_state = SupervisorState::new(strategy);

        supervisor_state.register_child("recoverable_child").await;

        let failure = ChildFailure {
            child_name: "recoverable_child".to_string(),
            error_message: "error".to_string(),
            timestamp: std::time::Instant::now(),
        };

        // Use up all retries
        supervisor_state.handle_child_failure(&failure).await;
        supervisor_state.handle_child_failure(&failure).await;

        // This would normally stop the child
        let result = supervisor_state.handle_child_failure(&failure).await;
        assert_eq!(result.directive, Directive::Stop);

        // Reset stats (simulating successful recovery)
        supervisor_state.reset_child_stats("recoverable_child").await;

        // Now it should restart again
        let result = supervisor_state.handle_child_failure(&failure).await;
        assert_eq!(result.directive, Directive::Restart);
    }

    // ============================================================
    // Actor System Integration Tests
    // ============================================================

    #[tokio::test]
    async fn test_spawn_with_props_enables_restart() {
        let system = ActorSystem::new("test-system");

        let start_count = Arc::new(AtomicU32::new(0));
        let sc = start_count.clone();

        let props = Props::from_factory(move || TestWorker::new("worker1", sc.clone()));

        // Spawn with props
        let _actor_ref = system.spawn_with_props(props, "worker1").await.unwrap();

        // Verify restart is possible
        assert!(system.can_restart("worker1").await);

        // Try restart
        let result = system.restart_actor("worker1").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_spawn_without_props_cannot_restart() {
        let system = ActorSystem::new("test-system");

        let start_count = Arc::new(AtomicU32::new(0));
        let worker = TestWorker::new("worker1", start_count);

        // Spawn without props
        let _actor_ref = system.spawn(worker, "worker1").await.unwrap();

        // Verify restart is NOT possible
        assert!(!system.can_restart("worker1").await);

        // Try restart - should fail
        let result = system.restart_actor("worker1").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_restart_preserves_actor_name() {
        let system = ActorSystem::new("test-system");

        let (tx, mut rx) = mpsc::channel::<String>(10);
        let start_count = Arc::new(AtomicU32::new(0));

        let tx_clone = tx.clone();
        let sc = start_count.clone();
        let props = Props::from_factory(move || {
            TestWorker::with_started_notification("my-worker", sc.clone(), tx_clone.clone())
        });

        // Spawn
        system.spawn_with_props(props, "my-worker").await.unwrap();

        // Wait for initial start
        let started = rx.recv().await.unwrap();
        assert_eq!(started, "my-worker");

        // Restart
        system.restart_actor("my-worker").await.unwrap();

        // Small delay for async processing
        sleep(Duration::from_millis(50)).await;

        // Should be able to get mailbox with same name
        let mailbox = system.get_mailbox("my-worker").await;
        assert!(mailbox.is_some());
    }

    #[tokio::test]
    async fn test_multiple_restarts_increment_count() {
        let system = ActorSystem::new("test-system");

        let start_count = Arc::new(AtomicU32::new(0));
        let sc = start_count.clone();

        let props = Props::from_factory(move || TestWorker::new("counter-worker", sc.clone()));

        system
            .spawn_with_props(props, "counter-worker")
            .await
            .unwrap();
        
        // Wait for initial spawn to complete
        sleep(Duration::from_millis(20)).await;

        // Initial start count should be 1 (from first spawn)
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Restart 3 times - each restart increments the count
        for i in 2..=4 {
            system.restart_actor("counter-worker").await.unwrap();
            sleep(Duration::from_millis(20)).await;
            assert_eq!(start_count.load(Ordering::SeqCst), i);
        }
    }

    // ============================================================
    // Supervision Decision Flow Tests
    // ============================================================

    #[tokio::test]
    async fn test_supervision_flow_one_for_one() {
        // This test simulates the full supervision flow

        let strategy = Arc::new(OneForOneStrategy::new(
            3,
            Duration::from_secs(60),
            |error| {
                let msg = error.to_string();
                if msg.contains("fatal") {
                    Directive::Stop
                } else if msg.contains("temporary") {
                    Directive::Resume
                } else {
                    Directive::Restart
                }
            },
        ));

        let supervisor_state = SupervisorState::new(strategy);

        // Simulate a supervisor with 3 workers
        supervisor_state.register_child("db-worker").await;
        supervisor_state.register_child("cache-worker").await;
        supervisor_state.register_child("api-worker").await;

        // Scenario 1: Temporary error on cache-worker -> Resume
        let failure = ChildFailure {
            child_name: "cache-worker".to_string(),
            error_message: "temporary connection timeout".to_string(),
            timestamp: std::time::Instant::now(),
        };
        let result = supervisor_state.handle_child_failure(&failure).await;
        assert_eq!(result.directive, Directive::Resume);
        assert!(result.children_to_restart.is_empty());

        // Scenario 2: Regular error on db-worker -> Restart only db-worker
        let failure = ChildFailure {
            child_name: "db-worker".to_string(),
            error_message: "query failed".to_string(),
            timestamp: std::time::Instant::now(),
        };
        let result = supervisor_state.handle_child_failure(&failure).await;
        assert_eq!(result.directive, Directive::Restart);
        assert_eq!(result.children_to_restart, vec!["db-worker"]);

        // Scenario 3: Fatal error on api-worker -> Stop
        let failure = ChildFailure {
            child_name: "api-worker".to_string(),
            error_message: "fatal: out of memory".to_string(),
            timestamp: std::time::Instant::now(),
        };
        let result = supervisor_state.handle_child_failure(&failure).await;
        assert_eq!(result.directive, Directive::Stop);
    }

    #[tokio::test]
    async fn test_supervision_flow_one_for_all_with_dependent_workers() {
        // Simulates workers that depend on each other

        let strategy = Arc::new(OneForAllStrategy::with_defaults(5, Duration::from_secs(60)));
        let supervisor_state = SupervisorState::new(strategy);

        supervisor_state.register_child("config-loader").await;
        supervisor_state.register_child("connection-pool").await;
        supervisor_state.register_child("request-handler").await;

        // If connection-pool fails, all need to restart because
        // request-handler depends on connection-pool which depends on config-loader
        let failure = ChildFailure {
            child_name: "connection-pool".to_string(),
            error_message: "pool exhausted".to_string(),
            timestamp: std::time::Instant::now(),
        };

        let result = supervisor_state.handle_child_failure(&failure).await;

        assert_eq!(result.directive, Directive::Restart);
        assert_eq!(result.children_to_restart.len(), 3);
        assert!(result.children_to_restart.contains(&"config-loader".to_string()));
        assert!(result.children_to_restart.contains(&"connection-pool".to_string()));
        assert!(result.children_to_restart.contains(&"request-handler".to_string()));
    }

    #[tokio::test]
    async fn test_supervision_flow_rest_for_one_pipeline() {
        // Simulates a data pipeline where later stages depend on earlier ones

        let strategy = Arc::new(RestForOneStrategy::with_defaults(5, Duration::from_secs(60)));
        let supervisor_state = SupervisorState::new(strategy);

        // Pipeline: ingester -> transformer -> validator -> writer
        supervisor_state.register_child("ingester").await;
        supervisor_state.register_child("transformer").await;
        supervisor_state.register_child("validator").await;
        supervisor_state.register_child("writer").await;

        // If transformer fails, transformer + validator + writer need restart
        // but ingester is fine
        let failure = ChildFailure {
            child_name: "transformer".to_string(),
            error_message: "transform error".to_string(),
            timestamp: std::time::Instant::now(),
        };

        let result = supervisor_state.handle_child_failure(&failure).await;

        assert_eq!(result.directive, Directive::Restart);
        assert_eq!(result.children_to_restart.len(), 3);
        assert!(!result.children_to_restart.contains(&"ingester".to_string()));
        assert!(result.children_to_restart.contains(&"transformer".to_string()));
        assert!(result.children_to_restart.contains(&"validator".to_string()));
        assert!(result.children_to_restart.contains(&"writer".to_string()));
    }

    #[tokio::test]
    async fn test_child_unregister_removes_from_supervision() {
        let strategy = Arc::new(RestForOneStrategy::with_defaults(5, Duration::from_secs(60)));
        let supervisor_state = SupervisorState::new(strategy);

        supervisor_state.register_child("child1").await;
        supervisor_state.register_child("child2").await;
        supervisor_state.register_child("child3").await;

        // Unregister child2
        supervisor_state.unregister_child("child2").await;

        // Now if child1 fails, only child1 and child3 should be in the restart list
        // (since child2 is no longer registered)
        let failure = ChildFailure {
            child_name: "child1".to_string(),
            error_message: "error".to_string(),
            timestamp: std::time::Instant::now(),
        };

        let result = supervisor_state.handle_child_failure(&failure).await;

        // With RestForOne, failing child1 should restart child1 and child3
        // (child2 was unregistered)
        assert_eq!(result.directive, Directive::Restart);
        assert!(result.children_to_restart.contains(&"child1".to_string()));
        assert!(result.children_to_restart.contains(&"child3".to_string()));
        assert!(!result.children_to_restart.contains(&"child2".to_string()));
    }
}
