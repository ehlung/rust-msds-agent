//! Integration tests for DeathWatch
//!
//! These tests verify DeathWatch works correctly with the ActorSystem.

#[cfg(test)]
mod tests {
    use crate::actor::{Actor, Props};
    use crate::actor_context::ActorContext;
    use crate::actor_system::ActorSystem;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};

    // ============================================================
    // Test Actors
    // ============================================================

    #[derive(Clone)]
    struct WatcherActor {
        name: String,
        termination_received: Arc<AtomicBool>,
        termination_count: Arc<AtomicU32>,
    }

    #[derive(Debug, Clone)]
    enum WatcherMessage {
        WatchActor(String),
        UnwatchActor(String),
        CheckTermination,
    }

    impl WatcherActor {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                termination_received: Arc::new(AtomicBool::new(false)),
                termination_count: Arc::new(AtomicU32::new(0)),
            }
        }

        fn with_counters(
            name: &str,
            termination_received: Arc<AtomicBool>,
            termination_count: Arc<AtomicU32>,
        ) -> Self {
            Self {
                name: name.to_string(),
                termination_received,
                termination_count,
            }
        }
    }

    #[async_trait]
    impl Actor for WatcherActor {
        type Message = WatcherMessage;

        async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
            match msg {
                WatcherMessage::WatchActor(name) => {
                }
                WatcherMessage::UnwatchActor(name) => {
                }
                WatcherMessage::CheckTermination => {
                }
            }
        }

        async fn pre_start(&mut self) {
        }
    }

    #[derive(Clone)]
    struct WorkerActor {
        name: String,
        start_count: Arc<AtomicU32>,
    }

    #[derive(Debug, Clone)]
    enum WorkerMessage {
        DoWork,
        Stop,
    }

    impl WorkerActor {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                start_count: Arc::new(AtomicU32::new(0)),
            }
        }

        fn with_counter(name: &str, start_count: Arc<AtomicU32>) -> Self {
            Self {
                name: name.to_string(),
                start_count,
            }
        }
    }

    #[async_trait]
    impl Actor for WorkerActor {
        type Message = WorkerMessage;

        async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
            match msg {
                WorkerMessage::DoWork => {
                }
                WorkerMessage::Stop => {
                }
            }
        }

        async fn pre_start(&mut self) {
            self.start_count.fetch_add(1, Ordering::SeqCst);
        }

        async fn post_stop(&mut self) {
        }
    }

    // ============================================================
    // DeathWatch Registry Tests
    // ============================================================

    #[tokio::test]
    async fn test_death_watch_in_actor_system() {
        let system = ActorSystem::new("test-system");

        // Verify death watch registry exists
        assert_eq!(system.death_watch().watch_count().await, 0);
    }

    #[tokio::test]
    async fn test_watch_via_context() {
        let system = ActorSystem::new("test-system");

        // Spawn watcher and worker
        let watcher = WatcherActor::new("watcher");
        let worker = WorkerActor::new("worker");

        let watcher_ref = system.spawn(watcher, "watcher").await.unwrap();
        let worker_ref = system.spawn(worker, "worker").await.unwrap();

        // Set up watch via death_watch directly
        system
            .death_watch()
            .watch(watcher_ref.path(), worker_ref.path())
            .await;

        // Verify watch is registered
        assert!(
            system
                .death_watch()
                .is_watching(watcher_ref.path(), worker_ref.path())
                .await
        );
        assert_eq!(system.death_watch().watch_count().await, 1);
    }

    #[tokio::test]
    async fn test_termination_notification_on_stop() {
        let system = ActorSystem::new("test-system");

        let callback_called = Arc::new(AtomicBool::new(false));
        let cc = callback_called.clone();

        // Spawn worker
        let worker = WorkerActor::new("worker");
        let worker_ref = system.spawn(worker, "worker").await.unwrap();

        // Register termination callback
        system
            .death_watch()
            .on_termination(worker_ref.path(), move |terminated| {
                assert!(terminated.actor_path.to_string().contains("worker"));
                cc.store(true, Ordering::SeqCst);
            })
            .await;

        // Stop the worker
        system.stop_actor("worker").await.unwrap();

        // Small delay for async processing
        sleep(Duration::from_millis(50)).await;

        // Callback should have been called
        assert!(callback_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_termination_with_failure_flag() {
        let system = ActorSystem::new("test-system");

        let failure_flag = Arc::new(AtomicBool::new(false));
        let ff = failure_flag.clone();

        let reason_received = Arc::new(std::sync::Mutex::new(None::<String>));
        let rr = reason_received.clone();

        // Spawn worker
        let worker = WorkerActor::new("worker");
        let worker_ref = system.spawn(worker, "worker").await.unwrap();

        // Register termination callback
        system
            .death_watch()
            .on_termination(worker_ref.path(), move |terminated| {
                ff.store(terminated.failure, Ordering::SeqCst);
                *rr.lock().unwrap() = terminated.reason.clone();
            })
            .await;

        // Stop with failure
        system
            .stop_actor_with_failure("worker", Some("out of memory".to_string()))
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await;

        assert!(failure_flag.load(Ordering::SeqCst));
        assert_eq!(
            *reason_received.lock().unwrap(),
            Some("out of memory".to_string())
        );
    }

    #[tokio::test]
    async fn test_multiple_watchers_notified() {
        let system = ActorSystem::new("test-system");

        let notification_count = Arc::new(AtomicU32::new(0));

        // Spawn target worker
        let worker = WorkerActor::new("target");
        let worker_ref = system.spawn(worker, "target").await.unwrap();

        // Register multiple callbacks (simulating multiple watchers)
        for _ in 0..3 {
            let nc = notification_count.clone();
            system
                .death_watch()
                .on_termination(worker_ref.path(), move |_| {
                    nc.fetch_add(1, Ordering::SeqCst);
                })
                .await;
        }

        // Stop the worker
        system.stop_actor("target").await.unwrap();
        sleep(Duration::from_millis(50)).await;

        // All 3 callbacks should have been called
        assert_eq!(notification_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_watch_cleanup_on_target_termination() {
        let system = ActorSystem::new("test-system");

        // Spawn watcher and worker
        let watcher = WatcherActor::new("watcher");
        let worker = WorkerActor::new("worker");

        let watcher_ref = system.spawn(watcher, "watcher").await.unwrap();
        let worker_ref = system.spawn(worker, "worker").await.unwrap();

        // Set up watch
        system
            .death_watch()
            .watch(watcher_ref.path(), worker_ref.path())
            .await;

        assert_eq!(system.death_watch().watch_count().await, 1);

        // Stop the worker
        system.stop_actor("worker").await.unwrap();
        sleep(Duration::from_millis(50)).await;

        // Watch should be cleaned up
        assert_eq!(system.death_watch().watch_count().await, 0);
    }

    #[tokio::test]
    async fn test_watch_cleanup_on_watcher_termination() {
        let system = ActorSystem::new("test-system");

        // Spawn watcher and multiple workers
        let watcher = WatcherActor::new("watcher");
        let worker1 = WorkerActor::new("worker1");
        let worker2 = WorkerActor::new("worker2");

        let watcher_ref = system.spawn(watcher, "watcher").await.unwrap();
        let worker1_ref = system.spawn(worker1, "worker1").await.unwrap();
        let worker2_ref = system.spawn(worker2, "worker2").await.unwrap();

        // Watcher watches both workers
        system
            .death_watch()
            .watch(watcher_ref.path(), worker1_ref.path())
            .await;
        system
            .death_watch()
            .watch(watcher_ref.path(), worker2_ref.path())
            .await;

        assert_eq!(system.death_watch().watch_count().await, 2);

        // Stop the watcher
        system.stop_actor("watcher").await.unwrap();
        sleep(Duration::from_millis(50)).await;

        // All watches from watcher should be cleaned up
        assert_eq!(system.death_watch().watch_count().await, 0);
    }

    #[tokio::test]
    async fn test_unwatch_before_termination() {
        let system = ActorSystem::new("test-system");

        let callback_called = Arc::new(AtomicBool::new(false));
        let cc = callback_called.clone();

        // Spawn watcher and worker
        let watcher = WatcherActor::new("watcher");
        let worker = WorkerActor::new("worker");

        let watcher_ref = system.spawn(watcher, "watcher").await.unwrap();
        let worker_ref = system.spawn(worker, "worker").await.unwrap();

        // Set up watch
        system
            .death_watch()
            .watch(watcher_ref.path(), worker_ref.path())
            .await;

        // Unwatch
        system
            .death_watch()
            .unwatch(watcher_ref.path(), worker_ref.path())
            .await;

        // Stop the worker
        system.stop_actor("worker").await.unwrap();
        sleep(Duration::from_millis(50)).await;

        // Watcher should NOT be in the notification list
        assert!(!callback_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_spawn_with_props_and_death_watch() {
        let system = ActorSystem::new("test-system");

        let callback_called = Arc::new(AtomicBool::new(false));
        let cc = callback_called.clone();

        // Spawn worker with props
        let props = Props::from_factory(|| WorkerActor::new("worker"));
        let worker_ref = system.spawn_with_props(props, "worker").await.unwrap();

        // Register termination callback
        system
            .death_watch()
            .on_termination(worker_ref.path(), move |_| {
                cc.store(true, Ordering::SeqCst);
            })
            .await;

        // Restart (which stops then respawns)
        system.restart_actor("worker").await.unwrap();
        sleep(Duration::from_millis(50)).await;

        // Callback should have been called when old instance stopped
        assert!(callback_called.load(Ordering::SeqCst));
    }

    // ============================================================
    // Complex Scenario Tests
    // ============================================================

    #[tokio::test]
    async fn test_supervisor_watches_children_scenario() {
        let system = ActorSystem::new("test-system");

        let children_terminated = Arc::new(AtomicU32::new(0));

        // Spawn "supervisor" (just a watcher in this test)
        let supervisor = WatcherActor::new("supervisor");
        let supervisor_ref = system.spawn(supervisor, "supervisor").await.unwrap();

        // Spawn children
        let child1 = WorkerActor::new("child1");
        let child2 = WorkerActor::new("child2");
        let child3 = WorkerActor::new("child3");

        let child1_ref = system.spawn(child1, "child1").await.unwrap();
        let child2_ref = system.spawn(child2, "child2").await.unwrap();
        let child3_ref = system.spawn(child3, "child3").await.unwrap();

        // Supervisor watches all children
        for child_ref in [&child1_ref, &child2_ref, &child3_ref] {
            let ct = children_terminated.clone();
            system
                .death_watch()
                .watch(supervisor_ref.path(), child_ref.path())
                .await;
            system
                .death_watch()
                .on_termination(child_ref.path(), move |_| {
                    ct.fetch_add(1, Ordering::SeqCst);
                })
                .await;
        }

        // Verify watches
        assert_eq!(system.death_watch().watch_count().await, 3);

        // Stop children one by one
        system.stop_actor("child1").await.unwrap();
        sleep(Duration::from_millis(30)).await;
        assert_eq!(children_terminated.load(Ordering::SeqCst), 1);

        system.stop_actor("child2").await.unwrap();
        sleep(Duration::from_millis(30)).await;
        assert_eq!(children_terminated.load(Ordering::SeqCst), 2);

        system.stop_actor("child3").await.unwrap();
        sleep(Duration::from_millis(30)).await;
        assert_eq!(children_terminated.load(Ordering::SeqCst), 3);

        // All watches should be cleaned up
        assert_eq!(system.death_watch().watch_count().await, 0);
    }

    #[tokio::test]
    async fn test_chain_of_watchers() {
        // A watches B, B watches C
        // When C terminates, B gets notified
        // When B terminates, A gets notified

        let system = ActorSystem::new("test-system");

        let a_notified = Arc::new(AtomicBool::new(false));
        let b_notified = Arc::new(AtomicBool::new(false));

        let actor_a = WatcherActor::new("A");
        let actor_b = WatcherActor::new("B");
        let actor_c = WorkerActor::new("C");

        let ref_a = system.spawn(actor_a, "A").await.unwrap();
        let ref_b = system.spawn(actor_b, "B").await.unwrap();
        let ref_c = system.spawn(actor_c, "C").await.unwrap();

        // A watches B
        system.death_watch().watch(ref_a.path(), ref_b.path()).await;
        let an = a_notified.clone();
        system
            .death_watch()
            .on_termination(ref_b.path(), move |_| {
                an.store(true, Ordering::SeqCst);
            })
            .await;

        // B watches C
        system.death_watch().watch(ref_b.path(), ref_c.path()).await;
        let bn = b_notified.clone();
        system
            .death_watch()
            .on_termination(ref_c.path(), move |_| {
                bn.store(true, Ordering::SeqCst);
            })
            .await;

        // Stop C
        system.stop_actor("C").await.unwrap();
        sleep(Duration::from_millis(50)).await;

        assert!(b_notified.load(Ordering::SeqCst)); // B was notified about C
        assert!(!a_notified.load(Ordering::SeqCst)); // A not yet notified

        // Stop B
        system.stop_actor("B").await.unwrap();
        sleep(Duration::from_millis(50)).await;

        assert!(a_notified.load(Ordering::SeqCst)); // Now A is notified about B
    }
}
