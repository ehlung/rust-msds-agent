//! Integration tests for persistence functionality
//! Tests the complete flow of persistence operations

#[cfg(test)]
mod integration_tests {
    use crate::actor::{PersistentRepr, SnapshotMetadata};
    use crate::journal::{InMemoryJournal, Journal};
    use crate::snapshot::{InMemorySnapshotStore, SnapshotStore};
    use crate::serialization::SerializerRegistry;
    use crate::recovery::{RecoveryManager, RecoveryMode, RecoveryConfig};
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use uuid::Uuid;

    /// Test that verifies the complete persistence flow with correct persistence_id
    #[tokio::test]
    async fn test_complete_persistence_flow_with_persistence_id() {
        let journal = Arc::new(InMemoryJournal::new());
        let snapshot_store = Arc::new(InMemorySnapshotStore::new());

        // Create two different actors with different persistence IDs
        let actor1_id = "bank-account-001";
        let actor2_id = "bank-account-002";

        // Actor 1: Write multiple events
        let events1 = vec![
            PersistentRepr::new(
                actor1_id.to_string(),
                1,
                "AccountOpened".to_string(),
                b"{\"balance\":1000}".to_vec(),
            ),
            PersistentRepr::new(
                actor1_id.to_string(),
                2,
                "MoneyDeposited".to_string(),
                b"{\"amount\":500}".to_vec(),
            ),
            PersistentRepr::new(
                actor1_id.to_string(),
                3,
                "MoneyWithdrawn".to_string(),
                b"{\"amount\":200}".to_vec(),
            ),
        ];

        // Actor 2: Write different events
        let events2 = vec![
            PersistentRepr::new(
                actor2_id.to_string(),
                1,
                "AccountOpened".to_string(),
                b"{\"balance\":2000}".to_vec(),
            ),
            PersistentRepr::new(
                actor2_id.to_string(),
                2,
                "MoneyDeposited".to_string(),
                b"{\"amount\":1000}".to_vec(),
            ),
        ];

        // Write events for both actors
        journal.write_messages(events1).await.unwrap();
        journal.write_messages(events2).await.unwrap();

        // Verify actor1's events are stored with correct persistence_id
        let replayed1 = journal
            .replay_messages(actor1_id, 1, u64::MAX, None)
            .await
            .unwrap();
        assert_eq!(replayed1.len(), 3);
        assert!(replayed1.iter().all(|e| e.persistence_id == actor1_id));
        assert_eq!(replayed1[0].sequence_nr, 1);
        assert_eq!(replayed1[1].sequence_nr, 2);
        assert_eq!(replayed1[2].sequence_nr, 3);

        // Verify actor2's events are stored with correct persistence_id
        let replayed2 = journal
            .replay_messages(actor2_id, 1, u64::MAX, None)
            .await
            .unwrap();
        assert_eq!(replayed2.len(), 2);
        assert!(replayed2.iter().all(|e| e.persistence_id == actor2_id));
        assert_eq!(replayed2[0].sequence_nr, 1);
        assert_eq!(replayed2[1].sequence_nr, 2);

        // Verify highest sequence numbers
        assert_eq!(journal.highest_sequence_nr(actor1_id).await.unwrap(), 3);
        assert_eq!(journal.highest_sequence_nr(actor2_id).await.unwrap(), 2);
    }

    /// Test sequence number validation across multiple persistence IDs
    #[tokio::test]
    async fn test_sequence_validation_multiple_actors() {
        let journal = InMemoryJournal::new();
        let actor1_id = "actor-1";
        let actor2_id = "actor-2";

        // Actor 1: Valid sequence
        let msg1 = PersistentRepr {
            persistence_id: actor1_id.to_string(),
            sequence_nr: 1,
            manifest: "Event".to_string(),
            payload: b"data1".to_vec(),
            timestamp: Utc::now(),
            writer_uuid: Uuid::new_v4(),
            tags: vec![],
        };
        assert!(journal.write_messages(vec![msg1]).await.is_ok());

        // Actor 2: Valid sequence (independent from actor 1)
        let msg2 = PersistentRepr {
            persistence_id: actor2_id.to_string(),
            sequence_nr: 1,
            manifest: "Event".to_string(),
            payload: b"data2".to_vec(),
            timestamp: Utc::now(),
            writer_uuid: Uuid::new_v4(),
            tags: vec![],
        };
        assert!(journal.write_messages(vec![msg2]).await.is_ok());

        // Actor 1: Continue with sequence 2
        let msg3 = PersistentRepr {
            persistence_id: actor1_id.to_string(),
            sequence_nr: 2,
            manifest: "Event".to_string(),
            payload: b"data3".to_vec(),
            timestamp: Utc::now(),
            writer_uuid: Uuid::new_v4(),
            tags: vec![],
        };
        assert!(journal.write_messages(vec![msg3]).await.is_ok());

        // Actor 1: Invalid sequence (should fail)
        let msg4 = PersistentRepr {
            persistence_id: actor1_id.to_string(),
            sequence_nr: 5, // Should be 3, not 5
            manifest: "Event".to_string(),
            payload: b"data4".to_vec(),
            timestamp: Utc::now(),
            writer_uuid: Uuid::new_v4(),
            tags: vec![],
        };
        let result = journal.write_messages(vec![msg4]).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::JournalError::SequenceNumberOutOfRange(5)
        ));

        // Actor 2: Valid sequence (independent, should still work)
        let msg5 = PersistentRepr {
            persistence_id: actor2_id.to_string(),
            sequence_nr: 2,
            manifest: "Event".to_string(),
            payload: b"data5".to_vec(),
            timestamp: Utc::now(),
            writer_uuid: Uuid::new_v4(),
            tags: vec![],
        };
        assert!(journal.write_messages(vec![msg5]).await.is_ok());
    }

    /// Test timestamp preservation through complete persistence cycle
    #[tokio::test]
    async fn test_timestamp_preservation_complete_cycle() {
        let journal = Arc::new(InMemoryJournal::new());
        let snapshot_store = Arc::new(InMemorySnapshotStore::new());
        let persistence_id = "timestamp-test-actor";

        // Create messages with specific timestamps
        let timestamp1 = DateTime::parse_from_rfc3339("2023-12-07T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let timestamp2 = DateTime::parse_from_rfc3339("2023-12-07T11:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let timestamp3 = DateTime::parse_from_rfc3339("2023-12-07T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let messages = vec![
            PersistentRepr {
                persistence_id: persistence_id.to_string(),
                sequence_nr: 1,
                manifest: "Event1".to_string(),
                payload: b"data1".to_vec(),
                timestamp: timestamp1,
                writer_uuid: Uuid::new_v4(),
                tags: vec![],
            },
            PersistentRepr {
                persistence_id: persistence_id.to_string(),
                sequence_nr: 2,
                manifest: "Event2".to_string(),
                payload: b"data2".to_vec(),
                timestamp: timestamp2,
                writer_uuid: Uuid::new_v4(),
                tags: vec![],
            },
            PersistentRepr {
                persistence_id: persistence_id.to_string(),
                sequence_nr: 3,
                manifest: "Event3".to_string(),
                payload: b"data3".to_vec(),
                timestamp: timestamp3,
                writer_uuid: Uuid::new_v4(),
                tags: vec![],
            },
        ];

        // Write messages
        journal.write_messages(messages).await.unwrap();

        // Replay and verify timestamps are preserved
        let replayed = journal
            .replay_messages(persistence_id, 1, u64::MAX, None)
            .await
            .unwrap();

        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0].timestamp, timestamp1);
        assert_eq!(replayed[1].timestamp, timestamp2);
        assert_eq!(replayed[2].timestamp, timestamp3);

        // Test snapshot with timestamp
        let snapshot_metadata = SnapshotMetadata {
            persistence_id: persistence_id.to_string(),
            sequence_nr: 3,
            timestamp: timestamp3,
        };
        let snapshot_data = b"snapshot-data".to_vec();

        snapshot_store
            .save_snapshot(&snapshot_metadata, snapshot_data.clone())
            .await
            .unwrap();

        // Load snapshot and verify timestamp
        let loaded = snapshot_store
            .load_snapshot(persistence_id, None, None)
            .await
            .unwrap();

        assert!(loaded.is_some());
        let (loaded_metadata, loaded_data) = loaded.unwrap();
        assert_eq!(loaded_metadata.timestamp, timestamp3);
        assert_eq!(loaded_data, snapshot_data);
    }

    /// Test that persistence_id is correctly used in snapshot operations
    #[tokio::test]
    async fn test_snapshot_with_correct_persistence_id() {
        let snapshot_store = InMemorySnapshotStore::new();
        let actor1_id = "actor-1";
        let actor2_id = "actor-2";

        // Create snapshots for different actors
        let snapshot1 = SnapshotMetadata {
            persistence_id: actor1_id.to_string(),
            sequence_nr: 100,
            timestamp: Utc::now(),
        };
        let snapshot2 = SnapshotMetadata {
            persistence_id: actor2_id.to_string(),
            sequence_nr: 50,
            timestamp: Utc::now(),
        };

        snapshot_store
            .save_snapshot(&snapshot1, b"snapshot1-data".to_vec())
            .await
            .unwrap();
        snapshot_store
            .save_snapshot(&snapshot2, b"snapshot2-data".to_vec())
            .await
            .unwrap();

        // Load snapshots and verify correct persistence_id
        let loaded1 = snapshot_store
            .load_snapshot(actor1_id, None, None)
            .await
            .unwrap();
        assert!(loaded1.is_some());
        let (metadata1, _) = loaded1.unwrap();
        assert_eq!(metadata1.persistence_id, actor1_id);
        assert_eq!(metadata1.sequence_nr, 100);

        let loaded2 = snapshot_store
            .load_snapshot(actor2_id, None, None)
            .await
            .unwrap();
        assert!(loaded2.is_some());
        let (metadata2, _) = loaded2.unwrap();
        assert_eq!(metadata2.persistence_id, actor2_id);
        assert_eq!(metadata2.sequence_nr, 50);
    }

    // ============================================================
    // End-to-End Recovery Tests
    // ============================================================

    /// Bank account events for E2E tests
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    enum BankEvent {
        AccountOpened { initial_balance: u64 },
        MoneyDeposited { amount: u64 },
        MoneyWithdrawn { amount: u64 },
    }

    /// Bank account state for E2E tests
    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
    struct BankState {
        balance: u64,
        is_open: bool,
        transaction_count: u64,
    }

    impl BankState {
        fn apply_event(&mut self, event: &BankEvent) {
            match event {
                BankEvent::AccountOpened { initial_balance } => {
                    self.balance = *initial_balance;
                    self.is_open = true;
                    self.transaction_count += 1;
                }
                BankEvent::MoneyDeposited { amount } => {
                    self.balance += amount;
                    self.transaction_count += 1;
                }
                BankEvent::MoneyWithdrawn { amount } => {
                    self.balance = self.balance.saturating_sub(*amount);
                    self.transaction_count += 1;
                }
            }
        }
    }

    /// E2E Test: Recovery from events only (no snapshot)
    #[tokio::test]
    async fn test_e2e_recovery_from_events_only() {
        let journal = Arc::new(InMemoryJournal::new());
        let persistence_id = "bank-account-e2e-001";

        // Step 1: Simulate actor writing events to journal
        let events = vec![
            BankEvent::AccountOpened { initial_balance: 1000 },
            BankEvent::MoneyDeposited { amount: 500 },
            BankEvent::MoneyWithdrawn { amount: 200 },
            BankEvent::MoneyDeposited { amount: 100 },
        ];

        // Write events to journal (simulating persist() calls)
        for (i, event) in events.iter().enumerate() {
            let payload = serde_json::to_vec(event).unwrap();
            let repr = PersistentRepr::new(
                persistence_id.to_string(),
                (i + 1) as u64,
                "BankEvent".to_string(),
                payload,
            );
            journal.write_messages(vec![repr]).await.unwrap();
        }

        // Step 2: Simulate actor restart - create new state
        let mut recovered_state = BankState::default();

        // Step 3: Replay events from journal
        let replayed_events = journal
            .replay_messages(persistence_id, 1, u64::MAX, None)
            .await
            .unwrap();

        assert_eq!(replayed_events.len(), 4, "Should have 4 events in journal");

        // Apply each event to recover state
        for repr in &replayed_events {
            let event: BankEvent = serde_json::from_slice(&repr.payload).unwrap();
            recovered_state.apply_event(&event);
        }

        // Step 4: Verify recovered state
        assert!(recovered_state.is_open, "Account should be open");
        assert_eq!(recovered_state.balance, 1400, "Balance should be 1000 + 500 - 200 + 100 = 1400");
        assert_eq!(recovered_state.transaction_count, 4, "Should have 4 transactions");

        // Verify sequence number
        let highest_seq = journal.highest_sequence_nr(persistence_id).await.unwrap();
        assert_eq!(highest_seq, 4, "Highest sequence should be 4");
    }

    /// E2E Test: Recovery from snapshot + events
    #[tokio::test]
    async fn test_e2e_recovery_from_snapshot_and_events() {
        let journal = Arc::new(InMemoryJournal::new());
        let snapshot_store = Arc::new(InMemorySnapshotStore::new());
        let persistence_id = "bank-account-e2e-002";

        // Step 1: Simulate actor history
        // Events 1-5 happened, then snapshot was taken
        let pre_snapshot_events = vec![
            BankEvent::AccountOpened { initial_balance: 1000 },
            BankEvent::MoneyDeposited { amount: 500 },
            BankEvent::MoneyWithdrawn { amount: 200 },
            BankEvent::MoneyDeposited { amount: 300 },
            BankEvent::MoneyWithdrawn { amount: 100 },
        ];

        // Write pre-snapshot events
        for (i, event) in pre_snapshot_events.iter().enumerate() {
            let payload = serde_json::to_vec(event).unwrap();
            let repr = PersistentRepr::new(
                persistence_id.to_string(),
                (i + 1) as u64,
                "BankEvent".to_string(),
                payload,
            );
            journal.write_messages(vec![repr]).await.unwrap();
        }

        // Calculate state at snapshot time (seq 5)
        let mut snapshot_state = BankState::default();
        for event in &pre_snapshot_events {
            snapshot_state.apply_event(event);
        }
        assert_eq!(snapshot_state.balance, 1500); // 1000 + 500 - 200 + 300 - 100

        // Step 2: Save snapshot at sequence 5
        let snapshot_data = serde_json::to_vec(&snapshot_state).unwrap();
        let snapshot_metadata = SnapshotMetadata {
            persistence_id: persistence_id.to_string(),
            sequence_nr: 5,
            timestamp: Utc::now(),
        };
        snapshot_store.save_snapshot(&snapshot_metadata, snapshot_data).await.unwrap();

        // Step 3: More events after snapshot (seq 6, 7, 8)
        let post_snapshot_events = vec![
            BankEvent::MoneyDeposited { amount: 1000 },
            BankEvent::MoneyWithdrawn { amount: 250 },
            BankEvent::MoneyDeposited { amount: 50 },
        ];

        for (i, event) in post_snapshot_events.iter().enumerate() {
            let payload = serde_json::to_vec(event).unwrap();
            let repr = PersistentRepr::new(
                persistence_id.to_string(),
                (i + 6) as u64, // Starting from seq 6
                "BankEvent".to_string(),
                payload,
            );
            journal.write_messages(vec![repr]).await.unwrap();
        }

        // Step 4: Simulate actor restart - RECOVERY
        let mut recovered_state = BankState::default();

        // Load snapshot
        let loaded_snapshot = snapshot_store
            .load_snapshot(persistence_id, None, None)
            .await
            .unwrap();
        
        assert!(loaded_snapshot.is_some(), "Snapshot should exist");
        let (snap_meta, snap_data) = loaded_snapshot.unwrap();
        
        // Apply snapshot
        recovered_state = serde_json::from_slice(&snap_data).unwrap();
        assert_eq!(recovered_state.balance, 1500, "State after snapshot should be 1500");
        assert_eq!(snap_meta.sequence_nr, 5, "Snapshot should be at seq 5");

        // Replay events AFTER snapshot (from seq 6)
        let events_after_snapshot = journal
            .replay_messages(persistence_id, snap_meta.sequence_nr + 1, u64::MAX, None)
            .await
            .unwrap();

        assert_eq!(events_after_snapshot.len(), 3, "Should have 3 events after snapshot");

        for repr in &events_after_snapshot {
            let event: BankEvent = serde_json::from_slice(&repr.payload).unwrap();
            recovered_state.apply_event(&event);
        }

        // Step 5: Verify final recovered state
        // 1500 (snapshot) + 1000 - 250 + 50 = 2300
        assert_eq!(recovered_state.balance, 2300, "Final balance should be 2300");
        assert_eq!(recovered_state.transaction_count, 8, "Should have 8 total transactions");
        assert!(recovered_state.is_open, "Account should be open");

        // Verify sequence number
        let highest_seq = journal.highest_sequence_nr(persistence_id).await.unwrap();
        assert_eq!(highest_seq, 8, "Highest sequence should be 8");
    }

    /// E2E Test: Recovery with multiple snapshots (should use latest)
    #[tokio::test]
    async fn test_e2e_recovery_uses_latest_snapshot() {
        let journal = Arc::new(InMemoryJournal::new());
        let snapshot_store = Arc::new(InMemorySnapshotStore::new());
        let persistence_id = "bank-account-e2e-003";

        // Write events 1-10
        for i in 1..=10 {
            let event = BankEvent::MoneyDeposited { amount: 100 };
            let payload = serde_json::to_vec(&event).unwrap();
            let repr = PersistentRepr::new(
                persistence_id.to_string(),
                i,
                "BankEvent".to_string(),
                payload,
            );
            journal.write_messages(vec![repr]).await.unwrap();
        }

        // Save snapshot at seq 3 (balance = 300)
        let state_at_3 = BankState {
            balance: 300,
            is_open: true, // Assume account was opened before
            transaction_count: 3,
        };
        snapshot_store.save_snapshot(
            &SnapshotMetadata {
                persistence_id: persistence_id.to_string(),
                sequence_nr: 3,
                timestamp: Utc::now(),
            },
            serde_json::to_vec(&state_at_3).unwrap(),
        ).await.unwrap();

        // Save snapshot at seq 7 (balance = 700)
        let state_at_7 = BankState {
            balance: 700,
            is_open: true,
            transaction_count: 7,
        };
        snapshot_store.save_snapshot(
            &SnapshotMetadata {
                persistence_id: persistence_id.to_string(),
                sequence_nr: 7,
                timestamp: Utc::now(),
            },
            serde_json::to_vec(&state_at_7).unwrap(),
        ).await.unwrap();

        // Recovery should use snapshot at seq 7 (latest)
        let loaded = snapshot_store
            .load_snapshot(persistence_id, None, None)
            .await
            .unwrap()
            .unwrap();

        let (snap_meta, snap_data) = loaded;
        assert_eq!(snap_meta.sequence_nr, 7, "Should load latest snapshot at seq 7");

        let mut recovered_state: BankState = serde_json::from_slice(&snap_data).unwrap();
        assert_eq!(recovered_state.balance, 700);

        // Replay events 8, 9, 10
        let events_after = journal
            .replay_messages(persistence_id, 8, u64::MAX, None)
            .await
            .unwrap();
        assert_eq!(events_after.len(), 3);

        for repr in &events_after {
            let event: BankEvent = serde_json::from_slice(&repr.payload).unwrap();
            recovered_state.apply_event(&event);
        }

        // Final state: 700 + 100 + 100 + 100 = 1000
        assert_eq!(recovered_state.balance, 1000);
        assert_eq!(recovered_state.transaction_count, 10);
    }

    /// E2E Test: Recovery with corrupted/missing snapshot falls back to events
    #[tokio::test]
    async fn test_e2e_recovery_no_snapshot_full_replay() {
        let journal = Arc::new(InMemoryJournal::new());
        let snapshot_store = Arc::new(InMemorySnapshotStore::new());
        let persistence_id = "bank-account-e2e-004";

        // Write 5 events
        let events = vec![
            BankEvent::AccountOpened { initial_balance: 500 },
            BankEvent::MoneyDeposited { amount: 200 },
            BankEvent::MoneyWithdrawn { amount: 50 },
            BankEvent::MoneyDeposited { amount: 150 },
            BankEvent::MoneyWithdrawn { amount: 100 },
        ];

        for (i, event) in events.iter().enumerate() {
            let payload = serde_json::to_vec(event).unwrap();
            let repr = PersistentRepr::new(
                persistence_id.to_string(),
                (i + 1) as u64,
                "BankEvent".to_string(),
                payload,
            );
            journal.write_messages(vec![repr]).await.unwrap();
        }

        // No snapshot saved!

        // Recovery: no snapshot found
        let loaded = snapshot_store
            .load_snapshot(persistence_id, None, None)
            .await
            .unwrap();
        assert!(loaded.is_none(), "No snapshot should exist");

        // Full replay from seq 1
        let mut recovered_state = BankState::default();
        let all_events = journal
            .replay_messages(persistence_id, 1, u64::MAX, None)
            .await
            .unwrap();

        for repr in &all_events {
            let event: BankEvent = serde_json::from_slice(&repr.payload).unwrap();
            recovered_state.apply_event(&event);
        }

        // Expected: 500 + 200 - 50 + 150 - 100 = 700
        assert_eq!(recovered_state.balance, 700);
        assert_eq!(recovered_state.transaction_count, 5);
    }

    /// E2E Test: Multiple actors with independent state
    #[tokio::test]
    async fn test_e2e_multiple_actors_independent_recovery() {
        let journal = Arc::new(InMemoryJournal::new());
        let snapshot_store = Arc::new(InMemorySnapshotStore::new());

        let alice_id = "bank-account-alice";
        let bob_id = "bank-account-bob";

        // Alice: opens with 1000, deposits 500
        let alice_events = vec![
            BankEvent::AccountOpened { initial_balance: 1000 },
            BankEvent::MoneyDeposited { amount: 500 },
        ];
        for (i, event) in alice_events.iter().enumerate() {
            let payload = serde_json::to_vec(event).unwrap();
            journal.write_messages(vec![PersistentRepr::new(
                alice_id.to_string(),
                (i + 1) as u64,
                "BankEvent".to_string(),
                payload,
            )]).await.unwrap();
        }

        // Bob: opens with 2000, withdraws 300, deposits 100
        let bob_events = vec![
            BankEvent::AccountOpened { initial_balance: 2000 },
            BankEvent::MoneyWithdrawn { amount: 300 },
            BankEvent::MoneyDeposited { amount: 100 },
        ];
        for (i, event) in bob_events.iter().enumerate() {
            let payload = serde_json::to_vec(event).unwrap();
            journal.write_messages(vec![PersistentRepr::new(
                bob_id.to_string(),
                (i + 1) as u64,
                "BankEvent".to_string(),
                payload,
            )]).await.unwrap();
        }

        // Recover Alice
        let mut alice_state = BankState::default();
        let alice_replayed = journal.replay_messages(alice_id, 1, u64::MAX, None).await.unwrap();
        for repr in &alice_replayed {
            let event: BankEvent = serde_json::from_slice(&repr.payload).unwrap();
            alice_state.apply_event(&event);
        }

        // Recover Bob
        let mut bob_state = BankState::default();
        let bob_replayed = journal.replay_messages(bob_id, 1, u64::MAX, None).await.unwrap();
        for repr in &bob_replayed {
            let event: BankEvent = serde_json::from_slice(&repr.payload).unwrap();
            bob_state.apply_event(&event);
        }

        // Verify independent states
        assert_eq!(alice_state.balance, 1500, "Alice: 1000 + 500 = 1500");
        assert_eq!(alice_state.transaction_count, 2);

        assert_eq!(bob_state.balance, 1800, "Bob: 2000 - 300 + 100 = 1800");
        assert_eq!(bob_state.transaction_count, 3);

        // Verify they don't interfere
        assert_eq!(journal.highest_sequence_nr(alice_id).await.unwrap(), 2);
        assert_eq!(journal.highest_sequence_nr(bob_id).await.unwrap(), 3);
    }

    // ============================================================
    // Snapshot Policy Tests
    // ============================================================

    use crate::actor::SnapshotPolicy;

    #[test]
    fn test_snapshot_policy_should_snapshot() {
        // Policy: snapshot every 5 events
        let mut policy = SnapshotPolicy::every(5);
        
        // Initial state: no snapshot taken yet
        assert!(!policy.should_snapshot(1));
        assert!(!policy.should_snapshot(4));
        assert!(policy.should_snapshot(5)); // Should snapshot at 5
        
        // After snapshot at seq 5
        policy.last_snapshot_seq = 5;
        assert!(!policy.should_snapshot(6));
        assert!(!policy.should_snapshot(9));
        assert!(policy.should_snapshot(10)); // Should snapshot at 10

        // After snapshot at seq 10
        policy.last_snapshot_seq = 10;
        assert!(!policy.should_snapshot(11));
        assert!(policy.should_snapshot(15));
    }

    #[test]
    fn test_snapshot_policy_disabled() {
        // Policy: disabled (every 0)
        let policy = SnapshotPolicy::default();
        assert_eq!(policy.snapshot_every, 0);
        
        assert!(!policy.should_snapshot(1));
        assert!(!policy.should_snapshot(100));
        assert!(!policy.should_snapshot(1000));
    }

    #[test]
    fn test_snapshot_policy_builder() {
        let policy = SnapshotPolicy::every(10)
            .with_event_deletion()
            .keep_snapshots(5);
        
        assert_eq!(policy.snapshot_every, 10);
        assert!(policy.delete_events_on_snapshot);
        assert_eq!(policy.keep_snapshots, 5);
    }

    #[tokio::test]
    async fn test_e2e_automatic_snapshot_policy() {
        let journal = Arc::new(InMemoryJournal::new());
        let snapshot_store = Arc::new(InMemorySnapshotStore::new());
        let persistence_id = "auto-snapshot-test";

        // Simulate actor with snapshot policy: every 5 events
        let mut policy = SnapshotPolicy::every(5);
        let mut state = BankState::default();

        // Event 1: Open account
        let event1 = BankEvent::AccountOpened { initial_balance: 1000 };
        let payload = serde_json::to_vec(&event1).unwrap();
        journal.write_messages(vec![PersistentRepr::new(
            persistence_id.to_string(), 1, "BankEvent".to_string(), payload,
        )]).await.unwrap();
        state.apply_event(&event1);
        assert!(!policy.should_snapshot(1), "No snapshot at seq 1");

        // Events 2-4: Deposits
        for i in 2u64..=4 {
            let event = BankEvent::MoneyDeposited { amount: 100 };
            let payload = serde_json::to_vec(&event).unwrap();
            journal.write_messages(vec![PersistentRepr::new(
                persistence_id.to_string(), i, "BankEvent".to_string(), payload,
            )]).await.unwrap();
            state.apply_event(&event);
            assert!(!policy.should_snapshot(i), "No snapshot at seq {}", i);
        }

        // Event 5: Should trigger snapshot
        let event5 = BankEvent::MoneyDeposited { amount: 100 };
        let payload = serde_json::to_vec(&event5).unwrap();
        journal.write_messages(vec![PersistentRepr::new(
            persistence_id.to_string(), 5, "BankEvent".to_string(), payload,
        )]).await.unwrap();
        state.apply_event(&event5);
        assert!(policy.should_snapshot(5), "Should snapshot at seq 5");

        // Take snapshot
        let snapshot_data = serde_json::to_vec(&state).unwrap();
        snapshot_store.save_snapshot(
            &SnapshotMetadata {
                persistence_id: persistence_id.to_string(),
                sequence_nr: 5,
                timestamp: Utc::now(),
            },
            snapshot_data,
        ).await.unwrap();
        policy.last_snapshot_seq = 5;

        // Verify state at snapshot: 1000 + 4*100 = 1400
        assert_eq!(state.balance, 1400);

        // Events 6-9: More operations
        for i in 6u64..=9 {
            let event = BankEvent::MoneyWithdrawn { amount: 50 };
            let payload = serde_json::to_vec(&event).unwrap();
            journal.write_messages(vec![PersistentRepr::new(
                persistence_id.to_string(), i, "BankEvent".to_string(), payload,
            )]).await.unwrap();
            state.apply_event(&event);
            assert!(!policy.should_snapshot(i), "No snapshot at seq {}", i);
        }

        // Event 10: Should trigger another snapshot
        let event10 = BankEvent::MoneyDeposited { amount: 200 };
        let payload = serde_json::to_vec(&event10).unwrap();
        journal.write_messages(vec![PersistentRepr::new(
            persistence_id.to_string(), 10, "BankEvent".to_string(), payload,
        )]).await.unwrap();
        state.apply_event(&event10);
        assert!(policy.should_snapshot(10), "Should snapshot at seq 10");

        // State at seq 10: 1400 - 4*50 + 200 = 1400
        assert_eq!(state.balance, 1400);

        // Verify snapshots in store
        let snapshots = snapshot_store.list_snapshots(persistence_id).await.unwrap();
        assert_eq!(snapshots.len(), 1); // Only one snapshot saved so far

        // Save second snapshot
        let snapshot_data = serde_json::to_vec(&state).unwrap();
        snapshot_store.save_snapshot(
            &SnapshotMetadata {
                persistence_id: persistence_id.to_string(),
                sequence_nr: 10,
                timestamp: Utc::now(),
            },
            snapshot_data,
        ).await.unwrap();

        // Now should have 2 snapshots
        let snapshots = snapshot_store.list_snapshots(persistence_id).await.unwrap();
        assert_eq!(snapshots.len(), 2);
    }

    #[tokio::test]
    async fn test_e2e_event_deletion_after_snapshot() {
        let journal = Arc::new(InMemoryJournal::new());
        let persistence_id = "event-deletion-test";

        // Write 10 events
        for i in 1..=10 {
            let event = BankEvent::MoneyDeposited { amount: 100 };
            let payload = serde_json::to_vec(&event).unwrap();
            journal.write_messages(vec![PersistentRepr::new(
                persistence_id.to_string(),
                i,
                "BankEvent".to_string(),
                payload,
            )]).await.unwrap();
        }

        // Verify all 10 events exist
        let events = journal.replay_messages(persistence_id, 1, u64::MAX, None).await.unwrap();
        assert_eq!(events.len(), 10);

        // Simulate snapshot at seq 7 and delete events before it
        let snapshot_seq = 7;
        journal.delete_messages_to(persistence_id, snapshot_seq - 1).await.unwrap();

        // Now only events 7-10 should remain
        let events_after_delete = journal.replay_messages(persistence_id, 1, u64::MAX, None).await.unwrap();
        assert_eq!(events_after_delete.len(), 4, "Should have events 7, 8, 9, 10");
        assert_eq!(events_after_delete[0].sequence_nr, 7);
        assert_eq!(events_after_delete[3].sequence_nr, 10);

        // Verify we can still get highest sequence number
        let highest = journal.highest_sequence_nr(persistence_id).await.unwrap();
        assert_eq!(highest, 10);
    }

    #[tokio::test]
    async fn test_e2e_snapshot_retention() {
        let snapshot_store = Arc::new(InMemorySnapshotStore::new());
        let persistence_id = "retention-test";

        // Save 5 snapshots at sequences 10, 20, 30, 40, 50
        for i in 1..=5 {
            let seq = i * 10;
            let state = BankState {
                balance: seq * 100,
                is_open: true,
                transaction_count: seq,
            };
            let snapshot_data = serde_json::to_vec(&state).unwrap();
            snapshot_store.save_snapshot(
                &SnapshotMetadata {
                    persistence_id: persistence_id.to_string(),
                    sequence_nr: seq,
                    timestamp: Utc::now(),
                },
                snapshot_data,
            ).await.unwrap();
        }

        // Should have 5 snapshots
        let snapshots = snapshot_store.list_snapshots(persistence_id).await.unwrap();
        assert_eq!(snapshots.len(), 5);

        // Delete oldest snapshots to keep only 3
        let keep = 3;
        if snapshots.len() as u64 > keep {
            let to_delete = snapshots.len() as u64 - keep;
            // Snapshots are sorted newest first, so delete from the end
            for snap in snapshots.iter().rev().take(to_delete as usize) {
                snapshot_store.delete_snapshot(snap).await.unwrap();
            }
        }

        // Should now have 3 snapshots
        let remaining = snapshot_store.list_snapshots(persistence_id).await.unwrap();
        assert_eq!(remaining.len(), 3);

        // Remaining should be the newest ones: 30, 40, 50
        let seqs: Vec<u64> = remaining.iter().map(|s| s.sequence_nr).collect();
        assert!(seqs.contains(&30));
        assert!(seqs.contains(&40));
        assert!(seqs.contains(&50));
    }
}
