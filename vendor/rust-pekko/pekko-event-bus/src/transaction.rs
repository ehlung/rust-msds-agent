// transaction.rs - Multi-Partition Atomic Writes
// Kafka-like transaction support: begin → produce → commit/abort
// Two-Phase Commit with WAL (Write-Ahead Log) for crash recovery

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::bus_errors::EventBusError;

// ============================================================================
// Transaction ID
// ============================================================================

static TX_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId(u64);

impl TransactionId {
    pub fn new() -> Self {
        Self(TX_ID_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tx-{}", self.0)
    }
}

// ============================================================================
// Transaction State Machine
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    /// Transaction created, not yet started
    Empty,
    /// Transaction started, accepting writes
    Ongoing,
    /// Prepare phase: all partitions acknowledged
    PrepareCommit,
    /// Successfully committed
    Committed,
    /// Aborted (explicitly or due to timeout)
    Aborted,
    /// Fatal error — cannot recover
    FatalError(String),
}

impl TransactionState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TransactionState::Committed
                | TransactionState::Aborted
                | TransactionState::FatalError(_)
        )
    }

    pub fn can_write(&self) -> bool {
        matches!(self, TransactionState::Empty | TransactionState::Ongoing)
    }
}

// ============================================================================
// Transaction Record (WAL Entry)
// ============================================================================

#[derive(Debug, Clone)]
pub struct TransactionRecord {
    pub tx_id: TransactionId,
    pub topic: String,
    pub partition: u32,
    pub data: Vec<u8>,
    pub sequence: u64,
    pub timestamp: Instant,
}

// ============================================================================
// Write-Ahead Log
// ============================================================================

#[derive(Debug)]
pub struct WriteAheadLog {
    entries: Vec<WalEntry>,
    max_size: usize,
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub tx_id: TransactionId,
    pub action: WalAction,
    pub timestamp: Instant,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WalAction {
    Begin,
    Write { topic: String, partition: u32, offset: u64 },
    PrepareCommit,
    Commit,
    Abort,
}

impl WriteAheadLog {
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: Vec::new(),
            max_size,
        }
    }

    pub fn append(&mut self, entry: WalEntry) -> Result<(), EventBusError> {
        if self.entries.len() >= self.max_size {
            self.compact();
        }
        self.entries.push(entry);
        Ok(())
    }

    pub fn entries_for(&self, tx_id: TransactionId) -> Vec<&WalEntry> {
        self.entries.iter().filter(|e| e.tx_id == tx_id).collect()
    }

    /// Remove entries for committed/aborted transactions
    pub fn compact(&mut self) {
        let terminal_txs: Vec<TransactionId> = self
            .entries
            .iter()
            .filter(|e| matches!(e.action, WalAction::Commit | WalAction::Abort))
            .map(|e| e.tx_id)
            .collect();

        self.entries
            .retain(|e| !terminal_txs.contains(&e.tx_id));
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Recover in-flight transactions from WAL (crash recovery)
    pub fn recover_pending(&self) -> Vec<TransactionId> {
        let mut begun: HashMap<TransactionId, bool> = HashMap::new();
        for entry in &self.entries {
            match &entry.action {
                WalAction::Begin => { begun.insert(entry.tx_id, true); }
                WalAction::Commit | WalAction::Abort => { begun.remove(&entry.tx_id); }
                _ => {}
            }
        }
        begun.keys().copied().collect()
    }
}

// ============================================================================
// Transaction
// ============================================================================

#[derive(Debug)]
pub struct Transaction {
    pub id: TransactionId,
    pub state: TransactionState,
    pub records: Vec<TransactionRecord>,
    pub started_at: Instant,
    pub timeout: Duration,
    sequence_counter: u64,
    affected_partitions: HashMap<String, Vec<u32>>,
}

impl Transaction {
    pub fn new(timeout: Duration) -> Self {
        Self {
            id: TransactionId::new(),
            state: TransactionState::Empty,
            records: Vec::new(),
            started_at: Instant::now(),
            timeout,
            sequence_counter: 0,
            affected_partitions: HashMap::new(),
        }
    }

    pub fn begin(&mut self) -> Result<(), EventBusError> {
        match &self.state {
            TransactionState::Empty => {
                self.state = TransactionState::Ongoing;
                self.started_at = Instant::now();
                Ok(())
            }
            _ => Err(EventBusError::Internal(format!(
                "Cannot begin transaction {}: state is {:?}",
                self.id, self.state
            ))),
        }
    }

    pub fn add_record(
        &mut self,
        topic: &str,
        partition: u32,
        data: Vec<u8>,
    ) -> Result<u64, EventBusError> {
        if !self.state.can_write() {
            return Err(EventBusError::Internal(format!(
                "Transaction {} not in writable state: {:?}",
                self.id, self.state
            )));
        }

        if self.is_timed_out() {
            self.state = TransactionState::Aborted;
            return Err(EventBusError::Timeout {
                operation: format!("transaction {}", self.id),
                duration_ms: self.timeout.as_millis() as u64,
            });
        }

        // If empty, implicitly transition to Ongoing
        if self.state == TransactionState::Empty {
            self.state = TransactionState::Ongoing;
        }

        let seq = self.sequence_counter;
        self.sequence_counter += 1;

        self.records.push(TransactionRecord {
            tx_id: self.id,
            topic: topic.to_string(),
            partition,
            data,
            sequence: seq,
            timestamp: Instant::now(),
        });

        self.affected_partitions
            .entry(topic.to_string())
            .or_default()
            .push(partition);

        Ok(seq)
    }

    pub fn prepare_commit(&mut self) -> Result<(), EventBusError> {
        match &self.state {
            TransactionState::Ongoing => {
                if self.is_timed_out() {
                    self.state = TransactionState::Aborted;
                    return Err(EventBusError::Timeout {
                        operation: format!("transaction {} prepare", self.id),
                        duration_ms: self.timeout.as_millis() as u64,
                    });
                }
                self.state = TransactionState::PrepareCommit;
                Ok(())
            }
            _ => Err(EventBusError::Internal(format!(
                "Cannot prepare commit for {}: state {:?}",
                self.id, self.state
            ))),
        }
    }

    pub fn commit(&mut self) -> Result<Vec<TransactionRecord>, EventBusError> {
        match &self.state {
            TransactionState::PrepareCommit => {
                self.state = TransactionState::Committed;
                Ok(self.records.clone())
            }
            TransactionState::Ongoing => {
                // Fast path: skip prepare for small transactions
                if self.is_timed_out() {
                    self.state = TransactionState::Aborted;
                    return Err(EventBusError::Timeout {
                        operation: format!("transaction {} commit", self.id),
                        duration_ms: self.timeout.as_millis() as u64,
                    });
                }
                self.state = TransactionState::Committed;
                Ok(self.records.clone())
            }
            _ => Err(EventBusError::Internal(format!(
                "Cannot commit {}: state {:?}",
                self.id, self.state
            ))),
        }
    }

    pub fn abort(&mut self) -> Result<(), EventBusError> {
        if self.state.is_terminal() {
            return Err(EventBusError::Internal(format!(
                "Cannot abort {}: already in terminal state {:?}",
                self.id, self.state
            )));
        }
        self.state = TransactionState::Aborted;
        self.records.clear();
        Ok(())
    }

    pub fn is_timed_out(&self) -> bool {
        self.started_at.elapsed() > self.timeout
    }

    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    pub fn affected_topics(&self) -> Vec<&str> {
        self.affected_partitions.keys().map(|s| s.as_str()).collect()
    }

    pub fn affected_partitions_for(&self, topic: &str) -> Option<&Vec<u32>> {
        self.affected_partitions.get(topic)
    }
}

// ============================================================================
// Transaction Coordinator
// ============================================================================

#[derive(Debug)]
pub struct TransactionCoordinator {
    active_transactions: HashMap<TransactionId, Transaction>,
    wal: WriteAheadLog,
    default_timeout: Duration,
    max_concurrent: usize,
    completed_count: u64,
    aborted_count: u64,
}

impl TransactionCoordinator {
    pub fn new(default_timeout: Duration, max_concurrent: usize) -> Self {
        Self {
            active_transactions: HashMap::new(),
            wal: WriteAheadLog::new(10_000),
            default_timeout,
            max_concurrent,
            completed_count: 0,
            aborted_count: 0,
        }
    }

    pub fn begin_transaction(&mut self) -> Result<TransactionId, EventBusError> {
        if self.active_transactions.len() >= self.max_concurrent {
            return Err(EventBusError::Internal(
                "Max concurrent transactions reached".into(),
            ));
        }

        let mut tx = Transaction::new(self.default_timeout);
        tx.begin()?;
        let id = tx.id;

        self.wal.append(WalEntry {
            tx_id: id,
            action: WalAction::Begin,
            timestamp: Instant::now(),
        })?;

        self.active_transactions.insert(id, tx);
        Ok(id)
    }

    pub fn begin_transaction_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<TransactionId, EventBusError> {
        if self.active_transactions.len() >= self.max_concurrent {
            return Err(EventBusError::Internal(
                "Max concurrent transactions reached".into(),
            ));
        }

        let mut tx = Transaction::new(timeout);
        tx.begin()?;
        let id = tx.id;

        self.wal.append(WalEntry {
            tx_id: id,
            action: WalAction::Begin,
            timestamp: Instant::now(),
        })?;

        self.active_transactions.insert(id, tx);
        Ok(id)
    }

    pub fn add_record(
        &mut self,
        tx_id: TransactionId,
        topic: &str,
        partition: u32,
        data: Vec<u8>,
    ) -> Result<u64, EventBusError> {
        let tx = self
            .active_transactions
            .get_mut(&tx_id)
            .ok_or(EventBusError::Internal(format!(
                "Transaction {} not found",
                tx_id
            )))?;

        let seq = tx.add_record(topic, partition, data)?;

        self.wal.append(WalEntry {
            tx_id,
            action: WalAction::Write {
                topic: topic.to_string(),
                partition,
                offset: seq,
            },
            timestamp: Instant::now(),
        })?;

        Ok(seq)
    }

    pub fn commit(
        &mut self,
        tx_id: TransactionId,
    ) -> Result<Vec<TransactionRecord>, EventBusError> {
        let tx = self
            .active_transactions
            .get_mut(&tx_id)
            .ok_or(EventBusError::Internal(format!(
                "Transaction {} not found",
                tx_id
            )))?;

        self.wal.append(WalEntry {
            tx_id,
            action: WalAction::PrepareCommit,
            timestamp: Instant::now(),
        })?;

        let records = tx.commit()?;

        self.wal.append(WalEntry {
            tx_id,
            action: WalAction::Commit,
            timestamp: Instant::now(),
        })?;

        self.completed_count += 1;
        self.active_transactions.remove(&tx_id);

        Ok(records)
    }

    pub fn abort(&mut self, tx_id: TransactionId) -> Result<(), EventBusError> {
        let tx = self
            .active_transactions
            .get_mut(&tx_id)
            .ok_or(EventBusError::Internal(format!(
                "Transaction {} not found",
                tx_id
            )))?;

        tx.abort()?;

        self.wal.append(WalEntry {
            tx_id,
            action: WalAction::Abort,
            timestamp: Instant::now(),
        })?;

        self.aborted_count += 1;
        self.active_transactions.remove(&tx_id);

        Ok(())
    }

    /// Abort all timed-out transactions
    pub fn cleanup_timed_out(&mut self) -> Vec<TransactionId> {
        let timed_out: Vec<TransactionId> = self
            .active_transactions
            .iter()
            .filter(|(_, tx)| tx.is_timed_out())
            .map(|(id, _)| *id)
            .collect();

        let mut aborted = Vec::new();
        for id in timed_out {
            if self.abort(id).is_ok() {
                aborted.push(id);
            }
        }
        aborted
    }

    pub fn active_count(&self) -> usize {
        self.active_transactions.len()
    }

    pub fn completed_count(&self) -> u64 {
        self.completed_count
    }

    pub fn aborted_count(&self) -> u64 {
        self.aborted_count
    }

    pub fn wal_size(&self) -> usize {
        self.wal.len()
    }

    /// Get pending transactions from WAL (for crash recovery)
    pub fn recover(&self) -> Vec<TransactionId> {
        self.wal.recover_pending()
    }

    pub fn get_transaction(&self, tx_id: TransactionId) -> Option<&Transaction> {
        self.active_transactions.get(&tx_id)
    }

    pub fn stats(&self) -> TransactionStats {
        TransactionStats {
            active: self.active_transactions.len(),
            committed: self.completed_count,
            aborted: self.aborted_count,
            wal_entries: self.wal.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionStats {
    pub active: usize,
    pub committed: u64,
    pub aborted: u64,
    pub wal_entries: usize,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_id_unique() {
        let id1 = TransactionId::new();
        let id2 = TransactionId::new();
        assert_ne!(id1, id2);
        assert!(id2.value() > id1.value());
    }

    #[test]
    fn test_transaction_id_display() {
        let id = TransactionId::new();
        let s = format!("{}", id);
        assert!(s.starts_with("tx-"));
    }

    #[test]
    fn test_transaction_lifecycle_begin_commit() {
        let mut tx = Transaction::new(Duration::from_secs(30));
        assert_eq!(tx.state, TransactionState::Empty);

        tx.begin().unwrap();
        assert_eq!(tx.state, TransactionState::Ongoing);

        tx.add_record("orders", 0, vec![1, 2, 3]).unwrap();
        tx.add_record("orders", 1, vec![4, 5, 6]).unwrap();
        assert_eq!(tx.record_count(), 2);

        tx.prepare_commit().unwrap();
        assert_eq!(tx.state, TransactionState::PrepareCommit);

        let records = tx.commit().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(tx.state, TransactionState::Committed);
    }

    #[test]
    fn test_transaction_fast_commit() {
        let mut tx = Transaction::new(Duration::from_secs(30));
        tx.begin().unwrap();
        tx.add_record("orders", 0, vec![1]).unwrap();

        // Fast commit skips prepare
        let records = tx.commit().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(tx.state, TransactionState::Committed);
    }

    #[test]
    fn test_transaction_abort() {
        let mut tx = Transaction::new(Duration::from_secs(30));
        tx.begin().unwrap();
        tx.add_record("orders", 0, vec![1]).unwrap();
        tx.abort().unwrap();

        assert_eq!(tx.state, TransactionState::Aborted);
        assert_eq!(tx.record_count(), 0); // records cleared
    }

    #[test]
    fn test_transaction_cannot_write_after_commit() {
        let mut tx = Transaction::new(Duration::from_secs(30));
        tx.begin().unwrap();
        tx.add_record("orders", 0, vec![1]).unwrap();
        tx.commit().unwrap();

        assert!(tx.add_record("orders", 0, vec![2]).is_err());
    }

    #[test]
    fn test_transaction_cannot_abort_after_commit() {
        let mut tx = Transaction::new(Duration::from_secs(30));
        tx.begin().unwrap();
        tx.commit().unwrap();
        assert!(tx.abort().is_err());
    }

    #[test]
    fn test_transaction_cannot_begin_twice() {
        let mut tx = Transaction::new(Duration::from_secs(30));
        tx.begin().unwrap();
        assert!(tx.begin().is_err());
    }

    #[test]
    fn test_transaction_affected_partitions() {
        let mut tx = Transaction::new(Duration::from_secs(30));
        tx.begin().unwrap();
        tx.add_record("orders", 0, vec![1]).unwrap();
        tx.add_record("orders", 2, vec![2]).unwrap();
        tx.add_record("payments", 1, vec![3]).unwrap();

        let topics = tx.affected_topics();
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"orders"));
        assert!(topics.contains(&"payments"));

        let parts = tx.affected_partitions_for("orders").unwrap();
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn test_transaction_sequence_numbers() {
        let mut tx = Transaction::new(Duration::from_secs(30));
        tx.begin().unwrap();
        assert_eq!(tx.add_record("t", 0, vec![1]).unwrap(), 0);
        assert_eq!(tx.add_record("t", 0, vec![2]).unwrap(), 1);
        assert_eq!(tx.add_record("t", 0, vec![3]).unwrap(), 2);
    }

    #[test]
    fn test_transaction_state_terminal_check() {
        assert!(!TransactionState::Empty.is_terminal());
        assert!(!TransactionState::Ongoing.is_terminal());
        assert!(!TransactionState::PrepareCommit.is_terminal());
        assert!(TransactionState::Committed.is_terminal());
        assert!(TransactionState::Aborted.is_terminal());
        assert!(TransactionState::FatalError("err".into()).is_terminal());
    }

    #[test]
    fn test_transaction_state_can_write() {
        assert!(TransactionState::Empty.can_write());
        assert!(TransactionState::Ongoing.can_write());
        assert!(!TransactionState::PrepareCommit.can_write());
        assert!(!TransactionState::Committed.can_write());
        assert!(!TransactionState::Aborted.can_write());
    }

    // === WAL Tests ===

    #[test]
    fn test_wal_append_and_query() {
        let mut wal = WriteAheadLog::new(100);
        let tx_id = TransactionId::new();

        wal.append(WalEntry {
            tx_id,
            action: WalAction::Begin,
            timestamp: Instant::now(),
        }).unwrap();

        wal.append(WalEntry {
            tx_id,
            action: WalAction::Write {
                topic: "orders".into(),
                partition: 0,
                offset: 0,
            },
            timestamp: Instant::now(),
        }).unwrap();

        assert_eq!(wal.len(), 2);
        assert_eq!(wal.entries_for(tx_id).len(), 2);
    }

    #[test]
    fn test_wal_compact() {
        let mut wal = WriteAheadLog::new(100);
        let tx1 = TransactionId::new();
        let tx2 = TransactionId::new();

        // tx1: begin → commit
        wal.append(WalEntry { tx_id: tx1, action: WalAction::Begin, timestamp: Instant::now() }).unwrap();
        wal.append(WalEntry { tx_id: tx1, action: WalAction::Commit, timestamp: Instant::now() }).unwrap();

        // tx2: begin only (pending)
        wal.append(WalEntry { tx_id: tx2, action: WalAction::Begin, timestamp: Instant::now() }).unwrap();

        assert_eq!(wal.len(), 3);
        wal.compact();
        // tx1 removed (committed), tx2 remains
        assert_eq!(wal.len(), 1);
        assert_eq!(wal.entries_for(tx2).len(), 1);
    }

    #[test]
    fn test_wal_recover_pending() {
        let mut wal = WriteAheadLog::new(100);
        let tx1 = TransactionId::new();
        let tx2 = TransactionId::new();
        let tx3 = TransactionId::new();

        wal.append(WalEntry { tx_id: tx1, action: WalAction::Begin, timestamp: Instant::now() }).unwrap();
        wal.append(WalEntry { tx_id: tx1, action: WalAction::Commit, timestamp: Instant::now() }).unwrap();

        wal.append(WalEntry { tx_id: tx2, action: WalAction::Begin, timestamp: Instant::now() }).unwrap();
        // tx2 left as pending

        wal.append(WalEntry { tx_id: tx3, action: WalAction::Begin, timestamp: Instant::now() }).unwrap();
        wal.append(WalEntry { tx_id: tx3, action: WalAction::Abort, timestamp: Instant::now() }).unwrap();

        let pending = wal.recover_pending();
        assert_eq!(pending.len(), 1);
        assert!(pending.contains(&tx2));
    }

    #[test]
    fn test_wal_auto_compact_on_full() {
        let mut wal = WriteAheadLog::new(3);
        let tx = TransactionId::new();

        wal.append(WalEntry { tx_id: tx, action: WalAction::Begin, timestamp: Instant::now() }).unwrap();
        wal.append(WalEntry { tx_id: tx, action: WalAction::Commit, timestamp: Instant::now() }).unwrap();

        // Next append triggers compact since len >= max_size
        // After compact, committed tx is removed
        let tx2 = TransactionId::new();
        wal.append(WalEntry { tx_id: tx2, action: WalAction::Begin, timestamp: Instant::now() }).unwrap();
        // compaction removed tx entries, only tx2 remains
    }

    // === Transaction Coordinator Tests ===

    #[test]
    fn test_coordinator_begin_commit() {
        let mut coord = TransactionCoordinator::new(Duration::from_secs(30), 10);

        let tx_id = coord.begin_transaction().unwrap();
        assert_eq!(coord.active_count(), 1);

        coord.add_record(tx_id, "orders", 0, vec![1, 2]).unwrap();
        coord.add_record(tx_id, "orders", 1, vec![3, 4]).unwrap();

        let records = coord.commit(tx_id).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(coord.active_count(), 0);
        assert_eq!(coord.completed_count(), 1);
    }

    #[test]
    fn test_coordinator_abort() {
        let mut coord = TransactionCoordinator::new(Duration::from_secs(30), 10);

        let tx_id = coord.begin_transaction().unwrap();
        coord.add_record(tx_id, "orders", 0, vec![1]).unwrap();
        coord.abort(tx_id).unwrap();

        assert_eq!(coord.active_count(), 0);
        assert_eq!(coord.aborted_count(), 1);
    }

    #[test]
    fn test_coordinator_max_concurrent() {
        let mut coord = TransactionCoordinator::new(Duration::from_secs(30), 2);

        coord.begin_transaction().unwrap();
        coord.begin_transaction().unwrap();
        assert!(coord.begin_transaction().is_err());
    }

    #[test]
    fn test_coordinator_custom_timeout() {
        let mut coord = TransactionCoordinator::new(Duration::from_secs(30), 10);
        let tx_id = coord
            .begin_transaction_with_timeout(Duration::from_secs(60))
            .unwrap();

        let tx = coord.get_transaction(tx_id).unwrap();
        assert_eq!(tx.timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_coordinator_stats() {
        let mut coord = TransactionCoordinator::new(Duration::from_secs(30), 10);

        let tx1 = coord.begin_transaction().unwrap();
        coord.commit(tx1).unwrap();

        let tx2 = coord.begin_transaction().unwrap();
        coord.abort(tx2).unwrap();

        let _tx3 = coord.begin_transaction().unwrap();

        let stats = coord.stats();
        assert_eq!(stats.active, 1);
        assert_eq!(stats.committed, 1);
        assert_eq!(stats.aborted, 1);
        assert!(stats.wal_entries > 0);
    }

    #[test]
    fn test_coordinator_not_found() {
        let mut coord = TransactionCoordinator::new(Duration::from_secs(30), 10);
        let fake_id = TransactionId::new();
        assert!(coord.commit(fake_id).is_err());
        assert!(coord.abort(fake_id).is_err());
    }

    #[test]
    fn test_coordinator_recover() {
        let mut coord = TransactionCoordinator::new(Duration::from_secs(30), 10);

        let tx1 = coord.begin_transaction().unwrap();
        coord.commit(tx1).unwrap();

        let _tx2 = coord.begin_transaction().unwrap();
        // tx2 not committed — will be in WAL as pending

        let pending = coord.recover();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn test_coordinator_multiple_topics() {
        let mut coord = TransactionCoordinator::new(Duration::from_secs(30), 10);

        let tx_id = coord.begin_transaction().unwrap();
        coord.add_record(tx_id, "orders", 0, vec![1]).unwrap();
        coord.add_record(tx_id, "payments", 0, vec![2]).unwrap();
        coord.add_record(tx_id, "notifications", 0, vec![3]).unwrap();

        let records = coord.commit(tx_id).unwrap();
        assert_eq!(records.len(), 3);

        let topics: Vec<&str> = records.iter().map(|r| r.topic.as_str()).collect();
        assert!(topics.contains(&"orders"));
        assert!(topics.contains(&"payments"));
        assert!(topics.contains(&"notifications"));
    }

    #[test]
    fn test_coordinator_wal_compaction() {
        let mut coord = TransactionCoordinator::new(Duration::from_secs(30), 10);

        // Create and commit several transactions
        for _ in 0..5 {
            let tx = coord.begin_transaction().unwrap();
            coord.add_record(tx, "t", 0, vec![1]).unwrap();
            coord.commit(tx).unwrap();
        }

        let wal_before = coord.wal_size();
        assert!(wal_before > 0);
    }

    #[test]
    fn test_implicit_begin_on_write() {
        let mut tx = Transaction::new(Duration::from_secs(30));
        assert_eq!(tx.state, TransactionState::Empty);

        // Writing implicitly starts the transaction
        tx.add_record("orders", 0, vec![1]).unwrap();
        assert_eq!(tx.state, TransactionState::Ongoing);
    }
}
