// exactly_once.rs - Exactly-Once Delivery Semantics
// Idempotent Producer + Transactional Consumer for EOS guarantee

use std::collections::{HashMap, HashSet, VecDeque};

use crate::bus_errors::EventBusError;
use crate::transaction::TransactionId;

// ============================================================================
// Producer ID & Sequence Tracking (Idempotent Producer)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProducerId(u64);

impl ProducerId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Tracks per-producer sequence numbers for idempotent writes
#[derive(Debug)]
pub struct ProducerStateManager {
    /// producer_id → (topic, partition) → last sequence
    sequences: HashMap<ProducerId, HashMap<(String, u32), u64>>,
    /// Producer epoch tracking for fencing
    epochs: HashMap<ProducerId, u64>,
    /// Max producers to track before eviction
    max_producers: usize,
    /// LRU order for eviction
    access_order: VecDeque<ProducerId>,
}

impl ProducerStateManager {
    pub fn new(max_producers: usize) -> Self {
        Self {
            sequences: HashMap::new(),
            epochs: HashMap::new(),
            max_producers,
            access_order: VecDeque::new(),
        }
    }

    /// Register a new producer and get its initial epoch
    pub fn register_producer(&mut self, producer_id: ProducerId) -> u64 {
        let epoch = self.epochs.entry(producer_id).or_insert(0);
        *epoch += 1;
        let epoch_val = *epoch;
        self.sequences.entry(producer_id).or_default();
        self.touch(producer_id);
        epoch_val
    }

    /// Check if a produce request is a duplicate
    pub fn check_sequence(
        &self,
        producer_id: ProducerId,
        epoch: u64,
        topic: &str,
        partition: u32,
        sequence: u64,
    ) -> SequenceCheckResult {
        // Check epoch
        match self.epochs.get(&producer_id) {
            None => return SequenceCheckResult::UnknownProducer,
            Some(&current_epoch) if epoch < current_epoch => {
                return SequenceCheckResult::FencedProducer {
                    current_epoch,
                    provided_epoch: epoch,
                };
            }
            _ => {}
        }

        let key = (topic.to_string(), partition);
        match self.sequences.get(&producer_id) {
            None => SequenceCheckResult::UnknownProducer,
            Some(seqs) => match seqs.get(&key) {
                None => {
                    if sequence == 0 {
                        SequenceCheckResult::Ok
                    } else {
                        SequenceCheckResult::OutOfOrder {
                            expected: 0,
                            received: sequence,
                        }
                    }
                }
                Some(&last_seq) => {
                    if sequence == last_seq + 1 {
                        SequenceCheckResult::Ok
                    } else if sequence <= last_seq {
                        SequenceCheckResult::Duplicate {
                            last_sequence: last_seq,
                        }
                    } else {
                        SequenceCheckResult::OutOfOrder {
                            expected: last_seq + 1,
                            received: sequence,
                        }
                    }
                }
            },
        }
    }

    /// Update sequence after successful write
    pub fn update_sequence(
        &mut self,
        producer_id: ProducerId,
        topic: &str,
        partition: u32,
        sequence: u64,
    ) -> Result<(), EventBusError> {
        let seqs = self
            .sequences
            .get_mut(&producer_id)
            .ok_or(EventBusError::Internal("Unknown producer".into()))?;

        seqs.insert((topic.to_string(), partition), sequence);
        self.touch(producer_id);
        self.maybe_evict();
        Ok(())
    }

    /// Fence old producer (bump epoch, invalidate old sequences)
    pub fn fence_producer(&mut self, producer_id: ProducerId) -> u64 {
        let epoch = self.epochs.entry(producer_id).or_insert(0);
        *epoch += 1;
        // Clear old sequences
        self.sequences.insert(producer_id, HashMap::new());
        *epoch
    }

    pub fn producer_count(&self) -> usize {
        self.sequences.len()
    }

    pub fn get_epoch(&self, producer_id: ProducerId) -> Option<u64> {
        self.epochs.get(&producer_id).copied()
    }

    fn touch(&mut self, producer_id: ProducerId) {
        self.access_order.retain(|&id| id != producer_id);
        self.access_order.push_back(producer_id);
    }

    fn maybe_evict(&mut self) {
        while self.sequences.len() > self.max_producers {
            if let Some(oldest) = self.access_order.pop_front() {
                self.sequences.remove(&oldest);
                self.epochs.remove(&oldest);
            } else {
                break;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SequenceCheckResult {
    Ok,
    Duplicate { last_sequence: u64 },
    OutOfOrder { expected: u64, received: u64 },
    FencedProducer { current_epoch: u64, provided_epoch: u64 },
    UnknownProducer,
}

// ============================================================================
// Idempotent Producer
// ============================================================================

#[derive(Debug)]
pub struct IdempotentProducer {
    pub producer_id: ProducerId,
    pub epoch: u64,
    /// topic+partition → next sequence
    sequences: HashMap<(String, u32), u64>,
    pub max_in_flight: usize,
    in_flight: HashSet<u64>,
    pub enable_retries: bool,
    pub max_retries: u32,
}

impl IdempotentProducer {
    pub fn new(producer_id: ProducerId, epoch: u64) -> Self {
        Self {
            producer_id,
            epoch,
            sequences: HashMap::new(),
            max_in_flight: 5,
            in_flight: HashSet::new(),
            enable_retries: true,
            max_retries: 3,
        }
    }

    /// Get next sequence number for a topic+partition
    pub fn next_sequence(&mut self, topic: &str, partition: u32) -> Result<u64, EventBusError> {
        if self.in_flight.len() >= self.max_in_flight {
            return Err(EventBusError::Internal(
                "Max in-flight requests exceeded".into(),
            ));
        }

        let key = (topic.to_string(), partition);
        let seq = self.sequences.entry(key).or_insert(0);
        let current = *seq;
        *seq += 1;
        self.in_flight.insert(current);
        Ok(current)
    }

    /// Acknowledge successful write
    pub fn ack(&mut self, sequence: u64) {
        self.in_flight.remove(&sequence);
    }

    /// Handle write failure with retry tracking
    pub fn nack(&mut self, sequence: u64, topic: &str, partition: u32) {
        self.in_flight.remove(&sequence);
        if self.enable_retries {
            // Reset sequence so it can be retried
            let key = (topic.to_string(), partition);
            if let Some(seq) = self.sequences.get_mut(&key) {
                if *seq > sequence {
                    *seq = sequence;
                }
            }
        }
    }

    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    pub fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
    }

    pub fn current_sequence(&self, topic: &str, partition: u32) -> u64 {
        self.sequences
            .get(&(topic.to_string(), partition))
            .copied()
            .unwrap_or(0)
    }
}

// ============================================================================
// Transactional Consumer
// ============================================================================

/// Consumer that participates in exactly-once processing
#[derive(Debug)]
pub struct TransactionalConsumer {
    pub consumer_id: String,
    pub group_id: String,
    /// Track consumed offsets per topic+partition within current transaction
    consumed_offsets: HashMap<(String, u32), u64>,
    /// Current transaction ID (if in transaction)
    current_tx: Option<TransactionId>,
    /// Processing state
    state: ConsumerProcessingState,
    /// Committed offsets (after transaction commit)
    committed_offsets: HashMap<(String, u32), u64>,
    /// Isolation level
    pub isolation_level: IsolationLevel,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConsumerProcessingState {
    Idle,
    Fetching,
    Processing,
    Committing,
    Error(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    /// Read all messages including uncommitted (default Kafka behavior)
    ReadUncommitted,
    /// Only read committed messages (for EOS)
    ReadCommitted,
}

impl TransactionalConsumer {
    pub fn new(consumer_id: &str, group_id: &str) -> Self {
        Self {
            consumer_id: consumer_id.to_string(),
            group_id: group_id.to_string(),
            consumed_offsets: HashMap::new(),
            current_tx: None,
            state: ConsumerProcessingState::Idle,
            committed_offsets: HashMap::new(),
            isolation_level: IsolationLevel::ReadCommitted,
        }
    }

    pub fn with_isolation(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }

    /// Begin consuming in a transaction context
    pub fn begin_transaction(&mut self, tx_id: TransactionId) -> Result<(), EventBusError> {
        if self.current_tx.is_some() {
            return Err(EventBusError::Internal(
                "Transaction already in progress".into(),
            ));
        }
        self.current_tx = Some(tx_id);
        self.state = ConsumerProcessingState::Fetching;
        self.consumed_offsets.clear();
        Ok(())
    }

    /// Record that a message at given offset was consumed
    pub fn record_consumed(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), EventBusError> {
        if self.current_tx.is_none() {
            return Err(EventBusError::Internal("No active transaction".into()));
        }

        let key = (topic.to_string(), partition);

        // Only advance offset (no backward)
        match self.consumed_offsets.get(&key) {
            Some(&current) if offset <= current => {
                return Err(EventBusError::OffsetOutOfRange {
                    topic: topic.into(),
                    partition,
                    offset,
                });
            }
            _ => {}
        }

        self.consumed_offsets.insert(key, offset);
        self.state = ConsumerProcessingState::Processing;
        Ok(())
    }

    /// Commit consumed offsets as part of the transaction
    pub fn commit_offsets(&mut self) -> Result<HashMap<(String, u32), u64>, EventBusError> {
        if self.current_tx.is_none() {
            return Err(EventBusError::Internal("No active transaction".into()));
        }

        self.state = ConsumerProcessingState::Committing;

        // Move consumed to committed
        let offsets = self.consumed_offsets.clone();
        for (key, offset) in &offsets {
            self.committed_offsets.insert(key.clone(), *offset);
        }
        self.consumed_offsets.clear();
        self.current_tx = None;
        self.state = ConsumerProcessingState::Idle;

        Ok(offsets)
    }

    /// Abort transaction — discard consumed offsets
    pub fn abort_transaction(&mut self) -> Result<(), EventBusError> {
        if self.current_tx.is_none() {
            return Err(EventBusError::Internal("No active transaction".into()));
        }

        self.consumed_offsets.clear();
        self.current_tx = None;
        self.state = ConsumerProcessingState::Idle;
        Ok(())
    }

    pub fn current_transaction(&self) -> Option<TransactionId> {
        self.current_tx
    }

    pub fn committed_offset(&self, topic: &str, partition: u32) -> Option<u64> {
        self.committed_offsets
            .get(&(topic.to_string(), partition))
            .copied()
    }

    pub fn state(&self) -> &ConsumerProcessingState {
        &self.state
    }

    pub fn is_in_transaction(&self) -> bool {
        self.current_tx.is_some()
    }
}

// ============================================================================
// EOS (Exactly-Once Semantics) Coordinator
// ============================================================================

/// Coordinates producer idempotency + consumer transactional offsets
#[derive(Debug)]
pub struct ExactlyOnceCoordinator {
    producer_states: ProducerStateManager,
    consumers: HashMap<String, TransactionalConsumer>,
    /// Transaction → Consumer mapping
    tx_consumer_map: HashMap<TransactionId, String>,
    stats: EosStats,
}

#[derive(Debug, Clone, Default)]
pub struct EosStats {
    pub duplicates_detected: u64,
    pub out_of_order_detected: u64,
    pub fenced_producers: u64,
    pub transactions_committed: u64,
    pub transactions_aborted: u64,
}

impl ExactlyOnceCoordinator {
    pub fn new(max_producers: usize) -> Self {
        Self {
            producer_states: ProducerStateManager::new(max_producers),
            consumers: HashMap::new(),
            tx_consumer_map: HashMap::new(),
            stats: EosStats::default(),
        }
    }

    /// Initialize a producer with idempotency
    pub fn init_producer(&mut self, producer_id: ProducerId) -> u64 {
        self.producer_states.register_producer(producer_id)
    }

    /// Validate and track a produce request
    pub fn validate_produce(
        &mut self,
        producer_id: ProducerId,
        epoch: u64,
        topic: &str,
        partition: u32,
        sequence: u64,
    ) -> Result<(), EventBusError> {
        let result = self
            .producer_states
            .check_sequence(producer_id, epoch, topic, partition, sequence);

        match result {
            SequenceCheckResult::Ok => {
                self.producer_states
                    .update_sequence(producer_id, topic, partition, sequence)?;
                Ok(())
            }
            SequenceCheckResult::Duplicate { last_sequence } => {
                self.stats.duplicates_detected += 1;
                Err(EventBusError::Internal(format!(
                    "Duplicate: seq {} already committed (last: {})",
                    sequence, last_sequence
                )))
            }
            SequenceCheckResult::OutOfOrder { expected, received } => {
                self.stats.out_of_order_detected += 1;
                Err(EventBusError::Internal(format!(
                    "Out of order: expected seq {}, got {}",
                    expected, received
                )))
            }
            SequenceCheckResult::FencedProducer {
                current_epoch,
                provided_epoch,
            } => {
                self.stats.fenced_producers += 1;
                Err(EventBusError::Internal(format!(
                    "Producer fenced: epoch {} < current {}",
                    provided_epoch, current_epoch
                )))
            }
            SequenceCheckResult::UnknownProducer => {
                Err(EventBusError::Internal("Unknown producer".into()))
            }
        }
    }

    /// Register a consumer for transactional processing
    pub fn register_consumer(&mut self, consumer_id: &str, group_id: &str) {
        self.consumers.insert(
            consumer_id.to_string(),
            TransactionalConsumer::new(consumer_id, group_id),
        );
    }

    /// Begin transactional consume-transform-produce
    pub fn begin_consume_transform_produce(
        &mut self,
        consumer_id: &str,
        tx_id: TransactionId,
    ) -> Result<(), EventBusError> {
        let consumer = self
            .consumers
            .get_mut(consumer_id)
            .ok_or(EventBusError::Internal(format!(
                "Consumer {} not found",
                consumer_id
            )))?;

        consumer.begin_transaction(tx_id)?;
        self.tx_consumer_map
            .insert(tx_id, consumer_id.to_string());
        Ok(())
    }

    /// Commit transactional consume (offsets + produces atomically)
    pub fn commit_consume_transform(
        &mut self,
        consumer_id: &str,
    ) -> Result<HashMap<(String, u32), u64>, EventBusError> {
        let consumer = self
            .consumers
            .get_mut(consumer_id)
            .ok_or(EventBusError::Internal(format!(
                "Consumer {} not found",
                consumer_id
            )))?;

        if let Some(tx_id) = consumer.current_transaction() {
            self.tx_consumer_map.remove(&tx_id);
        }

        let offsets = consumer.commit_offsets()?;
        self.stats.transactions_committed += 1;
        Ok(offsets)
    }

    /// Abort transactional consume
    pub fn abort_consume_transform(
        &mut self,
        consumer_id: &str,
    ) -> Result<(), EventBusError> {
        let consumer = self
            .consumers
            .get_mut(consumer_id)
            .ok_or(EventBusError::Internal(format!(
                "Consumer {} not found",
                consumer_id
            )))?;

        if let Some(tx_id) = consumer.current_transaction() {
            self.tx_consumer_map.remove(&tx_id);
        }

        consumer.abort_transaction()?;
        self.stats.transactions_aborted += 1;
        Ok(())
    }

    pub fn get_consumer(&self, consumer_id: &str) -> Option<&TransactionalConsumer> {
        self.consumers.get(consumer_id)
    }

    pub fn stats(&self) -> &EosStats {
        &self.stats
    }

    pub fn fence_producer(&mut self, producer_id: ProducerId) -> u64 {
        self.stats.fenced_producers += 1;
        self.producer_states.fence_producer(producer_id)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // === ProducerStateManager Tests ===

    #[test]
    fn test_register_producer() {
        let mut mgr = ProducerStateManager::new(100);
        let pid = ProducerId::new(1);
        let epoch = mgr.register_producer(pid);
        assert_eq!(epoch, 1);
        assert_eq!(mgr.get_epoch(pid), Some(1));
    }

    #[test]
    fn test_register_producer_increments_epoch() {
        let mut mgr = ProducerStateManager::new(100);
        let pid = ProducerId::new(1);
        assert_eq!(mgr.register_producer(pid), 1);
        assert_eq!(mgr.register_producer(pid), 2);
        assert_eq!(mgr.register_producer(pid), 3);
    }

    #[test]
    fn test_sequence_check_ok() {
        let mut mgr = ProducerStateManager::new(100);
        let pid = ProducerId::new(1);
        let epoch = mgr.register_producer(pid);

        // First write: sequence 0
        let result = mgr.check_sequence(pid, epoch, "orders", 0, 0);
        assert_eq!(result, SequenceCheckResult::Ok);

        mgr.update_sequence(pid, "orders", 0, 0).unwrap();

        // Second write: sequence 1
        let result = mgr.check_sequence(pid, epoch, "orders", 0, 1);
        assert_eq!(result, SequenceCheckResult::Ok);
    }

    #[test]
    fn test_sequence_check_duplicate() {
        let mut mgr = ProducerStateManager::new(100);
        let pid = ProducerId::new(1);
        let epoch = mgr.register_producer(pid);

        mgr.update_sequence(pid, "orders", 0, 5).unwrap();

        let result = mgr.check_sequence(pid, epoch, "orders", 0, 3);
        assert!(matches!(result, SequenceCheckResult::Duplicate { .. }));
    }

    #[test]
    fn test_sequence_check_out_of_order() {
        let mut mgr = ProducerStateManager::new(100);
        let pid = ProducerId::new(1);
        let epoch = mgr.register_producer(pid);

        mgr.update_sequence(pid, "orders", 0, 0).unwrap();

        // Skip sequence 1, send 5
        let result = mgr.check_sequence(pid, epoch, "orders", 0, 5);
        assert!(matches!(result, SequenceCheckResult::OutOfOrder { .. }));
    }

    #[test]
    fn test_sequence_check_fenced() {
        let mut mgr = ProducerStateManager::new(100);
        let pid = ProducerId::new(1);
        let _epoch1 = mgr.register_producer(pid);
        let epoch2 = mgr.register_producer(pid); // epoch bumped to 2

        // Old epoch should be rejected
        let result = mgr.check_sequence(pid, 1, "orders", 0, 0);
        assert!(matches!(result, SequenceCheckResult::FencedProducer { .. }));

        // New epoch should work
        let result = mgr.check_sequence(pid, epoch2, "orders", 0, 0);
        assert_eq!(result, SequenceCheckResult::Ok);
    }

    #[test]
    fn test_sequence_check_unknown() {
        let mgr = ProducerStateManager::new(100);
        let pid = ProducerId::new(999);
        let result = mgr.check_sequence(pid, 1, "orders", 0, 0);
        assert_eq!(result, SequenceCheckResult::UnknownProducer);
    }

    #[test]
    fn test_producer_eviction() {
        let mut mgr = ProducerStateManager::new(2);
        let p1 = ProducerId::new(100);
        let p2 = ProducerId::new(200);
        let p3 = ProducerId::new(300);

        mgr.register_producer(p1);
        mgr.register_producer(p2);
        mgr.update_sequence(p1, "t", 0, 0).unwrap();
        mgr.update_sequence(p2, "t", 0, 0).unwrap();

        // Adding p3 should evict p1 (least recently used)
        mgr.register_producer(p3);
        mgr.update_sequence(p3, "t", 0, 0).unwrap();

        assert_eq!(mgr.producer_count(), 2);
    }

    #[test]
    fn test_fence_producer() {
        let mut mgr = ProducerStateManager::new(100);
        let pid = ProducerId::new(1);
        mgr.register_producer(pid);
        mgr.update_sequence(pid, "orders", 0, 5).unwrap();

        let new_epoch = mgr.fence_producer(pid);
        assert_eq!(new_epoch, 2);

        // Old sequences cleared — seq 0 should be valid again
        let result = mgr.check_sequence(pid, new_epoch, "orders", 0, 0);
        assert_eq!(result, SequenceCheckResult::Ok);
    }

    // === IdempotentProducer Tests ===

    #[test]
    fn test_idempotent_producer_sequence() {
        let mut producer = IdempotentProducer::new(ProducerId::new(1), 1);

        let seq0 = producer.next_sequence("orders", 0).unwrap();
        assert_eq!(seq0, 0);

        let seq1 = producer.next_sequence("orders", 0).unwrap();
        assert_eq!(seq1, 1);

        assert_eq!(producer.in_flight_count(), 2);
    }

    #[test]
    fn test_idempotent_producer_ack() {
        let mut producer = IdempotentProducer::new(ProducerId::new(1), 1);

        let seq = producer.next_sequence("orders", 0).unwrap();
        assert_eq!(producer.in_flight_count(), 1);

        producer.ack(seq);
        assert_eq!(producer.in_flight_count(), 0);
    }

    #[test]
    fn test_idempotent_producer_max_in_flight() {
        let mut producer = IdempotentProducer::new(ProducerId::new(1), 1);
        producer.max_in_flight = 2;

        producer.next_sequence("t", 0).unwrap();
        producer.next_sequence("t", 0).unwrap();
        assert!(producer.next_sequence("t", 0).is_err());
    }

    #[test]
    fn test_idempotent_producer_nack_retry() {
        let mut producer = IdempotentProducer::new(ProducerId::new(1), 1);

        let seq = producer.next_sequence("orders", 0).unwrap();
        assert_eq!(seq, 0);

        // Nack resets sequence for retry
        producer.nack(seq, "orders", 0);
        assert_eq!(producer.current_sequence("orders", 0), 0);
    }

    #[test]
    fn test_idempotent_producer_multi_partition() {
        let mut producer = IdempotentProducer::new(ProducerId::new(1), 1);

        assert_eq!(producer.next_sequence("t", 0).unwrap(), 0);
        assert_eq!(producer.next_sequence("t", 1).unwrap(), 0);
        assert_eq!(producer.next_sequence("t", 0).unwrap(), 1);
        assert_eq!(producer.next_sequence("t", 1).unwrap(), 1);
    }

    // === TransactionalConsumer Tests ===

    #[test]
    fn test_consumer_transaction_lifecycle() {
        let mut consumer = TransactionalConsumer::new("c1", "group-1");
        let tx_id = TransactionId::new();

        consumer.begin_transaction(tx_id).unwrap();
        assert!(consumer.is_in_transaction());

        consumer.record_consumed("orders", 0, 100).unwrap();
        consumer.record_consumed("orders", 0, 101).unwrap();

        let offsets = consumer.commit_offsets().unwrap();
        assert_eq!(offsets.get(&("orders".to_string(), 0)), Some(&101));
        assert!(!consumer.is_in_transaction());
    }

    #[test]
    fn test_consumer_abort() {
        let mut consumer = TransactionalConsumer::new("c1", "group-1");
        let tx_id = TransactionId::new();

        consumer.begin_transaction(tx_id).unwrap();
        consumer.record_consumed("orders", 0, 100).unwrap();
        consumer.abort_transaction().unwrap();

        assert!(!consumer.is_in_transaction());
        assert!(consumer.committed_offset("orders", 0).is_none());
    }

    #[test]
    fn test_consumer_no_backward_offset() {
        let mut consumer = TransactionalConsumer::new("c1", "group-1");
        let tx_id = TransactionId::new();

        consumer.begin_transaction(tx_id).unwrap();
        consumer.record_consumed("orders", 0, 100).unwrap();

        // Can't go backward
        assert!(consumer.record_consumed("orders", 0, 99).is_err());
    }

    #[test]
    fn test_consumer_no_double_transaction() {
        let mut consumer = TransactionalConsumer::new("c1", "group-1");
        consumer.begin_transaction(TransactionId::new()).unwrap();
        assert!(consumer.begin_transaction(TransactionId::new()).is_err());
    }

    #[test]
    fn test_consumer_committed_offsets_persist() {
        let mut consumer = TransactionalConsumer::new("c1", "group-1");

        // First transaction
        let tx1 = TransactionId::new();
        consumer.begin_transaction(tx1).unwrap();
        consumer.record_consumed("orders", 0, 100).unwrap();
        consumer.commit_offsets().unwrap();

        assert_eq!(consumer.committed_offset("orders", 0), Some(100));

        // Second transaction
        let tx2 = TransactionId::new();
        consumer.begin_transaction(tx2).unwrap();
        consumer.record_consumed("orders", 0, 200).unwrap();
        consumer.commit_offsets().unwrap();

        assert_eq!(consumer.committed_offset("orders", 0), Some(200));
    }

    #[test]
    fn test_consumer_isolation_level() {
        let consumer = TransactionalConsumer::new("c1", "g1")
            .with_isolation(IsolationLevel::ReadUncommitted);
        assert_eq!(consumer.isolation_level, IsolationLevel::ReadUncommitted);
    }

    #[test]
    fn test_consumer_state_transitions() {
        let mut consumer = TransactionalConsumer::new("c1", "g1");
        assert_eq!(*consumer.state(), ConsumerProcessingState::Idle);

        consumer.begin_transaction(TransactionId::new()).unwrap();
        assert_eq!(*consumer.state(), ConsumerProcessingState::Fetching);

        consumer.record_consumed("t", 0, 0).unwrap();
        assert_eq!(*consumer.state(), ConsumerProcessingState::Processing);

        consumer.commit_offsets().unwrap();
        assert_eq!(*consumer.state(), ConsumerProcessingState::Idle);
    }

    // === ExactlyOnceCoordinator Tests ===

    #[test]
    fn test_eos_coordinator_produce() {
        let mut eos = ExactlyOnceCoordinator::new(100);
        let pid = ProducerId::new(1);
        let epoch = eos.init_producer(pid);

        eos.validate_produce(pid, epoch, "orders", 0, 0).unwrap();
        eos.validate_produce(pid, epoch, "orders", 0, 1).unwrap();

        // Duplicate
        assert!(eos.validate_produce(pid, epoch, "orders", 0, 0).is_err());
        assert_eq!(eos.stats().duplicates_detected, 1);
    }

    #[test]
    fn test_eos_coordinator_consume_transform_produce() {
        let mut eos = ExactlyOnceCoordinator::new(100);

        eos.register_consumer("c1", "group-1");
        let tx_id = TransactionId::new();

        // Begin consume-transform-produce
        eos.begin_consume_transform_produce("c1", tx_id).unwrap();

        // Record consumed messages
        let consumer = eos.consumers.get_mut("c1").unwrap();
        consumer.record_consumed("input-topic", 0, 50).unwrap();

        // Commit
        let offsets = eos.commit_consume_transform("c1").unwrap();
        assert_eq!(offsets.get(&("input-topic".to_string(), 0)), Some(&50));
        assert_eq!(eos.stats().transactions_committed, 1);
    }

    #[test]
    fn test_eos_coordinator_abort() {
        let mut eos = ExactlyOnceCoordinator::new(100);

        eos.register_consumer("c1", "group-1");
        let tx_id = TransactionId::new();

        eos.begin_consume_transform_produce("c1", tx_id).unwrap();
        eos.abort_consume_transform("c1").unwrap();

        assert_eq!(eos.stats().transactions_aborted, 1);
    }

    #[test]
    fn test_eos_fence_producer() {
        let mut eos = ExactlyOnceCoordinator::new(100);
        let pid = ProducerId::new(1);
        let epoch1 = eos.init_producer(pid);

        eos.validate_produce(pid, epoch1, "t", 0, 0).unwrap();

        // Fence
        let epoch2 = eos.fence_producer(pid);
        assert!(epoch2 > epoch1);

        // Old epoch rejected
        assert!(eos.validate_produce(pid, epoch1, "t", 0, 1).is_err());

        // New epoch, reset sequence
        eos.validate_produce(pid, epoch2, "t", 0, 0).unwrap();
    }

    #[test]
    fn test_eos_get_consumer() {
        let mut eos = ExactlyOnceCoordinator::new(100);
        eos.register_consumer("c1", "g1");

        assert!(eos.get_consumer("c1").is_some());
        assert!(eos.get_consumer("nope").is_none());
    }

    #[test]
    fn test_eos_unknown_consumer() {
        let mut eos = ExactlyOnceCoordinator::new(100);
        let tx = TransactionId::new();
        assert!(eos.begin_consume_transform_produce("nope", tx).is_err());
    }
}
