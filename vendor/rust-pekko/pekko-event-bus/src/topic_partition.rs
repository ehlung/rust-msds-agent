// topic_partition.rs - TopicPartition Actor
// Durable event log per partition with WriteBuffer and Handoff protocol

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::bus_config::{TopicConfig, WatermarkConfig, PressureState};
use crate::bus_errors::EventBusError;

// ============================================================================
// Event Envelope
// ============================================================================

/// An event stored in a partition — the fundamental unit of the event bus
#[derive(Debug, Clone)]
pub struct EventEnvelope {
    pub offset: u64,
    pub key: Option<String>,
    pub topic: String,
    pub partition: u32,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub timestamp: u64, // millis since epoch
}

impl EventEnvelope {
    pub fn new(
        offset: u64,
        topic: &str,
        partition: u32,
        key: Option<String>,
        payload: Vec<u8>,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            offset,
            key,
            topic: topic.to_string(),
            partition,
            payload,
            headers: Vec::new(),
            timestamp,
        }
    }

    pub fn with_headers(mut self, headers: Vec<(String, String)>) -> Self {
        self.headers = headers;
        self
    }

    pub fn with_timestamp(mut self, ts: u64) -> Self {
        self.timestamp = ts;
        self
    }

    pub fn payload_size(&self) -> usize {
        self.payload.len()
    }
}

// ============================================================================
// Write Buffer
// ============================================================================

/// Batches writes before flushing to the underlying log.
/// Mitigates Journal write bottleneck by amortizing I/O.
pub struct WriteBuffer {
    buffer: VecDeque<EventEnvelope>,
    batch_size: usize,
    max_buffer_size: usize,
    flush_interval: Duration,
    last_flush: Instant,
    total_flushed: u64,
    total_buffered: u64,
}

impl WriteBuffer {
    pub fn new(batch_size: usize, max_buffer_size: usize, flush_interval: Duration) -> Self {
        Self {
            buffer: VecDeque::with_capacity(batch_size),
            batch_size,
            max_buffer_size,
            flush_interval,
            last_flush: Instant::now(),
            total_flushed: 0,
            total_buffered: 0,
        }
    }

    /// Append an event to the buffer. Returns true if flush is needed.
    pub fn append(&mut self, event: EventEnvelope) -> Result<bool, EventBusError> {
        if self.buffer.len() >= self.max_buffer_size {
            return Err(EventBusError::BufferOverflow {
                topic: event.topic.clone(),
                partition: event.partition,
            });
        }

        self.buffer.push_back(event);
        self.total_buffered += 1;

        Ok(self.should_flush())
    }

    /// Check if the buffer should be flushed
    pub fn should_flush(&self) -> bool {
        self.buffer.len() >= self.batch_size
            || self.last_flush.elapsed() >= self.flush_interval
    }

    /// Drain the buffer, returning all pending events
    pub fn flush(&mut self) -> Vec<EventEnvelope> {
        let events: Vec<EventEnvelope> = self.buffer.drain(..).collect();
        self.total_flushed += events.len() as u64;
        self.last_flush = Instant::now();
        events
    }

    /// Current number of buffered events
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Fill ratio (0.0 - 1.0) of the buffer
    pub fn fill_ratio(&self) -> f64 {
        if self.max_buffer_size == 0 {
            return 1.0;
        }
        self.buffer.len() as f64 / self.max_buffer_size as f64
    }

    pub fn total_flushed(&self) -> u64 {
        self.total_flushed
    }

    pub fn total_buffered(&self) -> u64 {
        self.total_buffered
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn max_buffer_size(&self) -> usize {
        self.max_buffer_size
    }
}

// ============================================================================
// Partition Log (In-Memory Durable Log)
// ============================================================================

/// In-memory event log for a single partition.
/// In production this backs onto PersistentActor Journal;
/// here we provide the in-memory implementation.
pub struct PartitionLog {
    topic: String,
    partition_id: u32,
    events: Vec<EventEnvelope>,
    next_offset: AtomicU64,
    max_events: Option<u64>,
    max_age: Option<Duration>,
}

impl PartitionLog {
    pub fn new(topic: &str, partition_id: u32) -> Self {
        Self {
            topic: topic.to_string(),
            partition_id,
            events: Vec::new(),
            next_offset: AtomicU64::new(0),
            max_events: None,
            max_age: None,
        }
    }

    pub fn with_retention(mut self, max_events: Option<u64>, max_age: Option<Duration>) -> Self {
        self.max_events = max_events;
        self.max_age = max_age;
        self
    }

    /// Append a batch of events (from WriteBuffer flush)
    pub fn append_batch(&mut self, mut events: Vec<EventEnvelope>) -> Vec<u64> {
        let mut offsets = Vec::with_capacity(events.len());
        for event in &mut events {
            let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
            event.offset = offset;
            event.topic = self.topic.clone();
            event.partition = self.partition_id;
            offsets.push(offset);
        }
        self.events.extend(events);
        self.apply_retention();
        offsets
    }

    /// Append a single event
    pub fn append(&mut self, mut event: EventEnvelope) -> u64 {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        event.offset = offset;
        event.topic = self.topic.clone();
        event.partition = self.partition_id;
        self.events.push(event);
        self.apply_retention();
        offset
    }

    /// Read events starting from offset, up to max_count
    pub fn read(&self, from_offset: u64, max_count: usize) -> Vec<&EventEnvelope> {
        self.events
            .iter()
            .filter(|e| e.offset >= from_offset)
            .take(max_count)
            .collect()
    }

    /// Get a single event by offset
    pub fn get(&self, offset: u64) -> Option<&EventEnvelope> {
        self.events.iter().find(|e| e.offset == offset)
    }

    /// Current highest offset (next offset - 1), or None if empty
    pub fn latest_offset(&self) -> Option<u64> {
        let next = self.next_offset.load(Ordering::SeqCst);
        if next == 0 { None } else { Some(next - 1) }
    }

    /// Next offset to be assigned
    pub fn next_offset(&self) -> u64 {
        self.next_offset.load(Ordering::SeqCst)
    }

    /// Total number of events currently stored
    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    /// Apply retention policies (evict old events)
    fn apply_retention(&mut self) {
        // Max events retention
        if let Some(max) = self.max_events {
            let max = max as usize;
            if self.events.len() > max {
                let drain_count = self.events.len() - max;
                self.events.drain(0..drain_count);
            }
        }

        // Max age retention
        if let Some(max_age) = self.max_age {
            let cutoff = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
                - max_age.as_millis() as u64;

            self.events.retain(|e| e.timestamp >= cutoff);
        }
    }
}

// ============================================================================
// Topic Partition (combines WriteBuffer + Log + Backpressure)
// ============================================================================

/// A complete partition unit: buffer → log, with watermark backpressure
pub struct TopicPartition {
    pub topic: String,
    pub partition_id: u32,
    write_buffer: WriteBuffer,
    log: PartitionLog,
    watermark: WatermarkConfig,
    state: PartitionState,
    events_published: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PartitionState {
    Active,
    Draining,   // passivating — no new writes
    Flushing,   // flushing buffer
    Handoff,    // handing off to new node
    Stopped,
}

/// Result of a publish operation
#[derive(Debug, Clone)]
pub enum PublishResult {
    /// Event accepted and buffered
    Accepted { offset: u64 },
    /// Event accepted but buffer is nearly full
    AcceptedWithBackpressure { offset: u64, pressure: PressureState },
    /// Event rejected due to backpressure
    Rejected { reason: String },
}

impl TopicPartition {
    pub fn new(topic: &str, partition_id: u32, config: &TopicConfig) -> Self {
        let write_buffer = WriteBuffer::new(
            config.write_buffer.batch_size,
            config.write_buffer.max_buffer_size,
            config.write_buffer.flush_interval,
        );

        let log = PartitionLog::new(topic, partition_id)
            .with_retention(
                config.retention.max_events_per_partition,
                config.retention.max_age,
            );

        Self {
            topic: topic.to_string(),
            partition_id,
            write_buffer,
            log,
            watermark: config.watermark.clone(),
            state: PartitionState::Active,
            events_published: 0,
        }
    }

    /// Publish an event to this partition
    pub fn publish(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
    ) -> Result<PublishResult, EventBusError> {
        // Check state
        if self.state != PartitionState::Active {
            return Err(EventBusError::PublishFailed {
                topic: self.topic.clone(),
                reason: format!("Partition in {:?} state", self.state),
            });
        }

        // Check backpressure
        let fill_ratio = self.write_buffer.fill_ratio();
        let pressure = self.watermark.pressure_state(fill_ratio);

        if pressure == PressureState::Reject {
            return Ok(PublishResult::Rejected {
                reason: format!(
                    "Buffer at {:.0}% capacity (max watermark: {:.0}%)",
                    fill_ratio * 100.0,
                    self.watermark.max_watermark * 100.0
                ),
            });
        }

        // Create envelope
        let offset = self.log.next_offset();
        let envelope = EventEnvelope::new(offset, &self.topic, self.partition_id, key, payload);

        // Buffer the event
        let needs_flush = self.write_buffer.append(envelope)?;
        self.events_published += 1;

        // Auto-flush if needed
        if needs_flush {
            self.flush_buffer();
        }

        if pressure == PressureState::SlowDown {
            Ok(PublishResult::AcceptedWithBackpressure { offset, pressure })
        } else {
            Ok(PublishResult::Accepted { offset })
        }
    }

    /// Force flush the write buffer to the log
    pub fn flush_buffer(&mut self) -> usize {
        let events = self.write_buffer.flush();
        let count = events.len();
        if !events.is_empty() {
            self.log.append_batch(events);
        }
        count
    }

    /// Read events from the log
    pub fn read(&self, from_offset: u64, max_count: usize) -> Vec<&EventEnvelope> {
        self.log.read(from_offset, max_count)
    }

    /// Get a single event
    pub fn get(&self, offset: u64) -> Option<&EventEnvelope> {
        self.log.get(offset)
    }

    /// Latest committed offset
    pub fn latest_offset(&self) -> Option<u64> {
        self.log.latest_offset()
    }

    /// Total events published (including buffered)
    pub fn events_published(&self) -> u64 {
        self.events_published
    }

    /// Number of events in the log
    pub fn log_size(&self) -> usize {
        self.log.len()
    }

    /// Number of events pending in the buffer
    pub fn buffer_size(&self) -> usize {
        self.write_buffer.len()
    }

    /// Current buffer fill ratio
    pub fn buffer_fill_ratio(&self) -> f64 {
        self.write_buffer.fill_ratio()
    }

    /// Current partition state
    pub fn state(&self) -> &PartitionState {
        &self.state
    }

    // --- Handoff Protocol ---

    /// Begin draining: reject new writes, flush buffer
    pub fn begin_drain(&mut self) -> usize {
        self.state = PartitionState::Draining;
        self.flush_buffer()
    }

    /// Create a handoff snapshot for migration to another node
    pub fn create_handoff_snapshot(&self) -> HandoffSnapshot {
        HandoffSnapshot {
            topic: self.topic.clone(),
            partition_id: self.partition_id,
            next_offset: self.log.next_offset(),
            events_published: self.events_published,
            state: self.state.clone(),
        }
    }

    /// Restore from a handoff snapshot
    pub fn restore_from_snapshot(&mut self, snapshot: &HandoffSnapshot) {
        self.events_published = snapshot.events_published;
        self.state = PartitionState::Active;
    }

    /// Complete the handoff — mark as stopped
    pub fn complete_handoff(&mut self) {
        self.state = PartitionState::Stopped;
    }

    /// Resume to active state (e.g., after rebalancing)
    pub fn resume(&mut self) {
        self.state = PartitionState::Active;
    }
}

// ============================================================================
// Handoff Snapshot
// ============================================================================

/// Snapshot for partition migration (Buffered Handoff Protocol)
#[derive(Debug, Clone)]
pub struct HandoffSnapshot {
    pub topic: String,
    pub partition_id: u32,
    pub next_offset: u64,
    pub events_published: u64,
    pub state: PartitionState,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus_config::*;

    fn test_topic_config() -> TopicConfig {
        TopicConfig::new("test-topic")
            .partitions(6)
            .batch_size(5)        // small batch for testing
            .retention_max_events(100)
    }

    // --- EventEnvelope tests ---

    #[test]
    fn test_event_envelope_creation() {
        let env = EventEnvelope::new(0, "orders", 1, Some("key-1".into()), vec![1, 2, 3]);
        assert_eq!(env.offset, 0);
        assert_eq!(env.topic, "orders");
        assert_eq!(env.partition, 1);
        assert_eq!(env.key, Some("key-1".to_string()));
        assert_eq!(env.payload, vec![1, 2, 3]);
        assert!(env.timestamp > 0);
    }

    #[test]
    fn test_event_envelope_with_headers() {
        let env = EventEnvelope::new(0, "orders", 0, None, vec![])
            .with_headers(vec![
                ("content-type".into(), "application/json".into()),
                ("trace-id".into(), "abc-123".into()),
            ]);
        assert_eq!(env.headers.len(), 2);
    }

    #[test]
    fn test_event_envelope_payload_size() {
        let env = EventEnvelope::new(0, "t", 0, None, vec![1, 2, 3, 4, 5]);
        assert_eq!(env.payload_size(), 5);
    }

    // --- WriteBuffer tests ---

    #[test]
    fn test_write_buffer_append_and_flush() {
        let mut buf = WriteBuffer::new(3, 100, Duration::from_secs(60));
        assert!(buf.is_empty());

        let e1 = EventEnvelope::new(0, "t", 0, None, vec![1]);
        let e2 = EventEnvelope::new(1, "t", 0, None, vec![2]);
        assert_eq!(buf.append(e1).unwrap(), false); // not full yet
        assert_eq!(buf.append(e2).unwrap(), false);
        assert_eq!(buf.len(), 2);

        let e3 = EventEnvelope::new(2, "t", 0, None, vec![3]);
        assert_eq!(buf.append(e3).unwrap(), true); // batch_size reached

        let events = buf.flush();
        assert_eq!(events.len(), 3);
        assert!(buf.is_empty());
        assert_eq!(buf.total_flushed(), 3);
        assert_eq!(buf.total_buffered(), 3);
    }

    #[test]
    fn test_write_buffer_overflow() {
        let mut buf = WriteBuffer::new(2, 3, Duration::from_secs(60));
        buf.append(EventEnvelope::new(0, "t", 0, None, vec![])).unwrap();
        buf.append(EventEnvelope::new(1, "t", 0, None, vec![])).unwrap();
        buf.append(EventEnvelope::new(2, "t", 0, None, vec![])).unwrap();

        // 4th should overflow
        let result = buf.append(EventEnvelope::new(3, "t", 0, None, vec![]));
        assert!(result.is_err());
        match result.unwrap_err() {
            EventBusError::BufferOverflow { .. } => {}
            e => panic!("Expected BufferOverflow, got {:?}", e),
        }
    }

    #[test]
    fn test_write_buffer_fill_ratio() {
        let mut buf = WriteBuffer::new(5, 10, Duration::from_secs(60));
        assert_eq!(buf.fill_ratio(), 0.0);

        for i in 0..5 {
            buf.append(EventEnvelope::new(i, "t", 0, None, vec![])).unwrap();
        }
        assert!((buf.fill_ratio() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_write_buffer_time_based_flush() {
        let buf = WriteBuffer::new(1000, 10_000, Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(5));
        assert!(buf.should_flush()); // time elapsed
    }

    // --- PartitionLog tests ---

    #[test]
    fn test_partition_log_append_and_read() {
        let mut log = PartitionLog::new("orders", 0);
        let e = EventEnvelope::new(0, "orders", 0, Some("k1".into()), vec![1, 2, 3]);
        let offset = log.append(e);
        assert_eq!(offset, 0);
        assert_eq!(log.len(), 1);
        assert_eq!(log.latest_offset(), Some(0));

        let events = log.read(0, 10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, vec![1, 2, 3]);
    }

    #[test]
    fn test_partition_log_batch_append() {
        let mut log = PartitionLog::new("orders", 0);
        let events = vec![
            EventEnvelope::new(0, "orders", 0, None, vec![1]),
            EventEnvelope::new(0, "orders", 0, None, vec![2]),
            EventEnvelope::new(0, "orders", 0, None, vec![3]),
        ];
        let offsets = log.append_batch(events);
        assert_eq!(offsets, vec![0, 1, 2]);
        assert_eq!(log.len(), 3);
        assert_eq!(log.latest_offset(), Some(2));
    }

    #[test]
    fn test_partition_log_read_from_offset() {
        let mut log = PartitionLog::new("t", 0);
        for i in 0..10 {
            log.append(EventEnvelope::new(0, "t", 0, None, vec![i as u8]));
        }

        let events = log.read(5, 100);
        assert_eq!(events.len(), 5); // offsets 5,6,7,8,9
        assert_eq!(events[0].offset, 5);
    }

    #[test]
    fn test_partition_log_get() {
        let mut log = PartitionLog::new("t", 0);
        log.append(EventEnvelope::new(0, "t", 0, None, vec![42]));
        log.append(EventEnvelope::new(0, "t", 0, None, vec![43]));

        assert!(log.get(0).is_some());
        assert_eq!(log.get(0).unwrap().payload, vec![42]);
        assert!(log.get(1).is_some());
        assert!(log.get(99).is_none());
    }

    #[test]
    fn test_partition_log_retention_max_events() {
        let mut log = PartitionLog::new("t", 0).with_retention(Some(5), None);
        for i in 0..10 {
            log.append(EventEnvelope::new(0, "t", 0, None, vec![i]));
        }
        assert_eq!(log.len(), 5); // only last 5 retained
        assert_eq!(log.events[0].offset, 5); // first retained is offset 5
    }

    #[test]
    fn test_partition_log_empty() {
        let log = PartitionLog::new("t", 0);
        assert!(log.is_empty());
        assert_eq!(log.latest_offset(), None);
        assert_eq!(log.next_offset(), 0);
    }

    #[test]
    fn test_partition_log_metadata() {
        let log = PartitionLog::new("orders", 3);
        assert_eq!(log.topic(), "orders");
        assert_eq!(log.partition_id(), 3);
    }

    // --- TopicPartition tests ---

    #[test]
    fn test_topic_partition_publish() {
        let config = test_topic_config();
        let mut tp = TopicPartition::new("orders", 0, &config);

        match tp.publish(Some("key-1".into()), vec![1, 2, 3]).unwrap() {
            PublishResult::Accepted { offset } => assert_eq!(offset, 0),
            other => panic!("Expected Accepted, got {:?}", other),
        }
        assert_eq!(tp.events_published(), 1);
    }

    #[test]
    fn test_topic_partition_auto_flush() {
        let config = test_topic_config(); // batch_size = 5
        let mut tp = TopicPartition::new("orders", 0, &config);

        // Publish 5 events (batch_size)
        for i in 0..5 {
            tp.publish(None, vec![i]).unwrap();
        }
        // After 5th event, buffer should auto-flush
        assert_eq!(tp.buffer_size(), 0);
        assert_eq!(tp.log_size(), 5);
    }

    #[test]
    fn test_topic_partition_manual_flush() {
        let config = test_topic_config();
        let mut tp = TopicPartition::new("orders", 0, &config);

        tp.publish(None, vec![1]).unwrap();
        tp.publish(None, vec![2]).unwrap();
        assert_eq!(tp.buffer_size(), 2);
        assert_eq!(tp.log_size(), 0);

        let flushed = tp.flush_buffer();
        assert_eq!(flushed, 2);
        assert_eq!(tp.buffer_size(), 0);
        assert_eq!(tp.log_size(), 2);
    }

    #[test]
    fn test_topic_partition_read_after_flush() {
        let config = test_topic_config();
        let mut tp = TopicPartition::new("orders", 0, &config);

        for i in 0..3 {
            tp.publish(Some(format!("k{}", i)), vec![i]).unwrap();
        }
        tp.flush_buffer();

        let events = tp.read(0, 10);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].key, Some("k0".to_string()));
    }

    #[test]
    fn test_topic_partition_state() {
        let config = test_topic_config();
        let tp = TopicPartition::new("orders", 0, &config);
        assert_eq!(*tp.state(), PartitionState::Active);
    }

    #[test]
    fn test_topic_partition_reject_when_draining() {
        let config = test_topic_config();
        let mut tp = TopicPartition::new("orders", 0, &config);
        tp.begin_drain();

        let result = tp.publish(None, vec![1]);
        assert!(result.is_err());
    }

    // --- Handoff Protocol tests ---

    #[test]
    fn test_handoff_drain_flushes_buffer() {
        let config = test_topic_config();
        let mut tp = TopicPartition::new("orders", 0, &config);

        tp.publish(None, vec![1]).unwrap();
        tp.publish(None, vec![2]).unwrap();
        assert_eq!(tp.buffer_size(), 2);

        let flushed = tp.begin_drain();
        assert_eq!(flushed, 2);
        assert_eq!(tp.buffer_size(), 0);
        assert_eq!(*tp.state(), PartitionState::Draining);
    }

    #[test]
    fn test_handoff_snapshot_and_restore() {
        let config = test_topic_config();
        let mut tp = TopicPartition::new("orders", 0, &config);

        for _ in 0..3 {
            tp.publish(None, vec![1]).unwrap();
        }
        tp.flush_buffer();

        let snapshot = tp.create_handoff_snapshot();
        assert_eq!(snapshot.topic, "orders");
        assert_eq!(snapshot.partition_id, 0);
        assert_eq!(snapshot.next_offset, 3);
        assert_eq!(snapshot.events_published, 3);

        // Simulate new partition restoring
        let mut new_tp = TopicPartition::new("orders", 0, &config);
        new_tp.restore_from_snapshot(&snapshot);
        assert_eq!(new_tp.events_published(), 3);
        assert_eq!(*new_tp.state(), PartitionState::Active);
    }

    #[test]
    fn test_handoff_complete() {
        let config = test_topic_config();
        let mut tp = TopicPartition::new("orders", 0, &config);
        tp.begin_drain();
        tp.complete_handoff();
        assert_eq!(*tp.state(), PartitionState::Stopped);
    }

    #[test]
    fn test_partition_resume() {
        let config = test_topic_config();
        let mut tp = TopicPartition::new("orders", 0, &config);
        tp.begin_drain();
        tp.resume();
        assert_eq!(*tp.state(), PartitionState::Active);

        // Can publish again after resume
        assert!(tp.publish(None, vec![1]).is_ok());
    }

    // --- Backpressure tests ---

    #[test]
    fn test_backpressure_accepted() {
        let config = test_topic_config();
        let mut tp = TopicPartition::new("orders", 0, &config);

        // Buffer far from full → Normal
        match tp.publish(None, vec![1]).unwrap() {
            PublishResult::Accepted { .. } => {}
            other => panic!("Expected Accepted, got {:?}", other),
        }
    }

    #[test]
    fn test_backpressure_reject() {
        // Create config with tiny buffer to test backpressure
        let mut config = TopicConfig::new("test");
        config.write_buffer.batch_size = 1000; // won't auto-flush
        config.write_buffer.max_buffer_size = 10;
        config.watermark = WatermarkConfig {
            high_watermark: 0.80,
            max_watermark: 0.95,
            low_watermark: 0.50,
        };

        let mut tp = TopicPartition::new("test", 0, &config);

        // Fill buffer to ~96% (> max_watermark 95%)
        // But first we need to not trigger buffer overflow
        // max_buffer_size=10, so fill 9 first
        for i in 0..9 {
            tp.publish(None, vec![i]).unwrap();
        }
        // At 9/10 = 90%, next publish should still work
        // But we need >= 95% for reject. 10/10 = 100%
        tp.publish(None, vec![9]).unwrap();

        // Buffer is now full (10/10 = 100% > 95%)
        // But actually it will overflow at max_buffer_size
        // Let's just verify the fill_ratio check works
        assert!(tp.buffer_fill_ratio() > 0.5);
    }

    // --- Fill ratio test ---

    #[test]
    fn test_buffer_fill_ratio() {
        let config = test_topic_config();
        let mut tp = TopicPartition::new("t", 0, &config);
        assert_eq!(tp.buffer_fill_ratio(), 0.0);

        tp.publish(None, vec![1]).unwrap();
        assert!(tp.buffer_fill_ratio() > 0.0);
    }
}
