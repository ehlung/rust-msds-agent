// stream_integration.rs - Pekko-Streams Integration (Phase 3)
// EventSource, EventSink, StreamConsumerGroup for bridging EventBus ↔ Streams

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::bus_errors::EventBusError;
use crate::topic_partition::{EventEnvelope};

// ============================================================================
// Stream Position (Offset Tracking for Streams)
// ============================================================================

/// Where to start reading from a partition
#[derive(Debug, Clone, PartialEq)]
pub enum StartPosition {
    /// From the beginning (offset 0)
    Earliest,
    /// From the latest available offset
    Latest,
    /// From a specific offset
    Offset(u64),
    /// From a specific timestamp (find nearest offset)
    Timestamp(u64),
}

impl std::fmt::Display for StartPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartPosition::Earliest => write!(f, "Earliest"),
            StartPosition::Latest => write!(f, "Latest"),
            StartPosition::Offset(o) => write!(f, "Offset({})", o),
            StartPosition::Timestamp(ts) => write!(f, "Timestamp({})", ts),
        }
    }
}

// ============================================================================
// Event Source (Partition → Stream)
// ============================================================================

/// Configuration for an EventSource that reads from a partition
#[derive(Debug, Clone)]
pub struct EventSourceConfig {
    pub topic: String,
    pub partition_id: u32,
    pub start_position: StartPosition,
    pub max_buffer_size: usize,
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub auto_commit: bool,
    pub commit_interval: Duration,
}

impl EventSourceConfig {
    pub fn new(topic: &str, partition_id: u32) -> Self {
        Self {
            topic: topic.to_string(),
            partition_id,
            start_position: StartPosition::Latest,
            max_buffer_size: 1000,
            poll_interval: Duration::from_millis(100),
            batch_size: 100,
            auto_commit: true,
            commit_interval: Duration::from_secs(5),
        }
    }

    pub fn from_earliest(mut self) -> Self {
        self.start_position = StartPosition::Earliest;
        self
    }

    pub fn from_offset(mut self, offset: u64) -> Self {
        self.start_position = StartPosition::Offset(offset);
        self
    }

    pub fn from_timestamp(mut self, ts: u64) -> Self {
        self.start_position = StartPosition::Timestamp(ts);
        self
    }

    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn manual_commit(mut self) -> Self {
        self.auto_commit = false;
        self
    }
}

/// A stream source that reads events from a partition with backpressure
pub struct EventSource {
    config: EventSourceConfig,
    buffer: VecDeque<EventEnvelope>,
    current_offset: u64,
    committed_offset: Option<u64>,
    state: SourceState,
    created_at: Instant,
    last_poll: Option<Instant>,
    last_commit: Option<Instant>,
    events_emitted: u64,
    polls_count: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SourceState {
    /// Ready to emit events
    Active,
    /// Paused (backpressure from downstream)
    Paused,
    /// Reached end of current data (will resume when new data arrives)
    WaitingForData,
    /// Completed (end-of-stream signal received)
    Completed,
    /// Error state
    Error(String),
}

impl std::fmt::Display for SourceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceState::Active => write!(f, "Active"),
            SourceState::Paused => write!(f, "Paused"),
            SourceState::WaitingForData => write!(f, "WaitingForData"),
            SourceState::Completed => write!(f, "Completed"),
            SourceState::Error(e) => write!(f, "Error({})", e),
        }
    }
}

impl EventSource {
    pub fn new(config: EventSourceConfig) -> Self {
        let initial_offset = match &config.start_position {
            StartPosition::Offset(o) => *o,
            StartPosition::Earliest => 0,
            _ => 0, // Latest/Timestamp resolved at poll time
        };

        Self {
            config,
            buffer: VecDeque::new(),
            current_offset: initial_offset,
            committed_offset: None,
            state: SourceState::Active,
            created_at: Instant::now(),
            last_poll: None,
            last_commit: None,
            events_emitted: 0,
            polls_count: 0,
        }
    }

    pub fn config(&self) -> &EventSourceConfig {
        &self.config
    }

    pub fn state(&self) -> &SourceState {
        &self.state
    }

    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    pub fn committed_offset(&self) -> Option<u64> {
        self.committed_offset
    }

    pub fn events_emitted(&self) -> u64 {
        self.events_emitted
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_active(&self) -> bool {
        self.state == SourceState::Active
    }

    /// Feed events into the source buffer (simulates poll from partition)
    pub fn feed_events(&mut self, events: Vec<EventEnvelope>) -> Result<usize, EventBusError> {
        if self.state == SourceState::Completed {
            return Err(EventBusError::Internal("Source is completed".into()));
        }

        let available = self.config.max_buffer_size.saturating_sub(self.buffer.len());
        let to_add = events.len().min(available);

        for event in events.into_iter().take(to_add) {
            self.current_offset = event.offset;
            self.buffer.push_back(event);
        }

        self.polls_count += 1;
        self.last_poll = Some(Instant::now());

        if self.buffer.is_empty() {
            self.state = SourceState::WaitingForData;
        } else {
            self.state = SourceState::Active;
        }

        Ok(to_add)
    }

    /// Pull next batch of events from the source
    pub fn pull(&mut self, max_count: usize) -> Vec<EventEnvelope> {
        if self.state == SourceState::Completed || self.state == SourceState::Paused {
            return Vec::new();
        }

        let count = max_count.min(self.buffer.len());
        let batch: Vec<EventEnvelope> = self.buffer.drain(..count).collect();
        self.events_emitted += batch.len() as u64;

        if self.buffer.is_empty() {
            self.state = SourceState::WaitingForData;
        }

        batch
    }

    /// Commit the current offset
    pub fn commit(&mut self) -> Option<u64> {
        if self.events_emitted > 0 {
            self.committed_offset = Some(self.current_offset);
            self.last_commit = Some(Instant::now());
            self.committed_offset
        } else {
            None
        }
    }

    /// Check if auto-commit is due
    pub fn should_auto_commit(&self) -> bool {
        if !self.config.auto_commit {
            return false;
        }
        match self.last_commit {
            Some(last) => last.elapsed() >= self.config.commit_interval,
            None => self.events_emitted > 0,
        }
    }

    /// Pause the source (backpressure)
    pub fn pause(&mut self) {
        if self.state == SourceState::Active || self.state == SourceState::WaitingForData {
            self.state = SourceState::Paused;
        }
    }

    /// Resume the source
    pub fn resume(&mut self) {
        if self.state == SourceState::Paused {
            self.state = if self.buffer.is_empty() {
                SourceState::WaitingForData
            } else {
                SourceState::Active
            };
        }
    }

    /// Complete the source (no more events)
    pub fn complete(&mut self) {
        self.state = SourceState::Completed;
    }

    /// Set error state
    pub fn set_error(&mut self, msg: &str) {
        self.state = SourceState::Error(msg.to_string());
    }

    /// Lag: difference between current partition end and committed offset
    pub fn uncommitted_count(&self) -> u64 {
        match self.committed_offset {
            Some(committed) => self.current_offset.saturating_sub(committed),
            None => self.current_offset,
        }
    }

    /// Uptime
    pub fn uptime(&self) -> Duration {
        self.created_at.elapsed()
    }
}

impl std::fmt::Display for EventSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EventSource({}/{}, offset={}, committed={:?}, state={}, emitted={}, buffer={})",
            self.config.topic, self.config.partition_id,
            self.current_offset, self.committed_offset,
            self.state, self.events_emitted, self.buffer.len()
        )
    }
}

// ============================================================================
// Event Sink (Stream → Partition)
// ============================================================================

/// Configuration for an EventSink that writes to a partition
#[derive(Debug, Clone)]
pub struct EventSinkConfig {
    pub topic: String,
    pub max_in_flight: usize,
    pub batch_size: usize,
    pub linger_ms: Duration,
    pub retry_on_failure: bool,
    pub max_retries: u32,
}

impl EventSinkConfig {
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            max_in_flight: 100,
            batch_size: 50,
            linger_ms: Duration::from_millis(5),
            retry_on_failure: true,
            max_retries: 3,
        }
    }

    pub fn with_max_in_flight(mut self, max: usize) -> Self {
        self.max_in_flight = max;
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_linger(mut self, linger: Duration) -> Self {
        self.linger_ms = linger;
        self
    }

    pub fn no_retry(mut self) -> Self {
        self.retry_on_failure = false;
        self
    }
}

/// Sink state tracking
#[derive(Debug, Clone, PartialEq)]
pub enum SinkState {
    Active,
    Backpressured,
    Flushing,
    Closed,
    Error(String),
}

impl std::fmt::Display for SinkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkState::Active => write!(f, "Active"),
            SinkState::Backpressured => write!(f, "Backpressured"),
            SinkState::Flushing => write!(f, "Flushing"),
            SinkState::Closed => write!(f, "Closed"),
            SinkState::Error(e) => write!(f, "Error({})", e),
        }
    }
}

/// A stream sink that writes events to the event bus with batching
pub struct EventSink {
    config: EventSinkConfig,
    pending: VecDeque<EventEnvelope>,
    state: SinkState,
    events_accepted: u64,
    events_published: u64,
    events_failed: u64,
    batches_flushed: u64,
    created_at: Instant,
    last_flush: Option<Instant>,
}

impl EventSink {
    pub fn new(config: EventSinkConfig) -> Self {
        Self {
            config,
            pending: VecDeque::new(),
            state: SinkState::Active,
            events_accepted: 0,
            events_published: 0,
            events_failed: 0,
            batches_flushed: 0,
            created_at: Instant::now(),
            last_flush: None,
        }
    }

    pub fn config(&self) -> &EventSinkConfig {
        &self.config
    }

    pub fn state(&self) -> &SinkState {
        &self.state
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn events_accepted(&self) -> u64 {
        self.events_accepted
    }

    pub fn events_published(&self) -> u64 {
        self.events_published
    }

    pub fn events_failed(&self) -> u64 {
        self.events_failed
    }

    pub fn is_active(&self) -> bool {
        self.state == SinkState::Active
    }

    /// Accept an event into the sink buffer
    pub fn accept(&mut self, event: EventEnvelope) -> Result<(), EventBusError> {
        if self.state == SinkState::Closed {
            return Err(EventBusError::Internal("Sink is closed".into()));
        }

        if self.pending.len() >= self.config.max_in_flight {
            self.state = SinkState::Backpressured;
            return Err(EventBusError::BufferOverflow {
                topic: self.config.topic.clone(),
                partition: 0,
            });
        }

        self.pending.push_back(event);
        self.events_accepted += 1;
        Ok(())
    }

    /// Check if flush is needed (batch full or linger timeout)
    pub fn should_flush(&self) -> bool {
        if self.pending.is_empty() {
            return false;
        }
        if self.pending.len() >= self.config.batch_size {
            return true;
        }
        match self.last_flush {
            Some(last) => last.elapsed() >= self.config.linger_ms,
            None => true, // first batch, flush immediately
        }
    }

    /// Drain pending events for publishing (returns batch)
    pub fn drain_batch(&mut self) -> Vec<EventEnvelope> {
        let count = self.config.batch_size.min(self.pending.len());
        let batch: Vec<EventEnvelope> = self.pending.drain(..count).collect();

        if !batch.is_empty() {
            self.state = SinkState::Flushing;
        }

        batch
    }

    /// Acknowledge successful publish of a batch
    pub fn ack_batch(&mut self, count: u64) {
        self.events_published += count;
        self.batches_flushed += 1;
        self.last_flush = Some(Instant::now());

        if self.pending.len() < self.config.max_in_flight {
            self.state = SinkState::Active;
        }
    }

    /// Report batch failure
    pub fn nack_batch(&mut self, count: u64, error: &str) {
        self.events_failed += count;
        if !self.config.retry_on_failure {
            self.state = SinkState::Error(error.to_string());
        } else {
            self.state = SinkState::Active;
        }
    }

    /// Close the sink
    pub fn close(&mut self) {
        self.state = SinkState::Closed;
    }

    /// Uptime
    pub fn uptime(&self) -> Duration {
        self.created_at.elapsed()
    }
}

impl std::fmt::Display for EventSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EventSink(topic={}, state={}, pending={}, published={}, failed={})",
            self.config.topic, self.state,
            self.pending.len(), self.events_published, self.events_failed
        )
    }
}

// ============================================================================
// Stream Consumer Group
// ============================================================================

/// Bridges a consumer group with stream processing
pub struct StreamConsumerGroup {
    group_id: String,
    sources: HashMap<(String, u32), EventSource>,
    sink: Option<EventSink>,
    processing_state: StreamProcessingState,
    events_processed: u64,
    errors: Vec<StreamError>,
    started_at: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StreamProcessingState {
    Idle,
    Running,
    Rebalancing,
    Stopped,
    Failed(String),
}

impl std::fmt::Display for StreamProcessingState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamProcessingState::Idle => write!(f, "Idle"),
            StreamProcessingState::Running => write!(f, "Running"),
            StreamProcessingState::Rebalancing => write!(f, "Rebalancing"),
            StreamProcessingState::Stopped => write!(f, "Stopped"),
            StreamProcessingState::Failed(e) => write!(f, "Failed({})", e),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamError {
    pub source: String,
    pub message: String,
    pub timestamp: Instant,
    pub recoverable: bool,
}

impl StreamConsumerGroup {
    pub fn new(group_id: &str) -> Self {
        Self {
            group_id: group_id.to_string(),
            sources: HashMap::new(),
            sink: None,
            processing_state: StreamProcessingState::Idle,
            events_processed: 0,
            errors: Vec::new(),
            started_at: None,
        }
    }

    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    pub fn state(&self) -> &StreamProcessingState {
        &self.processing_state
    }

    pub fn events_processed(&self) -> u64 {
        self.events_processed
    }

    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    pub fn has_sink(&self) -> bool {
        self.sink.is_some()
    }

    /// Add a source for a topic-partition
    pub fn add_source(&mut self, config: EventSourceConfig) {
        let key = (config.topic.clone(), config.partition_id);
        self.sources.insert(key, EventSource::new(config));
    }

    /// Remove a source (partition revoked)
    pub fn remove_source(&mut self, topic: &str, partition_id: u32) -> Option<EventSource> {
        self.sources.remove(&(topic.to_string(), partition_id))
    }

    /// Set the output sink
    pub fn set_sink(&mut self, config: EventSinkConfig) {
        self.sink = Some(EventSink::new(config));
    }

    /// Get source for a specific partition
    pub fn source(&self, topic: &str, partition_id: u32) -> Option<&EventSource> {
        self.sources.get(&(topic.to_string(), partition_id))
    }

    /// Get mutable source
    pub fn source_mut(&mut self, topic: &str, partition_id: u32) -> Option<&mut EventSource> {
        self.sources.get_mut(&(topic.to_string(), partition_id))
    }

    /// Get sink reference
    pub fn sink(&self) -> Option<&EventSink> {
        self.sink.as_ref()
    }

    /// Start processing
    pub fn start(&mut self) {
        self.processing_state = StreamProcessingState::Running;
        self.started_at = Some(Instant::now());
    }

    /// Pause for rebalancing
    pub fn pause_for_rebalance(&mut self) {
        self.processing_state = StreamProcessingState::Rebalancing;
        for source in self.sources.values_mut() {
            source.pause();
        }
    }

    /// Resume after rebalancing
    pub fn resume_after_rebalance(&mut self) {
        self.processing_state = StreamProcessingState::Running;
        for source in self.sources.values_mut() {
            source.resume();
        }
    }

    /// Stop processing
    pub fn stop(&mut self) {
        self.processing_state = StreamProcessingState::Stopped;
        for source in self.sources.values_mut() {
            source.complete();
        }
        if let Some(sink) = &mut self.sink {
            sink.close();
        }
    }

    /// Pull events from all active sources
    pub fn pull_all(&mut self, max_per_source: usize) -> Vec<EventEnvelope> {
        if self.processing_state != StreamProcessingState::Running {
            return Vec::new();
        }

        let mut all_events = Vec::new();
        for source in self.sources.values_mut() {
            let batch = source.pull(max_per_source);
            all_events.extend(batch);
        }
        self.events_processed += all_events.len() as u64;
        all_events
    }

    /// Commit offsets for all sources
    pub fn commit_all(&mut self) -> Vec<(String, u32, u64)> {
        let mut commits = Vec::new();
        for ((topic, partition_id), source) in &mut self.sources {
            if let Some(offset) = source.commit() {
                commits.push((topic.clone(), *partition_id, offset));
            }
        }
        commits
    }

    /// Record an error
    pub fn record_error(&mut self, source: &str, message: &str, recoverable: bool) {
        self.errors.push(StreamError {
            source: source.to_string(),
            message: message.to_string(),
            timestamp: Instant::now(),
            recoverable,
        });

        if !recoverable {
            self.processing_state = StreamProcessingState::Failed(message.to_string());
        }

        // Keep only last 100 errors
        if self.errors.len() > 100 {
            self.errors.drain(0..self.errors.len() - 100);
        }
    }

    pub fn recent_errors(&self) -> &[StreamError] {
        &self.errors
    }

    /// Overall health summary
    pub fn health_summary(&self) -> StreamHealthSummary {
        let active_sources = self.sources.values()
            .filter(|s| s.is_active())
            .count();
        let total_buffered: usize = self.sources.values()
            .map(|s| s.buffer_size())
            .sum();
        let total_uncommitted: u64 = self.sources.values()
            .map(|s| s.uncommitted_count())
            .sum();

        StreamHealthSummary {
            group_id: self.group_id.clone(),
            state: self.processing_state.clone(),
            total_sources: self.sources.len(),
            active_sources,
            total_buffered,
            total_uncommitted,
            events_processed: self.events_processed,
            error_count: self.errors.len(),
            has_sink: self.sink.is_some(),
            uptime: self.started_at.map(|t| t.elapsed()),
        }
    }
}

impl std::fmt::Display for StreamConsumerGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StreamConsumerGroup(group={}, state={}, sources={}, processed={})",
            self.group_id, self.processing_state,
            self.sources.len(), self.events_processed
        )
    }
}

/// Health summary for monitoring
#[derive(Debug, Clone)]
pub struct StreamHealthSummary {
    pub group_id: String,
    pub state: StreamProcessingState,
    pub total_sources: usize,
    pub active_sources: usize,
    pub total_buffered: usize,
    pub total_uncommitted: u64,
    pub events_processed: u64,
    pub error_count: usize,
    pub has_sink: bool,
    pub uptime: Option<Duration>,
}

impl std::fmt::Display for StreamHealthSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Health(group={}, state={}, sources={}/{}, buffered={}, uncommitted={}, processed={}, errors={})",
            self.group_id, self.state,
            self.active_sources, self.total_sources,
            self.total_buffered, self.total_uncommitted,
            self.events_processed, self.error_count
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_envelope(topic: &str, partition: u32, offset: u64) -> EventEnvelope {
        EventEnvelope {
            offset,
            key: Some(format!("key-{}", offset)),
            topic: topic.to_string(),
            partition,
            payload: vec![offset as u8; 10],
            headers: vec![],
            timestamp: 1000 + offset,
        }
    }

    fn make_envelopes(topic: &str, partition: u32, start: u64, count: u64) -> Vec<EventEnvelope> {
        (start..start + count).map(|i| make_envelope(topic, partition, i)).collect()
    }

    // --- StartPosition ---

    #[test]
    fn test_start_position_display() {
        assert_eq!(StartPosition::Earliest.to_string(), "Earliest");
        assert_eq!(StartPosition::Latest.to_string(), "Latest");
        assert_eq!(StartPosition::Offset(42).to_string(), "Offset(42)");
    }

    // --- EventSourceConfig ---

    #[test]
    fn test_source_config_builder() {
        let config = EventSourceConfig::new("orders", 0)
            .from_earliest()
            .with_buffer_size(500)
            .with_batch_size(50)
            .manual_commit();

        assert_eq!(config.topic, "orders");
        assert_eq!(config.partition_id, 0);
        assert_eq!(config.start_position, StartPosition::Earliest);
        assert_eq!(config.max_buffer_size, 500);
        assert!(!config.auto_commit);
    }

    // --- EventSource ---

    #[test]
    fn test_source_new() {
        let config = EventSourceConfig::new("orders", 0);
        let source = EventSource::new(config);
        assert!(source.is_active());
        assert_eq!(source.events_emitted(), 0);
        assert_eq!(source.buffer_size(), 0);
    }

    #[test]
    fn test_source_feed_and_pull() {
        let config = EventSourceConfig::new("orders", 0).from_earliest();
        let mut source = EventSource::new(config);

        let events = make_envelopes("orders", 0, 0, 5);
        let fed = source.feed_events(events).unwrap();
        assert_eq!(fed, 5);
        assert_eq!(source.buffer_size(), 5);
        assert_eq!(source.state(), &SourceState::Active);

        let pulled = source.pull(3);
        assert_eq!(pulled.len(), 3);
        assert_eq!(source.buffer_size(), 2);
        assert_eq!(source.events_emitted(), 3);
    }

    #[test]
    fn test_source_buffer_limit() {
        let config = EventSourceConfig::new("orders", 0)
            .from_earliest()
            .with_buffer_size(3);
        let mut source = EventSource::new(config);

        let events = make_envelopes("orders", 0, 0, 5);
        let fed = source.feed_events(events).unwrap();
        assert_eq!(fed, 3); // capped at buffer size
    }

    #[test]
    fn test_source_commit() {
        let config = EventSourceConfig::new("orders", 0).from_earliest();
        let mut source = EventSource::new(config);

        let events = make_envelopes("orders", 0, 0, 3);
        source.feed_events(events).unwrap();
        source.pull(3);

        let committed = source.commit();
        assert_eq!(committed, Some(2));
        assert_eq!(source.committed_offset(), Some(2));
    }

    #[test]
    fn test_source_pause_resume() {
        let config = EventSourceConfig::new("orders", 0).from_earliest();
        let mut source = EventSource::new(config);

        source.feed_events(make_envelopes("orders", 0, 0, 3)).unwrap();
        source.pause();
        assert_eq!(source.state(), &SourceState::Paused);

        let pulled = source.pull(10);
        assert!(pulled.is_empty()); // paused

        source.resume();
        assert_eq!(source.state(), &SourceState::Active);

        let pulled = source.pull(10);
        assert_eq!(pulled.len(), 3);
    }

    #[test]
    fn test_source_complete() {
        let config = EventSourceConfig::new("orders", 0);
        let mut source = EventSource::new(config);
        source.complete();
        assert_eq!(source.state(), &SourceState::Completed);

        let result = source.feed_events(make_envelopes("orders", 0, 0, 1));
        assert!(result.is_err());
    }

    #[test]
    fn test_source_waiting_for_data() {
        let config = EventSourceConfig::new("orders", 0).from_earliest();
        let mut source = EventSource::new(config);

        source.feed_events(Vec::new()).unwrap();
        assert_eq!(source.state(), &SourceState::WaitingForData);
    }

    #[test]
    fn test_source_uncommitted_count() {
        let config = EventSourceConfig::new("orders", 0).from_earliest();
        let mut source = EventSource::new(config);

        source.feed_events(make_envelopes("orders", 0, 0, 5)).unwrap();
        source.pull(5);
        assert_eq!(source.uncommitted_count(), 4); // offset 4, no commit yet

        source.commit();
        assert_eq!(source.uncommitted_count(), 0);
    }

    #[test]
    fn test_source_display() {
        let config = EventSourceConfig::new("orders", 0);
        let source = EventSource::new(config);
        let display = source.to_string();
        assert!(display.contains("orders"));
        assert!(display.contains("EventSource"));
    }

    #[test]
    fn test_source_auto_commit_check() {
        let config = EventSourceConfig::new("orders", 0)
            .from_earliest();
        let mut source = EventSource::new(config);

        // No events emitted yet
        assert!(!source.should_auto_commit());

        source.feed_events(make_envelopes("orders", 0, 0, 1)).unwrap();
        source.pull(1);

        // First event emitted, no commit yet — should auto-commit
        assert!(source.should_auto_commit());

        source.commit();
        // Just committed, interval not elapsed
        assert!(!source.should_auto_commit());
    }

    #[test]
    fn test_source_manual_commit_no_auto() {
        let config = EventSourceConfig::new("orders", 0).manual_commit();
        let mut source = EventSource::new(config);
        source.feed_events(make_envelopes("orders", 0, 0, 1)).unwrap();
        source.pull(1);
        assert!(!source.should_auto_commit());
    }

    // --- EventSinkConfig ---

    #[test]
    fn test_sink_config_builder() {
        let config = EventSinkConfig::new("output")
            .with_max_in_flight(200)
            .with_batch_size(25)
            .with_linger(Duration::from_millis(10))
            .no_retry();

        assert_eq!(config.topic, "output");
        assert_eq!(config.max_in_flight, 200);
        assert_eq!(config.batch_size, 25);
        assert!(!config.retry_on_failure);
    }

    // --- EventSink ---

    #[test]
    fn test_sink_accept_and_drain() {
        let config = EventSinkConfig::new("output");
        let mut sink = EventSink::new(config);

        for i in 0..3 {
            sink.accept(make_envelope("output", 0, i)).unwrap();
        }
        assert_eq!(sink.pending_count(), 3);
        assert_eq!(sink.events_accepted(), 3);

        let batch = sink.drain_batch();
        assert_eq!(batch.len(), 3);
        assert_eq!(sink.state(), &SinkState::Flushing);
    }

    #[test]
    fn test_sink_backpressure() {
        let config = EventSinkConfig::new("output").with_max_in_flight(2);
        let mut sink = EventSink::new(config);

        sink.accept(make_envelope("output", 0, 0)).unwrap();
        sink.accept(make_envelope("output", 0, 1)).unwrap();

        let result = sink.accept(make_envelope("output", 0, 2));
        assert!(result.is_err());
        assert_eq!(sink.state(), &SinkState::Backpressured);
    }

    #[test]
    fn test_sink_ack_batch() {
        let config = EventSinkConfig::new("output");
        let mut sink = EventSink::new(config);

        sink.accept(make_envelope("output", 0, 0)).unwrap();
        sink.drain_batch();
        sink.ack_batch(1);

        assert_eq!(sink.events_published(), 1);
        assert_eq!(sink.state(), &SinkState::Active);
    }

    #[test]
    fn test_sink_nack_batch_no_retry() {
        let config = EventSinkConfig::new("output").no_retry();
        let mut sink = EventSink::new(config);

        sink.accept(make_envelope("output", 0, 0)).unwrap();
        sink.drain_batch();
        sink.nack_batch(1, "write failed");

        assert_eq!(sink.events_failed(), 1);
        assert!(matches!(sink.state(), SinkState::Error(_)));
    }

    #[test]
    fn test_sink_nack_batch_with_retry() {
        let config = EventSinkConfig::new("output"); // retry_on_failure = true by default
        let mut sink = EventSink::new(config);

        sink.accept(make_envelope("output", 0, 0)).unwrap();
        sink.drain_batch();
        sink.nack_batch(1, "write failed");

        assert_eq!(sink.events_failed(), 1);
        assert_eq!(sink.state(), &SinkState::Active); // recovered
    }

    #[test]
    fn test_sink_close() {
        let config = EventSinkConfig::new("output");
        let mut sink = EventSink::new(config);
        sink.close();
        assert_eq!(sink.state(), &SinkState::Closed);

        let result = sink.accept(make_envelope("output", 0, 0));
        assert!(result.is_err());
    }

    #[test]
    fn test_sink_should_flush() {
        let config = EventSinkConfig::new("output").with_batch_size(2);
        let mut sink = EventSink::new(config);

        assert!(!sink.should_flush()); // empty

        sink.accept(make_envelope("output", 0, 0)).unwrap();
        assert!(sink.should_flush()); // first event, no prior flush

        sink.drain_batch();
        sink.ack_batch(1);

        sink.accept(make_envelope("output", 0, 1)).unwrap();
        sink.accept(make_envelope("output", 0, 2)).unwrap();
        assert!(sink.should_flush()); // batch full
    }

    #[test]
    fn test_sink_display() {
        let config = EventSinkConfig::new("output");
        let sink = EventSink::new(config);
        let display = sink.to_string();
        assert!(display.contains("EventSink"));
        assert!(display.contains("output"));
    }

    // --- StreamConsumerGroup ---

    #[test]
    fn test_stream_group_new() {
        let group = StreamConsumerGroup::new("stream-group-1");
        assert_eq!(group.group_id(), "stream-group-1");
        assert_eq!(group.state(), &StreamProcessingState::Idle);
        assert_eq!(group.source_count(), 0);
    }

    #[test]
    fn test_stream_group_add_sources() {
        let mut group = StreamConsumerGroup::new("g1");
        group.add_source(EventSourceConfig::new("orders", 0).from_earliest());
        group.add_source(EventSourceConfig::new("orders", 1).from_earliest());
        assert_eq!(group.source_count(), 2);
    }

    #[test]
    fn test_stream_group_remove_source() {
        let mut group = StreamConsumerGroup::new("g1");
        group.add_source(EventSourceConfig::new("orders", 0).from_earliest());
        let removed = group.remove_source("orders", 0);
        assert!(removed.is_some());
        assert_eq!(group.source_count(), 0);
    }

    #[test]
    fn test_stream_group_start_and_pull() {
        let mut group = StreamConsumerGroup::new("g1");
        group.add_source(EventSourceConfig::new("orders", 0).from_earliest());
        group.add_source(EventSourceConfig::new("orders", 1).from_earliest());
        group.start();

        // Feed events to sources
        group.source_mut("orders", 0).unwrap()
            .feed_events(make_envelopes("orders", 0, 0, 3)).unwrap();
        group.source_mut("orders", 1).unwrap()
            .feed_events(make_envelopes("orders", 1, 0, 2)).unwrap();

        let events = group.pull_all(10);
        assert_eq!(events.len(), 5);
        assert_eq!(group.events_processed(), 5);
    }

    #[test]
    fn test_stream_group_pull_when_not_running() {
        let mut group = StreamConsumerGroup::new("g1");
        group.add_source(EventSourceConfig::new("orders", 0).from_earliest());

        // Not started, should return empty
        let events = group.pull_all(10);
        assert!(events.is_empty());
    }

    #[test]
    fn test_stream_group_commit_all() {
        let mut group = StreamConsumerGroup::new("g1");
        group.add_source(EventSourceConfig::new("orders", 0).from_earliest());
        group.start();

        group.source_mut("orders", 0).unwrap()
            .feed_events(make_envelopes("orders", 0, 0, 5)).unwrap();
        group.pull_all(10);

        let commits = group.commit_all();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0], ("orders".to_string(), 0, 4));
    }

    #[test]
    fn test_stream_group_rebalance_pause_resume() {
        let mut group = StreamConsumerGroup::new("g1");
        group.add_source(EventSourceConfig::new("orders", 0).from_earliest());
        group.start();

        group.pause_for_rebalance();
        assert_eq!(group.state(), &StreamProcessingState::Rebalancing);

        group.resume_after_rebalance();
        assert_eq!(group.state(), &StreamProcessingState::Running);
    }

    #[test]
    fn test_stream_group_stop() {
        let mut group = StreamConsumerGroup::new("g1");
        group.add_source(EventSourceConfig::new("orders", 0).from_earliest());
        group.set_sink(EventSinkConfig::new("output"));
        group.start();

        group.stop();
        assert_eq!(group.state(), &StreamProcessingState::Stopped);
    }

    #[test]
    fn test_stream_group_error_recording() {
        let mut group = StreamConsumerGroup::new("g1");
        group.start();

        group.record_error("source-0", "connection lost", true);
        assert_eq!(group.recent_errors().len(), 1);
        assert_eq!(group.state(), &StreamProcessingState::Running); // recoverable

        group.record_error("source-1", "fatal error", false);
        assert!(matches!(group.state(), StreamProcessingState::Failed(_)));
    }

    #[test]
    fn test_stream_group_health_summary() {
        let mut group = StreamConsumerGroup::new("g1");
        group.add_source(EventSourceConfig::new("orders", 0).from_earliest());
        group.start();

        group.source_mut("orders", 0).unwrap()
            .feed_events(make_envelopes("orders", 0, 0, 3)).unwrap();

        let health = group.health_summary();
        assert_eq!(health.total_sources, 1);
        assert_eq!(health.active_sources, 1);
        assert_eq!(health.total_buffered, 3);
        assert!(health.uptime.is_some());
    }

    #[test]
    fn test_stream_group_display() {
        let group = StreamConsumerGroup::new("g1");
        let display = group.to_string();
        assert!(display.contains("StreamConsumerGroup"));
        assert!(display.contains("g1"));
    }

    #[test]
    fn test_health_summary_display() {
        let health = StreamHealthSummary {
            group_id: "g1".into(),
            state: StreamProcessingState::Running,
            total_sources: 3,
            active_sources: 2,
            total_buffered: 100,
            total_uncommitted: 50,
            events_processed: 1000,
            error_count: 1,
            has_sink: true,
            uptime: Some(Duration::from_secs(60)),
        };
        let display = health.to_string();
        assert!(display.contains("g1"));
        assert!(display.contains("2/3"));
    }

    #[test]
    fn test_stream_group_with_sink() {
        let mut group = StreamConsumerGroup::new("g1");
        assert!(!group.has_sink());

        group.set_sink(EventSinkConfig::new("output"));
        assert!(group.has_sink());
    }

    #[test]
    fn test_source_error_state() {
        let mut source = EventSource::new(EventSourceConfig::new("t", 0));
        source.set_error("connection lost");
        assert!(matches!(source.state(), SourceState::Error(_)));
    }
}
