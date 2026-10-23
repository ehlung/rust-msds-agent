// mirror_maker.rs - Topic Mirroring & Cross-Bus Replication
// Kafka MirrorMaker-like topic replication between event buses

use std::collections::HashMap;
use std::time::{Duration, Instant};

// ============================================================================
// Mirror Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct MirrorConfig {
    pub source_bus_id: String,
    pub target_bus_id: String,
    pub topics: Vec<TopicMirrorRule>,
    pub consumer_group: String,
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub max_lag_threshold: u64,
    pub emit_heartbeats: bool,
    pub heartbeat_interval: Duration,
    pub sync_offsets: bool,
    pub sync_configs: bool,
}

impl MirrorConfig {
    pub fn new(source: &str, target: &str) -> Self {
        Self {
            source_bus_id: source.to_string(),
            target_bus_id: target.to_string(),
            topics: Vec::new(),
            consumer_group: format!("mirror-{}-to-{}", source, target),
            poll_interval: Duration::from_millis(100),
            batch_size: 500,
            max_lag_threshold: 10_000,
            emit_heartbeats: true,
            heartbeat_interval: Duration::from_secs(5),
            sync_offsets: true,
            sync_configs: false,
        }
    }

    pub fn add_topic(mut self, rule: TopicMirrorRule) -> Self {
        self.topics.push(rule);
        self
    }

    pub fn mirror_all(mut self) -> Self {
        self.topics.push(TopicMirrorRule::mirror_all());
        self
    }

    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn consumer_group(mut self, group: &str) -> Self {
        self.consumer_group = group.to_string();
        self
    }

    pub fn sync_configs(mut self, sync: bool) -> Self {
        self.sync_configs = sync;
        self
    }
}

// ============================================================================
// Topic Mirror Rule
// ============================================================================

#[derive(Debug, Clone)]
pub struct TopicMirrorRule {
    pub source_topic: TopicPattern,
    pub target_topic_rename: Option<String>,
    pub filter: Option<MirrorFilter>,
    pub preserve_partitioning: bool,
}

impl TopicMirrorRule {
    pub fn exact(topic: &str) -> Self {
        Self {
            source_topic: TopicPattern::Exact(topic.to_string()),
            target_topic_rename: None,
            filter: None,
            preserve_partitioning: true,
        }
    }

    pub fn prefix(prefix: &str) -> Self {
        Self {
            source_topic: TopicPattern::Prefix(prefix.to_string()),
            target_topic_rename: None,
            filter: None,
            preserve_partitioning: true,
        }
    }

    pub fn mirror_all() -> Self {
        Self {
            source_topic: TopicPattern::All,
            target_topic_rename: None,
            filter: None,
            preserve_partitioning: true,
        }
    }

    pub fn with_rename(mut self, target: &str) -> Self {
        self.target_topic_rename = Some(target.to_string());
        self
    }

    pub fn with_filter(mut self, filter: MirrorFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn repartition(mut self) -> Self {
        self.preserve_partitioning = false;
        self
    }

    pub fn matches_topic(&self, topic: &str) -> bool {
        self.source_topic.matches(topic)
    }

    pub fn target_name(&self, source_topic: &str) -> String {
        self.target_topic_rename.clone().unwrap_or_else(|| source_topic.to_string())
    }
}

// ============================================================================
// Topic Pattern
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum TopicPattern {
    Exact(String),
    Prefix(String),
    Regex(String),
    All,
}

impl TopicPattern {
    pub fn matches(&self, topic: &str) -> bool {
        match self {
            TopicPattern::Exact(name) => topic == name,
            TopicPattern::Prefix(prefix) => topic.starts_with(prefix.as_str()),
            TopicPattern::Regex(pattern) => topic.contains(pattern.as_str()),
            TopicPattern::All => true,
        }
    }
}

impl std::fmt::Display for TopicPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicPattern::Exact(n) => write!(f, "exact:{}", n),
            TopicPattern::Prefix(p) => write!(f, "prefix:{}", p),
            TopicPattern::Regex(r) => write!(f, "regex:{}", r),
            TopicPattern::All => write!(f, "*"),
        }
    }
}

// ============================================================================
// Mirror Filter
// ============================================================================

#[derive(Debug, Clone)]
pub enum MirrorFilter {
    HeaderExists(String),
    HeaderEquals(String, String),
    MinPayloadSize(usize),
    MaxPayloadSize(usize),
    KeyPrefix(String),
    Combined(Vec<MirrorFilter>),
}

impl MirrorFilter {
    pub fn matches(&self, record: &MirrorRecord) -> bool {
        match self {
            MirrorFilter::HeaderExists(key) => record.headers.contains_key(key),
            MirrorFilter::HeaderEquals(key, value) => {
                record.headers.get(key).map(|v| v == value).unwrap_or(false)
            }
            MirrorFilter::MinPayloadSize(min) => record.payload_size >= *min,
            MirrorFilter::MaxPayloadSize(max) => record.payload_size <= *max,
            MirrorFilter::KeyPrefix(prefix) => {
                record.key.as_ref().map(|k| k.starts_with(prefix.as_str())).unwrap_or(false)
            }
            MirrorFilter::Combined(filters) => filters.iter().all(|f| f.matches(record)),
        }
    }
}

// ============================================================================
// Mirror Record
// ============================================================================

#[derive(Debug, Clone)]
pub struct MirrorRecord {
    pub source_topic: String,
    pub source_partition: u32,
    pub source_offset: u64,
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub payload_size: usize,
    pub headers: HashMap<String, String>,
    pub timestamp: u64,
}

impl MirrorRecord {
    pub fn new(topic: &str, partition: u32, offset: u64, payload: Vec<u8>) -> Self {
        let size = payload.len();
        Self {
            source_topic: topic.to_string(),
            source_partition: partition,
            source_offset: offset,
            key: None,
            payload,
            payload_size: size,
            headers: HashMap::new(),
            timestamp: 0,
        }
    }

    pub fn with_key(mut self, key: &str) -> Self {
        self.key = Some(key.to_string());
        self
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_timestamp(mut self, ts: u64) -> Self {
        self.timestamp = ts;
        self
    }
}

// ============================================================================
// Mirror State
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum MirrorState {
    Idle,
    Starting,
    Running,
    Paused,
    Catching,
    Stopped,
    Failed(String),
}

impl std::fmt::Display for MirrorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MirrorState::Idle => write!(f, "Idle"),
            MirrorState::Starting => write!(f, "Starting"),
            MirrorState::Running => write!(f, "Running"),
            MirrorState::Paused => write!(f, "Paused"),
            MirrorState::Catching => write!(f, "CatchingUp"),
            MirrorState::Stopped => write!(f, "Stopped"),
            MirrorState::Failed(e) => write!(f, "Failed({})", e),
        }
    }
}

// ============================================================================
// Topic Mirror Status
// ============================================================================

#[derive(Debug, Clone)]
pub struct TopicMirrorStatus {
    pub source_topic: String,
    pub target_topic: String,
    pub partitions_mirrored: u32,
    pub current_lag: u64,
    pub records_mirrored: u64,
    pub bytes_mirrored: u64,
    pub errors: u64,
    pub last_mirrored_offset: HashMap<u32, u64>,
}

impl TopicMirrorStatus {
    pub fn new(source: &str, target: &str) -> Self {
        Self {
            source_topic: source.to_string(),
            target_topic: target.to_string(),
            partitions_mirrored: 0,
            current_lag: 0,
            records_mirrored: 0,
            bytes_mirrored: 0,
            errors: 0,
            last_mirrored_offset: HashMap::new(),
        }
    }

    pub fn record_mirrored(&mut self, partition: u32, offset: u64, bytes: u64) {
        self.records_mirrored += 1;
        self.bytes_mirrored += bytes;
        self.last_mirrored_offset.insert(partition, offset);
    }

    pub fn record_error(&mut self) {
        self.errors += 1;
    }

    pub fn is_healthy(&self) -> bool {
        self.errors == 0
    }

    pub fn avg_record_size(&self) -> f64 {
        if self.records_mirrored == 0 {
            0.0
        } else {
            self.bytes_mirrored as f64 / self.records_mirrored as f64
        }
    }
}

// ============================================================================
// Mirror Maker
// ============================================================================

#[derive(Debug)]
pub struct MirrorMaker {
    config: MirrorConfig,
    state: MirrorState,
    topic_statuses: HashMap<String, TopicMirrorStatus>,
    total_records: u64,
    total_bytes: u64,
    total_errors: u64,
    started_at: Option<Instant>,
    heartbeats_sent: u64,
    checkpoint_offsets: HashMap<String, HashMap<u32, u64>>,
}

impl MirrorMaker {
    pub fn new(config: MirrorConfig) -> Self {
        Self {
            config,
            state: MirrorState::Idle,
            topic_statuses: HashMap::new(),
            total_records: 0,
            total_bytes: 0,
            total_errors: 0,
            started_at: None,
            heartbeats_sent: 0,
            checkpoint_offsets: HashMap::new(),
        }
    }

    pub fn state(&self) -> &MirrorState {
        &self.state
    }

    pub fn config(&self) -> &MirrorConfig {
        &self.config
    }

    pub fn start(&mut self) -> Result<(), MirrorError> {
        match &self.state {
            MirrorState::Idle | MirrorState::Stopped | MirrorState::Failed(_) => {
                if self.config.topics.is_empty() {
                    return Err(MirrorError::ConfigError("no topics configured".into()));
                }
                self.state = MirrorState::Starting;
                self.started_at = Some(Instant::now());
                self.state = MirrorState::Running;
                Ok(())
            }
            _ => Err(MirrorError::InvalidState(format!(
                "cannot start from state {}",
                self.state
            ))),
        }
    }

    pub fn pause(&mut self) -> Result<(), MirrorError> {
        match &self.state {
            MirrorState::Running | MirrorState::Catching => {
                self.state = MirrorState::Paused;
                Ok(())
            }
            _ => Err(MirrorError::InvalidState(format!(
                "cannot pause from state {}",
                self.state
            ))),
        }
    }

    pub fn resume(&mut self) -> Result<(), MirrorError> {
        match &self.state {
            MirrorState::Paused => {
                self.state = MirrorState::Running;
                Ok(())
            }
            _ => Err(MirrorError::InvalidState(format!(
                "cannot resume from state {}",
                self.state
            ))),
        }
    }

    pub fn stop(&mut self) -> Result<(), MirrorError> {
        match &self.state {
            MirrorState::Idle | MirrorState::Stopped => {
                Err(MirrorError::InvalidState("already stopped".into()))
            }
            _ => {
                self.state = MirrorState::Stopped;
                Ok(())
            }
        }
    }

    pub fn fail(&mut self, reason: &str) {
        self.state = MirrorState::Failed(reason.to_string());
        self.total_errors += 1;
    }

    pub fn mirror_batch(&mut self, records: &[MirrorRecord]) -> MirrorBatchResult {
        if self.state != MirrorState::Running && self.state != MirrorState::Catching {
            return MirrorBatchResult {
                mirrored: 0,
                filtered: 0,
                failed: records.len(),
                bytes: 0,
            };
        }

        let mut mirrored = 0;
        let mut filtered = 0;
        let failed = 0;
        let mut bytes = 0u64;

        for record in records {
            // Extract what we need from config (immutable borrow) before mutable borrow
            let rule_info: Option<(String, bool)> = {
                let matching_rule = self.config.topics.iter().find(|r| r.matches_topic(&record.source_topic));
                match matching_rule {
                    Some(rule) => {
                        if let Some(ref filter) = rule.filter {
                            if !filter.matches(record) {
                                None // filtered
                            } else {
                                Some((rule.target_name(&record.source_topic), true))
                            }
                        } else {
                            Some((rule.target_name(&record.source_topic), true))
                        }
                    }
                    None => None,
                }
            };

            let target = match rule_info {
                Some((t, true)) => t,
                _ => {
                    filtered += 1;
                    continue;
                }
            };

            let source_topic = record.source_topic.clone();
            let status = self.topic_statuses
                .entry(source_topic.clone())
                .or_insert_with(|| TopicMirrorStatus::new(&source_topic, &target));

            status.record_mirrored(record.source_partition, record.source_offset, record.payload_size as u64);
            bytes += record.payload_size as u64;
            mirrored += 1;

            // Update checkpoint
            self.checkpoint_offsets
                .entry(record.source_topic.clone())
                .or_default()
                .insert(record.source_partition, record.source_offset);
        }

        self.total_records += mirrored as u64;
        self.total_bytes += bytes;

        MirrorBatchResult {
            mirrored,
            filtered,
            failed,
            bytes,
        }
    }

    pub fn send_heartbeat(&mut self) {
        self.heartbeats_sent += 1;
    }

    pub fn topic_status(&self, topic: &str) -> Option<&TopicMirrorStatus> {
        self.topic_statuses.get(topic)
    }

    pub fn all_topic_statuses(&self) -> &HashMap<String, TopicMirrorStatus> {
        &self.topic_statuses
    }

    pub fn checkpoint(&self, topic: &str) -> Option<&HashMap<u32, u64>> {
        self.checkpoint_offsets.get(topic)
    }

    pub fn stats(&self) -> MirrorMakerStats {
        let uptime = self.started_at.map(|s| s.elapsed());
        let throughput = uptime
            .filter(|d| d.as_secs_f64() > 0.0)
            .map(|d| self.total_records as f64 / d.as_secs_f64())
            .unwrap_or(0.0);

        let total_lag: u64 = self.topic_statuses.values().map(|s| s.current_lag).sum();

        MirrorMakerStats {
            state: self.state.clone(),
            total_records: self.total_records,
            total_bytes: self.total_bytes,
            total_errors: self.total_errors,
            topics_mirrored: self.topic_statuses.len(),
            total_lag,
            throughput_per_second: throughput,
            uptime,
            heartbeats_sent: self.heartbeats_sent,
        }
    }
}

// ============================================================================
// Mirror Batch Result
// ============================================================================

#[derive(Debug, Clone)]
pub struct MirrorBatchResult {
    pub mirrored: usize,
    pub filtered: usize,
    pub failed: usize,
    pub bytes: u64,
}

impl MirrorBatchResult {
    pub fn total(&self) -> usize {
        self.mirrored + self.filtered + self.failed
    }

    pub fn success_rate(&self) -> f64 {
        let total = self.total();
        if total == 0 {
            1.0
        } else {
            self.mirrored as f64 / total as f64
        }
    }
}

// ============================================================================
// Mirror Maker Stats
// ============================================================================

#[derive(Debug, Clone)]
pub struct MirrorMakerStats {
    pub state: MirrorState,
    pub total_records: u64,
    pub total_bytes: u64,
    pub total_errors: u64,
    pub topics_mirrored: usize,
    pub total_lag: u64,
    pub throughput_per_second: f64,
    pub uptime: Option<Duration>,
    pub heartbeats_sent: u64,
}

// ============================================================================
// Mirror Error
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum MirrorError {
    ConfigError(String),
    InvalidState(String),
    SourceUnavailable(String),
    TargetUnavailable(String),
    ReplicationFailed { topic: String, partition: u32, reason: String },
    OffsetSyncFailed(String),
}

impl std::fmt::Display for MirrorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MirrorError::ConfigError(e) => write!(f, "Mirror config error: {}", e),
            MirrorError::InvalidState(s) => write!(f, "Invalid mirror state: {}", s),
            MirrorError::SourceUnavailable(s) => write!(f, "Source bus unavailable: {}", s),
            MirrorError::TargetUnavailable(t) => write!(f, "Target bus unavailable: {}", t),
            MirrorError::ReplicationFailed { topic, partition, reason } =>
                write!(f, "Replication failed for {}/{}: {}", topic, partition, reason),
            MirrorError::OffsetSyncFailed(e) => write!(f, "Offset sync failed: {}", e),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MirrorConfig {
        MirrorConfig::new("dc-east", "dc-west")
            .add_topic(TopicMirrorRule::exact("orders"))
            .add_topic(TopicMirrorRule::prefix("events."))
    }

    #[test]
    fn test_mirror_config_creation() {
        let config = MirrorConfig::new("source", "target");
        assert_eq!(config.source_bus_id, "source");
        assert_eq!(config.target_bus_id, "target");
        assert!(config.topics.is_empty());
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn test_mirror_config_builder() {
        let config = MirrorConfig::new("a", "b")
            .mirror_all()
            .poll_interval(Duration::from_millis(50))
            .batch_size(1000)
            .consumer_group("my-mirror")
            .sync_configs(true);
        assert_eq!(config.topics.len(), 1);
        assert_eq!(config.poll_interval, Duration::from_millis(50));
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.consumer_group, "my-mirror");
        assert!(config.sync_configs);
    }

    #[test]
    fn test_topic_pattern_exact() {
        let pattern = TopicPattern::Exact("orders".into());
        assert!(pattern.matches("orders"));
        assert!(!pattern.matches("orders-v2"));
        assert_eq!(pattern.to_string(), "exact:orders");
    }

    #[test]
    fn test_topic_pattern_prefix() {
        let pattern = TopicPattern::Prefix("events.".into());
        assert!(pattern.matches("events.user"));
        assert!(pattern.matches("events.order.created"));
        assert!(!pattern.matches("commands.user"));
        assert_eq!(pattern.to_string(), "prefix:events.");
    }

    #[test]
    fn test_topic_pattern_all() {
        let pattern = TopicPattern::All;
        assert!(pattern.matches("anything"));
        assert!(pattern.matches(""));
        assert_eq!(pattern.to_string(), "*");
    }

    #[test]
    fn test_topic_pattern_regex() {
        let pattern = TopicPattern::Regex("order".into());
        assert!(pattern.matches("orders"));
        assert!(pattern.matches("order-events"));
        assert!(!pattern.matches("users"));
    }

    #[test]
    fn test_topic_mirror_rule_exact() {
        let rule = TopicMirrorRule::exact("orders");
        assert!(rule.matches_topic("orders"));
        assert!(!rule.matches_topic("users"));
        assert_eq!(rule.target_name("orders"), "orders");
    }

    #[test]
    fn test_topic_mirror_rule_rename() {
        let rule = TopicMirrorRule::exact("orders").with_rename("orders-replica");
        assert_eq!(rule.target_name("orders"), "orders-replica");
    }

    #[test]
    fn test_topic_mirror_rule_repartition() {
        let rule = TopicMirrorRule::exact("orders").repartition();
        assert!(!rule.preserve_partitioning);
    }

    #[test]
    fn test_mirror_filter_header_exists() {
        let filter = MirrorFilter::HeaderExists("priority".into());
        let record = MirrorRecord::new("t", 0, 0, vec![1, 2, 3])
            .with_header("priority", "high");
        assert!(filter.matches(&record));
        let record2 = MirrorRecord::new("t", 0, 0, vec![]);
        assert!(!filter.matches(&record2));
    }

    #[test]
    fn test_mirror_filter_header_equals() {
        let filter = MirrorFilter::HeaderEquals("type".into(), "important".into());
        let record = MirrorRecord::new("t", 0, 0, vec![])
            .with_header("type", "important");
        assert!(filter.matches(&record));
        let record2 = MirrorRecord::new("t", 0, 0, vec![])
            .with_header("type", "normal");
        assert!(!filter.matches(&record2));
    }

    #[test]
    fn test_mirror_filter_payload_size() {
        let record = MirrorRecord::new("t", 0, 0, vec![0; 100]);
        assert!(MirrorFilter::MinPayloadSize(50).matches(&record));
        assert!(!MirrorFilter::MinPayloadSize(200).matches(&record));
        assert!(MirrorFilter::MaxPayloadSize(200).matches(&record));
        assert!(!MirrorFilter::MaxPayloadSize(50).matches(&record));
    }

    #[test]
    fn test_mirror_filter_key_prefix() {
        let filter = MirrorFilter::KeyPrefix("user-".into());
        let record = MirrorRecord::new("t", 0, 0, vec![]).with_key("user-123");
        assert!(filter.matches(&record));
        let record2 = MirrorRecord::new("t", 0, 0, vec![]).with_key("order-1");
        assert!(!filter.matches(&record2));
    }

    #[test]
    fn test_mirror_filter_combined() {
        let filter = MirrorFilter::Combined(vec![
            MirrorFilter::MinPayloadSize(10),
            MirrorFilter::HeaderExists("type".into()),
        ]);
        let record = MirrorRecord::new("t", 0, 0, vec![0; 20])
            .with_header("type", "x");
        assert!(filter.matches(&record));
        let record2 = MirrorRecord::new("t", 0, 0, vec![0; 5])
            .with_header("type", "x");
        assert!(!filter.matches(&record2));
    }

    #[test]
    fn test_mirror_record_creation() {
        let record = MirrorRecord::new("orders", 2, 100, vec![1, 2, 3])
            .with_key("order-1")
            .with_header("type", "created")
            .with_timestamp(12345);
        assert_eq!(record.source_topic, "orders");
        assert_eq!(record.source_partition, 2);
        assert_eq!(record.source_offset, 100);
        assert_eq!(record.key, Some("order-1".into()));
        assert_eq!(record.payload_size, 3);
        assert_eq!(record.timestamp, 12345);
    }

    #[test]
    fn test_mirror_state_display() {
        assert_eq!(MirrorState::Idle.to_string(), "Idle");
        assert_eq!(MirrorState::Running.to_string(), "Running");
        assert_eq!(MirrorState::Paused.to_string(), "Paused");
        assert_eq!(MirrorState::Failed("err".into()).to_string(), "Failed(err)");
    }

    #[test]
    fn test_topic_mirror_status() {
        let mut status = TopicMirrorStatus::new("src", "tgt");
        assert!(status.is_healthy());
        assert_eq!(status.avg_record_size(), 0.0);
        status.record_mirrored(0, 10, 256);
        status.record_mirrored(0, 11, 512);
        assert_eq!(status.records_mirrored, 2);
        assert_eq!(status.bytes_mirrored, 768);
        assert_eq!(status.avg_record_size(), 384.0);
    }

    #[test]
    fn test_topic_mirror_status_error() {
        let mut status = TopicMirrorStatus::new("s", "t");
        status.record_error();
        assert!(!status.is_healthy());
        assert_eq!(status.errors, 1);
    }

    #[test]
    fn test_mirror_maker_lifecycle() {
        let config = test_config();
        let mut mm = MirrorMaker::new(config);
        assert_eq!(*mm.state(), MirrorState::Idle);
        assert!(mm.start().is_ok());
        assert_eq!(*mm.state(), MirrorState::Running);
        assert!(mm.pause().is_ok());
        assert_eq!(*mm.state(), MirrorState::Paused);
        assert!(mm.resume().is_ok());
        assert_eq!(*mm.state(), MirrorState::Running);
        assert!(mm.stop().is_ok());
        assert_eq!(*mm.state(), MirrorState::Stopped);
    }

    #[test]
    fn test_mirror_maker_start_no_topics() {
        let config = MirrorConfig::new("a", "b");
        let mut mm = MirrorMaker::new(config);
        assert!(mm.start().is_err());
    }

    #[test]
    fn test_mirror_maker_invalid_transitions() {
        let config = test_config();
        let mut mm = MirrorMaker::new(config);
        assert!(mm.pause().is_err());
        assert!(mm.resume().is_err());
        assert!(mm.stop().is_err());
    }

    #[test]
    fn test_mirror_maker_mirror_batch() {
        let config = test_config();
        let mut mm = MirrorMaker::new(config);
        mm.start().unwrap();

        let records = vec![
            MirrorRecord::new("orders", 0, 0, vec![1, 2, 3]),
            MirrorRecord::new("orders", 0, 1, vec![4, 5]),
            MirrorRecord::new("events.user", 0, 0, vec![10]),
            MirrorRecord::new("unknown-topic", 0, 0, vec![99]),
        ];

        let result = mm.mirror_batch(&records);
        assert_eq!(result.mirrored, 3);
        assert_eq!(result.filtered, 1);
        assert_eq!(result.total(), 4);
        assert_eq!(mm.stats().total_records, 3);
    }

    #[test]
    fn test_mirror_maker_batch_not_running() {
        let config = test_config();
        let mut mm = MirrorMaker::new(config);
        let records = vec![MirrorRecord::new("orders", 0, 0, vec![1])];
        let result = mm.mirror_batch(&records);
        assert_eq!(result.mirrored, 0);
        assert_eq!(result.failed, 1);
    }

    #[test]
    fn test_mirror_maker_with_filter() {
        let config = MirrorConfig::new("a", "b")
            .add_topic(
                TopicMirrorRule::exact("orders")
                    .with_filter(MirrorFilter::MinPayloadSize(5))
            );
        let mut mm = MirrorMaker::new(config);
        mm.start().unwrap();

        let records = vec![
            MirrorRecord::new("orders", 0, 0, vec![0; 10]),
            MirrorRecord::new("orders", 0, 1, vec![0; 2]),
        ];
        let result = mm.mirror_batch(&records);
        assert_eq!(result.mirrored, 1);
        assert_eq!(result.filtered, 1);
    }

    #[test]
    fn test_mirror_maker_checkpoint() {
        let config = test_config();
        let mut mm = MirrorMaker::new(config);
        mm.start().unwrap();

        let records = vec![
            MirrorRecord::new("orders", 0, 5, vec![1]),
            MirrorRecord::new("orders", 1, 10, vec![2]),
        ];
        mm.mirror_batch(&records);

        let cp = mm.checkpoint("orders").unwrap();
        assert_eq!(cp.get(&0), Some(&5));
        assert_eq!(cp.get(&1), Some(&10));
    }

    #[test]
    fn test_mirror_maker_heartbeat() {
        let config = test_config();
        let mut mm = MirrorMaker::new(config);
        mm.send_heartbeat();
        mm.send_heartbeat();
        assert_eq!(mm.stats().heartbeats_sent, 2);
    }

    #[test]
    fn test_mirror_maker_fail() {
        let config = test_config();
        let mut mm = MirrorMaker::new(config);
        mm.start().unwrap();
        mm.fail("connection lost");
        assert_eq!(*mm.state(), MirrorState::Failed("connection lost".into()));
        assert_eq!(mm.stats().total_errors, 1);
    }

    #[test]
    fn test_mirror_maker_restart_after_failure() {
        let config = test_config();
        let mut mm = MirrorMaker::new(config);
        mm.start().unwrap();
        mm.fail("error");
        assert!(mm.start().is_ok());
        assert_eq!(*mm.state(), MirrorState::Running);
    }

    #[test]
    fn test_mirror_maker_stats() {
        let config = test_config();
        let mut mm = MirrorMaker::new(config);
        mm.start().unwrap();
        let stats = mm.stats();
        assert_eq!(stats.total_records, 0);
        assert!(stats.uptime.is_some());
    }

    #[test]
    fn test_mirror_batch_result() {
        let result = MirrorBatchResult {
            mirrored: 8,
            filtered: 2,
            failed: 0,
            bytes: 1024,
        };
        assert_eq!(result.total(), 10);
        assert_eq!(result.success_rate(), 0.8);
    }

    #[test]
    fn test_mirror_batch_result_empty() {
        let result = MirrorBatchResult {
            mirrored: 0,
            filtered: 0,
            failed: 0,
            bytes: 0,
        };
        assert_eq!(result.success_rate(), 1.0);
    }

    #[test]
    fn test_mirror_error_display() {
        assert!(MirrorError::ConfigError("bad".into()).to_string().contains("bad"));
        assert!(MirrorError::InvalidState("x".into()).to_string().contains("x"));
        assert!(MirrorError::SourceUnavailable("s".into()).to_string().contains("s"));
        assert!(MirrorError::TargetUnavailable("t".into()).to_string().contains("t"));
        assert!(MirrorError::ReplicationFailed {
            topic: "t".into(), partition: 0, reason: "err".into()
        }.to_string().contains("t/0"));
        assert!(MirrorError::OffsetSyncFailed("sync".into()).to_string().contains("sync"));
    }
}
