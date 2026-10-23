// tiered_storage.rs - Tiered Storage for Event Bus (Phase 3)
// Hot/Warm/Cold storage tiers, segment management, migration policies

use std::collections::HashMap;
use std::time::{Duration, Instant};

// ============================================================================
// Storage Tiers
// ============================================================================

/// Storage tier levels in order of access speed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StorageTier {
    /// In-memory, fastest access — recent events
    Hot,
    /// Local disk or slower memory — older events
    Warm,
    /// Archive/remote storage — rarely accessed
    Cold,
}

impl std::fmt::Display for StorageTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageTier::Hot => write!(f, "Hot"),
            StorageTier::Warm => write!(f, "Warm"),
            StorageTier::Cold => write!(f, "Cold"),
        }
    }
}

impl StorageTier {
    /// Next colder tier (for downward migration)
    pub fn colder(&self) -> Option<StorageTier> {
        match self {
            StorageTier::Hot => Some(StorageTier::Warm),
            StorageTier::Warm => Some(StorageTier::Cold),
            StorageTier::Cold => None,
        }
    }

    /// Next warmer tier (for upward promotion)
    pub fn warmer(&self) -> Option<StorageTier> {
        match self {
            StorageTier::Hot => None,
            StorageTier::Warm => Some(StorageTier::Hot),
            StorageTier::Cold => Some(StorageTier::Warm),
        }
    }
}

// ============================================================================
// Storage Backend
// ============================================================================

/// Describes the storage backend for a tier
#[derive(Debug, Clone, PartialEq)]
pub enum StorageBackend {
    /// In-memory (VecDeque-based, like current PartitionLog)
    InMemory,
    /// Local filesystem
    LocalDisk { path: String },
    /// S3-compatible object storage
    S3 { bucket: String, region: String, prefix: String },
    /// Google Cloud Storage
    Gcs { bucket: String, prefix: String },
    /// Custom backend identified by name
    Custom { name: String, endpoint: String },
}

impl std::fmt::Display for StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageBackend::InMemory => write!(f, "InMemory"),
            StorageBackend::LocalDisk { path } => write!(f, "LocalDisk({})", path),
            StorageBackend::S3 { bucket, region, .. } => write!(f, "S3({}/{})", region, bucket),
            StorageBackend::Gcs { bucket, .. } => write!(f, "GCS({})", bucket),
            StorageBackend::Custom { name, .. } => write!(f, "Custom({})", name),
        }
    }
}

// ============================================================================
// Tier Configuration
// ============================================================================

/// Configuration for a single storage tier
#[derive(Debug, Clone)]
pub struct TierConfig {
    pub tier: StorageTier,
    pub backend: StorageBackend,
    pub retention: Option<Duration>,
    pub max_size_bytes: Option<u64>,
    pub max_segments: Option<usize>,
    pub read_only: bool,
}

impl TierConfig {
    pub fn new(tier: StorageTier, backend: StorageBackend) -> Self {
        Self {
            tier,
            backend,
            retention: None,
            max_size_bytes: None,
            max_segments: None,
            read_only: false,
        }
    }

    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = Some(retention);
        self
    }

    pub fn with_max_size(mut self, bytes: u64) -> Self {
        self.max_size_bytes = Some(bytes);
        self
    }

    pub fn with_max_segments(mut self, count: usize) -> Self {
        self.max_segments = Some(count);
        self
    }

    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if let Some(retention) = self.retention {
            if retention.as_secs() == 0 {
                return Err("Retention must be > 0 seconds".into());
            }
        }
        Ok(())
    }
}

// ============================================================================
// Segment Metadata
// ============================================================================

/// A contiguous range of events in a single tier
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentMetadata {
    pub segment_id: u64,
    pub topic: String,
    pub partition_id: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub tier: StorageTier,
    pub created_at: Instant,
    pub size_bytes: u64,
    pub event_count: u64,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    pub sealed: bool,
}

impl SegmentMetadata {
    pub fn new(
        segment_id: u64,
        topic: &str,
        partition_id: u32,
        start_offset: u64,
        tier: StorageTier,
    ) -> Self {
        Self {
            segment_id,
            topic: topic.to_string(),
            partition_id,
            start_offset,
            end_offset: start_offset,
            tier,
            created_at: Instant::now(),
            size_bytes: 0,
            event_count: 0,
            min_timestamp: u64::MAX,
            max_timestamp: 0,
            sealed: false,
        }
    }

    /// Record an event appended to this segment
    pub fn record_event(&mut self, offset: u64, size_bytes: u64, timestamp: u64) {
        self.end_offset = offset;
        self.size_bytes += size_bytes;
        self.event_count += 1;
        self.min_timestamp = self.min_timestamp.min(timestamp);
        self.max_timestamp = self.max_timestamp.max(timestamp);
    }

    /// Seal segment (no more writes)
    pub fn seal(&mut self) {
        self.sealed = true;
    }

    pub fn offset_range(&self) -> std::ops::RangeInclusive<u64> {
        self.start_offset..=self.end_offset
    }

    pub fn contains_offset(&self, offset: u64) -> bool {
        offset >= self.start_offset && offset <= self.end_offset
    }

    /// Age since creation
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Average event size
    pub fn avg_event_size(&self) -> f64 {
        if self.event_count == 0 { 0.0 }
        else { self.size_bytes as f64 / self.event_count as f64 }
    }
}

impl std::fmt::Display for SegmentMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f, "Segment(id={}, {}/{}, offsets={}..{}, tier={}, events={}, size={}B{})",
            self.segment_id, self.topic, self.partition_id,
            self.start_offset, self.end_offset, self.tier,
            self.event_count, self.size_bytes,
            if self.sealed { ", sealed" } else { "" }
        )
    }
}

// ============================================================================
// Migration Policy
// ============================================================================

/// Policy for when to migrate segments between tiers
#[derive(Debug, Clone, PartialEq)]
pub enum MigrationPolicy {
    /// Migrate based on segment age
    ByAge {
        hot_retention: Duration,
        warm_retention: Duration,
    },
    /// Migrate based on total tier size
    BySize {
        hot_max_bytes: u64,
        warm_max_bytes: u64,
    },
    /// Combine age and size triggers
    Hybrid {
        hot_retention: Duration,
        warm_retention: Duration,
        hot_max_bytes: u64,
        warm_max_bytes: u64,
    },
    /// Never migrate (single-tier)
    None,
}

impl std::fmt::Display for MigrationPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationPolicy::ByAge { hot_retention, warm_retention } => {
                write!(f, "ByAge(hot={}s, warm={}s)", hot_retention.as_secs(), warm_retention.as_secs())
            }
            MigrationPolicy::BySize { hot_max_bytes, warm_max_bytes } => {
                write!(f, "BySize(hot={}B, warm={}B)", hot_max_bytes, warm_max_bytes)
            }
            MigrationPolicy::Hybrid { .. } => write!(f, "Hybrid"),
            MigrationPolicy::None => write!(f, "None"),
        }
    }
}

// ============================================================================
// Migration Decision
// ============================================================================

/// A migration action to be executed
#[derive(Debug, Clone, PartialEq)]
pub struct MigrationAction {
    pub segment_id: u64,
    pub topic: String,
    pub partition_id: u32,
    pub from_tier: StorageTier,
    pub to_tier: StorageTier,
    pub reason: MigrationReason,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MigrationReason {
    AgeBased { age: Duration, threshold: Duration },
    SizeBased { current_size: u64, max_size: u64 },
    ManualRequest,
    TierFull,
    Compacted,
}

impl std::fmt::Display for MigrationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationReason::AgeBased { age, threshold } =>
                write!(f, "Age({}s > {}s)", age.as_secs(), threshold.as_secs()),
            MigrationReason::SizeBased { current_size, max_size } =>
                write!(f, "Size({}B > {}B)", current_size, max_size),
            MigrationReason::ManualRequest => write!(f, "Manual"),
            MigrationReason::TierFull => write!(f, "TierFull"),
            MigrationReason::Compacted => write!(f, "Compacted"),
        }
    }
}

// ============================================================================
// Tiered Storage Manager
// ============================================================================

/// Manages tiered storage for all topic-partitions
pub struct TieredStorageManager {
    tiers: HashMap<StorageTier, TierConfig>,
    /// (topic, partition) → segments sorted by start_offset
    segments: HashMap<(String, u32), Vec<SegmentMetadata>>,
    migration_policy: MigrationPolicy,
    next_segment_id: u64,
    stats: TieredStorageStats,
}

#[derive(Debug, Clone, Default)]
pub struct TieredStorageStats {
    pub total_segments: u64,
    pub total_bytes: u64,
    pub migrations_completed: u64,
    pub migrations_failed: u64,
    pub segments_deleted: u64,
    pub bytes_by_tier: HashMap<StorageTier, u64>,
    pub segments_by_tier: HashMap<StorageTier, u64>,
}

impl TieredStorageStats {
    pub fn hot_bytes(&self) -> u64 {
        self.bytes_by_tier.get(&StorageTier::Hot).copied().unwrap_or(0)
    }

    pub fn warm_bytes(&self) -> u64 {
        self.bytes_by_tier.get(&StorageTier::Warm).copied().unwrap_or(0)
    }

    pub fn cold_bytes(&self) -> u64 {
        self.bytes_by_tier.get(&StorageTier::Cold).copied().unwrap_or(0)
    }
}

impl std::fmt::Display for TieredStorageStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TieredStorage(segments={}, bytes={}, migrations={}, hot={}B, warm={}B, cold={}B)",
            self.total_segments, self.total_bytes, self.migrations_completed,
            self.hot_bytes(), self.warm_bytes(), self.cold_bytes()
        )
    }
}

impl TieredStorageManager {
    pub fn new(migration_policy: MigrationPolicy) -> Self {
        Self {
            tiers: HashMap::new(),
            segments: HashMap::new(),
            migration_policy,
            next_segment_id: 1,
            stats: TieredStorageStats::default(),
        }
    }

    /// Add a tier configuration
    pub fn add_tier(&mut self, config: TierConfig) {
        self.tiers.insert(config.tier, config);
    }

    /// Get tier configuration
    pub fn tier_config(&self, tier: &StorageTier) -> Option<&TierConfig> {
        self.tiers.get(tier)
    }

    pub fn migration_policy(&self) -> &MigrationPolicy {
        &self.migration_policy
    }

    pub fn stats(&self) -> &TieredStorageStats {
        &self.stats
    }

    /// Create a new segment in the specified tier
    pub fn create_segment(
        &mut self,
        topic: &str,
        partition_id: u32,
        start_offset: u64,
        tier: StorageTier,
    ) -> u64 {
        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;

        let segment = SegmentMetadata::new(segment_id, topic, partition_id, start_offset, tier);
        let key = (topic.to_string(), partition_id);
        self.segments.entry(key).or_default().push(segment);

        self.stats.total_segments += 1;
        *self.stats.segments_by_tier.entry(tier).or_default() += 1;

        segment_id
    }

    /// Record an event in a segment
    pub fn record_event(
        &mut self,
        topic: &str,
        partition_id: u32,
        segment_id: u64,
        offset: u64,
        size_bytes: u64,
        timestamp: u64,
    ) -> bool {
        let key = (topic.to_string(), partition_id);
        if let Some(segments) = self.segments.get_mut(&key) {
            if let Some(seg) = segments.iter_mut().find(|s| s.segment_id == segment_id) {
                seg.record_event(offset, size_bytes, timestamp);
                self.stats.total_bytes += size_bytes;
                *self.stats.bytes_by_tier.entry(seg.tier).or_default() += size_bytes;
                return true;
            }
        }
        false
    }

    /// Seal a segment (no more writes)
    pub fn seal_segment(&mut self, topic: &str, partition_id: u32, segment_id: u64) -> bool {
        let key = (topic.to_string(), partition_id);
        if let Some(segments) = self.segments.get_mut(&key) {
            if let Some(seg) = segments.iter_mut().find(|s| s.segment_id == segment_id) {
                seg.seal();
                return true;
            }
        }
        false
    }

    /// Find which segment contains a given offset
    pub fn find_segment(&self, topic: &str, partition_id: u32, offset: u64) -> Option<&SegmentMetadata> {
        let key = (topic.to_string(), partition_id);
        self.segments.get(&key)?
            .iter()
            .find(|s| s.contains_offset(offset))
    }

    /// Get all segments for a topic-partition, ordered by start_offset
    pub fn segments_for(&self, topic: &str, partition_id: u32) -> Vec<&SegmentMetadata> {
        let key = (topic.to_string(), partition_id);
        let mut segs: Vec<&SegmentMetadata> = self.segments.get(&key)
            .map(|v| v.iter().collect())
            .unwrap_or_default();
        segs.sort_by_key(|s| s.start_offset);
        segs
    }

    /// Get segments in a specific tier for a topic-partition
    pub fn segments_in_tier(&self, topic: &str, partition_id: u32, tier: StorageTier) -> Vec<&SegmentMetadata> {
        self.segments_for(topic, partition_id)
            .into_iter()
            .filter(|s| s.tier == tier)
            .collect()
    }

    /// Evaluate migration policy and return pending actions
    pub fn evaluate_migrations(&self) -> Vec<MigrationAction> {
        let mut actions = Vec::new();

        match &self.migration_policy {
            MigrationPolicy::None => {},

            MigrationPolicy::ByAge { hot_retention, warm_retention } => {
                for ((topic, partition_id), segments) in &self.segments {
                    for seg in segments {
                        if seg.sealed {
                            let age = seg.age();
                            if seg.tier == StorageTier::Hot && age > *hot_retention {
                                actions.push(MigrationAction {
                                    segment_id: seg.segment_id,
                                    topic: topic.clone(),
                                    partition_id: *partition_id,
                                    from_tier: StorageTier::Hot,
                                    to_tier: StorageTier::Warm,
                                    reason: MigrationReason::AgeBased {
                                        age, threshold: *hot_retention,
                                    },
                                });
                            } else if seg.tier == StorageTier::Warm && age > *warm_retention {
                                actions.push(MigrationAction {
                                    segment_id: seg.segment_id,
                                    topic: topic.clone(),
                                    partition_id: *partition_id,
                                    from_tier: StorageTier::Warm,
                                    to_tier: StorageTier::Cold,
                                    reason: MigrationReason::AgeBased {
                                        age, threshold: *warm_retention,
                                    },
                                });
                            }
                        }
                    }
                }
            }

            MigrationPolicy::BySize { hot_max_bytes, warm_max_bytes } => {
                let hot_size = self.stats.hot_bytes();
                let warm_size = self.stats.warm_bytes();

                if hot_size > *hot_max_bytes {
                    // Find oldest sealed hot segments to migrate
                    let mut hot_segs: Vec<&SegmentMetadata> = self.segments.values()
                        .flat_map(|v| v.iter())
                        .filter(|s| s.tier == StorageTier::Hot && s.sealed)
                        .collect();
                    hot_segs.sort_by_key(|s| s.created_at);

                    let mut to_migrate = hot_size - *hot_max_bytes;
                    for seg in hot_segs {
                        if to_migrate == 0 { break; }
                        actions.push(MigrationAction {
                            segment_id: seg.segment_id,
                            topic: seg.topic.clone(),
                            partition_id: seg.partition_id,
                            from_tier: StorageTier::Hot,
                            to_tier: StorageTier::Warm,
                            reason: MigrationReason::SizeBased {
                                current_size: hot_size, max_size: *hot_max_bytes,
                            },
                        });
                        to_migrate = to_migrate.saturating_sub(seg.size_bytes);
                    }
                }

                if warm_size > *warm_max_bytes {
                    let mut warm_segs: Vec<&SegmentMetadata> = self.segments.values()
                        .flat_map(|v| v.iter())
                        .filter(|s| s.tier == StorageTier::Warm && s.sealed)
                        .collect();
                    warm_segs.sort_by_key(|s| s.created_at);

                    let mut to_migrate = warm_size - *warm_max_bytes;
                    for seg in warm_segs {
                        if to_migrate == 0 { break; }
                        actions.push(MigrationAction {
                            segment_id: seg.segment_id,
                            topic: seg.topic.clone(),
                            partition_id: seg.partition_id,
                            from_tier: StorageTier::Warm,
                            to_tier: StorageTier::Cold,
                            reason: MigrationReason::SizeBased {
                                current_size: warm_size, max_size: *warm_max_bytes,
                            },
                        });
                        to_migrate = to_migrate.saturating_sub(seg.size_bytes);
                    }
                }
            }

            MigrationPolicy::Hybrid { hot_retention, warm_retention, hot_max_bytes, warm_max_bytes } => {
                // Age-based first
                let age_policy = MigrationPolicy::ByAge {
                    hot_retention: *hot_retention,
                    warm_retention: *warm_retention,
                };
                let mut temp = TieredStorageManager::new(age_policy);
                temp.segments = self.segments.clone();
                temp.stats = self.stats.clone();
                actions.extend(temp.evaluate_migrations());

                // Size-based additional
                let size_policy = MigrationPolicy::BySize {
                    hot_max_bytes: *hot_max_bytes,
                    warm_max_bytes: *warm_max_bytes,
                };
                let mut temp2 = TieredStorageManager::new(size_policy);
                temp2.segments = self.segments.clone();
                temp2.stats = self.stats.clone();

                // Only add size-based actions for segments not already age-migrated
                let already_migrating: std::collections::HashSet<u64> = actions.iter()
                    .map(|a| a.segment_id).collect();
                for action in temp2.evaluate_migrations() {
                    if !already_migrating.contains(&action.segment_id) {
                        actions.push(action);
                    }
                }
            }
        }

        actions
    }

    /// Execute a migration (update segment tier)
    pub fn execute_migration(&mut self, action: &MigrationAction) -> bool {
        let key = (action.topic.clone(), action.partition_id);
        if let Some(segments) = self.segments.get_mut(&key) {
            if let Some(seg) = segments.iter_mut().find(|s| s.segment_id == action.segment_id) {
                if seg.tier != action.from_tier {
                    self.stats.migrations_failed += 1;
                    return false;
                }

                // Update tier stats
                let bytes = seg.size_bytes;
                if let Some(from_bytes) = self.stats.bytes_by_tier.get_mut(&action.from_tier) {
                    *from_bytes = from_bytes.saturating_sub(bytes);
                }
                *self.stats.bytes_by_tier.entry(action.to_tier).or_default() += bytes;

                if let Some(from_count) = self.stats.segments_by_tier.get_mut(&action.from_tier) {
                    *from_count = from_count.saturating_sub(1);
                }
                *self.stats.segments_by_tier.entry(action.to_tier).or_default() += 1;

                seg.tier = action.to_tier;
                self.stats.migrations_completed += 1;
                return true;
            }
        }
        self.stats.migrations_failed += 1;
        false
    }

    /// Delete expired segments (past cold retention)
    pub fn delete_expired_segments(&mut self) -> Vec<SegmentMetadata> {
        let cold_retention = self.tiers.get(&StorageTier::Cold)
            .and_then(|c| c.retention);

        let mut deleted = Vec::new();

        if let Some(retention) = cold_retention {
            for segments in self.segments.values_mut() {
                let expired: Vec<SegmentMetadata> = segments.iter()
                    .filter(|s| s.tier == StorageTier::Cold && s.sealed && s.age() > retention)
                    .cloned()
                    .collect();

                for seg in &expired {
                    self.stats.total_bytes = self.stats.total_bytes.saturating_sub(seg.size_bytes);
                    if let Some(tier_bytes) = self.stats.bytes_by_tier.get_mut(&StorageTier::Cold) {
                        *tier_bytes = tier_bytes.saturating_sub(seg.size_bytes);
                    }
                    self.stats.segments_deleted += 1;
                    self.stats.total_segments = self.stats.total_segments.saturating_sub(1);
                    if let Some(count) = self.stats.segments_by_tier.get_mut(&StorageTier::Cold) {
                        *count = count.saturating_sub(1);
                    }
                }

                segments.retain(|s| !(s.tier == StorageTier::Cold && s.sealed && s.age() > retention));
                deleted.extend(expired);
            }
        }

        deleted
    }

    /// Total number of segments across all partitions
    pub fn total_segments(&self) -> usize {
        self.segments.values().map(|v| v.len()).sum()
    }

    /// List all topic-partitions with segment counts
    pub fn partition_summary(&self) -> Vec<(String, u32, usize)> {
        self.segments.iter()
            .map(|((topic, partition_id), segs)| (topic.clone(), *partition_id, segs.len()))
            .collect()
    }
}

impl std::fmt::Display for TieredStorageManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f, "TieredStorageManager(tiers={}, partitions={}, segments={}, policy={})",
            self.tiers.len(),
            self.segments.len(),
            self.total_segments(),
            self.migration_policy
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_manager() -> TieredStorageManager {
        let mut mgr = TieredStorageManager::new(MigrationPolicy::ByAge {
            hot_retention: Duration::from_millis(50),
            warm_retention: Duration::from_millis(100),
        });
        mgr.add_tier(TierConfig::new(StorageTier::Hot, StorageBackend::InMemory));
        mgr.add_tier(TierConfig::new(StorageTier::Warm, StorageBackend::LocalDisk { path: "/data/warm".into() }));
        mgr.add_tier(TierConfig::new(StorageTier::Cold, StorageBackend::S3 {
            bucket: "archive".into(), region: "us-east-1".into(), prefix: "events/".into(),
        }).with_retention(Duration::from_secs(365 * 86400)));
        mgr
    }

    // --- StorageTier ---

    #[test]
    fn test_storage_tier_ordering() {
        assert!(StorageTier::Hot < StorageTier::Warm);
        assert!(StorageTier::Warm < StorageTier::Cold);
    }

    #[test]
    fn test_storage_tier_transitions() {
        assert_eq!(StorageTier::Hot.colder(), Some(StorageTier::Warm));
        assert_eq!(StorageTier::Warm.colder(), Some(StorageTier::Cold));
        assert_eq!(StorageTier::Cold.colder(), None);

        assert_eq!(StorageTier::Cold.warmer(), Some(StorageTier::Warm));
        assert_eq!(StorageTier::Warm.warmer(), Some(StorageTier::Hot));
        assert_eq!(StorageTier::Hot.warmer(), None);
    }

    #[test]
    fn test_storage_tier_display() {
        assert_eq!(StorageTier::Hot.to_string(), "Hot");
        assert_eq!(StorageTier::Cold.to_string(), "Cold");
    }

    // --- StorageBackend ---

    #[test]
    fn test_storage_backend_display() {
        assert_eq!(StorageBackend::InMemory.to_string(), "InMemory");
        let s3 = StorageBackend::S3 { bucket: "b".into(), region: "r".into(), prefix: "p".into() };
        assert!(s3.to_string().contains("S3"));
    }

    // --- TierConfig ---

    #[test]
    fn test_tier_config_builder() {
        let config = TierConfig::new(StorageTier::Hot, StorageBackend::InMemory)
            .with_retention(Duration::from_secs(3600))
            .with_max_size(1_000_000)
            .with_max_segments(100);

        assert_eq!(config.tier, StorageTier::Hot);
        assert_eq!(config.retention, Some(Duration::from_secs(3600)));
        assert_eq!(config.max_size_bytes, Some(1_000_000));
        assert_eq!(config.max_segments, Some(100));
        assert!(!config.read_only);
    }

    #[test]
    fn test_tier_config_read_only() {
        let config = TierConfig::new(StorageTier::Cold, StorageBackend::InMemory).read_only();
        assert!(config.read_only);
    }

    #[test]
    fn test_tier_config_validation() {
        let good = TierConfig::new(StorageTier::Hot, StorageBackend::InMemory)
            .with_retention(Duration::from_secs(60));
        assert!(good.validate().is_ok());

        let bad = TierConfig::new(StorageTier::Hot, StorageBackend::InMemory)
            .with_retention(Duration::from_secs(0));
        assert!(bad.validate().is_err());
    }

    // --- SegmentMetadata ---

    #[test]
    fn test_segment_metadata_new() {
        let seg = SegmentMetadata::new(1, "orders", 0, 0, StorageTier::Hot);
        assert_eq!(seg.segment_id, 1);
        assert_eq!(seg.topic, "orders");
        assert_eq!(seg.partition_id, 0);
        assert_eq!(seg.start_offset, 0);
        assert!(!seg.sealed);
    }

    #[test]
    fn test_segment_record_events() {
        let mut seg = SegmentMetadata::new(1, "orders", 0, 0, StorageTier::Hot);
        seg.record_event(0, 100, 1000);
        seg.record_event(1, 200, 2000);

        assert_eq!(seg.end_offset, 1);
        assert_eq!(seg.size_bytes, 300);
        assert_eq!(seg.event_count, 2);
        assert_eq!(seg.min_timestamp, 1000);
        assert_eq!(seg.max_timestamp, 2000);
    }

    #[test]
    fn test_segment_contains_offset() {
        let mut seg = SegmentMetadata::new(1, "t", 0, 10, StorageTier::Hot);
        seg.record_event(10, 50, 100);
        seg.record_event(20, 50, 200);

        assert!(seg.contains_offset(10));
        assert!(seg.contains_offset(15));
        assert!(seg.contains_offset(20));
        assert!(!seg.contains_offset(9));
        assert!(!seg.contains_offset(21));
    }

    #[test]
    fn test_segment_seal() {
        let mut seg = SegmentMetadata::new(1, "t", 0, 0, StorageTier::Hot);
        assert!(!seg.sealed);
        seg.seal();
        assert!(seg.sealed);
    }

    #[test]
    fn test_segment_avg_event_size() {
        let mut seg = SegmentMetadata::new(1, "t", 0, 0, StorageTier::Hot);
        assert!((seg.avg_event_size() - 0.0).abs() < f64::EPSILON);

        seg.record_event(0, 100, 100);
        seg.record_event(1, 200, 200);
        assert!((seg.avg_event_size() - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_segment_display() {
        let seg = SegmentMetadata::new(1, "orders", 0, 0, StorageTier::Hot);
        let display = seg.to_string();
        assert!(display.contains("Segment"));
        assert!(display.contains("orders"));
        assert!(display.contains("Hot"));
    }

    // --- MigrationPolicy ---

    #[test]
    fn test_migration_policy_display() {
        let policy = MigrationPolicy::ByAge {
            hot_retention: Duration::from_secs(60),
            warm_retention: Duration::from_secs(300),
        };
        assert!(policy.to_string().contains("ByAge"));

        assert_eq!(MigrationPolicy::None.to_string(), "None");
    }

    // --- MigrationReason ---

    #[test]
    fn test_migration_reason_display() {
        let reason = MigrationReason::ManualRequest;
        assert_eq!(reason.to_string(), "Manual");

        let reason = MigrationReason::TierFull;
        assert_eq!(reason.to_string(), "TierFull");
    }

    // --- TieredStorageManager ---

    #[test]
    fn test_manager_create_segment() {
        let mut mgr = setup_manager();
        let id = mgr.create_segment("orders", 0, 0, StorageTier::Hot);
        assert_eq!(id, 1);
        assert_eq!(mgr.total_segments(), 1);
    }

    #[test]
    fn test_manager_record_events() {
        let mut mgr = setup_manager();
        let id = mgr.create_segment("orders", 0, 0, StorageTier::Hot);
        assert!(mgr.record_event("orders", 0, id, 0, 100, 1000));
        assert!(mgr.record_event("orders", 0, id, 1, 200, 2000));

        assert_eq!(mgr.stats().total_bytes, 300);
        assert_eq!(mgr.stats().hot_bytes(), 300);
    }

    #[test]
    fn test_manager_find_segment() {
        let mut mgr = setup_manager();
        let id = mgr.create_segment("orders", 0, 0, StorageTier::Hot);
        mgr.record_event("orders", 0, id, 0, 100, 1000);
        mgr.record_event("orders", 0, id, 5, 100, 5000);

        let found = mgr.find_segment("orders", 0, 3);
        assert!(found.is_some());
        assert_eq!(found.unwrap().segment_id, id);

        assert!(mgr.find_segment("orders", 0, 6).is_none());
    }

    #[test]
    fn test_manager_seal_segment() {
        let mut mgr = setup_manager();
        let id = mgr.create_segment("orders", 0, 0, StorageTier::Hot);
        assert!(mgr.seal_segment("orders", 0, id));

        let seg = mgr.find_segment("orders", 0, 0).unwrap();
        assert!(seg.sealed);
    }

    #[test]
    fn test_manager_segments_in_tier() {
        let mut mgr = setup_manager();
        mgr.create_segment("orders", 0, 0, StorageTier::Hot);
        mgr.create_segment("orders", 0, 100, StorageTier::Warm);

        let hot = mgr.segments_in_tier("orders", 0, StorageTier::Hot);
        assert_eq!(hot.len(), 1);

        let warm = mgr.segments_in_tier("orders", 0, StorageTier::Warm);
        assert_eq!(warm.len(), 1);
    }

    #[test]
    fn test_manager_evaluate_migrations_age_based() {
        let mut mgr = setup_manager();
        let id = mgr.create_segment("orders", 0, 0, StorageTier::Hot);
        mgr.record_event("orders", 0, id, 0, 100, 1000);
        mgr.seal_segment("orders", 0, id);

        // Wait for hot retention to expire
        std::thread::sleep(Duration::from_millis(60));

        let actions = mgr.evaluate_migrations();
        assert!(!actions.is_empty());
        assert_eq!(actions[0].from_tier, StorageTier::Hot);
        assert_eq!(actions[0].to_tier, StorageTier::Warm);
    }

    #[test]
    fn test_manager_execute_migration() {
        let mut mgr = setup_manager();
        let id = mgr.create_segment("orders", 0, 0, StorageTier::Hot);
        mgr.record_event("orders", 0, id, 0, 100, 1000);
        mgr.seal_segment("orders", 0, id);

        let action = MigrationAction {
            segment_id: id,
            topic: "orders".into(),
            partition_id: 0,
            from_tier: StorageTier::Hot,
            to_tier: StorageTier::Warm,
            reason: MigrationReason::ManualRequest,
        };

        assert!(mgr.execute_migration(&action));
        assert_eq!(mgr.stats().migrations_completed, 1);
        assert_eq!(mgr.stats().hot_bytes(), 0);
        assert_eq!(mgr.stats().warm_bytes(), 100);
    }

    #[test]
    fn test_manager_execute_migration_wrong_tier() {
        let mut mgr = setup_manager();
        let id = mgr.create_segment("orders", 0, 0, StorageTier::Hot);

        let action = MigrationAction {
            segment_id: id,
            topic: "orders".into(),
            partition_id: 0,
            from_tier: StorageTier::Warm, // wrong! segment is Hot
            to_tier: StorageTier::Cold,
            reason: MigrationReason::ManualRequest,
        };

        assert!(!mgr.execute_migration(&action));
        assert_eq!(mgr.stats().migrations_failed, 1);
    }

    #[test]
    fn test_manager_evaluate_size_based() {
        let mut mgr = TieredStorageManager::new(MigrationPolicy::BySize {
            hot_max_bytes: 100,
            warm_max_bytes: 200,
        });
        mgr.add_tier(TierConfig::new(StorageTier::Hot, StorageBackend::InMemory));
        mgr.add_tier(TierConfig::new(StorageTier::Warm, StorageBackend::InMemory));

        let id = mgr.create_segment("t", 0, 0, StorageTier::Hot);
        mgr.record_event("t", 0, id, 0, 150, 1000); // exceeds 100 byte limit
        mgr.seal_segment("t", 0, id);

        let actions = mgr.evaluate_migrations();
        assert!(!actions.is_empty());
    }

    #[test]
    fn test_manager_no_migration_none_policy() {
        let mut mgr = TieredStorageManager::new(MigrationPolicy::None);
        let id = mgr.create_segment("t", 0, 0, StorageTier::Hot);
        mgr.record_event("t", 0, id, 0, 100, 1000);
        mgr.seal_segment("t", 0, id);

        let actions = mgr.evaluate_migrations();
        assert!(actions.is_empty());
    }

    #[test]
    fn test_manager_partition_summary() {
        let mut mgr = setup_manager();
        mgr.create_segment("orders", 0, 0, StorageTier::Hot);
        mgr.create_segment("orders", 1, 0, StorageTier::Hot);
        mgr.create_segment("payments", 0, 0, StorageTier::Hot);

        let summary = mgr.partition_summary();
        assert_eq!(summary.len(), 3);
    }

    #[test]
    fn test_manager_display() {
        let mgr = setup_manager();
        let display = mgr.to_string();
        assert!(display.contains("TieredStorageManager"));
        assert!(display.contains("ByAge"));
    }

    #[test]
    fn test_tiered_storage_stats_display() {
        let stats = TieredStorageStats::default();
        let display = stats.to_string();
        assert!(display.contains("TieredStorage"));
    }

    #[test]
    fn test_manager_record_event_nonexistent() {
        let mut mgr = setup_manager();
        assert!(!mgr.record_event("t", 0, 999, 0, 100, 1000));
    }

    #[test]
    fn test_manager_seal_nonexistent() {
        let mut mgr = setup_manager();
        assert!(!mgr.seal_segment("t", 0, 999));
    }
}
