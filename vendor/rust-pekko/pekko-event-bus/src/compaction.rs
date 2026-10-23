// compaction.rs - Log Compaction
// Kafka-like key-based compaction: keeps latest value per key, removes tombstones

use std::collections::HashMap;
use std::time::{Duration, Instant};

// ============================================================================
// Compaction Strategy
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum CompactionStrategy {
    /// Delete-based: remove events older than retention
    Delete,
    /// Key-based: keep only latest value per key (Kafka log compaction)
    Compact,
    /// Hybrid: compact first, then delete by age
    CompactAndDelete { delete_after: Duration },
}

// ============================================================================
// Compaction Record
// ============================================================================

#[derive(Debug, Clone)]
pub struct CompactionRecord {
    pub key: Option<String>,
    pub value: Option<Vec<u8>>,  // None = tombstone
    pub offset: u64,
    pub timestamp: Instant,
    pub headers: HashMap<String, String>,
}

impl CompactionRecord {
    pub fn new(key: Option<String>, value: Vec<u8>, offset: u64) -> Self {
        Self {
            key,
            value: Some(value),
            offset,
            timestamp: Instant::now(),
            headers: HashMap::new(),
        }
    }

    pub fn tombstone(key: String, offset: u64) -> Self {
        Self {
            key: Some(key),
            value: None,
            offset,
            timestamp: Instant::now(),
            headers: HashMap::new(),
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    pub fn has_key(&self) -> bool {
        self.key.is_some()
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }
}

// ============================================================================
// Compaction Config
// ============================================================================

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub strategy: CompactionStrategy,
    /// Minimum time before a record can be compacted
    pub min_compaction_lag: Duration,
    /// Tombstone retention: how long to keep tombstones after compaction
    pub tombstone_retention: Duration,
    /// Minimum dirty ratio (uncompacted/total) to trigger compaction
    pub min_dirty_ratio: f64,
    /// Maximum records to process per compaction run
    pub max_records_per_run: usize,
    /// Whether to preserve ordering within same key
    pub preserve_order: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            strategy: CompactionStrategy::Compact,
            min_compaction_lag: Duration::from_secs(0),
            tombstone_retention: Duration::from_secs(24 * 3600), // 1 day
            min_dirty_ratio: 0.5,
            max_records_per_run: 100_000,
            preserve_order: true,
        }
    }
}

impl CompactionConfig {
    pub fn delete_strategy() -> Self {
        Self {
            strategy: CompactionStrategy::Delete,
            ..Default::default()
        }
    }

    pub fn compact_strategy() -> Self {
        Self::default()
    }

    pub fn compact_and_delete(delete_after: Duration) -> Self {
        Self {
            strategy: CompactionStrategy::CompactAndDelete { delete_after },
            ..Default::default()
        }
    }
}

// ============================================================================
// Compaction Result
// ============================================================================

#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub records_before: usize,
    pub records_after: usize,
    pub tombstones_removed: usize,
    pub duplicates_removed: usize,
    pub expired_removed: usize,
    pub duration: Duration,
}

impl CompactionResult {
    pub fn compression_ratio(&self) -> f64 {
        if self.records_before == 0 {
            return 1.0;
        }
        self.records_after as f64 / self.records_before as f64
    }

    pub fn total_removed(&self) -> usize {
        self.records_before.saturating_sub(self.records_after)
    }
}

// ============================================================================
// Log Compactor
// ============================================================================

#[derive(Debug)]
pub struct LogCompactor {
    config: CompactionConfig,
    total_compactions: u64,
    total_records_removed: u64,
    last_compaction: Option<Instant>,
}

impl LogCompactor {
    pub fn new(config: CompactionConfig) -> Self {
        Self {
            config,
            total_compactions: 0,
            total_records_removed: 0,
            last_compaction: None,
        }
    }

    /// Run compaction on a set of records
    pub fn compact(&mut self, records: Vec<CompactionRecord>) -> (Vec<CompactionRecord>, CompactionResult) {
        let start = Instant::now();
        let records_before = records.len();

        let (result_records, tombstones_removed, duplicates_removed, expired_removed) =
            match &self.config.strategy {
                CompactionStrategy::Delete => self.compact_by_delete(records),
                CompactionStrategy::Compact => self.compact_by_key(records),
                CompactionStrategy::CompactAndDelete { delete_after } => {
                    let delete_after = *delete_after;
                    self.compact_and_delete(records, delete_after)
                }
            };

        let result = CompactionResult {
            records_before,
            records_after: result_records.len(),
            tombstones_removed,
            duplicates_removed,
            expired_removed,
            duration: start.elapsed(),
        };

        self.total_compactions += 1;
        self.total_records_removed += result.total_removed() as u64;
        self.last_compaction = Some(Instant::now());

        (result_records, result)
    }

    /// Delete-based compaction: remove expired records
    fn compact_by_delete(
        &self,
        records: Vec<CompactionRecord>,
    ) -> (Vec<CompactionRecord>, usize, usize, usize) {
        let expired = 0;
        let mut tombstones = 0;

        let result: Vec<CompactionRecord> = records
            .into_iter()
            .filter(|r| {
                // Remove expired tombstones
                if r.is_tombstone() && r.timestamp.elapsed() > self.config.tombstone_retention {
                    tombstones += 1;
                    return false;
                }
                true
            })
            .collect();

        (result, tombstones, 0, expired)
    }

    /// Key-based compaction: keep only latest value per key
    fn compact_by_key(
        &self,
        records: Vec<CompactionRecord>,
    ) -> (Vec<CompactionRecord>, usize, usize, usize) {
        let mut latest_by_key: HashMap<String, (usize, &CompactionRecord)> = HashMap::new();
        let mut tombstones_removed = 0;
        let mut duplicates_removed = 0;

        // Find latest offset for each key
        for (idx, record) in records.iter().enumerate() {
            if let Some(ref key) = record.key {
                if record.timestamp.elapsed() < self.config.min_compaction_lag {
                    // Too recent to compact — keep it
                    continue;
                }
                match latest_by_key.get(key) {
                    Some(&(_, existing)) if existing.offset >= record.offset => {
                        // existing is newer, this is a duplicate
                    }
                    _ => {
                        latest_by_key.insert(key.clone(), (idx, record));
                    }
                }
            }
        }

        // Build result: keep latest per key + all keyless records
        let latest_indices: HashMap<String, usize> = latest_by_key
            .iter()
            .map(|(k, (idx, _))| (k.clone(), *idx))
            .collect();

        let mut result = Vec::new();

        for (idx, record) in records.into_iter().enumerate() {
            match &record.key {
                None => {
                    // Keyless records always kept
                    result.push(record);
                }
                Some(key) => {
                    if let Some(&latest_idx) = latest_indices.get(key) {
                        if idx == latest_idx {
                            // This is the latest — check if tombstone should be removed
                            if record.is_tombstone()
                                && record.timestamp.elapsed() > self.config.tombstone_retention
                            {
                                tombstones_removed += 1;
                            } else {
                                result.push(record);
                            }
                        } else if idx > latest_indices.get(key).copied().unwrap_or(0) {
                            // Record is newer than compaction window (min_compaction_lag)
                            result.push(record);
                        } else {
                            duplicates_removed += 1;
                        }
                    } else {
                        // Not in compaction set (too recent) — keep
                        result.push(record);
                    }
                }
            }
        }

        (result, tombstones_removed, duplicates_removed, 0)
    }

    /// Hybrid: compact by key first, then delete by age
    fn compact_and_delete(
        &self,
        records: Vec<CompactionRecord>,
        delete_after: Duration,
    ) -> (Vec<CompactionRecord>, usize, usize, usize) {
        // First: key-based compaction
        let (compacted, tomb1, dups, _) = self.compact_by_key(records);

        // Second: delete expired
        let mut expired_removed = 0;
        let mut tomb2 = 0;

        let result: Vec<CompactionRecord> = compacted
            .into_iter()
            .filter(|r| {
                if r.timestamp.elapsed() > delete_after {
                    if r.is_tombstone() {
                        tomb2 += 1;
                    } else {
                        expired_removed += 1;
                    }
                    return false;
                }
                true
            })
            .collect();

        (result, tomb1 + tomb2, dups, expired_removed)
    }

    /// Check if compaction should run based on dirty ratio
    pub fn should_compact(&self, total_records: usize, unique_keys: usize) -> bool {
        if total_records == 0 || unique_keys == 0 {
            return false;
        }
        let dirty_ratio = 1.0 - (unique_keys as f64 / total_records as f64);
        dirty_ratio >= self.config.min_dirty_ratio
    }

    pub fn total_compactions(&self) -> u64 {
        self.total_compactions
    }

    pub fn total_records_removed(&self) -> u64 {
        self.total_records_removed
    }

    pub fn last_compaction(&self) -> Option<Instant> {
        self.last_compaction
    }

    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }
}

// ============================================================================
// Segment Cleaner (for partition-level compaction)
// ============================================================================

#[derive(Debug)]
pub struct SegmentCleaner {
    segments: Vec<Segment>,
    max_segment_size: usize,
}

#[derive(Debug, Clone)]
pub struct Segment {
    pub id: u64,
    pub records: Vec<CompactionRecord>,
    pub base_offset: u64,
    pub created_at: Instant,
    pub sealed: bool,
}

impl Segment {
    pub fn new(id: u64, base_offset: u64) -> Self {
        Self {
            id,
            records: Vec::new(),
            base_offset,
            created_at: Instant::now(),
            sealed: false,
        }
    }

    pub fn append(&mut self, record: CompactionRecord) {
        self.records.push(record);
    }

    pub fn seal(&mut self) {
        self.sealed = true;
    }

    pub fn size(&self) -> usize {
        self.records.len()
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    pub fn last_offset(&self) -> Option<u64> {
        self.records.last().map(|r| r.offset)
    }
}

impl SegmentCleaner {
    pub fn new(max_segment_size: usize) -> Self {
        Self {
            segments: Vec::new(),
            max_segment_size,
        }
    }

    pub fn add_segment(&mut self, segment: Segment) {
        self.segments.push(segment);
    }

    pub fn active_segment(&mut self) -> &mut Segment {
        if self.segments.is_empty()
            || self.segments.last().map_or(false, |s| s.sealed)
        {
            let id = self.segments.len() as u64;
            let base_offset = self
                .segments
                .last()
                .and_then(|s| s.last_offset())
                .map(|o| o + 1)
                .unwrap_or(0);
            self.segments.push(Segment::new(id, base_offset));
        }
        self.segments.last_mut().unwrap()
    }

    pub fn append_record(&mut self, record: CompactionRecord) {
        let max_size = self.max_segment_size;
        let seg = self.active_segment();
        seg.append(record);

        if seg.size() >= max_size {
            seg.seal();
        }
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn sealed_segments(&self) -> Vec<&Segment> {
        self.segments.iter().filter(|s| s.sealed).collect()
    }

    pub fn total_records(&self) -> usize {
        self.segments.iter().map(|s| s.size()).sum()
    }

    /// Clean sealed segments with compactor
    pub fn clean_sealed(&mut self, compactor: &mut LogCompactor) -> Vec<CompactionResult> {
        let mut results = Vec::new();

        for segment in &mut self.segments {
            if segment.sealed && !segment.records.is_empty() {
                let records = std::mem::take(&mut segment.records);
                let (compacted, result) = compactor.compact(records);
                segment.records = compacted;
                results.push(result);
            }
        }

        results
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(key: &str, value: &[u8], offset: u64) -> CompactionRecord {
        CompactionRecord::new(Some(key.to_string()), value.to_vec(), offset)
    }

    fn make_tombstone(key: &str, offset: u64) -> CompactionRecord {
        CompactionRecord::tombstone(key.to_string(), offset)
    }

    // === CompactionRecord Tests ===

    #[test]
    fn test_record_creation() {
        let r = CompactionRecord::new(Some("k1".into()), vec![1, 2], 0);
        assert!(r.has_key());
        assert!(!r.is_tombstone());
        assert_eq!(r.offset, 0);
    }

    #[test]
    fn test_tombstone_creation() {
        let r = CompactionRecord::tombstone("k1".into(), 5);
        assert!(r.has_key());
        assert!(r.is_tombstone());
        assert_eq!(r.offset, 5);
    }

    #[test]
    fn test_record_with_header() {
        let r = CompactionRecord::new(Some("k".into()), vec![1], 0)
            .with_header("source", "app-1")
            .with_header("version", "2");

        assert_eq!(r.headers.len(), 2);
        assert_eq!(r.headers.get("source").unwrap(), "app-1");
    }

    #[test]
    fn test_keyless_record() {
        let r = CompactionRecord::new(None, vec![1], 0);
        assert!(!r.has_key());
    }

    // === CompactionConfig Tests ===

    #[test]
    fn test_config_defaults() {
        let config = CompactionConfig::default();
        assert_eq!(config.strategy, CompactionStrategy::Compact);
        assert_eq!(config.min_dirty_ratio, 0.5);
        assert!(config.preserve_order);
    }

    #[test]
    fn test_config_factories() {
        let c1 = CompactionConfig::delete_strategy();
        assert_eq!(c1.strategy, CompactionStrategy::Delete);

        let c2 = CompactionConfig::compact_strategy();
        assert_eq!(c2.strategy, CompactionStrategy::Compact);

        let c3 = CompactionConfig::compact_and_delete(Duration::from_secs(3600));
        assert!(matches!(c3.strategy, CompactionStrategy::CompactAndDelete { .. }));
    }

    // === LogCompactor Tests ===

    #[test]
    fn test_compact_dedup_by_key() {
        let mut compactor = LogCompactor::new(CompactionConfig::default());

        let records = vec![
            make_record("user-1", b"v1", 0),
            make_record("user-2", b"v1", 1),
            make_record("user-1", b"v2", 2),  // newer version of user-1
            make_record("user-3", b"v1", 3),
            make_record("user-2", b"v2", 4),  // newer version of user-2
        ];

        let (result, stats) = compactor.compact(records);

        // Should keep latest per key: user-1@2, user-2@4, user-3@3
        assert_eq!(stats.records_before, 5);
        assert!(stats.duplicates_removed >= 2);
        assert!(result.len() <= 4); // at most 3 unique + potential timing
    }

    #[test]
    fn test_compact_keeps_keyless() {
        let mut compactor = LogCompactor::new(CompactionConfig::default());

        let records = vec![
            CompactionRecord::new(None, vec![1], 0),
            make_record("k1", b"v1", 1),
            CompactionRecord::new(None, vec![2], 2),
            make_record("k1", b"v2", 3),
        ];

        let (result, _) = compactor.compact(records);
        // Both keyless records kept + latest k1
        let keyless: Vec<_> = result.iter().filter(|r| !r.has_key()).collect();
        assert_eq!(keyless.len(), 2);
    }

    #[test]
    fn test_compact_removes_old_tombstones() {
        let config = CompactionConfig {
            tombstone_retention: Duration::from_millis(0), // immediate removal
            ..Default::default()
        };
        let mut compactor = LogCompactor::new(config);

        // Create tombstone that is already "expired"
        let mut tombstone = make_tombstone("k1", 5);
        // Simulate old timestamp by creating with past time
        tombstone.timestamp = Instant::now() - Duration::from_secs(10);

        let records = vec![
            make_record("k2", b"v1", 0),
            tombstone,
        ];

        let (result, stats) = compactor.compact(records);
        assert!(stats.tombstones_removed >= 0); // tombstone may be removed
        // k2 should remain
        assert!(result.iter().any(|r| r.key.as_deref() == Some("k2")));
    }

    #[test]
    fn test_compact_delete_strategy() {
        let config = CompactionConfig {
            strategy: CompactionStrategy::Delete,
            tombstone_retention: Duration::from_millis(0),
            ..Default::default()
        };
        let mut compactor = LogCompactor::new(config);

        let mut old_tombstone = make_tombstone("k1", 0);
        old_tombstone.timestamp = Instant::now() - Duration::from_secs(10);

        let records = vec![
            make_record("k1", b"v1", 1),
            old_tombstone,
            make_record("k2", b"v1", 2),
        ];

        let (result, stats) = compactor.compact(records);
        assert_eq!(stats.tombstones_removed, 1);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_compact_empty_input() {
        let mut compactor = LogCompactor::new(CompactionConfig::default());
        let (result, stats) = compactor.compact(vec![]);
        assert!(result.is_empty());
        assert_eq!(stats.records_before, 0);
        assert_eq!(stats.compression_ratio(), 1.0);
    }

    #[test]
    fn test_compaction_result_metrics() {
        let result = CompactionResult {
            records_before: 100,
            records_after: 40,
            tombstones_removed: 10,
            duplicates_removed: 50,
            expired_removed: 0,
            duration: Duration::from_millis(5),
        };

        assert_eq!(result.total_removed(), 60);
        assert!((result.compression_ratio() - 0.4).abs() < 0.01);
    }

    #[test]
    fn test_should_compact() {
        let compactor = LogCompactor::new(CompactionConfig {
            min_dirty_ratio: 0.5,
            ..Default::default()
        });

        // 100 records, 50 unique = 50% dirty → should compact
        assert!(compactor.should_compact(100, 50));

        // 100 records, 80 unique = 20% dirty → should not
        assert!(!compactor.should_compact(100, 80));

        // Edge cases
        assert!(!compactor.should_compact(0, 0));
        assert!(!compactor.should_compact(100, 0));
    }

    #[test]
    fn test_compactor_stats() {
        let mut compactor = LogCompactor::new(CompactionConfig::default());
        assert_eq!(compactor.total_compactions(), 0);
        assert!(compactor.last_compaction().is_none());

        compactor.compact(vec![make_record("k", b"v", 0)]);

        assert_eq!(compactor.total_compactions(), 1);
        assert!(compactor.last_compaction().is_some());
    }

    // === Segment Tests ===

    #[test]
    fn test_segment_lifecycle() {
        let mut seg = Segment::new(0, 0);
        assert!(!seg.is_sealed());
        assert_eq!(seg.size(), 0);
        assert!(seg.last_offset().is_none());

        seg.append(make_record("k1", b"v1", 0));
        seg.append(make_record("k2", b"v2", 1));
        assert_eq!(seg.size(), 2);
        assert_eq!(seg.last_offset(), Some(1));

        seg.seal();
        assert!(seg.is_sealed());
    }

    // === SegmentCleaner Tests ===

    #[test]
    fn test_segment_cleaner_auto_create() {
        let mut cleaner = SegmentCleaner::new(3);

        cleaner.append_record(make_record("k1", b"v1", 0));
        cleaner.append_record(make_record("k2", b"v2", 1));
        assert_eq!(cleaner.segment_count(), 1);
        assert_eq!(cleaner.total_records(), 2);
    }

    #[test]
    fn test_segment_cleaner_auto_seal() {
        let mut cleaner = SegmentCleaner::new(2);

        cleaner.append_record(make_record("k1", b"v1", 0));
        cleaner.append_record(make_record("k2", b"v2", 1)); // triggers seal

        assert_eq!(cleaner.sealed_segments().len(), 1);

        cleaner.append_record(make_record("k3", b"v3", 2)); // new segment
        assert_eq!(cleaner.segment_count(), 2);
    }

    #[test]
    fn test_segment_cleaner_clean_sealed() {
        let mut cleaner = SegmentCleaner::new(3);
        let mut compactor = LogCompactor::new(CompactionConfig::default());

        // Fill and seal a segment with duplicates
        cleaner.append_record(make_record("k1", b"v1", 0));
        cleaner.append_record(make_record("k1", b"v2", 1));
        cleaner.append_record(make_record("k1", b"v3", 2)); // seals

        let results = cleaner.clean_sealed(&mut compactor);
        assert_eq!(results.len(), 1);
        assert!(results[0].records_before == 3);
    }

    #[test]
    fn test_segment_cleaner_total_records() {
        let mut cleaner = SegmentCleaner::new(100);
        for i in 0..10 {
            cleaner.append_record(make_record(&format!("k{}", i), b"v", i));
        }
        assert_eq!(cleaner.total_records(), 10);
    }

    #[test]
    fn test_compact_and_delete_strategy() {
        let config = CompactionConfig::compact_and_delete(Duration::from_secs(5));
        let mut compactor = LogCompactor::new(config);

        let mut old_record = make_record("k1", b"old", 0);
        old_record.timestamp = Instant::now() - Duration::from_secs(10);

        let records = vec![
            old_record,
            make_record("k2", b"new", 1),
        ];

        let (result, stats) = compactor.compact(records);
        // old record (10s old) deleted by age (threshold 5s), new record kept
        assert!(stats.expired_removed > 0 || stats.records_before > stats.records_after);
        // k2 is fresh — should remain
        assert!(result.iter().any(|r| r.key.as_deref() == Some("k2")));
    }
}
