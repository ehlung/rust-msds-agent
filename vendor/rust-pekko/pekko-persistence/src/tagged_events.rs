// tagged_events.rs - Enhanced tag management, indexing, and composite tag queries
// Extends basic Vec<String> tags with structured tagging, hierarchy, and efficient lookup

use std::collections::{HashMap, HashSet, BTreeMap};
use std::fmt;

// ============================================================================
// Tag - Structured tag with optional namespace and value
// ============================================================================

/// A structured tag with optional namespace and value
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tag {
    /// Namespace (e.g., "domain", "priority", "region")
    pub namespace: Option<String>,
    /// Key (e.g., "order", "high", "us-east")
    pub key: String,
    /// Optional value (e.g., "created", "1", "prod")
    pub value: Option<String>,
}

impl Tag {
    /// Simple tag: "order"
    pub fn simple(key: &str) -> Self {
        Self {
            namespace: None,
            key: key.to_string(),
            value: None,
        }
    }

    /// Namespaced tag: "domain:order"
    pub fn namespaced(namespace: &str, key: &str) -> Self {
        Self {
            namespace: Some(namespace.to_string()),
            key: key.to_string(),
            value: None,
        }
    }

    /// Key-value tag: "priority=high"
    pub fn key_value(key: &str, value: &str) -> Self {
        Self {
            namespace: None,
            key: key.to_string(),
            value: Some(value.to_string()),
        }
    }

    /// Full tag: "domain:order=created"
    pub fn full(namespace: &str, key: &str, value: &str) -> Self {
        Self {
            namespace: Some(namespace.to_string()),
            key: key.to_string(),
            value: Some(value.to_string()),
        }
    }

    /// Parse from string: "namespace:key=value" or "key=value" or "key"
    pub fn parse(s: &str) -> Self {
        let (ns, rest) = if let Some(idx) = s.find(':') {
            (Some(s[..idx].to_string()), &s[idx + 1..])
        } else {
            (None, s)
        };

        let (key, val) = if let Some(idx) = rest.find('=') {
            (rest[..idx].to_string(), Some(rest[idx + 1..].to_string()))
        } else {
            (rest.to_string(), None)
        };

        Self {
            namespace: ns,
            key,
            value: val,
        }
    }

    /// Convert to canonical string representation
    pub fn to_canonical(&self) -> String {
        let mut s = String::new();
        if let Some(ref ns) = self.namespace {
            s.push_str(ns);
            s.push(':');
        }
        s.push_str(&self.key);
        if let Some(ref val) = self.value {
            s.push('=');
            s.push_str(val);
        }
        s
    }

    /// Check if this tag matches a pattern (supports wildcards)
    pub fn matches_pattern(&self, pattern: &TagPattern) -> bool {
        // Namespace match
        if let Some(ref pns) = pattern.namespace {
            if pns != "*" {
                match &self.namespace {
                    Some(ns) => {
                        if ns != pns {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
        }

        // Key match
        if pattern.key != "*" && self.key != pattern.key {
            return false;
        }

        // Value match
        if let Some(ref pval) = pattern.value {
            if pval != "*" {
                match &self.value {
                    Some(val) => {
                        if val != pval {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
        }

        true
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_canonical())
    }
}

// ============================================================================
// TagPattern - For matching tags with wildcards
// ============================================================================

/// Pattern for matching tags (supports "*" wildcards)
#[derive(Debug, Clone)]
pub struct TagPattern {
    pub namespace: Option<String>,
    pub key: String,
    pub value: Option<String>,
}

impl TagPattern {
    pub fn any() -> Self {
        Self {
            namespace: None,
            key: "*".to_string(),
            value: None,
        }
    }

    pub fn key(key: &str) -> Self {
        Self {
            namespace: None,
            key: key.to_string(),
            value: None,
        }
    }

    pub fn namespaced(ns: &str, key: &str) -> Self {
        Self {
            namespace: Some(ns.to_string()),
            key: key.to_string(),
            value: None,
        }
    }

    pub fn key_value(key: &str, value: &str) -> Self {
        Self {
            namespace: None,
            key: key.to_string(),
            value: Some(value.to_string()),
        }
    }

    pub fn in_namespace(ns: &str) -> Self {
        Self {
            namespace: Some(ns.to_string()),
            key: "*".to_string(),
            value: None,
        }
    }
}

// ============================================================================
// Composite Tag Query
// ============================================================================

/// Composite query combining multiple tag conditions
#[derive(Debug, Clone)]
pub enum TagQuery {
    /// Single tag match
    Match(TagPattern),
    /// All conditions must match (AND)
    All(Vec<TagQuery>),
    /// Any condition must match (OR)
    Any(Vec<TagQuery>),
    /// Negate a condition (NOT)
    Not(Box<TagQuery>),
    /// Has any tag in namespace
    HasNamespace(String),
    /// Has a specific key regardless of value
    HasKey(String),
}

impl TagQuery {
    pub fn tag(key: &str) -> Self {
        TagQuery::Match(TagPattern::key(key))
    }

    pub fn tag_value(key: &str, value: &str) -> Self {
        TagQuery::Match(TagPattern::key_value(key, value))
    }

    pub fn and(queries: Vec<TagQuery>) -> Self {
        TagQuery::All(queries)
    }

    pub fn or(queries: Vec<TagQuery>) -> Self {
        TagQuery::Any(queries)
    }

    pub fn not(query: TagQuery) -> Self {
        TagQuery::Not(Box::new(query))
    }

    /// Evaluate this query against a set of tags
    pub fn evaluate(&self, tags: &[Tag]) -> bool {
        match self {
            TagQuery::Match(pattern) => tags.iter().any(|t| t.matches_pattern(pattern)),
            TagQuery::All(queries) => queries.iter().all(|q| q.evaluate(tags)),
            TagQuery::Any(queries) => queries.iter().any(|q| q.evaluate(tags)),
            TagQuery::Not(query) => !query.evaluate(tags),
            TagQuery::HasNamespace(ns) => {
                tags.iter().any(|t| t.namespace.as_deref() == Some(ns))
            }
            TagQuery::HasKey(key) => tags.iter().any(|t| t.key == *key),
        }
    }
}

// ============================================================================
// TagIndex - Efficient tag-based event lookup
// ============================================================================

/// An indexed entry for tag lookups
#[derive(Debug, Clone)]
pub struct TaggedEvent {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub tags: Vec<Tag>,
    pub timestamp: u64,
}

/// In-memory tag index for efficient tag-based queries
pub struct TagIndex {
    /// tag_canonical -> list of (persistence_id, sequence_nr)
    by_tag: HashMap<String, Vec<EventRef>>,
    /// namespace -> set of tag keys
    by_namespace: HashMap<String, HashSet<String>>,
    /// All known tags with counts
    tag_counts: BTreeMap<String, u64>,
    /// Total indexed events
    total_events: u64,
    /// Stats
    stats: TagIndexStats,
}

#[derive(Debug, Clone)]
struct EventRef {
    persistence_id: String,
    sequence_nr: u64
}

/// Statistics for the tag index
#[derive(Debug, Clone, Default)]
pub struct TagIndexStats {
    pub total_indexed: u64,
    pub total_queries: u64,
    pub total_results: u64,
    pub unique_tags: usize,
    pub unique_namespaces: usize,
}

impl TagIndex {
    pub fn new() -> Self {
        Self {
            by_tag: HashMap::new(),
            by_namespace: HashMap::new(),
            tag_counts: BTreeMap::new(),
            total_events: 0,
            stats: TagIndexStats::default(),
        }
    }

    /// Index a tagged event
    pub fn index(&mut self, event: &TaggedEvent) {
        self.total_events += 1;
        self.stats.total_indexed += 1;

        for tag in &event.tags {
            let canonical = tag.to_canonical();

            // Add to tag index
            self.by_tag
                .entry(canonical.clone())
                .or_default()
                .push(EventRef {
                    persistence_id: event.persistence_id.clone(),
                    sequence_nr: event.sequence_nr
                });

            // Update tag counts
            *self.tag_counts.entry(canonical).or_insert(0) += 1;

            // Update namespace index
            if let Some(ref ns) = tag.namespace {
                self.by_namespace
                    .entry(ns.clone())
                    .or_default()
                    .insert(tag.key.clone());
            }
        }

        self.stats.unique_tags = self.tag_counts.len();
        self.stats.unique_namespaces = self.by_namespace.len();
    }

    /// Query events by exact tag
    pub fn query_by_tag(&mut self, tag: &str) -> Vec<(String, u64)> {
        self.stats.total_queries += 1;
        let results: Vec<(String, u64)> = self
            .by_tag
            .get(tag)
            .map(|refs| {
                refs.iter()
                    .map(|r| (r.persistence_id.clone(), r.sequence_nr))
                    .collect()
            })
            .unwrap_or_default();
        self.stats.total_results += results.len() as u64;
        results
    }

    /// Query events matching a composite query
    pub fn query(&mut self, query: &TagQuery) -> Vec<(String, u64)> {
        self.stats.total_queries += 1;
        // Collect all unique events and check query
        let mut seen = HashSet::new();
        let mut results = Vec::new();

        for refs in self.by_tag.values() {
            for event_ref in refs {
                let key = (event_ref.persistence_id.clone(), event_ref.sequence_nr);
                if seen.contains(&key) {
                    continue;
                }

                // Reconstruct tags for this event from the index
                let event_tags: Vec<Tag> = self
                    .by_tag
                    .iter()
                    .filter(|(_, refs)| {
                        refs.iter().any(|r| {
                            r.persistence_id == event_ref.persistence_id
                                && r.sequence_nr == event_ref.sequence_nr
                        })
                    })
                    .map(|(tag_str, _)| Tag::parse(tag_str))
                    .collect();

                if query.evaluate(&event_tags) {
                    seen.insert(key.clone());
                    results.push(key);
                }
            }
        }

        self.stats.total_results += results.len() as u64;
        results
    }

    /// Get all tags in a namespace
    pub fn tags_in_namespace(&self, namespace: &str) -> Vec<String> {
        self.by_namespace
            .get(namespace)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get all known namespaces
    pub fn namespaces(&self) -> Vec<String> {
        self.by_namespace.keys().cloned().collect()
    }

    /// Get tag counts (top N)
    pub fn top_tags(&self, n: usize) -> Vec<(String, u64)> {
        let mut tags: Vec<(String, u64)> = self
            .tag_counts
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        tags.sort_by(|a, b| b.1.cmp(&a.1));
        tags.truncate(n);
        tags
    }

    /// Get count for a specific tag
    pub fn tag_count(&self, tag: &str) -> u64 {
        self.tag_counts.get(tag).copied().unwrap_or(0)
    }

    pub fn total_events(&self) -> u64 {
        self.total_events
    }

    pub fn stats(&self) -> &TagIndexStats {
        &self.stats
    }
}

impl Default for TagIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- Tag tests --

    #[test]
    fn test_simple_tag() {
        let tag = Tag::simple("order");
        assert_eq!(tag.key, "order");
        assert!(tag.namespace.is_none());
        assert!(tag.value.is_none());
        assert_eq!(tag.to_canonical(), "order");
    }

    #[test]
    fn test_namespaced_tag() {
        let tag = Tag::namespaced("domain", "order");
        assert_eq!(tag.namespace.as_deref(), Some("domain"));
        assert_eq!(tag.to_canonical(), "domain:order");
    }

    #[test]
    fn test_key_value_tag() {
        let tag = Tag::key_value("priority", "high");
        assert_eq!(tag.value.as_deref(), Some("high"));
        assert_eq!(tag.to_canonical(), "priority=high");
    }

    #[test]
    fn test_full_tag() {
        let tag = Tag::full("domain", "order", "created");
        assert_eq!(tag.to_canonical(), "domain:order=created");
    }

    #[test]
    fn test_tag_parse_simple() {
        let tag = Tag::parse("order");
        assert_eq!(tag.key, "order");
        assert!(tag.namespace.is_none());
        assert!(tag.value.is_none());
    }

    #[test]
    fn test_tag_parse_namespaced() {
        let tag = Tag::parse("domain:order");
        assert_eq!(tag.namespace.as_deref(), Some("domain"));
        assert_eq!(tag.key, "order");
    }

    #[test]
    fn test_tag_parse_key_value() {
        let tag = Tag::parse("priority=high");
        assert_eq!(tag.key, "priority");
        assert_eq!(tag.value.as_deref(), Some("high"));
    }

    #[test]
    fn test_tag_parse_full() {
        let tag = Tag::parse("domain:order=created");
        assert_eq!(tag.namespace.as_deref(), Some("domain"));
        assert_eq!(tag.key, "order");
        assert_eq!(tag.value.as_deref(), Some("created"));
    }

    #[test]
    fn test_tag_display() {
        let tag = Tag::full("ns", "key", "val");
        assert_eq!(format!("{}", tag), "ns:key=val");
    }

    // -- TagPattern matching --

    #[test]
    fn test_pattern_exact_key() {
        let tag = Tag::simple("order");
        let pattern = TagPattern::key("order");
        assert!(tag.matches_pattern(&pattern));
    }

    #[test]
    fn test_pattern_wildcard_key() {
        let tag = Tag::simple("order");
        let pattern = TagPattern::any();
        assert!(tag.matches_pattern(&pattern));
    }

    #[test]
    fn test_pattern_namespace() {
        let tag = Tag::namespaced("domain", "order");
        let pattern = TagPattern::namespaced("domain", "order");
        assert!(tag.matches_pattern(&pattern));
    }

    #[test]
    fn test_pattern_namespace_mismatch() {
        let tag = Tag::namespaced("domain", "order");
        let pattern = TagPattern::namespaced("other", "order");
        assert!(!tag.matches_pattern(&pattern));
    }

    #[test]
    fn test_pattern_in_namespace() {
        let tag = Tag::namespaced("domain", "order");
        let pattern = TagPattern::in_namespace("domain");
        assert!(tag.matches_pattern(&pattern));
    }

    #[test]
    fn test_pattern_key_value() {
        let tag = Tag::key_value("priority", "high");
        let pattern = TagPattern::key_value("priority", "high");
        assert!(tag.matches_pattern(&pattern));
    }

    #[test]
    fn test_pattern_key_value_mismatch() {
        let tag = Tag::key_value("priority", "low");
        let pattern = TagPattern::key_value("priority", "high");
        assert!(!tag.matches_pattern(&pattern));
    }

    // -- TagQuery tests --

    #[test]
    fn test_query_single_match() {
        let tags = vec![Tag::simple("order"), Tag::simple("financial")];
        let query = TagQuery::tag("order");
        assert!(query.evaluate(&tags));
    }

    #[test]
    fn test_query_no_match() {
        let tags = vec![Tag::simple("order")];
        let query = TagQuery::tag("user");
        assert!(!query.evaluate(&tags));
    }

    #[test]
    fn test_query_and() {
        let tags = vec![Tag::simple("order"), Tag::simple("financial")];
        let query = TagQuery::and(vec![TagQuery::tag("order"), TagQuery::tag("financial")]);
        assert!(query.evaluate(&tags));
    }

    #[test]
    fn test_query_and_partial() {
        let tags = vec![Tag::simple("order")];
        let query = TagQuery::and(vec![TagQuery::tag("order"), TagQuery::tag("financial")]);
        assert!(!query.evaluate(&tags));
    }

    #[test]
    fn test_query_or() {
        let tags = vec![Tag::simple("order")];
        let query = TagQuery::or(vec![TagQuery::tag("order"), TagQuery::tag("financial")]);
        assert!(query.evaluate(&tags));
    }

    #[test]
    fn test_query_not() {
        let tags = vec![Tag::simple("order")];
        let query = TagQuery::not(TagQuery::tag("financial"));
        assert!(query.evaluate(&tags));
    }

    #[test]
    fn test_query_has_namespace() {
        let tags = vec![Tag::namespaced("domain", "order")];
        let query = TagQuery::HasNamespace("domain".to_string());
        assert!(query.evaluate(&tags));
    }

    #[test]
    fn test_query_has_key() {
        let tags = vec![Tag::key_value("priority", "high")];
        let query = TagQuery::HasKey("priority".to_string());
        assert!(query.evaluate(&tags));
    }

    #[test]
    fn test_query_complex() {
        let tags = vec![
            Tag::simple("order"),
            Tag::key_value("priority", "high"),
            Tag::namespaced("domain", "commerce"),
        ];
        // (order AND priority=high) OR domain:*
        let query = TagQuery::or(vec![
            TagQuery::and(vec![
                TagQuery::tag("order"),
                TagQuery::tag_value("priority", "high"),
            ]),
            TagQuery::HasNamespace("domain".to_string()),
        ]);
        assert!(query.evaluate(&tags));
    }

    // -- TagIndex tests --

    fn make_event(pid: &str, seq: u64, tags: Vec<Tag>) -> TaggedEvent {
        TaggedEvent {
            persistence_id: pid.to_string(),
            sequence_nr: seq,
            tags,
            timestamp: seq * 1000,
        }
    }

    #[test]
    fn test_index_single_event() {
        let mut idx = TagIndex::new();
        idx.index(&make_event("order-1", 1, vec![Tag::simple("order")]));
        assert_eq!(idx.total_events(), 1);
        assert_eq!(idx.tag_count("order"), 1);
    }

    #[test]
    fn test_index_query_by_tag() {
        let mut idx = TagIndex::new();
        idx.index(&make_event("o-1", 1, vec![Tag::simple("order")]));
        idx.index(&make_event("o-2", 1, vec![Tag::simple("order")]));
        idx.index(&make_event("u-1", 1, vec![Tag::simple("user")]));
        let results = idx.query_by_tag("order");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_index_top_tags() {
        let mut idx = TagIndex::new();
        for i in 0..5 {
            idx.index(&make_event(&format!("o-{}", i), 1, vec![Tag::simple("order")]));
        }
        for i in 0..3 {
            idx.index(&make_event(&format!("u-{}", i), 1, vec![Tag::simple("user")]));
        }
        let top = idx.top_tags(2);
        assert_eq!(top[0].0, "order");
        assert_eq!(top[0].1, 5);
    }

    #[test]
    fn test_index_namespaces() {
        let mut idx = TagIndex::new();
        idx.index(&make_event("e1", 1, vec![Tag::namespaced("domain", "order")]));
        idx.index(&make_event("e2", 1, vec![Tag::namespaced("region", "us")]));
        let ns = idx.namespaces();
        assert_eq!(ns.len(), 2);
    }

    #[test]
    fn test_index_tags_in_namespace() {
        let mut idx = TagIndex::new();
        idx.index(&make_event("e1", 1, vec![Tag::namespaced("domain", "order")]));
        idx.index(&make_event("e2", 1, vec![Tag::namespaced("domain", "user")]));
        let tags = idx.tags_in_namespace("domain");
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn test_index_stats() {
        let mut idx = TagIndex::new();
        idx.index(&make_event("e1", 1, vec![Tag::simple("a"), Tag::simple("b")]));
        assert_eq!(idx.stats().total_indexed, 1);
        assert_eq!(idx.stats().unique_tags, 2);
    }

    #[test]
    fn test_index_default() {
        let idx = TagIndex::default();
        assert_eq!(idx.total_events(), 0);
    }
}
