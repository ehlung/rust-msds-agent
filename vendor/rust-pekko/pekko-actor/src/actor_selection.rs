// actor_selection.rs - ActorSelection with wildcard matching and path queries
// Provides path-based actor lookup with glob-style wildcards

use std::collections::HashMap;
use std::fmt;

// ============================================================================
// Selection Pattern
// ============================================================================

/// A segment in a selection pattern
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatternSegment {
    /// Exact match: "worker"
    Exact(String),
    /// Single-level wildcard: "*" matches any single path element
    Wildcard,
    /// Multi-level wildcard: "**" matches zero or more path elements
    DoubleWildcard,
    /// Prefix match: "worker-*"
    Prefix(String),
    /// Suffix match: "*-handler"
    Suffix(String),
    /// Contains match: "*work*"
    Contains(String),
}

/// A compiled selection pattern for matching actor paths
#[derive(Debug, Clone)]
pub struct SelectionPattern {
    segments: Vec<PatternSegment>,
    original: String,
}

impl SelectionPattern {
    /// Parse a selection pattern from a path string.
    /// Supports: exact, *, **, prefix-*, *-suffix, *contains*
    pub fn parse(pattern: &str) -> Self {
        let trimmed = pattern.trim_matches('/');
        let segments: Vec<PatternSegment> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed
                .split('/')
                .map(|seg| {
                    if seg == "**" {
                        PatternSegment::DoubleWildcard
                    } else if seg == "*" {
                        PatternSegment::Wildcard
                    } else if seg.starts_with('*') && seg.ends_with('*') && seg.len() > 2 {
                        PatternSegment::Contains(seg[1..seg.len() - 1].to_string())
                    } else if seg.starts_with('*') {
                        PatternSegment::Suffix(seg[1..].to_string())
                    } else if seg.ends_with('*') {
                        PatternSegment::Prefix(seg[..seg.len() - 1].to_string())
                    } else {
                        PatternSegment::Exact(seg.to_string())
                    }
                })
                .collect()
        };

        Self {
            segments,
            original: pattern.to_string(),
        }
    }

    /// Check if a path matches this pattern
    pub fn matches(&self, path: &str) -> bool {
        let trimmed = path.trim_matches('/');
        let path_parts: Vec<&str> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed.split('/').collect()
        };
        Self::match_recursive(&self.segments, &path_parts)
    }

    fn match_recursive(patterns: &[PatternSegment], parts: &[&str]) -> bool {
        if patterns.is_empty() {
            return parts.is_empty();
        }

        match &patterns[0] {
            PatternSegment::DoubleWildcard => {
                // ** matches zero or more segments
                // Try matching remaining patterns against current and subsequent positions
                for i in 0..=parts.len() {
                    if Self::match_recursive(&patterns[1..], &parts[i..]) {
                        return true;
                    }
                }
                false
            }
            _ => {
                if parts.is_empty() {
                    return false;
                }
                let segment_matches = match &patterns[0] {
                    PatternSegment::Exact(name) => parts[0] == name,
                    PatternSegment::Wildcard => true,
                    PatternSegment::Prefix(prefix) => parts[0].starts_with(prefix),
                    PatternSegment::Suffix(suffix) => parts[0].ends_with(suffix),
                    PatternSegment::Contains(sub) => parts[0].contains(sub),
                    PatternSegment::DoubleWildcard => unreachable!(),
                };
                if segment_matches {
                    Self::match_recursive(&patterns[1..], &parts[1..])
                } else {
                    false
                }
            }
        }
    }

    pub fn pattern_str(&self) -> &str {
        &self.original
    }

    pub fn segments(&self) -> &[PatternSegment] {
        &self.segments
    }

    /// Whether this pattern contains any wildcards
    pub fn is_wildcard(&self) -> bool {
        self.segments.iter().any(|s| {
            !matches!(s, PatternSegment::Exact(_))
        })
    }
}

impl fmt::Display for SelectionPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

// ============================================================================
// ActorSelection
// ============================================================================

/// Represents a selection of actors by path pattern.
/// Can resolve to zero, one, or many actors.
#[derive(Debug, Clone)]
pub struct ActorSelection {
    /// The selection pattern
    pattern: SelectionPattern,
    /// Anchor point for relative selections
    anchor: Option<String>,
}

impl ActorSelection {
    /// Create a selection from a pattern string
    pub fn from_pattern(pattern: &str) -> Self {
        Self {
            pattern: SelectionPattern::parse(pattern),
            anchor: None,
        }
    }

    /// Create a selection anchored at a specific path
    pub fn anchored(anchor: &str, pattern: &str) -> Self {
        let full_pattern = if pattern.starts_with('/') {
            pattern.to_string()
        } else {
            format!("{}/{}", anchor.trim_end_matches('/'), pattern)
        };
        Self {
            pattern: SelectionPattern::parse(&full_pattern),
            anchor: Some(anchor.to_string()),
        }
    }

    /// Resolve this selection against a registry of known actor paths
    pub fn resolve<'a>(&self, paths: &'a [String]) -> Vec<&'a String> {
        paths
            .iter()
            .filter(|p| self.pattern.matches(p))
            .collect()
    }

    /// Check if a single path matches this selection
    pub fn matches(&self, path: &str) -> bool {
        self.pattern.matches(path)
    }

    pub fn pattern(&self) -> &SelectionPattern {
        &self.pattern
    }

    pub fn anchor(&self) -> Option<&str> {
        self.anchor.as_deref()
    }

    /// Create a child selection
    pub fn child(&self, name: &str) -> Self {
        let new_pattern = format!("{}/{}", self.pattern.original.trim_end_matches('/'), name);
        Self {
            pattern: SelectionPattern::parse(&new_pattern),
            anchor: self.anchor.clone(),
        }
    }
}

impl fmt::Display for ActorSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref anchor) = self.anchor {
            write!(f, "ActorSelection({} from {})", self.pattern, anchor)
        } else {
            write!(f, "ActorSelection({})", self.pattern)
        }
    }
}

// ============================================================================
// ActorRegistry - For resolving selections
// ============================================================================

/// A simple registry of actor paths for selection resolution
#[derive(Debug, Clone)]
pub struct ActorPathRegistry {
    /// All registered paths
    paths: Vec<String>,
    /// Path -> metadata
    metadata: HashMap<String, PathMetadata>,
}

/// Metadata associated with a registered actor path
#[derive(Debug, Clone, Default)]
pub struct PathMetadata {
    /// Actor type name
    pub actor_type: Option<String>,
    /// Tags for filtering
    pub tags: Vec<String>,
    /// Whether the actor is active
    pub active: bool,
}

impl ActorPathRegistry {
    pub fn new() -> Self {
        Self {
            paths: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Register an actor path
    pub fn register(&mut self, path: &str) {
        if !self.paths.contains(&path.to_string()) {
            self.paths.push(path.to_string());
            self.metadata.insert(
                path.to_string(),
                PathMetadata {
                    active: true,
                    ..Default::default()
                },
            );
        }
    }

    /// Register with metadata
    pub fn register_with_metadata(&mut self, path: &str, meta: PathMetadata) {
        if !self.paths.contains(&path.to_string()) {
            self.paths.push(path.to_string());
        }
        self.metadata.insert(path.to_string(), meta);
    }

    /// Unregister an actor path
    pub fn unregister(&mut self, path: &str) -> bool {
        let before = self.paths.len();
        self.paths.retain(|p| p != path);
        self.metadata.remove(path);
        self.paths.len() < before
    }

    /// Resolve a selection against this registry
    pub fn resolve(&self, selection: &ActorSelection) -> Vec<String> {
        selection
            .resolve(&self.paths)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Resolve with active filter
    pub fn resolve_active(&self, selection: &ActorSelection) -> Vec<String> {
        self.paths
            .iter()
            .filter(|p| {
                selection.pattern.matches(p)
                    && self
                        .metadata
                        .get(p.as_str())
                        .map(|m| m.active)
                        .unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    /// Resolve with tag filter
    pub fn resolve_with_tag(&self, selection: &ActorSelection, tag: &str) -> Vec<String> {
        self.paths
            .iter()
            .filter(|p| {
                selection.pattern.matches(p)
                    && self
                        .metadata
                        .get(p.as_str())
                        .map(|m| m.tags.contains(&tag.to_string()))
                        .unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    /// Get all children of a path
    pub fn children_of(&self, parent: &str) -> Vec<String> {
        let prefix = format!("{}/", parent.trim_end_matches('/'));
        self.paths
            .iter()
            .filter(|p| {
                p.starts_with(&prefix) && !p[prefix.len()..].contains('/')
            })
            .cloned()
            .collect()
    }

    /// Get all descendants of a path
    pub fn descendants_of(&self, ancestor: &str) -> Vec<String> {
        let prefix = format!("{}/", ancestor.trim_end_matches('/'));
        self.paths
            .iter()
            .filter(|p| p.starts_with(&prefix))
            .cloned()
            .collect()
    }

    /// Set active status
    pub fn set_active(&mut self, path: &str, active: bool) {
        if let Some(meta) = self.metadata.get_mut(path) {
            meta.active = active;
        }
    }

    /// Add tag to a path
    pub fn add_tag(&mut self, path: &str, tag: &str) {
        if let Some(meta) = self.metadata.get_mut(path) {
            if !meta.tags.contains(&tag.to_string()) {
                meta.tags.push(tag.to_string());
            }
        }
    }

    pub fn get_metadata(&self, path: &str) -> Option<&PathMetadata> {
        self.metadata.get(path)
    }

    pub fn all_paths(&self) -> &[String] {
        &self.paths
    }

    pub fn count(&self) -> usize {
        self.paths.len()
    }
}

impl Default for ActorPathRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// PathQuery - Complex path queries
// ============================================================================

/// Fluent query builder for actor path lookups
pub struct PathQuery<'a> {
    registry: &'a ActorPathRegistry,
    pattern: Option<SelectionPattern>,
    tag_filter: Option<String>,
    active_only: bool,
    type_filter: Option<String>,
    limit: Option<usize>,
}

impl<'a> PathQuery<'a> {
    pub fn new(registry: &'a ActorPathRegistry) -> Self {
        Self {
            registry,
            pattern: None,
            tag_filter: None,
            active_only: false,
            type_filter: None,
            limit: None,
        }
    }

    pub fn matching(mut self, pattern: &str) -> Self {
        self.pattern = Some(SelectionPattern::parse(pattern));
        self
    }

    pub fn with_tag(mut self, tag: &str) -> Self {
        self.tag_filter = Some(tag.to_string());
        self
    }

    pub fn active_only(mut self) -> Self {
        self.active_only = true;
        self
    }

    pub fn of_type(mut self, actor_type: &str) -> Self {
        self.type_filter = Some(actor_type.to_string());
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Execute the query and return matching paths
    pub fn execute(&self) -> Vec<String> {
        let mut results: Vec<String> = self
            .registry
            .paths
            .iter()
            .filter(|p| {
                // Pattern filter
                if let Some(ref pat) = self.pattern {
                    if !pat.matches(p) {
                        return false;
                    }
                }
                // Metadata filters
                if let Some(meta) = self.registry.metadata.get(p.as_str()) {
                    if self.active_only && !meta.active {
                        return false;
                    }
                    if let Some(ref tag) = self.tag_filter {
                        if !meta.tags.contains(tag) {
                            return false;
                        }
                    }
                    if let Some(ref typ) = self.type_filter {
                        if meta.actor_type.as_deref() != Some(typ) {
                            return false;
                        }
                    }
                } else if self.active_only || self.tag_filter.is_some() || self.type_filter.is_some()
                {
                    return false;
                }
                true
            })
            .cloned()
            .collect();

        if let Some(limit) = self.limit {
            results.truncate(limit);
        }
        results
    }

    /// Count matching paths without collecting
    pub fn count(&self) -> usize {
        self.execute().len()
    }

    /// Check if any paths match
    pub fn exists(&self) -> bool {
        // Check without consuming self — just iterate and short-circuit
        self.registry
            .paths
            .iter()
            .any(|p| {
                if let Some(ref pat) = self.pattern {
                    if !pat.matches(p) {
                        return false;
                    }
                }
                if let Some(meta) = self.registry.metadata.get(p.as_str()) {
                    if self.active_only && !meta.active {
                        return false;
                    }
                    if let Some(ref tag) = self.tag_filter {
                        if !meta.tags.contains(tag) {
                            return false;
                        }
                    }
                    if let Some(ref typ) = self.type_filter {
                        if meta.actor_type.as_deref() != Some(typ) {
                            return false;
                        }
                    }
                } else if self.active_only || self.tag_filter.is_some() || self.type_filter.is_some() {
                    return false;
                }
                true
            })
    }

    /// Get the first match
    pub fn first(&self) -> Option<String> {
        self.execute().into_iter().next()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- SelectionPattern tests --

    #[test]
    fn test_exact_match() {
        let pat = SelectionPattern::parse("user/worker");
        assert!(pat.matches("user/worker"));
        assert!(!pat.matches("user/other"));
        assert!(!pat.matches("user/worker/child"));
    }

    #[test]
    fn test_single_wildcard() {
        let pat = SelectionPattern::parse("user/*");
        assert!(pat.matches("user/worker"));
        assert!(pat.matches("user/manager"));
        assert!(!pat.matches("user/worker/child"));
        assert!(!pat.matches("system/logger"));
    }

    #[test]
    fn test_double_wildcard() {
        let pat = SelectionPattern::parse("user/**");
        assert!(pat.matches("user/worker"));
        assert!(pat.matches("user/worker/child"));
        assert!(pat.matches("user/a/b/c/d"));
        assert!(!pat.matches("system/logger"));
    }

    #[test]
    fn test_double_wildcard_middle() {
        let pat = SelectionPattern::parse("user/**/handler");
        assert!(pat.matches("user/handler"));
        assert!(pat.matches("user/a/handler"));
        assert!(pat.matches("user/a/b/c/handler"));
        assert!(!pat.matches("user/a/b/c/worker"));
    }

    #[test]
    fn test_prefix_match() {
        let pat = SelectionPattern::parse("user/worker-*");
        assert!(pat.matches("user/worker-1"));
        assert!(pat.matches("user/worker-abc"));
        assert!(!pat.matches("user/manager-1"));
    }

    #[test]
    fn test_suffix_match() {
        let pat = SelectionPattern::parse("user/*-handler");
        assert!(pat.matches("user/msg-handler"));
        assert!(pat.matches("user/event-handler"));
        assert!(!pat.matches("user/msg-processor"));
    }

    #[test]
    fn test_contains_match() {
        let pat = SelectionPattern::parse("user/*work*");
        assert!(pat.matches("user/worker"));
        assert!(pat.matches("user/network"));
        assert!(pat.matches("user/framework"));
        assert!(!pat.matches("user/manager"));
    }

    #[test]
    fn test_mixed_pattern() {
        let pat = SelectionPattern::parse("user/*/handler-*");
        assert!(pat.matches("user/foo/handler-1"));
        assert!(pat.matches("user/bar/handler-abc"));
        assert!(!pat.matches("user/foo/processor-1"));
    }

    #[test]
    fn test_empty_pattern() {
        let pat = SelectionPattern::parse("");
        assert!(pat.matches(""));
        assert!(!pat.matches("user"));
    }

    #[test]
    fn test_leading_trailing_slashes() {
        let pat = SelectionPattern::parse("/user/worker/");
        assert!(pat.matches("user/worker"));
        assert!(pat.matches("/user/worker/"));
    }

    #[test]
    fn test_is_wildcard() {
        assert!(!SelectionPattern::parse("user/worker").is_wildcard());
        assert!(SelectionPattern::parse("user/*").is_wildcard());
        assert!(SelectionPattern::parse("user/**").is_wildcard());
        assert!(SelectionPattern::parse("user/worker-*").is_wildcard());
    }

    #[test]
    fn test_pattern_display() {
        let pat = SelectionPattern::parse("user/*/handler");
        assert_eq!(format!("{}", pat), "user/*/handler");
    }

    #[test]
    fn test_segments() {
        let pat = SelectionPattern::parse("user/*/handler");
        assert_eq!(pat.segments().len(), 3);
        assert_eq!(pat.segments()[0], PatternSegment::Exact("user".to_string()));
        assert_eq!(pat.segments()[1], PatternSegment::Wildcard);
    }

    // -- ActorSelection tests --

    #[test]
    fn test_selection_from_pattern() {
        let sel = ActorSelection::from_pattern("user/*");
        assert!(sel.matches("user/worker"));
        assert!(!sel.matches("system/logger"));
    }

    #[test]
    fn test_selection_anchored() {
        let sel = ActorSelection::anchored("/user/parent", "child-*");
        assert!(sel.matches("user/parent/child-1"));
        assert!(sel.matches("user/parent/child-abc"));
        assert!(!sel.matches("user/other/child-1"));
    }

    #[test]
    fn test_selection_anchored_absolute() {
        let sel = ActorSelection::anchored("/user/parent", "/system/*");
        assert!(sel.matches("system/logger"));
        assert!(!sel.matches("user/parent/system/logger"));
    }

    #[test]
    fn test_selection_resolve() {
        let paths = vec![
            "user/worker-1".to_string(),
            "user/worker-2".to_string(),
            "user/manager".to_string(),
            "system/logger".to_string(),
        ];
        let sel = ActorSelection::from_pattern("user/worker-*");
        let results = sel.resolve(&paths);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_selection_child() {
        let sel = ActorSelection::from_pattern("user/pool");
        let child = sel.child("worker-*");
        assert!(child.matches("user/pool/worker-1"));
        assert!(!child.matches("user/pool/manager"));
    }

    #[test]
    fn test_selection_display() {
        let sel = ActorSelection::from_pattern("user/*");
        assert!(format!("{}", sel).contains("user/*"));
    }

    #[test]
    fn test_selection_display_anchored() {
        let sel = ActorSelection::anchored("/user", "child");
        let display = format!("{}", sel);
        assert!(display.contains("from"));
    }

    // -- ActorPathRegistry tests --

    #[test]
    fn test_registry_register() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/worker-1");
        reg.register("user/worker-2");
        assert_eq!(reg.count(), 2);
    }

    #[test]
    fn test_registry_no_duplicates() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/worker");
        reg.register("user/worker");
        assert_eq!(reg.count(), 1);
    }

    #[test]
    fn test_registry_unregister() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/worker");
        assert!(reg.unregister("user/worker"));
        assert_eq!(reg.count(), 0);
    }

    #[test]
    fn test_registry_resolve() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/worker-1");
        reg.register("user/worker-2");
        reg.register("user/manager");
        let sel = ActorSelection::from_pattern("user/worker-*");
        let results = reg.resolve(&sel);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_registry_resolve_active() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/w1");
        reg.register("user/w2");
        reg.set_active("user/w2", false);
        let sel = ActorSelection::from_pattern("user/*");
        let results = reg.resolve_active(&sel);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "user/w1");
    }

    #[test]
    fn test_registry_resolve_with_tag() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/w1");
        reg.register("user/w2");
        reg.add_tag("user/w1", "priority");
        let sel = ActorSelection::from_pattern("user/*");
        let results = reg.resolve_with_tag(&sel, "priority");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "user/w1");
    }

    #[test]
    fn test_registry_children_of() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/parent/child1");
        reg.register("user/parent/child2");
        reg.register("user/parent/child1/grandchild");
        let children = reg.children_of("user/parent");
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn test_registry_descendants_of() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/parent/child1");
        reg.register("user/parent/child2");
        reg.register("user/parent/child1/grandchild");
        let descendants = reg.descendants_of("user/parent");
        assert_eq!(descendants.len(), 3);
    }

    #[test]
    fn test_registry_metadata() {
        let mut reg = ActorPathRegistry::new();
        reg.register_with_metadata(
            "user/w1",
            PathMetadata {
                actor_type: Some("Worker".to_string()),
                tags: vec!["fast".to_string()],
                active: true,
            },
        );
        let meta = reg.get_metadata("user/w1").unwrap();
        assert_eq!(meta.actor_type.as_deref(), Some("Worker"));
        assert!(meta.tags.contains(&"fast".to_string()));
    }

    // -- PathQuery tests --

    #[test]
    fn test_query_matching() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/worker-1");
        reg.register("user/worker-2");
        reg.register("user/manager");
        let results = PathQuery::new(&reg).matching("user/worker-*").execute();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_active_only() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/w1");
        reg.register("user/w2");
        reg.set_active("user/w2", false);
        let results = PathQuery::new(&reg)
            .matching("user/*")
            .active_only()
            .execute();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_query_with_tag() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/w1");
        reg.register("user/w2");
        reg.add_tag("user/w1", "fast");
        let results = PathQuery::new(&reg)
            .matching("user/*")
            .with_tag("fast")
            .execute();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_query_of_type() {
        let mut reg = ActorPathRegistry::new();
        reg.register_with_metadata(
            "user/w1",
            PathMetadata {
                actor_type: Some("Worker".to_string()),
                active: true,
                ..Default::default()
            },
        );
        reg.register_with_metadata(
            "user/m1",
            PathMetadata {
                actor_type: Some("Manager".to_string()),
                active: true,
                ..Default::default()
            },
        );
        let results = PathQuery::new(&reg).of_type("Worker").execute();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_query_limit() {
        let mut reg = ActorPathRegistry::new();
        for i in 0..10 {
            reg.register(&format!("user/w{}", i));
        }
        let results = PathQuery::new(&reg).matching("user/*").limit(3).execute();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_query_count() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/w1");
        reg.register("user/w2");
        let count = PathQuery::new(&reg).matching("user/*").count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_query_exists() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/w1");
        assert!(PathQuery::new(&reg).matching("user/*").exists());
        assert!(!PathQuery::new(&reg).matching("system/*").exists());
    }

    #[test]
    fn test_query_first() {
        let mut reg = ActorPathRegistry::new();
        reg.register("user/w1");
        reg.register("user/w2");
        let first = PathQuery::new(&reg).matching("user/*").first();
        assert!(first.is_some());
    }

    #[test]
    fn test_query_combined_filters() {
        let mut reg = ActorPathRegistry::new();
        reg.register_with_metadata(
            "user/w1",
            PathMetadata {
                actor_type: Some("Worker".to_string()),
                tags: vec!["fast".to_string()],
                active: true,
            },
        );
        reg.register_with_metadata(
            "user/w2",
            PathMetadata {
                actor_type: Some("Worker".to_string()),
                tags: vec!["slow".to_string()],
                active: false,
            },
        );
        let results = PathQuery::new(&reg)
            .matching("user/*")
            .of_type("Worker")
            .active_only()
            .with_tag("fast")
            .execute();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "user/w1");
    }

    #[test]
    fn test_registry_default() {
        let reg = ActorPathRegistry::default();
        assert_eq!(reg.count(), 0);
    }
}
