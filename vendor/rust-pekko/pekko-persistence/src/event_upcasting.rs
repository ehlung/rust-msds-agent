// event_upcasting.rs - Event Versioning and Upcasting for schema evolution
// Supports transforming old event versions to current schema transparently

use std::collections::HashMap;
use std::fmt;

// ============================================================================
// EventVersion
// ============================================================================

/// Version identifier for an event schema
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventVersion {
    pub major: u32,
    pub minor: u32,
}

impl EventVersion {
    pub fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    pub fn v(major: u32) -> Self {
        Self { major, minor: 0 }
    }

    pub fn is_compatible_with(&self, other: &EventVersion) -> bool {
        self.major == other.major
    }

    pub fn needs_upcast_to(&self, target: &EventVersion) -> bool {
        self < target
    }
}

impl fmt::Display for EventVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}.{}", self.major, self.minor)
    }
}

// ============================================================================
// VersionedPayload
// ============================================================================

/// An event payload with version metadata
#[derive(Debug, Clone)]
pub struct VersionedPayload {
    pub manifest: String,
    pub version: EventVersion,
    pub data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

impl VersionedPayload {
    pub fn new(manifest: &str, version: EventVersion, data: Vec<u8>) -> Self {
        Self {
            manifest: manifest.to_string(),
            version,
            data,
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

// ============================================================================
// UpcastResult
// ============================================================================

#[derive(Debug, Clone)]
pub enum UpcastResult {
    /// Successfully upcasted to the target version
    Success(VersionedPayload),
    /// Payload unchanged (already at target version)
    Unchanged(VersionedPayload),
    /// Upcasting failed
    Failed(UpcastError),
    /// Event should be filtered out (no longer relevant)
    Filtered,
}

impl UpcastResult {
    pub fn is_success(&self) -> bool {
        matches!(self, UpcastResult::Success(_) | UpcastResult::Unchanged(_))
    }

    pub fn payload(self) -> Option<VersionedPayload> {
        match self {
            UpcastResult::Success(p) | UpcastResult::Unchanged(p) => Some(p),
            _ => None,
        }
    }
}

// ============================================================================
// UpcastError
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum UpcastError {
    UnsupportedVersion { manifest: String, version: EventVersion },
    NoUpcastPath { from: EventVersion, to: EventVersion },
    TransformFailed { step: String, reason: String },
    InvalidData(String),
    ManifestNotFound(String),
}

impl fmt::Display for UpcastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpcastError::UnsupportedVersion { manifest, version } => {
                write!(f, "Unsupported version {} for manifest '{}'", version, manifest)
            }
            UpcastError::NoUpcastPath { from, to } => {
                write!(f, "No upcast path from {} to {}", from, to)
            }
            UpcastError::TransformFailed { step, reason } => {
                write!(f, "Transform '{}' failed: {}", step, reason)
            }
            UpcastError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            UpcastError::ManifestNotFound(m) => write!(f, "Manifest not found: {}", m),
        }
    }
}

// ============================================================================
// UpcastStep
// ============================================================================

/// A single upcast transformation step
#[derive(Clone)]
pub struct UpcastStep {
    pub name: String,
    pub from_version: EventVersion,
    pub to_version: EventVersion,
    transform: UpcastTransform,
}

#[derive(Clone)]
enum UpcastTransform {
    /// Add a field with a default value
    AddField { field: String, default_value: Vec<u8> },
    /// Remove a field
    RemoveField { field: String },
    /// Rename a field
    RenameField { from: String, to: String },
    /// Change the manifest name
    RenameManifest { new_manifest: String },
    /// Custom byte-level transform
    Custom(fn(&[u8]) -> Result<Vec<u8>, String>),
    /// Filter out this event
    Filter,
}

impl UpcastStep {
    pub fn add_field(name: &str, from: EventVersion, to: EventVersion, field: &str, default: Vec<u8>) -> Self {
        Self {
            name: name.to_string(),
            from_version: from,
            to_version: to,
            transform: UpcastTransform::AddField {
                field: field.to_string(),
                default_value: default,
            },
        }
    }    

    pub fn remove_field(name: &str, from: EventVersion, to: EventVersion, field: &str) -> Self {
        Self {
            name: name.to_string(),
            from_version: from,
            to_version: to,
            transform: UpcastTransform::RemoveField {
                field: field.to_string(),
            },
        }
    }

    pub fn rename_field(name: &str, from: EventVersion, to: EventVersion, old_name: &str, new_name: &str) -> Self {
        Self {
            name: name.to_string(),
            from_version: from,
            to_version: to,
            transform: UpcastTransform::RenameField {
                from: old_name.to_string(),
                to: new_name.to_string(),
            },
        }
    }

    pub fn rename_manifest(name: &str, from: EventVersion, to: EventVersion, new_manifest: &str) -> Self {
        Self {
            name: name.to_string(),
            from_version: from,
            to_version: to,
            transform: UpcastTransform::RenameManifest {
                new_manifest: new_manifest.to_string(),
            },
        }
    }

    pub fn custom(name: &str, from: EventVersion, to: EventVersion, f: fn(&[u8]) -> Result<Vec<u8>, String>) -> Self {
        Self {
            name: name.to_string(),
            from_version: from,
            to_version: to,
            transform: UpcastTransform::Custom(f),
        }
    }

    pub fn filter(name: &str, from: EventVersion, to: EventVersion) -> Self {
        Self {
            name: name.to_string(),
            from_version: from,
            to_version: to,
            transform: UpcastTransform::Filter,
        }
    }

    /// Apply the transform to a payload
    pub fn apply(&self, payload: &VersionedPayload) -> UpcastResult {
        match &self.transform {
            UpcastTransform::AddField { field, default_value } => {
                let mut new_data = payload.data.clone();
                new_data.extend_from_slice(field.as_bytes());
                new_data.push(b'=');
                new_data.extend_from_slice(default_value);
                UpcastResult::Success(VersionedPayload {
                    manifest: payload.manifest.clone(),
                    version: self.to_version.clone(),
                    data: new_data,
                    metadata: payload.metadata.clone(),
                })
            }            
            UpcastTransform::RemoveField { field : _field } => {
                // In real impl, parse and remove; here just bump version
                UpcastResult::Success(VersionedPayload {
                    manifest: payload.manifest.clone(),
                    version: self.to_version.clone(),
                    data: payload.data.clone(),
                    metadata: payload.metadata.clone(),
                })
            }
            UpcastTransform::RenameField { from, to } => {
                let data_str = String::from_utf8_lossy(&payload.data);
                let new_data = data_str.replace(from, to);
                UpcastResult::Success(VersionedPayload {
                    manifest: payload.manifest.clone(),
                    version: self.to_version.clone(),
                    data: new_data.into_bytes(),
                    metadata: payload.metadata.clone(),
                })
            }
            UpcastTransform::RenameManifest { new_manifest } => {
                UpcastResult::Success(VersionedPayload {
                    manifest: new_manifest.clone(),
                    version: self.to_version.clone(),
                    data: payload.data.clone(),
                    metadata: payload.metadata.clone(),
                })
            }
            UpcastTransform::Custom(f) => {
                match f(&payload.data) {
                    Ok(new_data) => UpcastResult::Success(VersionedPayload {
                        manifest: payload.manifest.clone(),
                        version: self.to_version.clone(),
                        data: new_data,
                        metadata: payload.metadata.clone(),
                    }),
                    Err(e) => UpcastResult::Failed(UpcastError::TransformFailed {
                        step: self.name.clone(),
                        reason: e,
                    }),
                }
            }
            UpcastTransform::Filter => UpcastResult::Filtered,
        }
    }
}

impl fmt::Debug for UpcastStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpcastStep")
            .field("name", &self.name)
            .field("from", &self.from_version)
            .field("to", &self.to_version)
            .finish()
    }
}

// ============================================================================
// EventUpcaster
// ============================================================================

/// Registry of upcast steps for event schema evolution
pub struct EventUpcaster {
    /// Map: manifest → list of upcast steps (ordered by from_version)
    steps: HashMap<String, Vec<UpcastStep>>,
    current_versions: HashMap<String, EventVersion>,
    stats: UpcastStats,
}

#[derive(Debug, Clone, Default)]
pub struct UpcastStats {
    pub events_processed: u64,
    pub events_upcasted: u64,
    pub events_unchanged: u64,
    pub events_filtered: u64,
    pub events_failed: u64,
}

impl EventUpcaster {
    pub fn new() -> Self {
        Self {
            steps: HashMap::new(),
            current_versions: HashMap::new(),
            stats: UpcastStats::default(),
        }
    }

    /// Register the current (latest) version for a manifest
    pub fn register_current_version(&mut self, manifest: &str, version: EventVersion) {
        self.current_versions.insert(manifest.to_string(), version);
    }

    /// Add an upcast step
    pub fn add_step(&mut self, manifest: &str, step: UpcastStep) {
        self.steps
            .entry(manifest.to_string())
            .or_default()
            .push(step);
        // Keep steps sorted by from_version
        if let Some(steps) = self.steps.get_mut(manifest) {
            steps.sort_by(|a, b| a.from_version.cmp(&b.from_version));
        }
    }

    /// Upcast a payload to the current version
    pub fn upcast(&mut self, payload: VersionedPayload) -> UpcastResult {
        self.stats.events_processed += 1;

        let target = match self.current_versions.get(&payload.manifest) {
            Some(v) => v.clone(),
            None => {
                // No current version registered — pass through
                self.stats.events_unchanged += 1;
                return UpcastResult::Unchanged(payload);
            }
        };

        if payload.version >= target {
            self.stats.events_unchanged += 1;
            return UpcastResult::Unchanged(payload);
        }

        self.upcast_to(payload, &target)
    }

    /// Upcast a payload to a specific target version
    fn upcast_to(&mut self, mut payload: VersionedPayload, target: &EventVersion) -> UpcastResult {
        let manifest = payload.manifest.clone();
        let steps = match self.steps.get(&manifest) {
            Some(s) => s.clone(),
            None => {
                self.stats.events_failed += 1;
                return UpcastResult::Failed(UpcastError::NoUpcastPath {
                    from: payload.version.clone(),
                    to: target.clone(),
                });
            }
        };

        // Apply steps sequentially
        for step in &steps {
            if payload.version >= *target {
                break;
            }
            if step.from_version != payload.version {
                continue;
            }

            match step.apply(&payload) {
                UpcastResult::Success(new_payload) => {
                    payload = new_payload;
                }
                UpcastResult::Filtered => {
                    self.stats.events_filtered += 1;
                    return UpcastResult::Filtered;
                }
                UpcastResult::Failed(e) => {
                    self.stats.events_failed += 1;
                    return UpcastResult::Failed(e);
                }
                UpcastResult::Unchanged(p) => {
                    payload = p;
                }
            }
        }

        if payload.version >= *target {
            self.stats.events_upcasted += 1;
            UpcastResult::Success(payload)
        } else {
            self.stats.events_failed += 1;
            UpcastResult::Failed(UpcastError::NoUpcastPath {
                from: payload.version.clone(),
                to: target.clone(),
            })
        }
    }

    /// Upcast multiple payloads (batch)
    pub fn upcast_batch(&mut self, payloads: Vec<VersionedPayload>) -> Vec<UpcastResult> {
        payloads.into_iter().map(|p| self.upcast(p)).collect()
    }

    pub fn stats(&self) -> &UpcastStats {
        &self.stats
    }

    pub fn current_version(&self, manifest: &str) -> Option<&EventVersion> {
        self.current_versions.get(manifest)
    }

    pub fn registered_manifests(&self) -> Vec<&String> {
        self.current_versions.keys().collect()
    }

    pub fn steps_for(&self, manifest: &str) -> Option<&Vec<UpcastStep>> {
        self.steps.get(manifest)
    }

    pub fn total_steps(&self) -> usize {
        self.steps.values().map(|v| v.len()).sum()
    }
}

impl Default for EventUpcaster {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for EventUpcaster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventUpcaster")
            .field("manifests", &self.current_versions.len())
            .field("total_steps", &self.total_steps())
            .field("stats", &self.stats)
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_payload(manifest: &str, version: EventVersion, data: &str) -> VersionedPayload {
        VersionedPayload::new(manifest, version, data.as_bytes().to_vec())
    }

    #[test]
    fn test_event_version() {
        let v1 = EventVersion::new(1, 0);
        let v2 = EventVersion::new(2, 0);
        assert!(v1 < v2);
        assert!(v1.needs_upcast_to(&v2));
        assert!(!v2.needs_upcast_to(&v1));
        assert_eq!(v1.to_string(), "v1.0");
    }

    #[test]
    fn test_version_compatibility() {
        let v10 = EventVersion::new(1, 0);
        let v11 = EventVersion::new(1, 1);
        let v20 = EventVersion::new(2, 0);
        assert!(v10.is_compatible_with(&v11));
        assert!(!v10.is_compatible_with(&v20));
    }

    #[test]
    fn test_versioned_payload() {
        let p = sample_payload("OrderCreated", EventVersion::v(1), "data")
            .with_metadata("source", "test");
        assert_eq!(p.manifest, "OrderCreated");
        assert_eq!(p.version, EventVersion::v(1));
        assert_eq!(p.size(), 4);
        assert_eq!(p.metadata.get("source"), Some(&"test".to_string()));
    }

    #[test]
    fn test_upcast_unchanged() {
        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("OrderCreated", EventVersion::v(1));
        let payload = sample_payload("OrderCreated", EventVersion::v(1), "data");
        let result = upcaster.upcast(payload);
        assert!(matches!(result, UpcastResult::Unchanged(_)));
        assert_eq!(upcaster.stats().events_unchanged, 1);
    }

    #[test]
    fn test_upcast_no_registered_version() {
        let mut upcaster = EventUpcaster::new();
        let payload = sample_payload("Unknown", EventVersion::v(1), "data");
        let result = upcaster.upcast(payload);
        assert!(matches!(result, UpcastResult::Unchanged(_)));
    }

    #[test]
    fn test_upcast_add_field() {
        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("OrderCreated", EventVersion::v(2));
        upcaster.add_step(
            "OrderCreated",
            UpcastStep::add_field("add-email", EventVersion::v(1), EventVersion::v(2), "email", b"default@test.com".to_vec()),
        );

        let payload = sample_payload("OrderCreated", EventVersion::v(1), "order-data");
        let result = upcaster.upcast(payload);
        assert!(result.is_success());
        let p = result.payload().unwrap();
        assert_eq!(p.version, EventVersion::v(2));
        assert!(String::from_utf8_lossy(&p.data).contains("email"));
    }

    #[test]
    fn test_upcast_rename_field() {
        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("UserEvent", EventVersion::v(2));
        upcaster.add_step(
            "UserEvent",
            UpcastStep::rename_field("rename-name", EventVersion::v(1), EventVersion::v(2), "userName", "fullName"),
        );

        let payload = sample_payload("UserEvent", EventVersion::v(1), "userName=John");
        let result = upcaster.upcast(payload);
        let p = result.payload().unwrap();
        assert!(String::from_utf8_lossy(&p.data).contains("fullName"));
        assert!(!String::from_utf8_lossy(&p.data).contains("userName"));
    }

    #[test]
    fn test_upcast_rename_manifest() {
        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("ItemCreated", EventVersion::v(2));
        upcaster.add_step(
            "ItemCreated",
            UpcastStep::rename_manifest("rename-manifest", EventVersion::v(1), EventVersion::v(2), "ProductCreated"),
        );

        let payload = sample_payload("ItemCreated", EventVersion::v(1), "data");
        let result = upcaster.upcast(payload);
        let p = result.payload().unwrap();
        assert_eq!(p.manifest, "ProductCreated");
    }

    #[test]
    fn test_upcast_filter() {
        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("DeprecatedEvent", EventVersion::v(2));
        upcaster.add_step(
            "DeprecatedEvent",
            UpcastStep::filter("filter-deprecated", EventVersion::v(1), EventVersion::v(2)),
        );

        let payload = sample_payload("DeprecatedEvent", EventVersion::v(1), "old-data");
        let result = upcaster.upcast(payload);
        assert!(matches!(result, UpcastResult::Filtered));
        assert_eq!(upcaster.stats().events_filtered, 1);
    }

    #[test]
    fn test_upcast_custom_transform() {
        fn double_data(data: &[u8]) -> Result<Vec<u8>, String> {
            let mut result = data.to_vec();
            result.extend_from_slice(data);
            Ok(result)
        }

        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("Custom", EventVersion::v(2));
        upcaster.add_step(
            "Custom",
            UpcastStep::custom("double", EventVersion::v(1), EventVersion::v(2), double_data),
        );

        let payload = sample_payload("Custom", EventVersion::v(1), "abc");
        let result = upcaster.upcast(payload);
        let p = result.payload().unwrap();
        assert_eq!(p.data.len(), 6); // doubled
    }

    #[test]
    fn test_upcast_custom_failure() {
        fn fail_transform(_data: &[u8]) -> Result<Vec<u8>, String> {
            Err("transform error".to_string())
        }

        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("FailEvent", EventVersion::v(2));
        upcaster.add_step(
            "FailEvent",
            UpcastStep::custom("fail", EventVersion::v(1), EventVersion::v(2), fail_transform),
        );

        let payload = sample_payload("FailEvent", EventVersion::v(1), "data");
        let result = upcaster.upcast(payload);
        assert!(matches!(result, UpcastResult::Failed(_)));
        assert_eq!(upcaster.stats().events_failed, 1);
    }

    #[test]
    fn test_multi_step_upcast() {
        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("Order", EventVersion::new(3, 0));

        upcaster.add_step("Order", UpcastStep::add_field(
            "v1→v2", EventVersion::v(1), EventVersion::v(2), "status", b"pending".to_vec(),
        ));
        upcaster.add_step("Order", UpcastStep::add_field(
            "v2→v3", EventVersion::v(2), EventVersion::v(3), "priority", b"normal".to_vec(),
        ));

        let payload = sample_payload("Order", EventVersion::v(1), "order");
        let result = upcaster.upcast(payload);
        assert!(result.is_success());
        let p = result.payload().unwrap();
        assert_eq!(p.version, EventVersion::v(3));
        let data = String::from_utf8_lossy(&p.data);
        assert!(data.contains("status"));
        assert!(data.contains("priority"));
    }

    #[test]
    fn test_no_upcast_path() {
        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("Gap", EventVersion::v(3));
        // Only v1→v2, missing v2→v3
        upcaster.add_step("Gap", UpcastStep::add_field(
            "v1→v2", EventVersion::v(1), EventVersion::v(2), "f", b"x".to_vec(),
        ));

        let payload = sample_payload("Gap", EventVersion::v(1), "data");
        let result = upcaster.upcast(payload);
        assert!(matches!(result, UpcastResult::Failed(UpcastError::NoUpcastPath { .. })));
    }

    #[test]
    fn test_batch_upcast() {
        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("Batch", EventVersion::v(2));
        upcaster.add_step("Batch", UpcastStep::remove_field(
            "v1→v2", EventVersion::v(1), EventVersion::v(2), "old",
        ));

        let payloads = vec![
            sample_payload("Batch", EventVersion::v(1), "d1"),
            sample_payload("Batch", EventVersion::v(2), "d2"),
            sample_payload("Batch", EventVersion::v(1), "d3"),
        ];
        let results = upcaster.upcast_batch(payloads);
        assert_eq!(results.len(), 3);
        assert!(results[0].is_success()); // upcasted
        assert!(results[1].is_success()); // unchanged
        assert!(results[2].is_success()); // upcasted
    }

    #[test]
    fn test_upcaster_stats() {
        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("E", EventVersion::v(2));
        upcaster.add_step("E", UpcastStep::remove_field("v1→v2", EventVersion::v(1), EventVersion::v(2), "x"));

        upcaster.upcast(sample_payload("E", EventVersion::v(1), "d"));
        upcaster.upcast(sample_payload("E", EventVersion::v(2), "d"));
        let stats = upcaster.stats();
        assert_eq!(stats.events_processed, 2);
        assert_eq!(stats.events_upcasted, 1);
        assert_eq!(stats.events_unchanged, 1);
    }

    #[test]
    fn test_registered_manifests() {
        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("A", EventVersion::v(1));
        upcaster.register_current_version("B", EventVersion::v(2));
        assert_eq!(upcaster.registered_manifests().len(), 2);
    }

    #[test]
    fn test_current_version() {
        let mut upcaster = EventUpcaster::new();
        upcaster.register_current_version("X", EventVersion::new(3, 1));
        assert_eq!(upcaster.current_version("X"), Some(&EventVersion::new(3, 1)));
        assert_eq!(upcaster.current_version("Y"), None);
    }

    #[test]
    fn test_total_steps() {
        let mut upcaster = EventUpcaster::new();
        upcaster.add_step("A", UpcastStep::filter("f1", EventVersion::v(1), EventVersion::v(2)));
        upcaster.add_step("A", UpcastStep::filter("f2", EventVersion::v(2), EventVersion::v(3)));
        upcaster.add_step("B", UpcastStep::filter("f3", EventVersion::v(1), EventVersion::v(2)));
        assert_eq!(upcaster.total_steps(), 3);
    }

    #[test]
    fn test_upcast_error_display() {
        let err = UpcastError::UnsupportedVersion {
            manifest: "E".into(),
            version: EventVersion::v(1),
        };
        assert!(err.to_string().contains("v1.0"));
        assert!(err.to_string().contains("E"));
    }

    #[test]
    fn test_upcaster_debug() {
        let upcaster = EventUpcaster::new();
        let debug = format!("{:?}", upcaster);
        assert!(debug.contains("EventUpcaster"));
    }

    #[test]
    fn test_upcast_step_debug() {
        let step = UpcastStep::filter("test", EventVersion::v(1), EventVersion::v(2));
        let debug = format!("{:?}", step);
        assert!(debug.contains("UpcastStep"));
        assert!(debug.contains("test"));
    }

    #[test]
    fn test_upcast_result_methods() {
        let p = sample_payload("X", EventVersion::v(1), "d");
        let r = UpcastResult::Success(p);
        assert!(r.is_success());
        assert!(r.payload().is_some());

        let r2 = UpcastResult::Filtered;
        assert!(!r2.is_success());
        assert!(r2.payload().is_none());
    }
}
