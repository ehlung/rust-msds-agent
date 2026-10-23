//! Event Adapters — Event versioning and migration
//!
//! When the schema of persisted events changes over time, Event Adapters
//! allow transforming old events into the current format during replay.
//!
//! Modeled after Scala Pekko's EventAdapter:
//! - **EventAdapter** trait: bidirectional transform (toJournal / fromJournal)
//! - **EventAdapterChain**: compose multiple adapters in sequence
//! - **EventAdapterRegistry**: map manifests to adapters
//! - **VersionedEvent**: envelope for schema-versioned events

use crate::actor::PersistentRepr;
use crate::errors::{SerializationError, SerializationResult};
use std::collections::HashMap;
use std::fmt;
use tracing::{debug};

// ============= Event Adapter Trait =============

/// An adapter that transforms events between the domain model and the journal representation.
///
/// `to_journal` is called when persisting (write path).
/// `from_journal` is called when replaying (read path).
pub trait EventAdapter: Send + Sync {
    /// Transform an event before writing to the journal.
    /// Can add metadata, change format, or wrap in a versioned envelope.
    fn to_journal(&self, event: EventAdapterInput) -> SerializationResult<EventAdapterOutput>;

    /// Transform an event after reading from the journal.
    /// Used for schema migration: transform old event formats to current.
    fn from_journal(&self, event: EventAdapterInput) -> SerializationResult<EventAdapterOutput>;

    /// The manifest this adapter handles. Empty string means "handle all".
    fn manifest(&self) -> &str;
}

/// Input to an EventAdapter
#[derive(Debug, Clone)]
pub struct EventAdapterInput {
    /// Original manifest (type name)
    pub manifest: String,
    /// Serialized event payload
    pub payload: Vec<u8>,
    /// Event tags
    pub tags: Vec<String>,
}

impl EventAdapterInput {
    pub fn new(manifest: String, payload: Vec<u8>) -> Self {
        Self {
            manifest,
            payload,
            tags: Vec::new(),
        }
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Create from a PersistentRepr
    pub fn from_persistent_repr(repr: &PersistentRepr) -> Self {
        Self {
            manifest: repr.manifest.clone(),
            payload: repr.payload.clone(),
            tags: repr.tags.clone(),
        }
    }
}

/// Output of an EventAdapter
#[derive(Debug, Clone)]
pub struct EventAdapterOutput {
    /// The (possibly transformed) manifest
    pub manifest: String,
    /// The (possibly transformed) payload
    pub payload: Vec<u8>,
    /// Possibly updated tags
    pub tags: Vec<String>,
}

impl EventAdapterOutput {
    pub fn new(manifest: String, payload: Vec<u8>) -> Self {
        Self {
            manifest,
            payload,
            tags: Vec::new(),
        }
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }
}

// ============= Identity Adapter =============

/// A no-op adapter that passes events through unchanged.
pub struct IdentityAdapter;

impl EventAdapter for IdentityAdapter {
    fn to_journal(&self, event: EventAdapterInput) -> SerializationResult<EventAdapterOutput> {
        Ok(EventAdapterOutput {
            manifest: event.manifest,
            payload: event.payload,
            tags: event.tags,
        })
    }

    fn from_journal(&self, event: EventAdapterInput) -> SerializationResult<EventAdapterOutput> {
        Ok(EventAdapterOutput {
            manifest: event.manifest,
            payload: event.payload,
            tags: event.tags,
        })
    }

    fn manifest(&self) -> &str {
        "" // handles all
    }
}

// ============= Versioned Event =============

/// Schema version for events
pub type SchemaVersion = u32;

/// A versioned event envelope that wraps the actual event payload
/// with schema version information for migration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VersionedEvent {
    /// Schema version of this event
    pub version: SchemaVersion,
    /// Original manifest (type name)
    pub manifest: String,
    /// The serialized event payload
    pub payload: Vec<u8>,
}

impl VersionedEvent {
    pub fn new(version: SchemaVersion, manifest: String, payload: Vec<u8>) -> Self {
        Self { version, manifest, payload }
    }

    /// Serialize this versioned event to bytes
    pub fn to_bytes(&self) -> SerializationResult<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(SerializationError::Json)
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> SerializationResult<Self> {
        serde_json::from_slice(data)
            .map_err(SerializationError::Json)
    }
}

// ============= Version Migrator =============

/// A function that migrates an event payload from one version to the next.
pub type MigrationFn = Box<dyn Fn(&[u8]) -> SerializationResult<Vec<u8>> + Send + Sync>;

/// Manages a chain of version migrations for a specific event type.
pub struct VersionMigrator {
    /// Manifest (event type) this migrator handles
    manifest: String,
    /// Current (latest) schema version
    current_version: SchemaVersion,
    /// Migration functions: version N → version N+1
    migrations: HashMap<SchemaVersion, MigrationFn>,
}

impl VersionMigrator {
    pub fn new(manifest: &str, current_version: SchemaVersion) -> Self {
        Self {
            manifest: manifest.to_string(),
            current_version,
            migrations: HashMap::new(),
        }
    }

    /// Register a migration from `from_version` to `from_version + 1`.
    pub fn add_migration<F>(mut self, from_version: SchemaVersion, migration: F) -> Self
    where
        F: Fn(&[u8]) -> SerializationResult<Vec<u8>> + Send + Sync + 'static,
    {
        self.migrations.insert(from_version, Box::new(migration));
        self
    }

    /// Migrate a payload from `from_version` to `current_version`.
    pub fn migrate(&self, payload: &[u8], from_version: SchemaVersion) -> SerializationResult<Vec<u8>> {
        if from_version >= self.current_version {
            return Ok(payload.to_vec());
        }

        let mut current_payload = payload.to_vec();
        for version in from_version..self.current_version {
            if let Some(migration) = self.migrations.get(&version) {
                debug!(
                    "Migrating {} from v{} to v{}",
                    self.manifest, version, version + 1
                );
                current_payload = migration(&current_payload)?;
            } else {
                return Err(SerializationError::UnknownManifest(format!(
                    "No migration found for {} v{} -> v{}",
                    self.manifest, version, version + 1
                )));
            }
        }

        Ok(current_payload)
    }

    pub fn current_version(&self) -> SchemaVersion {
        self.current_version
    }

    pub fn manifest(&self) -> &str {
        &self.manifest
    }
}

impl fmt::Debug for VersionMigrator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VersionMigrator")
            .field("manifest", &self.manifest)
            .field("current_version", &self.current_version)
            .field("migrations_count", &self.migrations.len())
            .finish()
    }
}

// ============= Versioning Adapter =============

/// An EventAdapter that handles schema versioning using VersionedEvent envelopes.
pub struct VersioningAdapter {
    migrators: HashMap<String, VersionMigrator>,
}

impl VersioningAdapter {
    pub fn new() -> Self {
        Self {
            migrators: HashMap::new(),
        }
    }

    /// Register a version migrator for a specific manifest.
    pub fn register_migrator(mut self, migrator: VersionMigrator) -> Self {
        self.migrators.insert(migrator.manifest().to_string(), migrator);
        self
    }
}

impl Default for VersioningAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl EventAdapter for VersioningAdapter {
    fn to_journal(&self, event: EventAdapterInput) -> SerializationResult<EventAdapterOutput> {
        // On write: wrap the payload in a VersionedEvent with the current version
        let current_version = self.migrators
            .get(&event.manifest)
            .map(|m| m.current_version())
            .unwrap_or(1);

        let versioned = VersionedEvent::new(
            current_version,
            event.manifest.clone(),
            event.payload,
        );

        let wrapped_payload = versioned.to_bytes()?;

        Ok(EventAdapterOutput {
            manifest: event.manifest,
            payload: wrapped_payload,
            tags: event.tags,
        })
    }

    fn from_journal(&self, event: EventAdapterInput) -> SerializationResult<EventAdapterOutput> {
        // On read: unwrap VersionedEvent and migrate if needed
        match VersionedEvent::from_bytes(&event.payload) {
            Ok(versioned) => {
                let migrated_payload = if let Some(migrator) = self.migrators.get(&versioned.manifest) {
                    migrator.migrate(&versioned.payload, versioned.version)?
                } else {
                    // No migrator registered — pass through as-is
                    versioned.payload
                };

                Ok(EventAdapterOutput {
                    manifest: versioned.manifest,
                    payload: migrated_payload,
                    tags: event.tags,
                })
            }
            Err(_) => {
                // Not a versioned event — pass through (backward compatibility)
                debug!("Event not versioned, passing through: {}", event.manifest);
                Ok(EventAdapterOutput {
                    manifest: event.manifest,
                    payload: event.payload,
                    tags: event.tags,
                })
            }
        }
    }

    fn manifest(&self) -> &str {
        "" // handles all manifests
    }
}

// ============= Event Adapter Chain =============

/// Composes multiple EventAdapters in sequence.
/// On write: applies adapters left-to-right.
/// On read: applies adapters right-to-left.
pub struct EventAdapterChain {
    adapters: Vec<Box<dyn EventAdapter>>,
}

impl EventAdapterChain {
    pub fn new() -> Self {
        Self {
            adapters: Vec::new(),
        }
    }

    /// Add an adapter to the chain.
    pub fn add<A: EventAdapter + 'static>(mut self, adapter: A) -> Self {
        self.adapters.push(Box::new(adapter));
        self
    }

    /// Apply adapters for writing (left-to-right).
    pub fn adapt_to_journal(&self, event: EventAdapterInput) -> SerializationResult<EventAdapterOutput> {
        let mut current = event;
        for adapter in &self.adapters {
            let output = adapter.to_journal(current)?;
            current = EventAdapterInput {
                manifest: output.manifest,
                payload: output.payload,
                tags: output.tags,
            };
        }
        Ok(EventAdapterOutput {
            manifest: current.manifest,
            payload: current.payload,
            tags: current.tags,
        })
    }

    /// Apply adapters for reading (right-to-left).
    pub fn adapt_from_journal(&self, event: EventAdapterInput) -> SerializationResult<EventAdapterOutput> {
        let mut current = event;
        for adapter in self.adapters.iter().rev() {
            let output = adapter.from_journal(current)?;
            current = EventAdapterInput {
                manifest: output.manifest,
                payload: output.payload,
                tags: output.tags,
            };
        }
        Ok(EventAdapterOutput {
            manifest: current.manifest,
            payload: current.payload,
            tags: current.tags,
        })
    }

    pub fn len(&self) -> usize {
        self.adapters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.adapters.is_empty()
    }
}

impl Default for EventAdapterChain {
    fn default() -> Self {
        Self::new()
    }
}

// ============= Event Adapter Registry =============

/// Maps manifests to their EventAdapter chains.
pub struct EventAdapterRegistry {
    chains: HashMap<String, EventAdapterChain>,
    default_chain: EventAdapterChain,
}

impl EventAdapterRegistry {
    pub fn new() -> Self {
        Self {
            chains: HashMap::new(),
            default_chain: EventAdapterChain::new().add(IdentityAdapter),
        }
    }

    /// Register an adapter chain for a specific manifest.
    pub fn register(mut self, manifest: &str, chain: EventAdapterChain) -> Self {
        self.chains.insert(manifest.to_string(), chain);
        self
    }

    /// Set the default adapter chain for unregistered manifests.
    pub fn with_default(mut self, chain: EventAdapterChain) -> Self {
        self.default_chain = chain;
        self
    }

    /// Get the adapter chain for a manifest.
    pub fn chain_for(&self, manifest: &str) -> &EventAdapterChain {
        self.chains.get(manifest).unwrap_or(&self.default_chain)
    }

    /// Adapt an event for writing.
    pub fn adapt_to_journal(&self, event: EventAdapterInput) -> SerializationResult<EventAdapterOutput> {
        self.chain_for(&event.manifest).adapt_to_journal(event)
    }

    /// Adapt an event for reading.
    pub fn adapt_from_journal(&self, event: EventAdapterInput) -> SerializationResult<EventAdapterOutput> {
        self.chain_for(&event.manifest).adapt_from_journal(event)
    }
}

impl Default for EventAdapterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== IdentityAdapter Tests =====

    #[test]
    fn test_identity_adapter() {
        let adapter = IdentityAdapter;
        let input = EventAdapterInput::new("TestEvent".to_string(), vec![1, 2, 3]);

        let output = adapter.to_journal(input.clone()).unwrap();
        assert_eq!(output.manifest, "TestEvent");
        assert_eq!(output.payload, vec![1, 2, 3]);

        let output = adapter.from_journal(input).unwrap();
        assert_eq!(output.manifest, "TestEvent");
        assert_eq!(output.payload, vec![1, 2, 3]);
    }

    // ===== VersionedEvent Tests =====

    #[test]
    fn test_versioned_event_roundtrip() {
        let event = VersionedEvent::new(2, "OrderCreated".to_string(), vec![10, 20, 30]);
        let bytes = event.to_bytes().unwrap();
        let decoded = VersionedEvent::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.version, 2);
        assert_eq!(decoded.manifest, "OrderCreated");
        assert_eq!(decoded.payload, vec![10, 20, 30]);
    }

    // ===== VersionMigrator Tests =====

    #[test]
    fn test_version_migrator_no_migration_needed() {
        let migrator = VersionMigrator::new("TestEvent", 3);
        let payload = vec![1, 2, 3];

        // Already at current version
        let result = migrator.migrate(&payload, 3).unwrap();
        assert_eq!(result, payload);
    }

    #[test]
    fn test_version_migrator_single_step() {
        let migrator = VersionMigrator::new("TestEvent", 2)
            .add_migration(1, |payload| {
                // v1 → v2: append a byte
                let mut new = payload.to_vec();
                new.push(99);
                Ok(new)
            });

        let result = migrator.migrate(&[1, 2, 3], 1).unwrap();
        assert_eq!(result, vec![1, 2, 3, 99]);
    }

    #[test]
    fn test_version_migrator_multi_step() {
        let migrator = VersionMigrator::new("TestEvent", 3)
            .add_migration(1, |payload| {
                let mut new = payload.to_vec();
                new.push(10);
                Ok(new)
            })
            .add_migration(2, |payload| {
                let mut new = payload.to_vec();
                new.push(20);
                Ok(new)
            });

        // Migrate from v1 → v3 (two steps)
        let result = migrator.migrate(&[1], 1).unwrap();
        assert_eq!(result, vec![1, 10, 20]);

        // Migrate from v2 → v3 (one step)
        let result = migrator.migrate(&[1, 10], 2).unwrap();
        assert_eq!(result, vec![1, 10, 20]);
    }

    #[test]
    fn test_version_migrator_missing_migration() {
        let migrator = VersionMigrator::new("TestEvent", 3)
            .add_migration(1, |p| Ok(p.to_vec()));
        // Missing migration from v2 → v3

        let result = migrator.migrate(&[1], 1);
        assert!(result.is_err());
    }

    // ===== VersioningAdapter Tests =====

    #[test]
    fn test_versioning_adapter_write_wraps() {
        let adapter = VersioningAdapter::new()
            .register_migrator(VersionMigrator::new("OrderCreated", 2));

        let input = EventAdapterInput::new("OrderCreated".to_string(), vec![1, 2, 3]);
        let output = adapter.to_journal(input).unwrap();

        // Should be wrapped in VersionedEvent
        let versioned = VersionedEvent::from_bytes(&output.payload).unwrap();
        assert_eq!(versioned.version, 2);
        assert_eq!(versioned.payload, vec![1, 2, 3]);
    }

    #[test]
    fn test_versioning_adapter_read_migrates() {
        let adapter = VersioningAdapter::new()
            .register_migrator(
                VersionMigrator::new("OrderCreated", 2)
                    .add_migration(1, |payload| {
                        // v1 → v2: add a field
                        let mut p = payload.to_vec();
                        p.push(42);
                        Ok(p)
                    }),
            );

        // Simulate reading an old v1 event from the journal
        let old_versioned = VersionedEvent::new(1, "OrderCreated".to_string(), vec![1, 2, 3]);
        let input = EventAdapterInput::new(
            "OrderCreated".to_string(),
            old_versioned.to_bytes().unwrap(),
        );

        let output = adapter.from_journal(input).unwrap();
        assert_eq!(output.payload, vec![1, 2, 3, 42]); // Migrated!
    }

    #[test]
    fn test_versioning_adapter_read_no_migration_needed() {
        let adapter = VersioningAdapter::new()
            .register_migrator(VersionMigrator::new("OrderCreated", 2));

        // Event already at current version
        let current = VersionedEvent::new(2, "OrderCreated".to_string(), vec![1, 2, 3]);
        let input = EventAdapterInput::new(
            "OrderCreated".to_string(),
            current.to_bytes().unwrap(),
        );

        let output = adapter.from_journal(input).unwrap();
        assert_eq!(output.payload, vec![1, 2, 3]);
    }

    #[test]
    fn test_versioning_adapter_non_versioned_passthrough() {
        let adapter = VersioningAdapter::new();

        // Raw (non-versioned) event — should pass through
        let input = EventAdapterInput::new("TestEvent".to_string(), vec![1, 2, 3]);
        let output = adapter.from_journal(input).unwrap();
        assert_eq!(output.payload, vec![1, 2, 3]);
    }

    // ===== EventAdapterChain Tests =====

    #[test]
    fn test_adapter_chain_empty() {
        let chain = EventAdapterChain::new();
        assert!(chain.is_empty());

        let input = EventAdapterInput::new("Test".to_string(), vec![1]);
        let output = chain.adapt_to_journal(input).unwrap();
        assert_eq!(output.payload, vec![1]);
    }

    #[test]
    fn test_adapter_chain_identity() {
        let chain = EventAdapterChain::new()
            .add(IdentityAdapter)
            .add(IdentityAdapter);

        assert_eq!(chain.len(), 2);

        let input = EventAdapterInput::new("Test".to_string(), vec![1, 2]);
        let output = chain.adapt_to_journal(input).unwrap();
        assert_eq!(output.payload, vec![1, 2]);
    }

    // ===== EventAdapterRegistry Tests =====

    #[test]
    fn test_registry_default_adapter() {
        let registry = EventAdapterRegistry::new();

        let input = EventAdapterInput::new("Unknown".to_string(), vec![1, 2, 3]);
        let output = registry.adapt_to_journal(input).unwrap();
        assert_eq!(output.payload, vec![1, 2, 3]); // Identity
    }

    #[test]
    fn test_registry_custom_adapter() {
        let versioning = VersioningAdapter::new()
            .register_migrator(VersionMigrator::new("OrderCreated", 1));

        let registry = EventAdapterRegistry::new()
            .register("OrderCreated",
                EventAdapterChain::new().add(versioning),
            );

        let input = EventAdapterInput::new("OrderCreated".to_string(), vec![1, 2, 3]);
        let output = registry.adapt_to_journal(input).unwrap();

        // Should be wrapped in VersionedEvent
        let versioned = VersionedEvent::from_bytes(&output.payload).unwrap();
        assert_eq!(versioned.version, 1);
    }

    #[test]
    fn test_registry_unregistered_uses_default() {
        let registry = EventAdapterRegistry::new();

        let input = EventAdapterInput::new("SomeEvent".to_string(), vec![5, 6]);
        let output = registry.adapt_from_journal(input).unwrap();
        assert_eq!(output.payload, vec![5, 6]); // Default identity
    }
}
