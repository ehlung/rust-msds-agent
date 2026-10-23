//! # Receptionist - Actor Service Discovery
//!
//! Provides service registration and discovery for typed actors.
//! Based on Akka/Pekko's Receptionist pattern for finding actors by service key.
//!
//! ## Features
//! - Register actors with typed ServiceKeys
//! - Discover actors by key with listing snapshots
//! - Subscribe to service changes (additions/removals)
//! - Deregister actors when they stop
//! - Multi-instance services (multiple actors per key)

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// A key that identifies a service type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServiceKey {
    /// Unique identifier for this service
    pub id: String,
    /// Human-readable description
    pub description: Option<String>,
}

impl ServiceKey {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            description: None,
        }
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }
}

impl std::fmt::Display for ServiceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ServiceKey({})", self.id)
    }
}

/// Reference to a registered service instance
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceInstance {
    /// Unique instance identifier
    pub instance_id: String,
    /// Path/address of the actor providing this service
    pub actor_path: String,
    /// Optional metadata tags
    pub tags: HashSet<String>,
    /// When this instance was registered
    pub registered_at: Instant,
}

impl ServiceInstance {
    pub fn new(instance_id: impl Into<String>, actor_path: impl Into<String>) -> Self {
        Self {
            instance_id: instance_id.into(),
            actor_path: actor_path.into(),
            tags: HashSet::new(),
            registered_at: Instant::now(),
        }
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.insert(tag.into());
        self
    }

    pub fn with_tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tags.extend(tags.into_iter().map(|t| t.into()));
        self
    }

    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.contains(tag)
    }

    pub fn age(&self) -> std::time::Duration {
        self.registered_at.elapsed()
    }
}

/// A listing of service instances for a given key
#[derive(Debug, Clone)]
pub struct Listing {
    pub key: ServiceKey,
    pub instances: Vec<ServiceInstance>,
    pub version: u64,
}

impl Listing {
    pub fn is_empty(&self) -> bool {
        self.instances.is_empty()
    }

    pub fn count(&self) -> usize {
        self.instances.len()
    }

    pub fn instance_ids(&self) -> Vec<&str> {
        self.instances.iter().map(|i| i.instance_id.as_str()).collect()
    }

    pub fn actor_paths(&self) -> Vec<&str> {
        self.instances.iter().map(|i| i.actor_path.as_str()).collect()
    }

    /// Filter instances by tag
    pub fn with_tag(&self, tag: &str) -> Vec<&ServiceInstance> {
        self.instances.iter().filter(|i| i.has_tag(tag)).collect()
    }

    /// Get a single instance (first registered)
    pub fn first(&self) -> Option<&ServiceInstance> {
        self.instances.first()
    }
}

/// Events emitted when service registrations change
#[derive(Debug, Clone)]
pub enum ReceptionistEvent {
    /// A new service instance was registered
    Registered {
        key: ServiceKey,
        instance: ServiceInstance,
        listing: Listing,
    },
    /// A service instance was deregistered
    Deregistered {
        key: ServiceKey,
        instance_id: String,
        listing: Listing,
    },
    /// A listing was requested
    ListingResponse {
        key: ServiceKey,
        listing: Listing,
    },
}

/// Subscription to service changes
#[derive(Debug, Clone)]
pub struct Subscription {
    pub id: u64,
    pub key: ServiceKey,
    pub subscriber_id: String,
    pub created_at: Instant,
}

/// Error types for receptionist operations
#[derive(Debug, thiserror::Error, Clone)]
pub enum ReceptionistError {
    #[error("Service key not found: {0}")]
    KeyNotFound(String),
    #[error("Instance not found: {0}")]
    InstanceNotFound(String),
    #[error("Instance already registered: {0}")]
    AlreadyRegistered(String),
    #[error("Subscription not found: {0}")]
    SubscriptionNotFound(u64),
}

/// The Receptionist manages service registration and discovery
pub struct Receptionist {
    /// Map from service key to registered instances
    services: HashMap<String, Vec<ServiceInstance>>,
    /// Version counter for each service key
    versions: HashMap<String, u64>,
    /// Active subscriptions
    subscriptions: HashMap<String, Vec<Subscription>>,
    /// Next subscription ID
    next_sub_id: AtomicU64,
    /// Event log for tracking changes
    events: Vec<ReceptionistEvent>,
    /// Max events to retain
    max_events: usize,
}

impl Receptionist {
    pub fn new() -> Self {
        Self {
            services: HashMap::new(),
            versions: HashMap::new(),
            subscriptions: HashMap::new(),
            next_sub_id: AtomicU64::new(1),
            events: Vec::new(),
            max_events: 1000,
        }
    }

    pub fn with_max_events(mut self, max: usize) -> Self {
        self.max_events = max;
        self
    }

    /// Register a service instance under a key
    pub fn register(
        &mut self,
        key: &ServiceKey,
        instance: ServiceInstance,
    ) -> Result<Listing, ReceptionistError> {
        let instances = self.services.entry(key.id.clone()).or_default();

        // Check for duplicate
        if instances.iter().any(|i| i.instance_id == instance.instance_id) {
            return Err(ReceptionistError::AlreadyRegistered(instance.instance_id));
        }

        instances.push(instance.clone());
        let version = self.bump_version(&key.id);
        let listing = self.create_listing(key, version);

        let event = ReceptionistEvent::Registered {
            key: key.clone(),
            instance,
            listing: listing.clone(),
        };
        self.record_event(event);

        Ok(listing)
    }

    /// Deregister a service instance
    pub fn deregister(
        &mut self,
        key: &ServiceKey,
        instance_id: &str,
    ) -> Result<Listing, ReceptionistError> {
        let instances = self.services.get_mut(&key.id)
            .ok_or_else(|| ReceptionistError::KeyNotFound(key.id.clone()))?;

        let idx = instances.iter()
            .position(|i| i.instance_id == instance_id)
            .ok_or_else(|| ReceptionistError::InstanceNotFound(instance_id.to_string()))?;

        instances.remove(idx);
        let version = self.bump_version(&key.id);
        let listing = self.create_listing(key, version);

        let event = ReceptionistEvent::Deregistered {
            key: key.clone(),
            instance_id: instance_id.to_string(),
            listing: listing.clone(),
        };
        self.record_event(event);

        Ok(listing)
    }

    /// Find instances registered under a key
    pub fn find(&self, key: &ServiceKey) -> Listing {
        let version = self.versions.get(&key.id).copied().unwrap_or(0);
        self.create_listing(key, version)
    }

    /// Subscribe to changes for a service key
    pub fn subscribe(
        &mut self,
        key: &ServiceKey,
        subscriber_id: impl Into<String>,
    ) -> Subscription {
        let sub = Subscription {
            id: self.next_sub_id.fetch_add(1, Ordering::Relaxed),
            key: key.clone(),
            subscriber_id: subscriber_id.into(),
            created_at: Instant::now(),
        };

        self.subscriptions
            .entry(key.id.clone())
            .or_default()
            .push(sub.clone());

        sub
    }

    /// Unsubscribe from service changes
    pub fn unsubscribe(&mut self, subscription_id: u64) -> Result<(), ReceptionistError> {
        for subs in self.subscriptions.values_mut() {
            if let Some(pos) = subs.iter().position(|s| s.id == subscription_id) {
                subs.remove(pos);
                return Ok(());
            }
        }
        Err(ReceptionistError::SubscriptionNotFound(subscription_id))
    }

    /// Get all subscribers for a key
    pub fn subscribers_for(&self, key: &ServiceKey) -> Vec<&Subscription> {
        self.subscriptions.get(&key.id)
            .map(|subs| subs.iter().collect())
            .unwrap_or_default()
    }

    /// Get all registered service keys
    pub fn keys(&self) -> Vec<ServiceKey> {
        self.services.keys()
            .map(|id| ServiceKey::new(id.clone()))
            .collect()
    }

    /// Get total number of registered instances across all keys
    pub fn total_instances(&self) -> usize {
        self.services.values().map(|v| v.len()).sum()
    }

    /// Get total number of active subscriptions
    pub fn total_subscriptions(&self) -> usize {
        self.subscriptions.values().map(|v| v.len()).sum()
    }

    /// Deregister all instances for an actor path (e.g., when actor terminates)
    pub fn deregister_all_for_actor(&mut self, actor_path: &str) -> Vec<(ServiceKey, String)> {
        let mut removed = Vec::new();

        for (key_id, instances) in &mut self.services {
            let before = instances.len();
            let removed_ids: Vec<String> = instances.iter()
                .filter(|i| i.actor_path == actor_path)
                .map(|i| i.instance_id.clone())
                .collect();

            instances.retain(|i| i.actor_path != actor_path);

            if instances.len() < before {
                let key = ServiceKey::new(key_id.clone());
                for id in removed_ids {
                    removed.push((key.clone(), id));
                }
            }
        }

        // Bump versions for affected keys
        for (key, _) in &removed {
            self.bump_version(&key.id);
        }

        removed
    }

    /// Get recent events
    pub fn recent_events(&self, count: usize) -> &[ReceptionistEvent] {
        let start = self.events.len().saturating_sub(count);
        &self.events[start..]
    }

    /// Get stats about the receptionist
    pub fn stats(&self) -> ReceptionistStats {
        ReceptionistStats {
            service_count: self.services.len(),
            total_instances: self.total_instances(),
            total_subscriptions: self.total_subscriptions(),
            event_count: self.events.len(),
        }
    }

    fn create_listing(&self, key: &ServiceKey, version: u64) -> Listing {
        let instances = self.services.get(&key.id)
            .cloned()
            .unwrap_or_default();
        Listing {
            key: key.clone(),
            instances,
            version,
        }
    }

    fn bump_version(&mut self, key_id: &str) -> u64 {
        let version = self.versions.entry(key_id.to_string()).or_insert(0);
        *version += 1;
        *version
    }

    fn record_event(&mut self, event: ReceptionistEvent) {
        self.events.push(event);
        if self.events.len() > self.max_events {
            self.events.drain(0..self.events.len() - self.max_events);
        }
    }
}

impl Default for Receptionist {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the receptionist
#[derive(Debug, Clone)]
pub struct ReceptionistStats {
    pub service_count: usize,
    pub total_instances: usize,
    pub total_subscriptions: usize,
    pub event_count: usize,
}

/// Lookup strategy for finding service instances
#[derive(Debug, Clone)]
pub enum LookupStrategy {
    /// Return the first registered instance
    First,
    /// Return a random instance
    Random(u64),
    /// Return all instances
    All,
    /// Round-robin across instances
    RoundRobin { index: usize },
    /// Filter by tag
    Tagged(String),
}

impl LookupStrategy {
    /// Apply the lookup strategy to a listing
    pub fn select<'a>(&mut self, listing: &'a Listing) -> Vec<&'a ServiceInstance> {
        if listing.is_empty() {
            return vec![];
        }
        match self {
            LookupStrategy::First => {
                listing.first().into_iter().collect()
            }
            LookupStrategy::Random(seed) => {
                let idx = (*seed as usize) % listing.instances.len();
                *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                vec![&listing.instances[idx]]
            }
            LookupStrategy::All => {
                listing.instances.iter().collect()
            }
            LookupStrategy::RoundRobin { index } => {
                let idx = *index % listing.instances.len();
                *index = index.wrapping_add(1);
                vec![&listing.instances[idx]]
            }
            LookupStrategy::Tagged(tag) => {
                listing.with_tag(tag)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_instance(id: &str, path: &str) -> ServiceInstance {
        ServiceInstance::new(id, path)
    }

    // === ServiceKey Tests ===

    #[test]
    fn test_service_key() {
        let key = ServiceKey::new("auth-service");
        assert_eq!(key.id, "auth-service");
        assert!(key.description.is_none());

        let key2 = ServiceKey::new("db-service")
            .with_description("Database connection pool");
        assert_eq!(key2.description, Some("Database connection pool".to_string()));
        assert_eq!(format!("{}", key2), "ServiceKey(db-service)");
    }

    #[test]
    fn test_service_key_equality() {
        let k1 = ServiceKey::new("svc");
        let k2 = ServiceKey::new("svc");
        let k3 = ServiceKey::new("other");
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
    }

    // === ServiceInstance Tests ===

    #[test]
    fn test_service_instance() {
        let inst = ServiceInstance::new("inst-1", "/user/worker-1")
            .with_tag("region-us")
            .with_tag("primary");
        assert_eq!(inst.instance_id, "inst-1");
        assert_eq!(inst.actor_path, "/user/worker-1");
        assert!(inst.has_tag("region-us"));
        assert!(inst.has_tag("primary"));
        assert!(!inst.has_tag("backup"));
    }

    #[test]
    fn test_service_instance_tags_iter() {
        let inst = ServiceInstance::new("i1", "/path")
            .with_tags(vec!["a", "b", "c"]);
        assert_eq!(inst.tags.len(), 3);
    }

    #[test]
    fn test_service_instance_age() {
        let inst = ServiceInstance::new("i1", "/path");
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(inst.age() >= std::time::Duration::from_millis(5));
    }

    // === Registration Tests ===

    #[test]
    fn test_register_single() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");
        let inst = make_instance("i1", "/path/1");

        let listing = r.register(&key, inst).unwrap();
        assert_eq!(listing.count(), 1);
        assert_eq!(listing.version, 1);
    }

    #[test]
    fn test_register_multiple_instances() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");

        r.register(&key, make_instance("i1", "/path/1")).unwrap();
        let listing = r.register(&key, make_instance("i2", "/path/2")).unwrap();
        assert_eq!(listing.count(), 2);
        assert_eq!(listing.version, 2);
    }

    #[test]
    fn test_register_duplicate() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");

        r.register(&key, make_instance("i1", "/path/1")).unwrap();
        let err = r.register(&key, make_instance("i1", "/path/2")).unwrap_err();
        assert!(matches!(err, ReceptionistError::AlreadyRegistered(_)));
    }

    #[test]
    fn test_register_different_keys() {
        let mut r = Receptionist::new();
        let k1 = ServiceKey::new("auth");
        let k2 = ServiceKey::new("db");

        r.register(&k1, make_instance("a1", "/auth/1")).unwrap();
        r.register(&k2, make_instance("d1", "/db/1")).unwrap();

        assert_eq!(r.find(&k1).count(), 1);
        assert_eq!(r.find(&k2).count(), 1);
        assert_eq!(r.total_instances(), 2);
    }

    // === Deregistration Tests ===

    #[test]
    fn test_deregister() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");

        r.register(&key, make_instance("i1", "/path/1")).unwrap();
        r.register(&key, make_instance("i2", "/path/2")).unwrap();

        let listing = r.deregister(&key, "i1").unwrap();
        assert_eq!(listing.count(), 1);
        assert_eq!(listing.instances[0].instance_id, "i2");
    }

    #[test]
    fn test_deregister_not_found() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");
        r.register(&key, make_instance("i1", "/path/1")).unwrap();

        let err = r.deregister(&key, "nonexistent").unwrap_err();
        assert!(matches!(err, ReceptionistError::InstanceNotFound(_)));
    }

    #[test]
    fn test_deregister_key_not_found() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("nope");
        let err = r.deregister(&key, "i1").unwrap_err();
        assert!(matches!(err, ReceptionistError::KeyNotFound(_)));
    }

    #[test]
    fn test_deregister_all_for_actor() {
        let mut r = Receptionist::new();
        let k1 = ServiceKey::new("svc1");
        let k2 = ServiceKey::new("svc2");

        r.register(&k1, make_instance("a1", "/actor/1")).unwrap();
        r.register(&k1, make_instance("a2", "/actor/2")).unwrap();
        r.register(&k2, make_instance("b1", "/actor/1")).unwrap();

        let removed = r.deregister_all_for_actor("/actor/1");
        assert_eq!(removed.len(), 2);
        assert_eq!(r.find(&k1).count(), 1); // only a2 remains
        assert_eq!(r.find(&k2).count(), 0); // b1 removed
    }

    // === Find/Listing Tests ===

    #[test]
    fn test_find_empty() {
        let r = Receptionist::new();
        let key = ServiceKey::new("nothing");
        let listing = r.find(&key);
        assert!(listing.is_empty());
        assert_eq!(listing.count(), 0);
    }

    #[test]
    fn test_listing_methods() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");
        r.register(&key, make_instance("i1", "/path/1").with_tag("primary")).unwrap();
        r.register(&key, make_instance("i2", "/path/2").with_tag("backup")).unwrap();

        let listing = r.find(&key);
        assert_eq!(listing.instance_ids(), vec!["i1", "i2"]);
        assert_eq!(listing.actor_paths(), vec!["/path/1", "/path/2"]);
        assert_eq!(listing.with_tag("primary").len(), 1);
        assert_eq!(listing.with_tag("primary")[0].instance_id, "i1");
        assert_eq!(listing.first().unwrap().instance_id, "i1");
    }

    // === Subscription Tests ===

    #[test]
    fn test_subscribe() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");

        let sub = r.subscribe(&key, "watcher-1");
        assert_eq!(sub.key, key);
        assert_eq!(sub.subscriber_id, "watcher-1");
        assert!(sub.id > 0);

        assert_eq!(r.subscribers_for(&key).len(), 1);
    }

    #[test]
    fn test_multiple_subscriptions() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");

        r.subscribe(&key, "w1");
        r.subscribe(&key, "w2");

        assert_eq!(r.subscribers_for(&key).len(), 2);
        assert_eq!(r.total_subscriptions(), 2);
    }

    #[test]
    fn test_unsubscribe() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");

        let sub = r.subscribe(&key, "w1");
        assert_eq!(r.total_subscriptions(), 1);

        r.unsubscribe(sub.id).unwrap();
        assert_eq!(r.total_subscriptions(), 0);
    }

    #[test]
    fn test_unsubscribe_not_found() {
        let mut r = Receptionist::new();
        let err = r.unsubscribe(999).unwrap_err();
        assert!(matches!(err, ReceptionistError::SubscriptionNotFound(999)));
    }

    // === Stats & Keys Tests ===

    #[test]
    fn test_keys() {
        let mut r = Receptionist::new();
        r.register(&ServiceKey::new("auth"), make_instance("a1", "/a")).unwrap();
        r.register(&ServiceKey::new("db"), make_instance("d1", "/d")).unwrap();

        let keys = r.keys();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_stats() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");
        r.register(&key, make_instance("i1", "/p1")).unwrap();
        r.register(&key, make_instance("i2", "/p2")).unwrap();
        r.subscribe(&key, "w1");

        let stats = r.stats();
        assert_eq!(stats.service_count, 1);
        assert_eq!(stats.total_instances, 2);
        assert_eq!(stats.total_subscriptions, 1);
        assert_eq!(stats.event_count, 2); // 2 registrations
    }

    // === Events Tests ===

    #[test]
    fn test_events_recorded() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");
        r.register(&key, make_instance("i1", "/p")).unwrap();
        r.deregister(&key, "i1").unwrap();

        let events = r.recent_events(10);
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], ReceptionistEvent::Registered { .. }));
        assert!(matches!(&events[1], ReceptionistEvent::Deregistered { .. }));
    }

    #[test]
    fn test_events_max_cap() {
        let mut r = Receptionist::new().with_max_events(3);
        let key = ServiceKey::new("svc");

        for i in 0..5 {
            r.register(&key, make_instance(&format!("i{}", i), &format!("/p{}", i))).unwrap();
        }

        assert_eq!(r.events.len(), 3); // capped at 3
    }

    // === LookupStrategy Tests ===

    #[test]
    fn test_lookup_first() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");
        r.register(&key, make_instance("i1", "/p1")).unwrap();
        r.register(&key, make_instance("i2", "/p2")).unwrap();

        let listing = r.find(&key);
        let mut strategy = LookupStrategy::First;
        let selected = strategy.select(&listing);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].instance_id, "i1");
    }

    #[test]
    fn test_lookup_all() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");
        r.register(&key, make_instance("i1", "/p1")).unwrap();
        r.register(&key, make_instance("i2", "/p2")).unwrap();

        let listing = r.find(&key);
        let mut strategy = LookupStrategy::All;
        let selected = strategy.select(&listing);
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn test_lookup_round_robin() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");
        r.register(&key, make_instance("i1", "/p1")).unwrap();
        r.register(&key, make_instance("i2", "/p2")).unwrap();
        r.register(&key, make_instance("i3", "/p3")).unwrap();

        let listing = r.find(&key);
        let mut strategy = LookupStrategy::RoundRobin { index: 0 };

        let s1 = strategy.select(&listing);
        assert_eq!(s1[0].instance_id, "i1");
        let s2 = strategy.select(&listing);
        assert_eq!(s2[0].instance_id, "i2");
        let s3 = strategy.select(&listing);
        assert_eq!(s3[0].instance_id, "i3");
        let s4 = strategy.select(&listing);
        assert_eq!(s4[0].instance_id, "i1"); // wraps around
    }

    #[test]
    fn test_lookup_tagged() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");
        r.register(&key, make_instance("i1", "/p1").with_tag("primary")).unwrap();
        r.register(&key, make_instance("i2", "/p2").with_tag("backup")).unwrap();
        r.register(&key, make_instance("i3", "/p3").with_tag("primary")).unwrap();

        let listing = r.find(&key);
        let mut strategy = LookupStrategy::Tagged("primary".into());
        let selected = strategy.select(&listing);
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn test_lookup_random() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");
        r.register(&key, make_instance("i1", "/p1")).unwrap();
        r.register(&key, make_instance("i2", "/p2")).unwrap();

        let listing = r.find(&key);
        let mut strategy = LookupStrategy::Random(42);
        let s1 = strategy.select(&listing);
        assert_eq!(s1.len(), 1);
        // Just verify it picks one
    }

    #[test]
    fn test_lookup_empty_listing() {
        let listing = Listing {
            key: ServiceKey::new("empty"),
            instances: vec![],
            version: 0,
        };
        let mut strategy = LookupStrategy::First;
        assert!(strategy.select(&listing).is_empty());

        let mut strategy = LookupStrategy::RoundRobin { index: 0 };
        assert!(strategy.select(&listing).is_empty());
    }

    // === Version Tracking Tests ===

    #[test]
    fn test_version_increments() {
        let mut r = Receptionist::new();
        let key = ServiceKey::new("svc");

        let l1 = r.register(&key, make_instance("i1", "/p1")).unwrap();
        assert_eq!(l1.version, 1);

        let l2 = r.register(&key, make_instance("i2", "/p2")).unwrap();
        assert_eq!(l2.version, 2);

        let l3 = r.deregister(&key, "i1").unwrap();
        assert_eq!(l3.version, 3);
    }

    // === Default Implementation ===

    #[test]
    fn test_default() {
        let r = Receptionist::default();
        assert_eq!(r.total_instances(), 0);
        assert_eq!(r.total_subscriptions(), 0);
    }
}
