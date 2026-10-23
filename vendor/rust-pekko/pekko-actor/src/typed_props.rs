// typed_props.rs - Typed Actor Protocol system for type-safe actor interactions
// Provides strongly-typed actor references and protocol contracts

use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;

// ============================================================================
// Protocol trait
// ============================================================================

/// Marker trait for actor message protocols
/// Each actor defines a Protocol that specifies its accepted message types
pub trait Protocol: Send + 'static {
    /// Human-readable name for this protocol
    fn protocol_name() -> &'static str;
}

// ============================================================================
// TypedActorRef
// ============================================================================

/// A strongly-typed reference to an actor that only accepts messages of type P
#[derive(Clone)]
pub struct TypedActorRef<P: Protocol> {
    path: String,
    id: u64,
    _protocol: PhantomData<P>,
}

impl<P: Protocol> TypedActorRef<P> {
    pub fn new(path: &str, id: u64) -> Self {
        Self {
            path: path.to_string(),
            id,
            _protocol: PhantomData,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn protocol_name(&self) -> &'static str {
        P::protocol_name()
    }

    /// Narrow this reference to a sub-protocol
    pub fn narrow<Sub: Protocol>(&self) -> TypedActorRef<Sub> {
        TypedActorRef::new(&self.path, self.id)
    }

    /// Widen this reference (losing type info)
    pub fn widen(&self) -> UntypedActorRef {
        UntypedActorRef {
            path: self.path.clone(),
            id: self.id,
            protocol_name: P::protocol_name().to_string(),
        }
    }
}

impl<P: Protocol> fmt::Debug for TypedActorRef<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TypedActorRef<{}>({}#{})", P::protocol_name(), self.path, self.id)
    }
}

impl<P: Protocol> fmt::Display for TypedActorRef<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.path, self.id)
    }
}

impl<P: Protocol> PartialEq for TypedActorRef<P> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.path == other.path
    }
}

impl<P: Protocol> Eq for TypedActorRef<P> {}

impl<P: Protocol> std::hash::Hash for TypedActorRef<P> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
        self.id.hash(state);
    }
}

// ============================================================================
// UntypedActorRef
// ============================================================================

/// A type-erased actor reference
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UntypedActorRef {
    pub path: String,
    pub id: u64,
    pub protocol_name: String,
}

impl UntypedActorRef {
    /// Try to restore type information
    pub fn typed<P: Protocol>(&self) -> Option<TypedActorRef<P>> {
        if self.protocol_name == P::protocol_name() {
            Some(TypedActorRef::new(&self.path, self.id))
        } else {
            None
        }
    }
}

impl fmt::Display for UntypedActorRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}[{}]", self.path, self.id, self.protocol_name)
    }
}

// ============================================================================
// TypedProps
// ============================================================================

/// Typed actor properties for creating actors with protocol guarantees
#[derive(Clone)]
pub struct TypedProps<P: Protocol> {
    pub name: Option<String>,
    pub dispatcher: Option<String>,
    pub mailbox_size: Option<usize>,
    pub tags: Vec<String>,
    _protocol: PhantomData<P>,
}

impl<P: Protocol> TypedProps<P> {
    pub fn new() -> Self {
        Self {
            name: None,
            dispatcher: None,
            mailbox_size: None,
            tags: Vec::new(),
            _protocol: PhantomData,
        }
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    pub fn with_dispatcher(mut self, dispatcher: &str) -> Self {
        self.dispatcher = Some(dispatcher.to_string());
        self
    }

    pub fn with_mailbox_size(mut self, size: usize) -> Self {
        self.mailbox_size = Some(size);
        self
    }

    pub fn with_tag(mut self, tag: &str) -> Self {
        self.tags.push(tag.to_string());
        self
    }

    pub fn protocol_name(&self) -> &'static str {
        P::protocol_name()
    }
}

impl<P: Protocol> Default for TypedProps<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: Protocol> fmt::Debug for TypedProps<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypedProps")
            .field("protocol", &P::protocol_name())
            .field("name", &self.name)
            .field("dispatcher", &self.dispatcher)
            .finish()
    }
}

// ============================================================================
// ProtocolRegistry
// ============================================================================

/// Registry of known protocols and their actor refs
pub struct ProtocolRegistry {
    /// Map of (protocol_name, actor_path) → untyped ref
    entries: HashMap<String, Vec<UntypedActorRef>>,
    stats: ProtocolRegistryStats,
}

#[derive(Debug, Clone, Default)]
pub struct ProtocolRegistryStats {
    pub registrations: u64,
    pub lookups: u64,
    pub removals: u64,
}

impl ProtocolRegistry {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            stats: ProtocolRegistryStats::default(),
        }
    }

    /// Register a typed actor ref
    pub fn register<P: Protocol>(&mut self, actor_ref: &TypedActorRef<P>) {
        let key = P::protocol_name().to_string();
        let untyped = actor_ref.widen();
        let entries = self.entries.entry(key).or_default();
        if !entries.contains(&untyped) {
            entries.push(untyped);
            self.stats.registrations += 1;
        }
    }

    /// Unregister a typed actor ref
    pub fn unregister<P: Protocol>(&mut self, actor_ref: &TypedActorRef<P>) {
        let key = P::protocol_name().to_string();
        if let Some(entries) = self.entries.get_mut(&key) {
            let untyped = actor_ref.widen();
            if let Some(pos) = entries.iter().position(|e| e == &untyped) {
                entries.remove(pos);
                self.stats.removals += 1;
            }
        }
    }

    /// Lookup all actors implementing a protocol
    pub fn lookup<P: Protocol>(&mut self) -> Vec<TypedActorRef<P>> {
        self.stats.lookups += 1;
        let key = P::protocol_name().to_string();
        self.entries
            .get(&key)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|u| u.typed::<P>())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Lookup first actor implementing a protocol
    pub fn lookup_first<P: Protocol>(&mut self) -> Option<TypedActorRef<P>> {
        self.stats.lookups += 1;
        let key = P::protocol_name().to_string();
        self.entries
            .get(&key)
            .and_then(|entries| entries.first())
            .and_then(|u| u.typed::<P>())
    }

    /// Get count of actors for a protocol
    pub fn count<P: Protocol>(&self) -> usize {
        let key = P::protocol_name().to_string();
        self.entries.get(&key).map_or(0, |e| e.len())
    }

    /// Get all registered protocol names
    pub fn protocols(&self) -> Vec<&str> {
        self.entries.keys().map(|k| k.as_str()).collect()
    }

    /// Total registered actors
    pub fn total_actors(&self) -> usize {
        self.entries.values().map(|e| e.len()).sum()
    }

    pub fn stats(&self) -> &ProtocolRegistryStats {
        &self.stats
    }
}

impl Default for ProtocolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ProtocolRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtocolRegistry")
            .field("protocols", &self.entries.len())
            .field("total_actors", &self.total_actors())
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Test protocols
    #[derive(Debug)]
    struct GreetProtocol;
    impl Protocol for GreetProtocol {
        fn protocol_name() -> &'static str { "Greet" }
    }

    #[derive(Debug)]
    struct MathProtocol;
    impl Protocol for MathProtocol {
        fn protocol_name() -> &'static str { "Math" }
    }

    #[derive(Debug)]
    struct SubProtocol;
    impl Protocol for SubProtocol {
        fn protocol_name() -> &'static str { "Sub" }
    }

    #[test]
    fn test_typed_ref_creation() {
        let r: TypedActorRef<GreetProtocol> = TypedActorRef::new("/user/greeter", 1);
        assert_eq!(r.path(), "/user/greeter");
        assert_eq!(r.id(), 1);
        assert_eq!(r.protocol_name(), "Greet");
    }

    #[test]
    fn test_typed_ref_display() {
        let r: TypedActorRef<GreetProtocol> = TypedActorRef::new("/user/greeter", 1);
        assert_eq!(format!("{}", r), "/user/greeter#1");
        let debug = format!("{:?}", r);
        assert!(debug.contains("Greet"));
    }

    #[test]
    fn test_typed_ref_equality() {
        let r1: TypedActorRef<GreetProtocol> = TypedActorRef::new("/user/a", 1);
        let r2: TypedActorRef<GreetProtocol> = TypedActorRef::new("/user/a", 1);
        let r3: TypedActorRef<GreetProtocol> = TypedActorRef::new("/user/b", 2);
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
    }

    #[test]
    fn test_narrow() {
        let r: TypedActorRef<GreetProtocol> = TypedActorRef::new("/user/a", 1);
        let sub: TypedActorRef<SubProtocol> = r.narrow();
        assert_eq!(sub.path(), "/user/a");
        assert_eq!(sub.protocol_name(), "Sub");
    }

    #[test]
    fn test_widen_and_restore() {
        let r: TypedActorRef<GreetProtocol> = TypedActorRef::new("/user/a", 1);
        let untyped = r.widen();
        assert_eq!(untyped.protocol_name, "Greet");

        let restored: Option<TypedActorRef<GreetProtocol>> = untyped.typed();
        assert!(restored.is_some());
        assert_eq!(restored.unwrap().path(), "/user/a");
    }

    #[test]
    fn test_widen_wrong_type() {
        let r: TypedActorRef<GreetProtocol> = TypedActorRef::new("/user/a", 1);
        let untyped = r.widen();
        let wrong: Option<TypedActorRef<MathProtocol>> = untyped.typed();
        assert!(wrong.is_none());
    }

    #[test]
    fn test_untyped_display() {
        let r: TypedActorRef<GreetProtocol> = TypedActorRef::new("/user/a", 1);
        let untyped = r.widen();
        assert_eq!(format!("{}", untyped), "/user/a#1[Greet]");
    }

    #[test]
    fn test_typed_props() {
        let props = TypedProps::<GreetProtocol>::new()
            .with_name("greeter")
            .with_dispatcher("custom")
            .with_mailbox_size(100)
            .with_tag("service");
        assert_eq!(props.name, Some("greeter".to_string()));
        assert_eq!(props.dispatcher, Some("custom".to_string()));
        assert_eq!(props.mailbox_size, Some(100));
        assert_eq!(props.tags, vec!["service"]);
        assert_eq!(props.protocol_name(), "Greet");
    }

    #[test]
    fn test_typed_props_default() {
        let props = TypedProps::<MathProtocol>::default();
        assert!(props.name.is_none());
        assert!(props.dispatcher.is_none());
    }

    #[test]
    fn test_typed_props_debug() {
        let props = TypedProps::<GreetProtocol>::new().with_name("g");
        let debug = format!("{:?}", props);
        assert!(debug.contains("Greet"));
    }

    #[test]
    fn test_registry_register_lookup() {
        let mut reg = ProtocolRegistry::new();
        let r: TypedActorRef<GreetProtocol> = TypedActorRef::new("/user/g1", 1);
        reg.register(&r);
        let found = reg.lookup::<GreetProtocol>();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].path(), "/user/g1");
    }

    #[test]
    fn test_registry_multiple() {
        let mut reg = ProtocolRegistry::new();
        reg.register(&TypedActorRef::<GreetProtocol>::new("/user/g1", 1));
        reg.register(&TypedActorRef::<GreetProtocol>::new("/user/g2", 2));
        reg.register(&TypedActorRef::<MathProtocol>::new("/user/m1", 3));
        assert_eq!(reg.count::<GreetProtocol>(), 2);
        assert_eq!(reg.count::<MathProtocol>(), 1);
        assert_eq!(reg.total_actors(), 3);
    }

    #[test]
    fn test_registry_no_duplicate() {
        let mut reg = ProtocolRegistry::new();
        let r = TypedActorRef::<GreetProtocol>::new("/user/g", 1);
        reg.register(&r);
        reg.register(&r);
        assert_eq!(reg.count::<GreetProtocol>(), 1);
    }

    #[test]
    fn test_registry_unregister() {
        let mut reg = ProtocolRegistry::new();
        let r = TypedActorRef::<GreetProtocol>::new("/user/g", 1);
        reg.register(&r);
        reg.unregister(&r);
        assert_eq!(reg.count::<GreetProtocol>(), 0);
    }

    #[test]
    fn test_registry_lookup_first() {
        let mut reg = ProtocolRegistry::new();
        reg.register(&TypedActorRef::<GreetProtocol>::new("/user/g1", 1));
        reg.register(&TypedActorRef::<GreetProtocol>::new("/user/g2", 2));
        let first = reg.lookup_first::<GreetProtocol>().unwrap();
        assert_eq!(first.path(), "/user/g1");
    }

    #[test]
    fn test_registry_lookup_first_empty() {
        let mut reg = ProtocolRegistry::new();
        assert!(reg.lookup_first::<MathProtocol>().is_none());
    }

    #[test]
    fn test_registry_protocols() {
        let mut reg = ProtocolRegistry::new();
        reg.register(&TypedActorRef::<GreetProtocol>::new("/a", 1));
        reg.register(&TypedActorRef::<MathProtocol>::new("/b", 2));
        let protos = reg.protocols();
        assert_eq!(protos.len(), 2);
    }

    #[test]
    fn test_registry_stats() {
        let mut reg = ProtocolRegistry::new();
        reg.register(&TypedActorRef::<GreetProtocol>::new("/a", 1));
        reg.lookup::<GreetProtocol>();
        reg.unregister(&TypedActorRef::<GreetProtocol>::new("/a", 1));
        assert_eq!(reg.stats().registrations, 1);
        assert_eq!(reg.stats().lookups, 1);
        assert_eq!(reg.stats().removals, 1);
    }

    #[test]
    fn test_registry_debug() {
        let reg = ProtocolRegistry::new();
        let debug = format!("{:?}", reg);
        assert!(debug.contains("ProtocolRegistry"));
    }

    #[test]
    fn test_hash_typed_ref() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TypedActorRef::<GreetProtocol>::new("/a", 1));
        set.insert(TypedActorRef::<GreetProtocol>::new("/a", 1));
        set.insert(TypedActorRef::<GreetProtocol>::new("/b", 2));
        assert_eq!(set.len(), 2);
    }
}
