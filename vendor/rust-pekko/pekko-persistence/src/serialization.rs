//! Event serialization for persistence

use crate::{SerializationError, SerializationResult};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for event serializers
pub trait EventSerializer: Send + Sync {
    /// Serialize an event to bytes
    fn serialize(&self, event: &dyn Any, manifest: &str) -> SerializationResult<Vec<u8>>;

    /// Deserialize bytes to an event
    fn deserialize(&self, data: &[u8], manifest: &str) -> SerializationResult<Box<dyn Any + Send>>;

    /// Get the identifier for this serializer
    fn identifier(&self) -> u32;

    /// Check if this serializer can handle the given manifest
    fn can_handle(&self, manifest: &str) -> bool;
}

/// JSON-based event serializer using serde
pub struct JsonEventSerializer;

impl EventSerializer for JsonEventSerializer {
    fn serialize(&self, event: &dyn Any, _manifest: &str) -> SerializationResult<Vec<u8>> {
        // In a real implementation, we would need type-specific serialization
        // This is a simplified version that assumes the event implements Serialize
        
        // For now, we'll serialize as a generic JSON value
        // In practice, you'd need a registry of serializable types
        if let Some(serializable) = event.downcast_ref::<JsonSerializable>() {
            serde_json::to_vec(&serializable.value).map_err(SerializationError::Json)
        } else {
            Err(SerializationError::Serialization(
                "Event does not implement JsonSerializable".to_string(),
            ))
        }
    }

    fn deserialize(&self, data: &[u8], manifest: &str) -> SerializationResult<Box<dyn Any + Send>> {
        let json_value: serde_json::Value = serde_json::from_slice(data)?;
        
        // In a real implementation, you'd use the manifest to determine the target type
        // and have a registry of deserializable types
        Ok(Box::new(JsonSerializable { 
            value: json_value,
            manifest: manifest.to_string(),
        }))
    }

    fn identifier(&self) -> u32 {
        1 // JSON serializer ID
    }

    fn can_handle(&self, _manifest: &str) -> bool {
        true // JSON serializer can handle any manifest
    }
}

/// Wrapper for JSON-serializable events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonSerializable {
    pub value: serde_json::Value,
    pub manifest: String,
}

/// Protocol Buffers event serializer
pub struct ProtobufEventSerializer {
    type_registry: HashMap<String, Box<dyn ProtobufType>>,
}

impl ProtobufEventSerializer {
    pub fn new() -> Self {
        Self {
            type_registry: HashMap::new(),
        }
    }

    pub fn register_type<T: ProtobufType + 'static>(&mut self, manifest: &str, type_instance: T) {
        self.type_registry.insert(manifest.to_string(), Box::new(type_instance));
    }
}

impl Default for ProtobufEventSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl EventSerializer for ProtobufEventSerializer {
    fn serialize(&self, event: &dyn Any, manifest: &str) -> SerializationResult<Vec<u8>> {
        if let Some(proto_type) = self.type_registry.get(manifest) {
            proto_type.encode_event(event)
        } else {
            Err(SerializationError::SerializerNotFound(manifest.to_string()))
        }
    }

    fn deserialize(&self, data: &[u8], manifest: &str) -> SerializationResult<Box<dyn Any + Send>> {
        if let Some(proto_type) = self.type_registry.get(manifest) {
            proto_type.decode_event(data)
        } else {
            Err(SerializationError::SerializerNotFound(manifest.to_string()))
        }
    }

    fn identifier(&self) -> u32 {
        2 // Protobuf serializer ID
    }

    fn can_handle(&self, manifest: &str) -> bool {
        self.type_registry.contains_key(manifest)
    }
}

/// Trait for protobuf-serializable types
pub trait ProtobufType: Send + Sync {
    fn encode_event(&self, event: &dyn Any) -> SerializationResult<Vec<u8>>;
    fn decode_event(&self, data: &[u8]) -> SerializationResult<Box<dyn Any + Send>>;
}

/// Serializer registry for managing multiple serializers
pub struct SerializerRegistry {
    serializers: HashMap<u32, Arc<dyn EventSerializer>>,
    manifest_mapping: HashMap<String, u32>,
}

impl SerializerRegistry {
    pub fn new() -> Self {
        Self {
            serializers: HashMap::new(),
            manifest_mapping: HashMap::new(),
        }
    }

    /// Register a serializer
    pub fn register_serializer(&mut self, serializer: Arc<dyn EventSerializer>) {
        let id = serializer.identifier();
        self.serializers.insert(id, serializer);
    }

    /// Map a manifest to a specific serializer
    pub fn map_manifest_to_serializer(&mut self, manifest: &str, serializer_id: u32) {
        self.manifest_mapping.insert(manifest.to_string(), serializer_id);
    }

    /// Serialize an event
    pub fn serialize(&self, event: &dyn Any, manifest: &str) -> SerializationResult<(Vec<u8>, u32)> {
        // Find the appropriate serializer
        let serializer_id = if let Some(&id) = self.manifest_mapping.get(manifest) {
            id
        } else {
            // Find a serializer that can handle this manifest
            self.serializers
                .iter()
                .find(|(_, serializer)| serializer.can_handle(manifest))
                .map(|(&id, _)| id)
                .ok_or_else(|| SerializationError::SerializerNotFound(manifest.to_string()))?
        };

        let serializer = self.serializers
            .get(&serializer_id)
            .ok_or_else(|| SerializationError::SerializerNotFound(format!("ID: {}", serializer_id)))?;

        let data = serializer.serialize(event, manifest)?;
        Ok((data, serializer_id))
    }

    /// Deserialize an event
    pub fn deserialize(
        &self,
        data: &[u8],
        manifest: &str,
        serializer_id: u32,
    ) -> SerializationResult<Box<dyn Any + Send>> {
        let serializer = self.serializers
            .get(&serializer_id)
            .ok_or_else(|| SerializationError::SerializerNotFound(format!("ID: {}", serializer_id)))?;

        serializer.deserialize(data, manifest)
    }
}

impl Default for SerializerRegistry {
    fn default() -> Self {
        let mut registry = Self::new();
        
        // Register default serializers
        registry.register_serializer(Arc::new(JsonEventSerializer));
        registry.register_serializer(Arc::new(ProtobufEventSerializer::new()));
        
        registry
    }
}

/// Macro to help with type-safe event serialization
#[macro_export]
macro_rules! register_event_type {
    ($registry:expr, $event_type:ty, $manifest:expr) => {
        // This would be implemented to register types with their manifests
        // Implementation depends on the specific serialization approach
    };
}

/// Utility functions for common serialization tasks
pub mod utils {
    use super::*;

    /// Create a default serializer registry with common serializers
    pub fn default_registry() -> SerializerRegistry {
        SerializerRegistry::default()
    }

    /// Serialize an event to JSON
    pub fn serialize_to_json<T: Serialize>(event: &T) -> SerializationResult<Vec<u8>> {
        serde_json::to_vec(event).map_err(SerializationError::Json)
    }

    /// Deserialize an event from JSON
    pub fn deserialize_from_json<T: for<'de> Deserialize<'de>>(data: &[u8]) -> SerializationResult<T> {
        serde_json::from_slice(data).map_err(SerializationError::Json)
    }

    /// Get the type manifest for a Rust type
    pub fn type_manifest<T: 'static>() -> String {
        std::any::type_name::<T>().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestEvent {
        id: u64,
        name: String,
        value: f64,
    }

    #[test]
    fn test_json_serialization_utils() {
        let event = TestEvent {
            id: 42,
            name: "test".to_string(),
            value: 3.14,
        };

        // Serialize
        let data = utils::serialize_to_json(&event).unwrap();
        assert!(!data.is_empty());

        // Deserialize
        let deserialized: TestEvent = utils::deserialize_from_json(&data).unwrap();
        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_type_manifest() {
        let manifest = utils::type_manifest::<TestEvent>();
        assert!(manifest.contains("TestEvent"));
    }

    #[test]
    fn test_json_serializer() {
        let serializer = JsonEventSerializer;
        
        // Test identifier
        assert_eq!(serializer.identifier(), 1);
        
        // Test can_handle
        assert!(serializer.can_handle("any_manifest"));
    }

    #[test]
    fn test_protobuf_serializer() {
        let serializer = ProtobufEventSerializer::new();
        
        // Test identifier
        assert_eq!(serializer.identifier(), 2);
        
        // Test can_handle with empty registry
        assert!(!serializer.can_handle("unknown_manifest"));
    }

    #[test]
    fn test_serializer_registry() {
        let mut registry = SerializerRegistry::new();
        
        // Register JSON serializer
        registry.register_serializer(Arc::new(JsonEventSerializer));
        
        // Map manifest to serializer
        registry.map_manifest_to_serializer("TestEvent", 1);
        
        // Test that we can find the mapped serializer
        // Note: This test is limited because we can't easily test the full serialize/deserialize
        // cycle without more complex type registration
    }

    #[test]
    fn test_default_registry() {
        let registry = utils::default_registry();
        
        // Should have at least the JSON and Protobuf serializers
        assert!(registry.serializers.len() >= 2);
    }
}