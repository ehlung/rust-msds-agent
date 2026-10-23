//! Configuration for persistence backends

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Main persistence configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    pub journal: JournalBackendConfig,
    pub snapshot_store: SnapshotBackendConfig,
    pub recovery: RecoverySettings,
    pub serialization: SerializationSettings,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            journal: JournalBackendConfig::default(),
            snapshot_store: SnapshotBackendConfig::default(),
            recovery: RecoverySettings::default(),
            serialization: SerializationSettings::default(),
        }
    }
}

/// Journal backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend")]
pub enum JournalBackendConfig {
    #[serde(rename = "cassandra")]
    Cassandra {
        contact_points: Vec<String>,
        keyspace: String,
        table_name: String,
        replication_factor: u32,
        consistency_level: ConsistencyLevelConfig,
        max_message_batch_size: usize,
        request_timeout_ms: u64,
        connection_pool_size: usize,
        retry_policy: RetryPolicyConfig,
    },
    #[serde(rename = "in-memory")]
    InMemory,
}

impl Default for JournalBackendConfig {
    fn default() -> Self {
        Self::Cassandra {
            contact_points: vec!["127.0.0.1:9042".to_string()],
            keyspace: "pekko".to_string(),
            table_name: "messages".to_string(),
            replication_factor: 3,
            consistency_level: ConsistencyLevelConfig::Quorum,
            max_message_batch_size: 100,
            request_timeout_ms: 5000,
            connection_pool_size: 10,
            retry_policy: RetryPolicyConfig::default(),
        }
    }
}

/// Snapshot store configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend")]
pub enum SnapshotBackendConfig {
    #[serde(rename = "cassandra")]
    Cassandra {
        contact_points: Vec<String>,
        keyspace: String,
        table_name: String,
        max_snapshots_per_persistence_id: Option<u64>,
        retention_period_days: Option<u64>,
        compression: CompressionConfig,
    },
    #[serde(rename = "in-memory")]
    InMemory,
}

impl Default for SnapshotBackendConfig {
    fn default() -> Self {
        Self::Cassandra {
            contact_points: vec!["127.0.0.1:9042".to_string()],
            keyspace: "pekko".to_string(),
            table_name: "snapshots".to_string(),
            max_snapshots_per_persistence_id: Some(10),
            retention_period_days: Some(30),
            compression: CompressionConfig::None,
        }
    }
}

/// Consistency level configuration
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConsistencyLevelConfig {
    #[serde(rename = "any")]
    Any,
    #[serde(rename = "one")]
    One,
    #[serde(rename = "two")]
    Two,
    #[serde(rename = "three")]
    Three,
    #[serde(rename = "quorum")]
    Quorum,
    #[serde(rename = "all")]
    All,
    #[serde(rename = "local_quorum")]
    LocalQuorum,
    #[serde(rename = "each_quorum")]
    EachQuorum,
    #[serde(rename = "serial")]
    Serial,
    #[serde(rename = "local_serial")]
    LocalSerial,
    #[serde(rename = "local_one")]
    LocalOne,
}

/// Compression configuration
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionConfig {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "gzip")]
    Gzip,
    #[serde(rename = "lz4")]
    Lz4,
    #[serde(rename = "snappy")]
    Snappy,
}

/// Retry policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicyConfig {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub exponential_backoff: bool,
}

impl Default for RetryPolicyConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 100,
            max_delay_ms: 5000,
            exponential_backoff: true,
        }
    }
}

/// Recovery settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoverySettings {
    pub batch_size: u64,
    pub max_recovery_time_ms: Option<u64>,
    pub snapshot_after_events: Option<u64>,
    pub delete_events_after_snapshot: bool,
    pub parallel_recovery: bool,
    pub recovery_timeout_ms: u64,
}

impl Default for RecoverySettings {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            max_recovery_time_ms: Some(300_000), // 5 minutes
            snapshot_after_events: Some(10_000),
            delete_events_after_snapshot: false,
            parallel_recovery: false,
            recovery_timeout_ms: 60_000, // 1 minute
        }
    }
}

/// Serialization settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializationSettings {
    pub default_serializer: String,
    pub manifest_mapping: HashMap<String, String>,
    pub compression: CompressionConfig,
    pub include_manifest_in_payload: bool,
}

impl Default for SerializationSettings {
    fn default() -> Self {
        Self {
            default_serializer: "json".to_string(),
            manifest_mapping: HashMap::new(),
            compression: CompressionConfig::None,
            include_manifest_in_payload: true,
        }
    }
}

/// Configuration builder for fluent API
pub struct PersistenceConfigBuilder {
    config: PersistenceConfig,
}

impl PersistenceConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: PersistenceConfig::default(),
        }
    }

    pub fn with_cassandra_journal(
        mut self,
        contact_points: Vec<String>,
        keyspace: String,
    ) -> Self {
        self.config.journal = JournalBackendConfig::Cassandra {
            contact_points,
            keyspace,
            table_name: "messages".to_string(),
            replication_factor: 3,
            consistency_level: ConsistencyLevelConfig::Quorum,
            max_message_batch_size: 100,
            request_timeout_ms: 5000,
            connection_pool_size: 10,
            retry_policy: RetryPolicyConfig::default(),
        };
        self
    }

    pub fn with_in_memory_journal(mut self) -> Self {
        self.config.journal = JournalBackendConfig::InMemory;
        self
    }

    pub fn with_cassandra_snapshots(
        mut self,
        contact_points: Vec<String>,
        keyspace: String,
    ) -> Self {
        self.config.snapshot_store = SnapshotBackendConfig::Cassandra {
            contact_points,
            keyspace,
            table_name: "snapshots".to_string(),
            max_snapshots_per_persistence_id: Some(10),
            retention_period_days: Some(30),
            compression: CompressionConfig::None,
        };
        self
    }

    pub fn with_in_memory_snapshots(mut self) -> Self {
        self.config.snapshot_store = SnapshotBackendConfig::InMemory;
        self
    }

    pub fn with_recovery_batch_size(mut self, batch_size: u64) -> Self {
        self.config.recovery.batch_size = batch_size;
        self
    }

    pub fn with_snapshot_frequency(mut self, events: u64) -> Self {
        self.config.recovery.snapshot_after_events = Some(events);
        self
    }

    pub fn with_default_serializer(mut self, serializer: String) -> Self {
        self.config.serialization.default_serializer = serializer;
        self
    }

    pub fn with_manifest_mapping(mut self, manifest: String, serializer: String) -> Self {
        self.config
            .serialization
            .manifest_mapping
            .insert(manifest, serializer);
        self
    }

    pub fn build(self) -> PersistenceConfig {
        self.config
    }
}

impl Default for PersistenceConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Load configuration from file
pub fn load_config_from_file(path: &str) -> Result<PersistenceConfig, ConfigError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::FileRead(path.to_string(), e.to_string()))?;

    if path.ends_with(".toml") {
        toml::from_str(&content).map_err(|e| ConfigError::Parse(e.to_string()))
    } else if path.ends_with(".json") {
        serde_json::from_str(&content).map_err(|e| ConfigError::Parse(e.to_string()))
    } else {
        Err(ConfigError::UnsupportedFormat(path.to_string()))
    }
}

/// Load configuration from environment variables
pub fn load_config_from_env() -> Result<PersistenceConfig, ConfigError> {
    let mut config = PersistenceConfig::default();

    // Journal configuration
    if let Ok(contact_points) = std::env::var("PEKKO_JOURNAL_CONTACT_POINTS") {
        let points: Vec<String> = contact_points.split(',').map(|s| s.trim().to_string()).collect();
        if let JournalBackendConfig::Cassandra { contact_points: ref mut cp, .. } = config.journal {
            *cp = points;
        }
    }

    if let Ok(keyspace) = std::env::var("PEKKO_JOURNAL_KEYSPACE") {
        if let JournalBackendConfig::Cassandra { keyspace: ref mut ks, .. } = config.journal {
            *ks = keyspace;
        }
    }

    // Snapshot configuration
    if let Ok(contact_points) = std::env::var("PEKKO_SNAPSHOT_CONTACT_POINTS") {
        let points: Vec<String> = contact_points.split(',').map(|s| s.trim().to_string()).collect();
        if let SnapshotBackendConfig::Cassandra { contact_points: ref mut cp, .. } = config.snapshot_store {
            *cp = points;
        }
    }

    // Recovery configuration
    if let Ok(batch_size) = std::env::var("PEKKO_RECOVERY_BATCH_SIZE") {
        config.recovery.batch_size = batch_size
            .parse()
            .map_err(|e| ConfigError::Parse(format!("Invalid batch size: {}", e)))?;
    }

    Ok(config)
}

/// Configuration errors
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config file {0}: {1}")]
    FileRead(String, String),

    #[error("Failed to parse config: {0}")]
    Parse(String),

    #[error("Unsupported config format: {0}")]
    UnsupportedFormat(String),

    #[error("Missing required configuration: {0}")]
    MissingRequired(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PersistenceConfig::default();
        
        assert!(matches!(config.journal, JournalBackendConfig::Cassandra { .. }));
        assert!(matches!(config.snapshot_store, SnapshotBackendConfig::Cassandra { .. }));
        assert_eq!(config.recovery.batch_size, 1000);
        assert_eq!(config.serialization.default_serializer, "json");
    }

    #[test]
    fn test_config_builder() {
        let config = PersistenceConfigBuilder::new()
            .with_in_memory_journal()
            .with_in_memory_snapshots()
            .with_recovery_batch_size(500)
            .with_default_serializer("protobuf".to_string())
            .with_manifest_mapping("TestEvent".to_string(), "json".to_string())
            .build();

        assert!(matches!(config.journal, JournalBackendConfig::InMemory));
        assert!(matches!(config.snapshot_store, SnapshotBackendConfig::InMemory));
        assert_eq!(config.recovery.batch_size, 500);
        assert_eq!(config.serialization.default_serializer, "protobuf");
        assert_eq!(
            config.serialization.manifest_mapping.get("TestEvent"),
            Some(&"json".to_string())
        );
    }

    #[test]
    fn test_toml_serialization() {
        let config = PersistenceConfig::default();
        let toml_str = toml::to_string(&config).unwrap();
        let deserialized: PersistenceConfig = toml::from_str(&toml_str).unwrap();
        
        // Check a few key values
        assert_eq!(config.recovery.batch_size, deserialized.recovery.batch_size);
        assert_eq!(
            config.serialization.default_serializer,
            deserialized.serialization.default_serializer
        );
    }

    #[test]
    fn test_json_serialization() {
        let config = PersistenceConfig::default();
        let json_str = serde_json::to_string(&config).unwrap();
        let deserialized: PersistenceConfig = serde_json::from_str(&json_str).unwrap();
        
        assert_eq!(config.recovery.batch_size, deserialized.recovery.batch_size);
    }

    #[test]
    fn test_retry_policy_defaults() {
        let retry_policy = RetryPolicyConfig::default();
        
        assert_eq!(retry_policy.max_retries, 3);
        assert_eq!(retry_policy.base_delay_ms, 100);
        assert_eq!(retry_policy.max_delay_ms, 5000);
        assert!(retry_policy.exponential_backoff);
    }
}