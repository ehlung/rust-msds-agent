//! Error types for persistence operations

use thiserror::Error;

/// Errors that can occur during persistence operations
#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("Journal error: {0}")]
    Journal(#[from] JournalError),
    
    #[error("Snapshot error: {0}")]
    Snapshot(#[from] SnapshotError),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] SerializationError),
    
    #[error("Recovery error: {0}")]
    Recovery(String),
    
    #[error("Configuration error: {0}")]
    Configuration(String),
    
    #[error("Actor not found: {persistence_id}")]
    ActorNotFound { persistence_id: String },
    
    #[error("Sequence number mismatch: expected {expected}, got {actual}")]
    SequenceNumberMismatch { expected: u64, actual: u64 },
}

/// Errors from journal operations
#[derive(Debug, Error)]
pub enum JournalError {
    #[error("Database connection error: {0}")]
    Connection(String),
    
    #[error("Write operation failed: {0}")]
    Write(String),
    
    #[error("Read operation failed: {0}")]
    Read(String),
    
    #[error("Query execution failed: {0}")]
    Query(String),
    
    #[error("Invalid persistence ID: {0}")]
    InvalidPersistenceId(String),
    
    #[error("Sequence number out of range: {0}")]
    SequenceNumberOutOfRange(u64),
    
    #[error("Journal not available")]
    Unavailable,
}

/// Errors from snapshot operations
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("Database connection error: {0}")]
    Connection(String),
    
    #[error("Snapshot save failed: {0}")]
    Save(String),
    
    #[error("Snapshot load failed: {0}")]
    Load(String),
    
    #[error("Snapshot delete failed: {0}")]
    Delete(String),
    
    #[error("Snapshot not found for persistence ID: {0}")]
    NotFound(String),
    
    #[error("Snapshot store not available")]
    Unavailable,
}

/// Errors from serialization operations
#[derive(Debug, Error)]
pub enum SerializationError {
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("Protobuf encoding error: {0}")]
    ProtobufEncode(#[from] prost::EncodeError),
    
    #[error("Protobuf decoding error: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),
    
    #[error("Unknown manifest: {0}")]
    UnknownManifest(String),
    
    #[error("Serializer not found for manifest: {0}")]
    SerializerNotFound(String),
    
    #[error("Deserialization failed: {0}")]
    Deserialization(String),
    
    #[error("Serialization failed: {0}")]
    Serialization(String),
}

/// Result type for persistence operations
pub type PersistenceResult<T> = Result<T, PersistenceError>;

/// Result type for journal operations
pub type JournalResult<T> = Result<T, JournalError>;

/// Result type for snapshot operations
pub type SnapshotResult<T> = Result<T, SnapshotError>;

/// Result type for serialization operations
pub type SerializationResult<T> = Result<T, SerializationError>;