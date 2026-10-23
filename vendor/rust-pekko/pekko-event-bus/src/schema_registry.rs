// schema_registry.rs - Schema Registry
// Confluent-like schema versioning with compatibility checking
// Supports JSON Schema, Avro-like, and custom schema types

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::bus_errors::EventBusError;

// ============================================================================
// Schema ID
// ============================================================================

static SCHEMA_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaId(u64);

impl SchemaId {
    pub fn new() -> Self {
        Self(SCHEMA_ID_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "schema-{}", self.0)
    }
}

// ============================================================================
// Schema Types
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum SchemaType {
    Json,
    Avro,
    Protobuf,
    Custom(String),
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaType::Json => write!(f, "JSON"),
            SchemaType::Avro => write!(f, "AVRO"),
            SchemaType::Protobuf => write!(f, "PROTOBUF"),
            SchemaType::Custom(name) => write!(f, "CUSTOM({})", name),
        }
    }
}

// ============================================================================
// Schema Definition
// ============================================================================

#[derive(Debug, Clone)]
pub struct SchemaDefinition {
    pub id: SchemaId,
    pub subject: String,
    pub version: u32,
    pub schema_type: SchemaType,
    pub schema: String,
    pub fields: Vec<SchemaField>,
    pub registered_at: Instant,
    pub references: Vec<SchemaReference>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaField {
    pub name: String,
    pub field_type: FieldType,
    pub required: bool,
    pub default_value: Option<String>,
    pub doc: Option<String>,
}

impl SchemaField {
    pub fn new(name: &str, field_type: FieldType) -> Self {
        Self {
            name: name.to_string(),
            field_type,
            required: true,
            default_value: None,
            doc: None,
        }
    }

    pub fn optional(mut self) -> Self {
        self.required = false;
        self
    }

    pub fn with_default(mut self, value: &str) -> Self {
        self.default_value = Some(value.to_string());
        self.required = false;
        self
    }

    pub fn with_doc(mut self, doc: &str) -> Self {
        self.doc = Some(doc.to_string());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    Bytes,
    Array(Box<FieldType>),
    Map(Box<FieldType>, Box<FieldType>),
    Nullable(Box<FieldType>),
    Enum(Vec<String>),
    Record(String),
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldType::String => write!(f, "string"),
            FieldType::Int32 => write!(f, "int32"),
            FieldType::Int64 => write!(f, "int64"),
            FieldType::Float32 => write!(f, "float32"),
            FieldType::Float64 => write!(f, "float64"),
            FieldType::Boolean => write!(f, "boolean"),
            FieldType::Bytes => write!(f, "bytes"),
            FieldType::Array(inner) => write!(f, "array<{}>", inner),
            FieldType::Map(k, v) => write!(f, "map<{}, {}>", k, v),
            FieldType::Nullable(inner) => write!(f, "nullable<{}>", inner),
            FieldType::Enum(vals) => write!(f, "enum[{}]", vals.join(",")),
            FieldType::Record(name) => write!(f, "record({})", name),
        }
    }
}

/// Reference to another schema (for nested/composed schemas)
#[derive(Debug, Clone)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: u32,
}

// ============================================================================
// Compatibility Mode
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum CompatibilityMode {
    /// No compatibility checking
    None,
    /// New schema can read data written by old schema
    Backward,
    /// Old schema can read data written by new schema
    Forward,
    /// Both backward and forward compatible
    Full,
    /// Backward compatible with all previous versions
    BackwardTransitive,
    /// Forward compatible with all previous versions
    ForwardTransitive,
    /// Full compatible with all previous versions
    FullTransitive,
}

// ============================================================================
// Compatibility Checker
// ============================================================================

#[derive(Debug)]
pub struct CompatibilityChecker;

#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    pub compatible: bool,
    pub issues: Vec<CompatibilityIssue>,
}

#[derive(Debug, Clone)]
pub struct CompatibilityIssue {
    pub severity: IssueSeverity,
    pub field: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IssueSeverity {
    Error,
    Warning,
    Info,
}

impl CompatibilityChecker {
    /// Check if new_schema is compatible with old_schema under given mode
    pub fn check(
        mode: &CompatibilityMode,
        old_schema: &SchemaDefinition,
        new_schema: &SchemaDefinition,
    ) -> CompatibilityResult {
        match mode {
            CompatibilityMode::None => CompatibilityResult {
                compatible: true,
                issues: vec![],
            },
            CompatibilityMode::Backward => Self::check_backward(old_schema, new_schema),
            CompatibilityMode::Forward => Self::check_forward(old_schema, new_schema),
            CompatibilityMode::Full => Self::check_full(old_schema, new_schema),
            // Transitive modes are handled at registry level (check all versions)
            CompatibilityMode::BackwardTransitive => Self::check_backward(old_schema, new_schema),
            CompatibilityMode::ForwardTransitive => Self::check_forward(old_schema, new_schema),
            CompatibilityMode::FullTransitive => Self::check_full(old_schema, new_schema),
        }
    }

    /// Backward: new can read old. Rules:
    /// - New schema can add optional fields
    /// - New schema cannot remove required fields
    /// - New schema cannot change field types incompatibly
    fn check_backward(
        old_schema: &SchemaDefinition,
        new_schema: &SchemaDefinition,
    ) -> CompatibilityResult {
        let mut issues = Vec::new();

        // Check all old required fields exist in new schema
        for old_field in &old_schema.fields {
            if old_field.required {
                let found = new_schema.fields.iter().find(|f| f.name == old_field.name);
                match found {
                    None => {
                        issues.push(CompatibilityIssue {
                            severity: IssueSeverity::Error,
                            field: old_field.name.clone(),
                            message: format!(
                                "Required field '{}' removed in new schema",
                                old_field.name
                            ),
                        });
                    }
                    Some(new_field) => {
                        if !Self::is_type_compatible(&old_field.field_type, &new_field.field_type) {
                            issues.push(CompatibilityIssue {
                                severity: IssueSeverity::Error,
                                field: old_field.name.clone(),
                                message: format!(
                                    "Type change incompatible: {} → {}",
                                    old_field.field_type, new_field.field_type
                                ),
                            });
                        }
                    }
                }
            }
        }

        // New fields must have defaults or be optional
        for new_field in &new_schema.fields {
            if !old_schema.fields.iter().any(|f| f.name == new_field.name) {
                if new_field.required && new_field.default_value.is_none() {
                    issues.push(CompatibilityIssue {
                        severity: IssueSeverity::Error,
                        field: new_field.name.clone(),
                        message: format!(
                            "New required field '{}' without default is not backward compatible",
                            new_field.name
                        ),
                    });
                }
            }
        }

        CompatibilityResult {
            compatible: !issues.iter().any(|i| i.severity == IssueSeverity::Error),
            issues,
        }
    }

    /// Forward: old can read new. Rules:
    /// - New schema cannot add required fields without defaults
    /// - New schema can remove optional fields
    fn check_forward(
        old_schema: &SchemaDefinition,
        new_schema: &SchemaDefinition,
    ) -> CompatibilityResult {
        let mut issues = Vec::new();

        // Check new required fields have counterparts in old
        for new_field in &new_schema.fields {
            if new_field.required {
                let found = old_schema.fields.iter().find(|f| f.name == new_field.name);
                match found {
                    None => {
                        issues.push(CompatibilityIssue {
                            severity: IssueSeverity::Error,
                            field: new_field.name.clone(),
                            message: format!(
                                "New required field '{}' not in old schema — old readers can't process it",
                                new_field.name
                            ),
                        });
                    }
                    Some(old_field) => {
                        if !Self::is_type_compatible(&new_field.field_type, &old_field.field_type) {
                            issues.push(CompatibilityIssue {
                                severity: IssueSeverity::Error,
                                field: new_field.name.clone(),
                                message: format!(
                                    "Type change incompatible: {} → {}",
                                    old_field.field_type, new_field.field_type
                                ),
                            });
                        }
                    }
                }
            }
        }

        CompatibilityResult {
            compatible: !issues.iter().any(|i| i.severity == IssueSeverity::Error),
            issues,
        }
    }

    /// Full: both backward and forward compatible
    fn check_full(
        old_schema: &SchemaDefinition,
        new_schema: &SchemaDefinition,
    ) -> CompatibilityResult {
        let back = Self::check_backward(old_schema, new_schema);
        let fwd = Self::check_forward(old_schema, new_schema);

        let mut issues = back.issues;
        issues.extend(fwd.issues);

        CompatibilityResult {
            compatible: !issues.iter().any(|i| i.severity == IssueSeverity::Error),
            issues,
        }
    }

    /// Simple type compatibility check
    fn is_type_compatible(from: &FieldType, to: &FieldType) -> bool {
        if from == to {
            return true;
        }

        // Allow widening: Int32 → Int64, Float32 → Float64
        matches!(
            (from, to),
            (FieldType::Int32, FieldType::Int64)
                | (FieldType::Float32, FieldType::Float64)
                | (_, FieldType::Nullable(_))
        )
    }
}

// ============================================================================
// Schema Registry
// ============================================================================

#[derive(Debug)]
pub struct SchemaRegistry {
    schemas: HashMap<SchemaId, SchemaDefinition>,
    subjects: HashMap<String, Vec<SchemaId>>,
    compatibility_modes: HashMap<String, CompatibilityMode>,
    default_compatibility: CompatibilityMode,
}

impl SchemaRegistry {
    pub fn new(default_compatibility: CompatibilityMode) -> Self {
        Self {
            schemas: HashMap::new(),
            subjects: HashMap::new(),
            compatibility_modes: HashMap::new(),
            default_compatibility,
        }
    }

    /// Register a new schema under a subject
    pub fn register(
        &mut self,
        subject: &str,
        schema_type: SchemaType,
        schema: &str,
        fields: Vec<SchemaField>,
    ) -> Result<SchemaId, EventBusError> {
        let version = self.latest_version(subject).unwrap_or(0) + 1;

        // Check compatibility with previous version
        if version > 1 {
            let mode = self.get_compatibility(subject);
            let prev = self.get_latest(subject).ok_or_else(|| {
                EventBusError::Internal(format!("No previous schema for {}", subject))
            })?;

            let new_def = SchemaDefinition {
                id: SchemaId::new(),
                subject: subject.to_string(),
                version,
                schema_type: schema_type.clone(),
                schema: schema.to_string(),
                fields: fields.clone(),
                registered_at: Instant::now(),
                references: Vec::new(),
            };

            let result = CompatibilityChecker::check(&mode, prev, &new_def);
            if !result.compatible {
                let error_msgs: Vec<String> = result
                    .issues
                    .iter()
                    .filter(|i| i.severity == IssueSeverity::Error)
                    .map(|i| format!("{}: {}", i.field, i.message))
                    .collect();
                return Err(EventBusError::SerializationError(format!(
                    "Schema incompatible: {}",
                    error_msgs.join("; ")
                )));
            }
        }

        let id = SchemaId::new();
        let definition = SchemaDefinition {
            id,
            subject: subject.to_string(),
            version,
            schema_type,
            schema: schema.to_string(),
            fields,
            registered_at: Instant::now(),
            references: Vec::new(),
        };

        self.schemas.insert(id, definition);
        self.subjects
            .entry(subject.to_string())
            .or_default()
            .push(id);

        Ok(id)
    }

    /// Register with schema references
    pub fn register_with_refs(
        &mut self,
        subject: &str,
        schema_type: SchemaType,
        schema: &str,
        fields: Vec<SchemaField>,
        references: Vec<SchemaReference>,
    ) -> Result<SchemaId, EventBusError> {
        let id = self.register(subject, schema_type, schema, fields)?;
        if let Some(def) = self.schemas.get_mut(&id) {
            def.references = references;
        }
        Ok(id)
    }

    pub fn get_by_id(&self, id: SchemaId) -> Option<&SchemaDefinition> {
        self.schemas.get(&id)
    }

    pub fn get_latest(&self, subject: &str) -> Option<&SchemaDefinition> {
        self.subjects
            .get(subject)
            .and_then(|ids| ids.last())
            .and_then(|id| self.schemas.get(id))
    }

    pub fn get_version(&self, subject: &str, version: u32) -> Option<&SchemaDefinition> {
        self.subjects.get(subject).and_then(|ids| {
            ids.iter()
                .find_map(|id| {
                    self.schemas.get(id).filter(|s| s.version == version)
                })
        })
    }

    pub fn latest_version(&self, subject: &str) -> Option<u32> {
        self.get_latest(subject).map(|s| s.version)
    }

    pub fn all_versions(&self, subject: &str) -> Vec<u32> {
        self.subjects
            .get(subject)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.schemas.get(id).map(|s| s.version))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn all_subjects(&self) -> Vec<&str> {
        self.subjects.keys().map(|s| s.as_str()).collect()
    }

    pub fn set_compatibility(&mut self, subject: &str, mode: CompatibilityMode) {
        self.compatibility_modes
            .insert(subject.to_string(), mode);
    }

    pub fn get_compatibility(&self, subject: &str) -> CompatibilityMode {
        self.compatibility_modes
            .get(subject)
            .cloned()
            .unwrap_or_else(|| self.default_compatibility.clone())
    }

    pub fn delete_subject(&mut self, subject: &str) -> bool {
        if let Some(ids) = self.subjects.remove(subject) {
            for id in ids {
                self.schemas.remove(&id);
            }
            self.compatibility_modes.remove(subject);
            true
        } else {
            false
        }
    }

    pub fn schema_count(&self) -> usize {
        self.schemas.len()
    }

    pub fn subject_count(&self) -> usize {
        self.subjects.len()
    }

    /// Test compatibility of a proposed schema without registering
    pub fn test_compatibility(
        &self,
        subject: &str,
        fields: &[SchemaField],
    ) -> CompatibilityResult {
        let mode = self.get_compatibility(subject);
        match self.get_latest(subject) {
            None => CompatibilityResult {
                compatible: true,
                issues: vec![],
            },
            Some(existing) => {
                let proposed = SchemaDefinition {
                    id: SchemaId::new(),
                    subject: subject.to_string(),
                    version: 0,
                    schema_type: existing.schema_type.clone(),
                    schema: String::new(),
                    fields: fields.to_vec(),
                    registered_at: Instant::now(),
                    references: Vec::new(),
                };
                CompatibilityChecker::check(&mode, existing, &proposed)
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn order_schema_v1() -> Vec<SchemaField> {
        vec![
            SchemaField::new("order_id", FieldType::String),
            SchemaField::new("amount", FieldType::Float64),
            SchemaField::new("customer_id", FieldType::String),
        ]
    }

    // === SchemaId Tests ===

    #[test]
    fn test_schema_id_unique() {
        let id1 = SchemaId::new();
        let id2 = SchemaId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_schema_id_display() {
        let id = SchemaId::new();
        assert!(format!("{}", id).starts_with("schema-"));
    }

    // === SchemaField Tests ===

    #[test]
    fn test_field_creation() {
        let f = SchemaField::new("name", FieldType::String);
        assert_eq!(f.name, "name");
        assert!(f.required);
        assert!(f.default_value.is_none());
    }

    #[test]
    fn test_field_optional() {
        let f = SchemaField::new("age", FieldType::Int32).optional();
        assert!(!f.required);
    }

    #[test]
    fn test_field_with_default() {
        let f = SchemaField::new("status", FieldType::String)
            .with_default("active");
        assert!(!f.required);
        assert_eq!(f.default_value.as_deref(), Some("active"));
    }

    #[test]
    fn test_field_with_doc() {
        let f = SchemaField::new("id", FieldType::String)
            .with_doc("Unique identifier");
        assert_eq!(f.doc.as_deref(), Some("Unique identifier"));
    }

    // === FieldType Tests ===

    #[test]
    fn test_field_type_display() {
        assert_eq!(format!("{}", FieldType::String), "string");
        assert_eq!(format!("{}", FieldType::Int64), "int64");
        assert_eq!(
            format!("{}", FieldType::Array(Box::new(FieldType::String))),
            "array<string>"
        );
        assert_eq!(
            format!("{}", FieldType::Map(
                Box::new(FieldType::String),
                Box::new(FieldType::Int32)
            )),
            "map<string, int32>"
        );
        assert_eq!(
            format!("{}", FieldType::Nullable(Box::new(FieldType::Int32))),
            "nullable<int32>"
        );
    }

    #[test]
    fn test_schema_type_display() {
        assert_eq!(format!("{}", SchemaType::Json), "JSON");
        assert_eq!(format!("{}", SchemaType::Avro), "AVRO");
        assert_eq!(format!("{}", SchemaType::Custom("xml".into())), "CUSTOM(xml)");
    }

    // === Registry Tests ===

    #[test]
    fn test_register_first_schema() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::Backward);

        let id = reg
            .register("orders-value", SchemaType::Json, "{}", order_schema_v1())
            .unwrap();

        let schema = reg.get_by_id(id).unwrap();
        assert_eq!(schema.version, 1);
        assert_eq!(schema.subject, "orders-value");
        assert_eq!(schema.fields.len(), 3);
    }

    #[test]
    fn test_register_backward_compatible() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::Backward);

        reg.register("orders", SchemaType::Json, "{}", order_schema_v1())
            .unwrap();

        // v2: add optional field — backward compatible
        let mut v2_fields = order_schema_v1();
        v2_fields.push(SchemaField::new("notes", FieldType::String).optional());

        let id2 = reg
            .register("orders", SchemaType::Json, "{}", v2_fields)
            .unwrap();

        let schema = reg.get_by_id(id2).unwrap();
        assert_eq!(schema.version, 2);
    }

    #[test]
    fn test_register_backward_incompatible_rejected() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::Backward);

        reg.register("orders", SchemaType::Json, "{}", order_schema_v1())
            .unwrap();

        // v2: remove required field — NOT backward compatible
        let v2_fields = vec![
            SchemaField::new("order_id", FieldType::String),
            // missing "amount" — required field removed
        ];

        assert!(reg
            .register("orders", SchemaType::Json, "{}", v2_fields)
            .is_err());
    }

    #[test]
    fn test_register_new_required_field_without_default() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::Backward);

        reg.register("orders", SchemaType::Json, "{}", order_schema_v1())
            .unwrap();

        // v2: add required field without default — NOT backward compatible
        let mut v2_fields = order_schema_v1();
        v2_fields.push(SchemaField::new("priority", FieldType::Int32)); // required, no default

        assert!(reg
            .register("orders", SchemaType::Json, "{}", v2_fields)
            .is_err());
    }

    #[test]
    fn test_register_new_required_field_with_default() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::Backward);

        reg.register("orders", SchemaType::Json, "{}", order_schema_v1())
            .unwrap();

        // v2: add field with default — backward compatible
        let mut v2_fields = order_schema_v1();
        v2_fields.push(
            SchemaField::new("priority", FieldType::Int32).with_default("0"),
        );

        assert!(reg
            .register("orders", SchemaType::Json, "{}", v2_fields)
            .is_ok());
    }

    #[test]
    fn test_no_compatibility_mode() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::None);

        reg.register("orders", SchemaType::Json, "{}", order_schema_v1())
            .unwrap();

        // Completely different schema — allowed with None mode
        let v2_fields = vec![SchemaField::new("x", FieldType::Boolean)];
        assert!(reg
            .register("orders", SchemaType::Json, "{}", v2_fields)
            .is_ok());
    }

    #[test]
    fn test_get_latest_and_version() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::None);

        reg.register("t", SchemaType::Json, "{}", vec![SchemaField::new("a", FieldType::String)])
            .unwrap();
        reg.register("t", SchemaType::Json, "{}", vec![SchemaField::new("b", FieldType::String)])
            .unwrap();

        assert_eq!(reg.latest_version("t"), Some(2));
        assert_eq!(reg.get_version("t", 1).unwrap().fields[0].name, "a");
        assert_eq!(reg.get_version("t", 2).unwrap().fields[0].name, "b");
    }

    #[test]
    fn test_all_versions() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::None);

        for _ in 0..3 {
            reg.register("t", SchemaType::Json, "{}", vec![SchemaField::new("a", FieldType::String)])
                .unwrap();
        }

        assert_eq!(reg.all_versions("t"), vec![1, 2, 3]);
    }

    #[test]
    fn test_all_subjects() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::None);
        reg.register("orders", SchemaType::Json, "{}", vec![]).unwrap();
        reg.register("payments", SchemaType::Json, "{}", vec![]).unwrap();

        let subjects = reg.all_subjects();
        assert_eq!(subjects.len(), 2);
    }

    #[test]
    fn test_per_subject_compatibility() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::Backward);

        reg.set_compatibility("relaxed-topic", CompatibilityMode::None);

        assert_eq!(
            reg.get_compatibility("relaxed-topic"),
            CompatibilityMode::None
        );
        assert_eq!(
            reg.get_compatibility("strict-topic"),
            CompatibilityMode::Backward
        );
    }

    #[test]
    fn test_delete_subject() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::None);
        reg.register("t", SchemaType::Json, "{}", vec![]).unwrap();
        assert_eq!(reg.schema_count(), 1);

        assert!(reg.delete_subject("t"));
        assert_eq!(reg.schema_count(), 0);
        assert_eq!(reg.subject_count(), 0);

        // Delete non-existent
        assert!(!reg.delete_subject("nope"));
    }

    #[test]
    fn test_test_compatibility() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::Backward);
        reg.register("t", SchemaType::Json, "{}", order_schema_v1())
            .unwrap();

        // Compatible: add optional
        let mut compat_fields = order_schema_v1();
        compat_fields.push(SchemaField::new("x", FieldType::String).optional());
        let result = reg.test_compatibility("t", &compat_fields);
        assert!(result.compatible);

        // Incompatible: remove required
        let incompat_fields = vec![SchemaField::new("order_id", FieldType::String)];
        let result = reg.test_compatibility("t", &incompat_fields);
        assert!(!result.compatible);
    }

    #[test]
    fn test_test_compatibility_no_existing() {
        let reg = SchemaRegistry::new(CompatibilityMode::Backward);
        let result = reg.test_compatibility("new-topic", &[]);
        assert!(result.compatible);
    }

    // === Compatibility Checker Tests ===

    #[test]
    fn test_type_widening_compatible() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::Backward);

        let v1 = vec![SchemaField::new("count", FieldType::Int32)];
        reg.register("t", SchemaType::Json, "{}", v1).unwrap();

        // Int32 → Int64 is widening — should be compatible
        let v2 = vec![SchemaField::new("count", FieldType::Int64)];
        assert!(reg.register("t", SchemaType::Json, "{}", v2).is_ok());
    }

    #[test]
    fn test_type_narrowing_incompatible() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::Backward);

        let v1 = vec![SchemaField::new("count", FieldType::Int64)];
        reg.register("t", SchemaType::Json, "{}", v1).unwrap();

        // Int64 → Int32 is narrowing — incompatible
        let v2 = vec![SchemaField::new("count", FieldType::Int32)];
        assert!(reg.register("t", SchemaType::Json, "{}", v2).is_err());
    }

    #[test]
    fn test_forward_compatibility() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::Forward);

        let v1 = vec![
            SchemaField::new("a", FieldType::String),
            SchemaField::new("b", FieldType::String),
        ];
        reg.register("t", SchemaType::Json, "{}", v1).unwrap();

        // Remove optional: old can still read new (missing field ignored)
        // But adding required field that old doesn't know about is NOT forward-compatible
        let v2 = vec![
            SchemaField::new("a", FieldType::String),
            SchemaField::new("b", FieldType::String),
            SchemaField::new("c", FieldType::String), // new required field
        ];
        assert!(reg.register("t", SchemaType::Json, "{}", v2).is_err());
    }

    #[test]
    fn test_full_compatibility() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::Full);

        let v1 = vec![
            SchemaField::new("a", FieldType::String),
            SchemaField::new("b", FieldType::String),
        ];
        reg.register("t", SchemaType::Json, "{}", v1).unwrap();

        // Full: must be both backward and forward
        // Only compatible change: add optional with default
        let v2 = vec![
            SchemaField::new("a", FieldType::String),
            SchemaField::new("b", FieldType::String),
            SchemaField::new("c", FieldType::String).optional(),
        ];
        // Forward-incompatible: old schema doesn't have "c" as required
        // But c is optional so forward check passes
        assert!(reg.register("t", SchemaType::Json, "{}", v2).is_ok());
    }

    #[test]
    fn test_register_with_references() {
        let mut reg = SchemaRegistry::new(CompatibilityMode::None);

        let addr_id = reg
            .register(
                "address",
                SchemaType::Json,
                "{}",
                vec![
                    SchemaField::new("street", FieldType::String),
                    SchemaField::new("city", FieldType::String),
                ],
            )
            .unwrap();

        let refs = vec![SchemaReference {
            name: "Address".into(),
            subject: "address".into(),
            version: 1,
        }];

        let order_id = reg
            .register_with_refs(
                "order",
                SchemaType::Json,
                "{}",
                vec![
                    SchemaField::new("id", FieldType::String),
                    SchemaField::new("address", FieldType::Record("Address".into())),
                ],
                refs,
            )
            .unwrap();

        let schema = reg.get_by_id(order_id).unwrap();
        assert_eq!(schema.references.len(), 1);
        assert_eq!(schema.references[0].subject, "address");
    }

    #[test]
    fn test_issue_severity() {
        let issue = CompatibilityIssue {
            severity: IssueSeverity::Error,
            field: "x".into(),
            message: "bad".into(),
        };
        assert_eq!(issue.severity, IssueSeverity::Error);
    }
}
