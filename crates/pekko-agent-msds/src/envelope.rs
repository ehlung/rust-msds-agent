use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 모든 Kafka 메시지의 공통 봉투.
///
/// 필드는 `pekko_agent_events::AgentEventEnvelope`와 같고 `schema_version`만 더했다.
/// `payload`를 타입으로 받아, 받는 쪽이 `serde_json::Value`를 다시 해석하지 않게 한다.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Envelope<T> {
    pub event_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub source_service: String,
    pub event_type: String,
    pub tenant_id: String,
    /// 작업 ID. Kafka 메시지 키로도 쓴다.
    pub correlation_id: Uuid,
    pub schema_version: u32,
    pub payload: T,
}

pub const SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_TENANT: &str = "default";

impl<T> Envelope<T> {
    pub fn new(source_service: &str, event_type: &str, job_id: Uuid, payload: T) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            source_service: source_service.to_string(),
            event_type: event_type.to_string(),
            tenant_id: DEFAULT_TENANT.to_string(),
            correlation_id: job_id,
            schema_version: SCHEMA_VERSION,
            payload,
        }
    }

    /// Kafka 메시지 키. 한 작업의 메시지가 같은 파티션에 들어가 순서가 지켜진다.
    pub fn key(&self) -> String {
        self.correlation_id.to_string()
    }
}
