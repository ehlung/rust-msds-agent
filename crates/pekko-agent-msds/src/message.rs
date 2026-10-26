//! 각 토픽의 payload 타입 (설계 문서 4절).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::profile::{DocumentProfile, Figure, PageImage, SectionText};
use crate::result::Warning;
use crate::section::SectionKind;

/// `msds.job.created` (gateway → orchestrator)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct JobCreated {
    pub job_id: Uuid,
    pub pdf_key: String,
    pub file_name: String,
    pub sha256: String,
}

/// `msds.ingest.request` (orchestrator → ingest)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IngestRequest {
    pub job_id: Uuid,
    pub pdf_key: String,
}

/// `msds.ingest.done` (ingest → orchestrator)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IngestDone {
    pub job_id: Uuid,
    #[serde(flatten)]
    pub outcome: IngestOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum IngestOutcome {
    Ok { profile: DocumentProfile },
    Failed { error: ErrorInfo },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AgentKind {
    Translate,
    Composition,
    Handling,
}

impl AgentKind {
    pub fn task_topic(self) -> &'static str {
        match self {
            Self::Translate => crate::topics::TASK_TRANSLATE,
            Self::Composition => crate::topics::TASK_COMPOSITION,
            Self::Handling => crate::topics::TASK_HANDLING,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Effort {
    Medium,
    High,
}

/// 호출할 모델. 오케스트레이터가 정하고 에이전트는 받은 대로 호출한다.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModelSpec {
    pub name: String,
    pub effort: Effort,
}

/// 오케스트레이터 에이전트의 경로 판단 (구조화 출력으로 받는다).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RoutingPlan {
    pub verdict: Verdict,
    pub plan: Vec<PlannedTask>,
    pub skipped: Vec<SkippedTask>,
    pub summary: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Msds,
    NotMsds,
    Unsupported,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PlannedTask {
    pub agent: AgentKind,
    pub product_index: u32,
    /// 빈 배열이면 그 제품의 모든 섹션.
    pub sections: Vec<SectionKind>,
    pub page_images: Vec<u32>,
    pub note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SkippedTask {
    pub agent: AgentKind,
    pub product_index: u32,
    pub reason: String,
}

/// `msds.task.*` (orchestrator → 에이전트)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentTask {
    pub task_id: Uuid,
    pub job_id: Uuid,
    pub agent: AgentKind,
    pub attempt: u32,
    pub model: ModelSpec,
    pub prompt_version: String,
    pub product_index: u32,
    pub language: crate::profile::Language,
    pub sections: Vec<SectionText>,
    pub page_images: Vec<PageImage>,
    /// 그림 후보. 취급 작업에만 넣는다(그 제품의 쪽에 있는 후보 전부).
    #[serde(default)]
    pub figures: Vec<TaskFigure>,
    pub note: String,
    pub limits: TaskLimits,
}

/// 작업에 넣는 그림 후보. 에이전트가 페이지 이미지에서 이 영역을 잘라 본다.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskFigure {
    /// 문서 안에서 유일한 번호(`fig-<프로필 figures의 순번>`). 고른 그림은 이 이름으로 저장한다.
    pub id: String,
    #[serde(flatten)]
    pub figure: Figure,
    pub page_image: PageImage,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskLimits {
    pub max_tokens: u32,
    pub max_tool_rounds: u32,
}

/// `msds.task.result` (에이전트 → orchestrator)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskResult {
    pub task_id: Uuid,
    pub job_id: Uuid,
    pub agent: AgentKind,
    pub attempt: u32,
    /// JSON에서는 `status`와 `output` 또는 `error`가 이 객체 바로 아래에 온다.
    #[serde(flatten)]
    pub outcome: TaskOutcome,
    pub warnings: Vec<Warning>,
    pub usage: Vec<LlmUsage>,
    pub duration_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TaskOutcome {
    /// `output`은 결과 스키마 중 그 에이전트가 맡은 부분이다.
    Ok { output: serde_json::Value },
    Failed { error: ErrorInfo },
}

/// Claude 호출 한 번의 사용량.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct LlmUsage {
    pub model: String,
    pub effort: Option<Effort>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ErrorInfo {
    pub code: String,
    pub message: String,
}

/// `msds.job.status` (orchestrator → gateway)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct JobStatusChanged {
    pub job_id: Uuid,
    pub status: JobStatus,
    pub detail: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Received,
    Ingesting,
    Routing,
    Processing,
    Merging,
    PendingReview,
    Approved,
    Published,
    Failed,
}
