//! MSDS 작업 API: PDF 업로드, 작업 목록·상태·결과 조회.
//!
//! gateway는 추론하지 않는다. PDF를 저장하고 작업을 DB에 만든 뒤 `msds.job.created`를 보내면 끝이고,
//! 이후 단계는 msds-orchestrator가 이끈다. 상태와 결과는 DB(`msds_jobs`, `msds_results`)에서 읽는다.

use std::sync::Arc;

use axum::{
    async_trait,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use pekko_agent_msds::message::{JobCreated, JobStatus};
use pekko_agent_msds::{topics, Envelope};
use pekko_agent_msds_io::job_store::{JobRow, UsageSummary};
use pekko_agent_msds_io::storage::job_pdf_key;
use pekko_agent_msds_io::{Bus, JobStore, LocalStorage};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{make_error, require_permission, service_unavailable, AppState, AuthUser, ErrorResponse};

const SERVICE: &str = "api-gateway";
/// 테스트 PDF 중 가장 큰 것이 약 1MB다.
const MAX_PDF_BYTES: usize = 20 * 1024 * 1024;
/// 업로드·조회 권한. 에이전트에게 일을 맡기는 일이라 기존 권한 `agent.delegate`를 쓴다.
const PERMISSION: &str = "agent.delegate";

type ApiError = (StatusCode, Json<ErrorResponse>);

/// `msds.job.created`를 보내는 곳. 운영은 Kafka, 테스트는 보낸 것을 기록만 하는 구현을 쓴다.
#[async_trait]
pub(crate) trait JobPublisher: Send + Sync {
    async fn job_created(&self, envelope: &Envelope<JobCreated>) -> anyhow::Result<()>;
}

struct KafkaPublisher(Bus);

#[async_trait]
impl JobPublisher for KafkaPublisher {
    async fn job_created(&self, envelope: &Envelope<JobCreated>) -> anyhow::Result<()> {
        self.0.publish(topics::JOB_CREATED, envelope).await
    }
}

#[derive(Clone)]
pub(crate) struct MsdsApi {
    pub(crate) store: JobStore,
    pub(crate) storage: LocalStorage,
    pub(crate) publisher: Arc<dyn JobPublisher>,
}

impl MsdsApi {
    /// `MSDS_STORAGE_DIR`가 없으면 MSDS API를 끈다(`None`). 원본 EHS 기능만 쓰는 실행에 영향을 주지 않는다.
    pub(crate) async fn from_env(pool: PgPool) -> anyhow::Result<Option<Arc<Self>>> {
        let Ok(dir) = std::env::var("MSDS_STORAGE_DIR") else { return Ok(None) };
        let store = JobStore::new(pool);
        store.migrate().await?;
        let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
        let bus = Bus::new(&brokers, SERVICE)?;
        if let Err(e) = bus.ensure_topics(topics::ALL, 3).await {
            warn!(error = %e, "Kafka 토픽을 확인하지 못함 — 업로드하면 작업이 실패로 남는다");
        }
        Ok(Some(Arc::new(Self { store, storage: LocalStorage::new(dir)?, publisher: Arc::new(KafkaPublisher(bus)) })))
    }
}

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/msds/jobs", post(upload_job).get(list_jobs).layer(DefaultBodyLimit::max(MAX_PDF_BYTES)))
        .route("/api/msds/jobs/:job_id", get(get_job))
        .route("/api/msds/jobs/:job_id/result", get(get_result))
}

fn api(state: &AppState) -> Result<&MsdsApi, ApiError> {
    state.msds.as_deref().ok_or_else(|| service_unavailable("MSDS 기능이 꺼져 있음 (MSDS_STORAGE_DIR 미설정)"))
}

fn bad_request(msg: impl Into<String>) -> ApiError {
    make_error(StatusCode::BAD_REQUEST, "BAD_REQUEST", msg)
}

fn not_found(msg: impl Into<String>) -> ApiError {
    make_error(StatusCode::NOT_FOUND, "NOT_FOUND", msg)
}

fn db_error(e: anyhow::Error) -> ApiError {
    crate::internal_error(format!("DB 오류: {e:#}"))
}

/// POST /api/msds/jobs — multipart의 `file` 필드로 PDF를 받아 작업을 만든다. 202와 작업 정보를 돌려준다.
async fn upload_job(auth: AuthUser, State(state): State<AppState>, mut form: Multipart) -> Result<(StatusCode, Json<JobRow>), ApiError> {
    require_permission(&state, &auth.0, PERMISSION).await?;
    let api = api(&state)?;

    let mut file: Option<(String, Vec<u8>)> = None;
    while let Some(field) = form.next_field().await.map_err(|e| bad_request(format!("업로드를 읽지 못함: {e}")))? {
        if field.name() == Some("file") {
            let name = field.file_name().unwrap_or("upload.pdf").chars().take(255).collect::<String>();
            let bytes = field.bytes().await.map_err(|e| bad_request(format!("파일을 읽지 못함: {e}")))?;
            file = Some((name, bytes.to_vec()));
        }
    }
    let (file_name, bytes) = file.ok_or_else(|| bad_request("`file` 필드가 없음"))?;
    if !bytes.starts_with(b"%PDF-") {
        return Err(bad_request("PDF 파일이 아님"));
    }

    let job_id = Uuid::new_v4();
    let pdf_key = job_pdf_key(job_id);
    api.storage.put(&pdf_key, &bytes).await.map_err(|e| crate::internal_error(format!("PDF 저장 실패: {e:#}")))?;
    let sha256: String = Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect();
    let created = JobCreated { job_id, pdf_key, file_name, sha256 };
    api.store.create(&created, Some(&auth.0.sub)).await.map_err(db_error)?;

    let envelope = Envelope::new(SERVICE, topics::JOB_CREATED, job_id, created);
    if let Err(e) = api.publisher.job_created(&envelope).await {
        let detail = format!("작업 요청을 보내지 못함: {e:#}");
        api.store.set_status(job_id, JobStatus::Failed, Some(&detail)).await.map_err(db_error)?;
        return Err(service_unavailable(detail));
    }
    info!(%job_id, user = %auth.0.sub, bytes = bytes.len(), "MSDS 작업 생성");
    let row = api.store.get(job_id).await.map_err(db_error)?.ok_or_else(|| not_found("방금 만든 작업이 없음"))?;
    Ok((StatusCode::ACCEPTED, Json(row)))
}

#[derive(Deserialize)]
struct ListQuery {
    limit: Option<i64>,
}

/// GET /api/msds/jobs?limit=50 — 최근 작업부터.
async fn list_jobs(auth: AuthUser, State(state): State<AppState>, Query(q): Query<ListQuery>) -> Result<Json<Vec<JobRow>>, ApiError> {
    require_permission(&state, &auth.0, PERMISSION).await?;
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    Ok(Json(api(&state)?.store.list(limit).await.map_err(db_error)?))
}

#[derive(Serialize)]
struct JobDetail {
    #[serde(flatten)]
    job: JobRow,
    usage: UsageSummary,
}

/// GET /api/msds/jobs/:job_id — 상태와 Claude 사용량 합계.
async fn get_job(auth: AuthUser, State(state): State<AppState>, Path(job_id): Path<Uuid>) -> Result<Json<JobDetail>, ApiError> {
    require_permission(&state, &auth.0, PERMISSION).await?;
    let api = api(&state)?;
    let job = api.store.get(job_id).await.map_err(db_error)?.ok_or_else(|| not_found(format!("작업 {job_id}가 없음")))?;
    let usage = api.store.usage_summary(job_id).await.map_err(db_error)?;
    Ok(Json(JobDetail { job, usage }))
}

#[derive(Serialize)]
struct ResultResponse {
    version: i32,
    result: serde_json::Value,
}

/// GET /api/msds/jobs/:job_id/result — 가장 최근 버전의 결과(`msds-result/1`). 아직 없으면 404.
async fn get_result(auth: AuthUser, State(state): State<AppState>, Path(job_id): Path<Uuid>) -> Result<Json<ResultResponse>, ApiError> {
    require_permission(&state, &auth.0, PERMISSION).await?;
    let (version, result) = api(&state)?
        .store
        .latest_result(job_id)
        .await
        .map_err(db_error)?
        .ok_or_else(|| not_found(format!("작업 {job_id}의 결과가 아직 없음")))?;
    Ok(Json(ResultResponse { version, result }))
}
