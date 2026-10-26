//! MSDS 작업 API: PDF 업로드, 작업 목록·상태·결과 조회, 진행 상태(SSE), 검토(고친 결과 저장·확인완료).
//!
//! gateway는 추론하지 않는다. PDF를 저장하고 작업을 DB에 만든 뒤 `msds.job.created`를 보내면 끝이고,
//! 이후 단계는 msds-orchestrator가 이끈다. 상태와 결과는 DB(`msds_jobs`, `msds_results`)에서 읽는다.
//! 검토자가 고친 결과는 오케스트레이터와 같은 코드 검사(`checks::recheck`)를 거쳐 새 버전으로 저장한다(설계 3-3).

use std::sync::Arc;

use axum::{
    async_trait,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::{header, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures::{stream, Stream, StreamExt};
use pekko_agent_msds::checks;
use pekko_agent_msds::message::{JobCreated, JobStatus, JobStatusChanged};
use pekko_agent_msds::result::{MsdsResult, SCHEMA};
use pekko_agent_msds::{topics, Envelope};
use pekko_agent_msds_io::job_store::{status_str, JobRow, UsageSummary};
use pekko_agent_msds_io::storage::job_pdf_key;
use pekko_agent_msds_io::{Bus, JobStore, LocalStorage};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::sync::broadcast;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{make_error, require_permission, service_unavailable, AppState, AuthUser, ErrorResponse};

const SERVICE: &str = "api-gateway";
/// 테스트 PDF 중 가장 큰 것이 약 1MB다.
const MAX_PDF_BYTES: usize = 20 * 1024 * 1024;
/// 고친 결과 JSON. 10건 중 가장 큰 결과의 몇 배로 잡았다(번역 본문이 대부분이다).
const MAX_RESULT_BYTES: usize = 10 * 1024 * 1024;
/// 업로드·조회 권한. 에이전트에게 일을 맡기는 일이라 기존 권한 `agent.delegate`를 쓴다.
const PERMISSION: &str = "agent.delegate";

type ApiError = (StatusCode, Json<ErrorResponse>);

/// gateway가 보내는 메시지(`msds.job.created`, 확인완료 때 `msds.job.status`). 운영은 Kafka, 테스트는 기록만 하는 구현을 쓴다.
#[async_trait]
pub(crate) trait JobPublisher: Send + Sync {
    async fn job_created(&self, envelope: &Envelope<JobCreated>) -> anyhow::Result<()>;
    async fn status_changed(&self, envelope: &Envelope<JobStatusChanged>) -> anyhow::Result<()>;
}

struct KafkaPublisher(Bus);

#[async_trait]
impl JobPublisher for KafkaPublisher {
    async fn job_created(&self, envelope: &Envelope<JobCreated>) -> anyhow::Result<()> {
        self.0.publish(topics::JOB_CREATED, envelope).await
    }

    async fn status_changed(&self, envelope: &Envelope<JobStatusChanged>) -> anyhow::Result<()> {
        self.0.publish(topics::JOB_STATUS, envelope).await
    }
}

#[derive(Clone)]
pub(crate) struct MsdsApi {
    pub(crate) store: JobStore,
    pub(crate) storage: LocalStorage,
    pub(crate) publisher: Arc<dyn JobPublisher>,
    /// `msds.job.status`를 받아 진행 상태 SSE 구독자에게 나눠 준다.
    pub(crate) statuses: broadcast::Sender<JobStatusChanged>,
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
        let (statuses, _) = broadcast::channel(256);
        if let Err(e) = bus.forward_live(topics::JOB_STATUS, statuses.clone()) {
            warn!(error = %e, "상태 메시지를 받지 못함 — 진행 상태 SSE는 처음 상태만 보낸다");
        }
        Ok(Some(Arc::new(Self { store, storage: LocalStorage::new(dir)?, publisher: Arc::new(KafkaPublisher(bus)), statuses })))
    }
}

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/msds/jobs", post(upload_job).get(list_jobs).layer(DefaultBodyLimit::max(MAX_PDF_BYTES)))
        .route("/api/msds/jobs/:job_id", get(get_job))
        .route("/api/msds/jobs/:job_id/result", get(get_result).put(save_result).layer(DefaultBodyLimit::max(MAX_RESULT_BYTES)))
        .route("/api/msds/jobs/:job_id/approve", post(approve))
        .route("/api/msds/jobs/:job_id/files/*path", get(get_file))
}

/// 오래 열려 있는 연결이라 요청 시간 제한이 없는 쪽에 붙인다.
pub(crate) fn stream_routes() -> Router<AppState> {
    Router::new().route("/api/msds/jobs/:job_id/events", get(job_events))
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

fn conflict(msg: impl Into<String>) -> ApiError {
    make_error(StatusCode::CONFLICT, "CONFLICT", msg)
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

#[derive(Deserialize)]
struct ResultQuery {
    version: Option<i32>,
}

/// GET /api/msds/jobs/:job_id/result?version=1 — 결과(`msds-result/1`). 버전을 빼면 가장 최근 버전. 없으면 404.
/// 처음 버전(에이전트 결과)과 최신 버전을 비교해 무엇을 고쳤는지 볼 수 있다.
async fn get_result(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    Query(q): Query<ResultQuery>,
) -> Result<Json<ResultResponse>, ApiError> {
    require_permission(&state, &auth.0, PERMISSION).await?;
    let store = &api(&state)?.store;
    let found = match q.version {
        Some(v) => store.result_version(job_id, v).await.map_err(db_error)?.map(|r| (v, r)),
        None => store.latest_result(job_id).await.map_err(db_error)?,
    };
    let (version, result) = found.ok_or_else(|| not_found(format!("작업 {job_id}의 결과가 아직 없음")))?;
    Ok(Json(ResultResponse { version, result }))
}

#[derive(Deserialize)]
struct SaveRequest {
    /// 고치기 시작한 버전. 그 사이 다른 사람이 저장했으면 409로 알린다.
    base_version: i32,
    result: MsdsResult,
}

/// PUT /api/msds/jobs/:job_id/result — 검토자가 고친 결과를 다음 버전으로 저장한다.
/// 코드 검사(CAS 체크 디지트, 그림문자 대조)를 다시 돌려 경고를 바꾼 결과를 돌려준다. 고친 값 자체는 막지 않는다.
async fn save_result(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    Json(req): Json<SaveRequest>,
) -> Result<Json<ResultResponse>, ApiError> {
    require_permission(&state, &auth.0, PERMISSION).await?;
    let api = api(&state)?;
    let mut result = req.result;
    if result.schema != SCHEMA || result.job_id != job_id {
        return Err(bad_request(format!("이 작업의 `{SCHEMA}` 결과가 아님")));
    }
    checks::recheck(&mut result).map_err(bad_request)?;
    let value = serde_json::to_value(&result).map_err(|e| crate::internal_error(format!("결과를 JSON으로 바꾸지 못함: {e}")))?;
    if !api.store.save_review(job_id, req.base_version, &value, &auth.0.sub).await.map_err(db_error)? {
        return Err(review_conflict(api, job_id, req.base_version).await);
    }
    let version = req.base_version + 1;
    info!(%job_id, version, user = %auth.0.sub, "MSDS 결과 수정 저장");
    Ok(Json(ResultResponse { version, result: value }))
}

#[derive(Deserialize)]
struct ApproveRequest {
    /// 화면에 보이던 버전. 그 사이 새 버전이 저장됐으면 409로 알린다.
    version: i32,
}

/// POST /api/msds/jobs/:job_id/approve — 확인완료.
/// 경고가 남아 있어도 막지 않는다. 경고는 판단 재료이고 판단은 검토자가 한다(설계 5-5).
async fn approve(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    Json(req): Json<ApproveRequest>,
) -> Result<Json<JobRow>, ApiError> {
    require_permission(&state, &auth.0, PERMISSION).await?;
    let api = api(&state)?;
    if !api.store.approve(job_id, req.version, &auth.0.sub).await.map_err(db_error)? {
        return Err(review_conflict(api, job_id, req.version).await);
    }
    info!(%job_id, version = req.version, user = %auth.0.sub, "MSDS 확인완료");
    // 확인완료는 DB에 이미 남았다. 상태 메시지를 못 보내면 열려 있는 화면에 바로 안 보일 뿐이라 실패로 돌리지 않는다
    let changed = JobStatusChanged { job_id, status: JobStatus::Approved, detail: None };
    if let Err(e) = api.publisher.status_changed(&Envelope::new(SERVICE, topics::JOB_STATUS, job_id, changed)).await {
        warn!(%job_id, error = %e, "확인완료 상태 메시지를 보내지 못함");
    }
    let row = api.store.get(job_id).await.map_err(db_error)?.ok_or_else(|| not_found(format!("작업 {job_id}가 없음")))?;
    Ok(Json(row))
}

/// 저장이나 확인완료가 안 된 이유를 찾아 알려 준다.
async fn review_conflict(api: &MsdsApi, job_id: Uuid, version: i32) -> ApiError {
    let job = match api.store.get(job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => return not_found(format!("작업 {job_id}가 없음")),
        Err(e) => return db_error(e),
    };
    if job.status != JobStatus::PendingReview {
        return conflict(format!("검토 대기 상태가 아님: {}", status_str(job.status)));
    }
    match api.store.latest_result(job_id).await {
        Ok(Some((latest, _))) => conflict(format!("버전 {version} 기준인데 최신 버전은 {latest} — 다시 불러와야 함")),
        Ok(None) => not_found(format!("작업 {job_id}의 결과가 아직 없음")),
        Err(e) => db_error(e),
    }
}

/// GET /api/msds/jobs/:job_id/files/*path — 원문 PDF(`source.pdf`), 페이지 이미지(`pages/3.png`),
/// 잘라 낸 그림(`images/fig-2.png`). 작업 폴더 안에서도 이 세 가지만 내준다.
async fn get_file(auth: AuthUser, State(state): State<AppState>, Path((job_id, path)): Path<(Uuid, String)>) -> Result<Response, ApiError> {
    require_permission(&state, &auth.0, PERMISSION).await?;
    let content_type = file_type(&path).ok_or_else(|| not_found(format!("내줄 수 없는 파일: {path}")))?;
    let bytes = api(&state)?.storage.get(&format!("jobs/{job_id}/{path}")).await.map_err(|_| not_found(format!("파일이 없음: {path}")))?;
    Ok(([(header::CONTENT_TYPE, content_type), (header::CACHE_CONTROL, "private, max-age=3600")], bytes).into_response())
}

fn file_type(path: &str) -> Option<&'static str> {
    let named = |file: &str, ok: fn(char) -> bool| file.strip_suffix(".png").is_some_and(|n| !n.is_empty() && n.chars().all(ok));
    match path.split('/').collect::<Vec<_>>().as_slice() {
        ["source.pdf"] => Some("application/pdf"),
        ["pages", f] if named(f, |c| c.is_ascii_digit()) => Some("image/png"),
        ["images", f] if named(f, |c| c.is_ascii_alphanumeric() || c == '-') => Some("image/png"),
        _ => None,
    }
}

/// GET /api/msds/jobs/:job_id/events — 진행 상태(SSE `status` 이벤트). 처음에 지금 상태를 한 번 보내고, 바뀔 때마다 보낸다.
/// 브라우저의 EventSource는 Authorization 헤더를 붙이지 못해서, 화면은 fetch 스트림으로 받는다.
async fn job_events(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
) -> Result<Sse<impl Stream<Item = Result<Event, axum::Error>>>, ApiError> {
    require_permission(&state, &auth.0, PERMISSION).await?;
    let api = api(&state)?;
    // 먼저 구독하고 지금 상태를 읽는다. 반대 순서면 그 사이에 바뀐 상태를 놓친다
    let rx = api.statuses.subscribe();
    let job = api.store.get(job_id).await.map_err(db_error)?.ok_or_else(|| not_found(format!("작업 {job_id}가 없음")))?;
    let now = JobStatusChanged { job_id, status: job.status, detail: job.detail };
    let updates = stream::unfold(rx, move |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(s) if s.job_id == job_id => return Some((s, rx)),
                // 다른 작업, 또는 밀려서 놓친 것(다음 상태가 오면 화면이 따라잡는다)
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });
    let events = stream::once(async move { now }).chain(updates).map(|s| Event::default().event("status").json_data(s));
    Ok(Sse::new(events).keep_alive(KeepAlive::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serves_only_known_files() {
        assert_eq!(file_type("source.pdf"), Some("application/pdf"));
        assert_eq!(file_type("pages/12.png"), Some("image/png"));
        assert_eq!(file_type("images/fig-3.png"), Some("image/png"));
        for p in ["", "state.json", "pages/a.png", "pages/.png", "pages/1.jpg", "images/../source.pdf", "images/fig_3.png", "x/pages/1.png", "pages/1.png/x"] {
            assert_eq!(file_type(p), None, "{p:?}");
        }
    }
}
