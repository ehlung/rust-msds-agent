//! msds-orchestrator — 작업 하나를 처음부터 끝까지 이끄는 서비스.
//!
//! - 서비스: `msds.job.created`·`msds.ingest.done`·`msds.task.result`를 받아 오케스트레이터 액터에 넘긴다.
//! - 개발용: `msds-orchestrator submit <pdf> [--wait]`은 업로드 API 대신 PDF를 저장소에 넣고 `msds.job.created`를 보낸다.

mod jobs;
mod merge;
mod routing;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use pekko_actor::ActorSystem;
use pekko_agent_msds::message::{AgentKind, Effort, IngestDone, JobCreated, JobStatus, JobStatusChanged, ModelSpec, TaskResult};
use pekko_agent_msds::{topics, Envelope};
use pekko_agent_msds_io::bus::{commit, decode};
use pekko_agent_msds_io::storage::job_pdf_key;
use pekko_agent_msds_io::{Bus, JobStore, LocalStorage, MsdsLlm};
use rdkafka::Message;
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::jobs::{Config, OrchMsg, Orchestrator};

pub const SERVICE: &str = "msds-orchestrator";
/// 모든 에이전트의 첫 호출 모델 (시작 전 정리 3-6 원칙 3).
const MODEL: &str = "claude-sonnet-5-5";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?)).init();

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let storage = LocalStorage::new(std::env::var("MSDS_STORAGE_DIR").context("MSDS_STORAGE_DIR가 필요함")?)?;
    let bus = Bus::new(&brokers, SERVICE)?;
    bus.ensure_topics(topics::ALL, 3).await?;

    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("submit") => submit(&bus, &storage, &args[2..]).await,
        Some(other) => bail!("알 수 없는 명령: {other} (사용: msds-orchestrator [submit <pdf> [--wait]])"),
        None => serve(bus, storage).await,
    }
}

fn config_from_env() -> anyhow::Result<Config> {
    // 작업을 보낼 에이전트. 기본은 서비스가 있는 성분 에이전트만.
    let enabled = std::env::var("MSDS_AGENTS").unwrap_or_else(|_| "composition".into());
    let enabled: HashSet<AgentKind> = enabled
        .split(',')
        .map(|s| serde_json::from_value(serde_json::Value::String(s.trim().into())).with_context(|| format!("MSDS_AGENTS 값 오류: {s}")))
        .collect::<anyhow::Result<_>>()?;
    let task_timeout = std::env::var("MSDS_TASK_TIMEOUT_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(20 * 60);
    Ok(Config {
        model: ModelSpec { name: MODEL.into(), effort: Effort::High },
        enabled,
        task_timeout: Duration::from_secs(task_timeout),
        max_task_attempts: 2,
    })
}

async fn serve(bus: Bus, storage: LocalStorage) -> anyhow::Result<()> {
    let llm = Arc::new(MsdsLlm::from_env(storage)?);
    let cfg = config_from_env()?;
    let store = JobStore::connect(&std::env::var("DATABASE_URL").context("DATABASE_URL이 필요함")?).await?;
    store.migrate().await?;
    info!(mode = ?llm.mode(), agents = ?cfg.enabled, "msds-orchestrator 시작");

    let consumer = bus.consumer(SERVICE, &[topics::JOB_CREATED, topics::INGEST_DONE, topics::TASK_RESULT])?;
    let system = ActorSystem::new("msds-orchestrator");
    let orch = system
        .spawn(Orchestrator::new(bus.clone(), store, llm, cfg), "orchestrator")
        .await
        .map_err(|e| anyhow::anyhow!("액터 시작 실패: {e}"))?;
    // 메시지를 받기 전에, 지난번에 끝나지 않은 작업부터 이어서 처리한다
    let recovered = orch.ask_with_timeout(OrchMsg::Recover, Duration::from_secs(120)).await.map_err(|e| anyhow::anyhow!("복구 실패: {e}"))?;
    info!(recovered, "끝나지 않은 작업 불러옴");

    loop {
        let msg = tokio::select! {
            m = consumer.recv() => m,
            _ = tokio::signal::ctrl_c() => { info!("종료"); return Ok(()); }
        };
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "받기 실패");
                continue;
            }
        };
        // 액터가 메시지를 처리했다고 답한 뒤에 커밋한다
        let timeout = Duration::from_secs(60);
        let handled = match msg.topic() {
            t if t == topics::JOB_CREATED => match decode::<JobCreated>(&msg) {
                Ok(env) => Some(orch.ask_with_timeout(|r| OrchMsg::JobCreated(env.payload, r), timeout).await),
                Err(e) => {
                    error!(error = %e, "건너뜀");
                    None
                }
            },
            t if t == topics::INGEST_DONE => match decode::<IngestDone>(&msg) {
                Ok(env) => Some(orch.ask_with_timeout(|r| OrchMsg::IngestDone(env.payload, r), timeout).await),
                Err(e) => {
                    error!(error = %e, "건너뜀");
                    None
                }
            },
            t if t == topics::TASK_RESULT => match decode::<TaskResult>(&msg) {
                Ok(env) => Some(orch.ask_with_timeout(|r| OrchMsg::TaskResult(env.payload, r), timeout).await),
                Err(e) => {
                    error!(error = %e, "건너뜀");
                    None
                }
            },
            other => {
                warn!(topic = other, "구독하지 않은 토픽");
                None
            }
        };
        if let Some(Err(e)) = handled {
            error!(error = %e, "오케스트레이터가 답하지 않음 — 커밋하지 않음");
            continue;
        }
        commit(&consumer, &msg)?;
    }
}

/// 개발용 업로드: PDF를 저장소에 넣고 작업을 만든다. `--wait`면 끝날 때까지 상태를 출력한다.
async fn submit(bus: &Bus, storage: &LocalStorage, args: &[String]) -> anyhow::Result<()> {
    let path = args.first().context("PDF 경로가 필요함")?;
    let wait = args.iter().any(|a| a == "--wait");
    let bytes = std::fs::read(path).with_context(|| format!("읽기 실패: {path}"))?;
    let job_id = Uuid::new_v4();
    let pdf_key = job_pdf_key(job_id);
    storage.put(&pdf_key, &bytes).await?;
    let file_name = std::path::Path::new(path).file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let sha256: String = Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect();
    let created = JobCreated { job_id, pdf_key, file_name, sha256 };
    bus.publish(topics::JOB_CREATED, &Envelope::new(SERVICE, topics::JOB_CREATED, job_id, created)).await?;
    println!("job_id={job_id}");
    if !wait {
        return Ok(());
    }

    // 이 작업의 상태 메시지만 골라 출력한다
    let consumer = bus.consumer(&format!("msds-submit-{job_id}"), &[topics::JOB_STATUS])?;
    loop {
        let msg = consumer.recv().await?;
        let Ok(env) = decode::<JobStatusChanged>(&msg) else { continue };
        let s = env.payload;
        if s.job_id != job_id {
            continue;
        }
        println!("{:?}{}", s.status, s.detail.map(|d| format!(" — {d}")).unwrap_or_default());
        match s.status {
            JobStatus::PendingReview => {
                println!("결과: DB msds_results (job_id={job_id}), API GET /api/msds/jobs/{job_id}/result");
                return Ok(());
            }
            JobStatus::Failed => bail!("작업 실패"),
            _ => {}
        }
    }
}
