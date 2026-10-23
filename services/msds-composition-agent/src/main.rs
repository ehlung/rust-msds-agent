//! msds-composition-agent — 성분 에이전트. 제품 정보, 유해성 분류, 성분·CAS, 위험물질을 뽑는다.
//!
//! - `msds.task.composition`을 받아 Claude를 부르고 `msds.task.result`로 결과를 보낸다.
//! - 에이전트 하나 = 액터 하나: `CompositionAgent` 액터가 작업을 하나씩 처리하고,
//!   Kafka 루프는 메시지를 액터에 넘기고 답을 받아 보내는 일만 한다.

mod extract;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use pekko_actor::{Actor, ActorContext, ActorSystem, ReplyTo};
use pekko_agent_msds::message::{AgentKind, AgentTask, ErrorInfo, LlmUsage, TaskOutcome, TaskResult};
use pekko_agent_msds::result::Warning;
use pekko_agent_msds::{topics, Envelope};
use pekko_agent_msds_io::bus::{commit, decode};
use pekko_agent_msds_io::{Bus, LocalStorage, MsdsLlm};
use tracing::{error, info, warn};

const SERVICE: &str = "msds-composition-agent";
/// Claude 호출(최대 10분)과 재시도를 넉넉히 덮는 시간.
const TASK_TIMEOUT: Duration = Duration::from_secs(30 * 60);

struct CompositionAgent {
    llm: Arc<MsdsLlm>,
    storage: LocalStorage,
}

enum AgentMsg {
    Run { task: AgentTask, reply: ReplyTo<TaskResult> },
}

#[async_trait]
impl Actor for CompositionAgent {
    type Message = AgentMsg;

    async fn receive(&mut self, msg: AgentMsg, _ctx: &mut ActorContext<Self>) {
        match msg {
            AgentMsg::Run { task, reply } => {
                let result = self.run(task).await;
                reply.reply(result);
            }
        }
    }
}

impl CompositionAgent {
    async fn run(&self, task: AgentTask) -> TaskResult {
        let started = Instant::now();
        if task.prompt_version != extract::PROMPT_VERSION {
            warn!(expected = extract::PROMPT_VERSION, got = %task.prompt_version, "오케스트레이터가 적은 프롬프트 버전과 다름");
        }
        let (outcome, warnings, usage) = match self.extract(&task).await {
            Ok((output, warnings, usage)) => (TaskOutcome::Ok { output }, warnings, usage),
            Err((code, e, usage)) => {
                error!(task_id = %task.task_id, code, error = %format!("{e:#}"), "성분 추출 실패");
                (TaskOutcome::Failed { error: ErrorInfo { code: code.into(), message: format!("{e:#}") } }, Vec::new(), usage)
            }
        };
        TaskResult {
            task_id: task.task_id,
            job_id: task.job_id,
            agent: AgentKind::Composition,
            attempt: task.attempt,
            outcome,
            warnings,
            usage,
            duration_ms: started.elapsed().as_millis() as u64,
        }
    }

    /// 성공: (결과, 경고, 사용량) / 실패: (오류 코드, 오류, 실패 전까지 쓴 사용량)
    async fn extract(&self, task: &AgentTask) -> Result<(serde_json::Value, Vec<Warning>, Vec<LlmUsage>), (&'static str, anyhow::Error, Vec<LlmUsage>)> {
        let mut images = Vec::new();
        for img in &task.page_images {
            let png = self.storage.get(&img.key).await.map_err(|e| ("storage_read_failed", e, Vec::new()))?;
            images.push((img.page, png));
        }
        let request = extract::build_request(task, &images);
        let call = self.llm.call_json("composition", &task.model, request).await.map_err(|e| ("llm_failed", e, Vec::new()))?;
        let usage = vec![call.usage];
        let (output, warnings) = extract::convert(call.value, &format!("products[{}]", task.product_index))
            .map_err(|e| ("output_invalid", e, usage.clone()))?;
        info!(
            task_id = %task.task_id,
            components = output.components.len(),
            hazardous = output.hazardous_substances.len(),
            warnings = warnings.len(),
            cached = call.cached,
            "성분 추출 완료"
        );
        let output = serde_json::to_value(output).map_err(|e| ("internal", e.into(), usage.clone()))?;
        Ok((output, warnings, usage))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 개발용: 구조화 출력 스키마를 출력한다 (스키마 크기 확인용)
    if std::env::args().nth(1).as_deref() == Some("schema") {
        println!("{}", serde_json::to_string_pretty(&extract::schema())?);
        return Ok(());
    }
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?)).init();

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let storage = LocalStorage::new(std::env::var("MSDS_STORAGE_DIR").context("MSDS_STORAGE_DIR가 필요함")?)?;
    let llm = Arc::new(MsdsLlm::from_env(storage.clone())?);

    let bus = Bus::new(&brokers, SERVICE)?;
    bus.ensure_topics(topics::ALL, 3).await?;
    let consumer = bus.consumer(SERVICE, &[topics::TASK_COMPOSITION])?;

    let system = ActorSystem::new("msds-composition");
    let agent = system
        .spawn(CompositionAgent { llm: llm.clone(), storage: storage.clone() }, "composition-agent")
        .await
        .map_err(|e| anyhow::anyhow!("액터 시작 실패: {e}"))?;
    info!(brokers, mode = ?llm.mode(), "msds-composition-agent 시작");

    // Kafka는 같은 메시지를 두 번 줄 수 있다 → (task_id, attempt)로 거른다
    let mut done: HashSet<(uuid::Uuid, u32)> = HashSet::new();
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
        match decode::<AgentTask>(&msg) {
            Ok(env) if done.contains(&(env.payload.task_id, env.payload.attempt)) => {
                info!(task_id = %env.payload.task_id, attempt = env.payload.attempt, "이미 처리한 작업이라 건너뜀");
            }
            Ok(env) => {
                let task = env.payload;
                let key = (task.task_id, task.attempt);
                let job_id = task.job_id;
                info!(task_id = %task.task_id, job_id = %job_id, attempt = task.attempt, sections = task.sections.len(), "작업 받음");
                let result = match agent.ask_with_timeout(|reply| AgentMsg::Run { task, reply }, TASK_TIMEOUT).await {
                    Ok(r) => r,
                    Err(e) => {
                        // 답을 못 받았으면 커밋하지 않는다 → 재시작하면 다시 받는다
                        error!(job_id = %job_id, error = %e, "에이전트 응답 없음");
                        continue;
                    }
                };
                let out = Envelope::new(SERVICE, topics::TASK_RESULT, job_id, result);
                if let Err(e) = bus.publish(topics::TASK_RESULT, &out).await {
                    error!(job_id = %job_id, error = %e, "결과 보내기 실패");
                    continue;
                }
                done.insert(key);
            }
            Err(e) => error!(error = %e, "작업을 읽지 못해 건너뜀"),
        }
        commit(&consumer, &msg)?;
    }
}
