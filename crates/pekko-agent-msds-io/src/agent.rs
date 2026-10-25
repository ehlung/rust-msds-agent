//! 에이전트 서비스의 공통 실행 틀. 성분·번역·취급 에이전트가 같이 쓴다.
//!
//! - 에이전트 하나 = 액터 하나: [`AgentActor`]가 작업을 하나씩 받아 [`TaskHandler::handle`]을 부른다.
//! - [`serve`]의 Kafka 루프는 작업 메시지를 액터에 넘기고, 답을 `msds.task.result`로 보낸 뒤 커밋한다.
//!
//! 에이전트마다 다른 것은 `TaskHandler` 구현(프롬프트, Claude 호출, 결과 변환)뿐이다.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pekko_actor::{Actor, ActorContext, ActorSystem, ReplyTo};
use pekko_agent_msds::message::{AgentKind, AgentTask, ErrorInfo, LlmUsage, TaskOutcome, TaskResult};
use pekko_agent_msds::result::Warning;
use pekko_agent_msds::{topics, Envelope};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::bus::{commit, decode};
use crate::Bus;

/// Claude 호출(최대 10분)과 재시도, 여러 번 나눠 부르는 번역까지 넉넉히 덮는 시간.
const TASK_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// 에이전트 하나가 맡은 일.
#[async_trait]
pub trait TaskHandler: Send + Sync + 'static {
    fn kind(&self) -> AgentKind;
    /// 이 에이전트의 프롬프트 버전. 오케스트레이터가 작업에 적은 값과 다르면 경고를 남긴다.
    fn prompt_version(&self) -> &'static str;
    async fn handle(&self, task: &AgentTask) -> Result<Done, Failed>;
}

/// 성공한 작업의 결과.
pub struct Done {
    pub output: serde_json::Value,
    pub warnings: Vec<Warning>,
    pub usage: Vec<LlmUsage>,
}

/// 실패한 작업. 실패 전까지 쓴 사용량도 같이 돌려준다(비용 기록).
pub struct Failed {
    pub code: &'static str,
    pub error: anyhow::Error,
    pub usage: Vec<LlmUsage>,
}

impl Failed {
    pub fn new(code: &'static str, error: impl Into<anyhow::Error>, usage: Vec<LlmUsage>) -> Self {
        Self { code, error: error.into(), usage }
    }
}

pub struct AgentActor<H: TaskHandler> {
    handler: H,
}

pub enum AgentMsg {
    Run { task: AgentTask, reply: ReplyTo<TaskResult> },
}

#[async_trait]
impl<H: TaskHandler> Actor for AgentActor<H> {
    type Message = AgentMsg;

    async fn receive(&mut self, msg: AgentMsg, _ctx: &mut ActorContext<Self>) {
        match msg {
            AgentMsg::Run { task, reply } => {
                let result = run(&self.handler, task).await;
                reply.reply(result);
            }
        }
    }
}

async fn run<H: TaskHandler>(handler: &H, task: AgentTask) -> TaskResult {
    let started = Instant::now();
    if task.prompt_version != handler.prompt_version() {
        warn!(expected = handler.prompt_version(), got = %task.prompt_version, "오케스트레이터가 적은 프롬프트 버전과 다름");
    }
    let (outcome, warnings, usage) = match handler.handle(&task).await {
        Ok(d) => (TaskOutcome::Ok { output: d.output }, d.warnings, d.usage),
        Err(f) => {
            error!(task_id = %task.task_id, code = f.code, error = %format!("{:#}", f.error), "작업 실패");
            (TaskOutcome::Failed { error: ErrorInfo { code: f.code.into(), message: format!("{:#}", f.error) } }, Vec::new(), f.usage)
        }
    };
    TaskResult {
        task_id: task.task_id,
        job_id: task.job_id,
        agent: handler.kind(),
        attempt: task.attempt,
        outcome,
        warnings,
        usage,
        duration_ms: started.elapsed().as_millis() as u64,
    }
}

/// 에이전트 서비스를 실행한다. 자기 작업 토픽만 구독한다(설계 4-1).
pub async fn serve<H: TaskHandler>(service: &str, handler: H) -> anyhow::Result<()> {
    let topic = handler.kind().task_topic();
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let bus = Bus::new(&brokers, service)?;
    bus.ensure_topics(topics::ALL, 3).await?;
    // 같은 서비스를 여러 개 띄우면 같은 소비자 그룹이라 작업을 나눠 받는다
    let consumer = bus.consumer(service, &[topic])?;

    let system = ActorSystem::new(service);
    let agent = system
        .spawn(AgentActor { handler }, "agent")
        .await
        .map_err(|e| anyhow::anyhow!("액터 시작 실패: {e}"))?;
    info!(brokers, topic, "{service} 시작");

    // Kafka는 같은 메시지를 두 번 줄 수 있다 → (task_id, attempt)로 거른다
    let mut done: HashSet<(Uuid, u32)> = HashSet::new();
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
                let out = Envelope::new(service, topics::TASK_RESULT, job_id, result);
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
