//! 오케스트레이터 액터. 모든 작업의 상태를 들고, 받은 메시지를 하나씩 처리해 다음 단계로 넘긴다.
//!
//! `received → ingesting → routing → processing → merging → pending_review` (실패하면 `failed`)
//!
//! 오래 걸리는 Claude 호출(경로 판단)은 액터 안에서 기다리지 않는다. 별도 태스크로 돌리고,
//! 끝나면 그 태스크가 액터에게 `RoutingDone` 메시지를 보낸다. 그동안 액터는 다른 작업의 메시지를 처리한다.
//!
//! 작업 상태는 아직 메모리에만 있다. PostgreSQL 저장은 다음 단계(업로드 API)에서 붙인다.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use pekko_actor::{Actor, ActorContext, ReplyTo};
use pekko_agent_msds::message::{
    AgentKind, AgentTask, IngestDone, IngestOutcome, IngestRequest, JobCreated, JobStatus, JobStatusChanged, LlmUsage,
    ModelSpec, PlannedTask, RoutingPlan, SkippedTask, TaskLimits, TaskOutcome, TaskResult, Verdict,
};
use pekko_agent_msds::profile::DocumentProfile;
use pekko_agent_msds::result::{Severity, Warning};
use pekko_agent_msds::{topics, Envelope};
use pekko_agent_msds_io::{Bus, LocalStorage, MsdsLlm};
use serde::Serialize;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{merge, routing, SERVICE};

/// 경로 판단은 안전장치 검사에 걸리면 한 번 더 묻는다(설계 4-5).
const MAX_ROUTING_ATTEMPTS: u32 = 2;

pub struct Config {
    pub model: ModelSpec,
    /// 작업을 보낼 에이전트. 서비스가 아직 없는 에이전트의 작업은 `skipped`로 남긴다.
    pub enabled: HashSet<AgentKind>,
    pub task_timeout: Duration,
    pub max_task_attempts: u32,
}

pub struct Orchestrator {
    bus: Bus,
    storage: LocalStorage,
    llm: Arc<MsdsLlm>,
    cfg: Config,
    jobs: HashMap<Uuid, Job>,
}

struct Job {
    created: JobCreated,
    status: JobStatus,
    profile: Option<DocumentProfile>,
    routing_attempts: u32,
    skipped: Vec<SkippedTask>,
    tasks: Vec<TaskState>,
    usage: Vec<UsageRow>,
}

struct TaskState {
    task: AgentTask,
    result: Option<TaskResult>,
    done: bool,
}

/// Claude 호출 한 번의 사용량 기록. 기록된 응답을 쓴 호출(`cached`)은 토큰이 0이다.
#[derive(Serialize)]
struct UsageRow {
    stage: String,
    cached: bool,
    #[serde(flatten)]
    usage: LlmUsage,
}

pub enum OrchMsg {
    JobCreated(JobCreated, ReplyTo<()>),
    IngestDone(IngestDone, ReplyTo<()>),
    TaskResult(TaskResult, ReplyTo<()>),
    RoutingDone { job_id: Uuid, result: Result<(RoutingPlan, LlmUsage, bool), String> },
    TaskTimeout { job_id: Uuid, task_id: Uuid, attempt: u32 },
}

#[async_trait]
impl Actor for Orchestrator {
    type Message = OrchMsg;

    async fn receive(&mut self, msg: OrchMsg, ctx: &mut ActorContext<Self>) {
        match msg {
            OrchMsg::JobCreated(m, reply) => {
                self.on_job_created(m).await;
                reply.reply(());
            }
            OrchMsg::IngestDone(m, reply) => {
                self.on_ingest_done(m, ctx).await;
                reply.reply(());
            }
            OrchMsg::TaskResult(m, reply) => {
                self.on_task_result(m, ctx).await;
                reply.reply(());
            }
            OrchMsg::RoutingDone { job_id, result } => self.on_routing_done(job_id, result, ctx).await,
            OrchMsg::TaskTimeout { job_id, task_id, attempt } => self.on_task_timeout(job_id, task_id, attempt, ctx).await,
        }
    }
}

impl Orchestrator {
    pub fn new(bus: Bus, storage: LocalStorage, llm: Arc<MsdsLlm>, cfg: Config) -> Self {
        Self { bus, storage, llm, cfg, jobs: HashMap::new() }
    }

    async fn on_job_created(&mut self, m: JobCreated) {
        let job_id = m.job_id;
        if self.jobs.contains_key(&job_id) {
            return;
        }
        let pdf_key = m.pdf_key.clone();
        self.jobs.insert(
            job_id,
            Job { created: m, status: JobStatus::Received, profile: None, routing_attempts: 0, skipped: vec![], tasks: vec![], usage: vec![] },
        );
        self.set_status(job_id, JobStatus::Ingesting, None).await;
        let req = Envelope::new(SERVICE, topics::INGEST_REQUEST, job_id, IngestRequest { job_id, pdf_key });
        if let Err(e) = self.bus.publish(topics::INGEST_REQUEST, &req).await {
            self.fail(job_id, format!("판정 요청을 보내지 못함: {e}")).await;
        }
    }

    async fn on_ingest_done(&mut self, m: IngestDone, ctx: &ActorContext<Self>) {
        let job_id = m.job_id;
        let Some(job) = self.jobs.get_mut(&job_id) else { return warn!(job_id = %job_id, "모르는 작업의 판정 결과") };
        if job.status != JobStatus::Ingesting {
            return;
        }
        match m.outcome {
            IngestOutcome::Ok { profile } => {
                job.profile = Some(profile);
                self.start_routing(job_id, None, ctx).await;
            }
            IngestOutcome::Failed { error } => {
                self.fail(job_id, format!("문서 판정 실패 ({}): {}", error.code, error.message)).await;
            }
        }
    }

    /// 경로 판단을 별도 태스크로 시작한다. `previous`는 안전장치에 걸린 직전 계획과 문제다.
    async fn start_routing(&mut self, job_id: Uuid, previous: Option<(RoutingPlan, Vec<String>)>, ctx: &ActorContext<Self>) {
        let Some(job) = self.jobs.get_mut(&job_id) else { return };
        let Some(profile) = job.profile.clone() else { return };
        job.routing_attempts += 1;
        let attempt = job.routing_attempts;
        self.set_status(job_id, JobStatus::Routing, Some(format!("경로 판단 {attempt}회차"))).await;
        let llm = self.llm.clone();
        let model = self.cfg.model.clone();
        let me = ctx.self_ref().clone();
        tokio::spawn(async move {
            let request = routing::build_request(&profile, previous.as_ref().map(|(p, probs)| (p, probs.as_slice())));
            let result = match llm.call_json("routing", &model, request).await {
                Ok(call) => serde_json::from_value::<RoutingPlan>(call.value)
                    .map(|plan| (plan, call.usage, call.cached))
                    .map_err(|e| format!("경로 판단 결과를 읽지 못함: {e}")),
                Err(e) => Err(format!("경로 판단 호출 실패: {e:#}")),
            };
            let _ = me.tell(OrchMsg::RoutingDone { job_id, result });
        });
    }

    async fn on_routing_done(&mut self, job_id: Uuid, result: Result<(RoutingPlan, LlmUsage, bool), String>, ctx: &ActorContext<Self>) {
        let (plan, usage, cached) = match result {
            Ok(r) => r,
            Err(e) => return self.fail(job_id, e).await,
        };
        let (max_tasks, attempt) = {
            let Some(job) = self.jobs.get_mut(&job_id) else { return };
            if job.status != JobStatus::Routing {
                return;
            }
            job.usage.push(UsageRow { stage: format!("routing#{}", job.routing_attempts), cached, usage });
            (job.profile.as_ref().map(|p| p.products.len() * 3).unwrap_or(0), job.routing_attempts)
        };
        info!(job_id = %job_id, verdict = ?plan.verdict, tasks = plan.plan.len(), summary = %plan.summary, "경로 판단");
        let record = serde_json::json!({
            "prompt_version": routing::PROMPT_VERSION,
            "model": self.cfg.model,
            "attempt": attempt,
            "plan": plan,
        });
        self.save(job_id, &format!("routing-{attempt}.json"), &record).await;

        if plan.verdict != Verdict::Msds {
            return self.fail(job_id, format!("처리하지 않는 문서 ({:?}): {}", plan.verdict, plan.summary)).await;
        }
        let problems = {
            let job = &self.jobs[&job_id];
            routing::validate(&plan, job.profile.as_ref().unwrap(), max_tasks)
        };
        if !problems.is_empty() {
            let attempts = self.jobs[&job_id].routing_attempts;
            warn!(job_id = %job_id, attempts, ?problems, "경로 판단이 안전장치에 걸림");
            if attempts < MAX_ROUTING_ATTEMPTS {
                return self.start_routing(job_id, Some((plan, problems)), ctx).await;
            }
            return self.fail(job_id, format!("경로 판단이 {attempts}번 모두 검사에 걸림: {}", problems.join("; "))).await;
        }
        self.dispatch(job_id, plan, ctx).await;
    }

    async fn dispatch(&mut self, job_id: Uuid, plan: RoutingPlan, ctx: &ActorContext<Self>) {
        let mut to_send = Vec::new();
        {
            let job = self.jobs.get_mut(&job_id).unwrap();
            let profile = job.profile.as_ref().unwrap();
            job.skipped = plan.skipped.clone();
            for planned in &plan.plan {
                if !self.cfg.enabled.contains(&planned.agent) {
                    job.skipped.push(SkippedTask {
                        agent: planned.agent,
                        product_index: planned.product_index,
                        reason: "에이전트 서비스가 아직 없음".into(),
                    });
                    continue;
                }
                let task = build_task(job_id, planned, profile, &self.cfg.model);
                job.tasks.push(TaskState { task: task.clone(), result: None, done: false });
                to_send.push(task);
            }
        }
        self.set_status(job_id, JobStatus::Processing, Some(format!("작업 {}개", to_send.len()))).await;
        for task in to_send {
            self.send_task(&task, ctx).await;
        }
        self.finish_if_complete(job_id).await;
    }

    async fn send_task(&self, task: &AgentTask, ctx: &ActorContext<Self>) {
        let env = Envelope::new(SERVICE, task.agent.task_topic(), task.job_id, task.clone());
        if let Err(e) = self.bus.publish(task.agent.task_topic(), &env).await {
            // 보내지 못해도 제한 시간이 지나면 다시 보낸다
            error!(task_id = %task.task_id, error = %e, "작업을 보내지 못함");
        }
        let me = ctx.self_ref().clone();
        let (job_id, task_id, attempt, wait) = (task.job_id, task.task_id, task.attempt, self.cfg.task_timeout);
        tokio::spawn(async move {
            tokio::time::sleep(wait).await;
            let _ = me.tell(OrchMsg::TaskTimeout { job_id, task_id, attempt });
        });
    }

    async fn on_task_result(&mut self, r: TaskResult, ctx: &ActorContext<Self>) {
        let max = self.cfg.max_task_attempts;
        let resend = {
            let Some(job) = self.jobs.get_mut(&r.job_id) else { return };
            let stage = format!("{:?}#{}", r.agent, r.attempt).to_lowercase();
            let Some(state) = job.tasks.iter_mut().find(|s| s.task.task_id == r.task_id) else { return };
            if state.done {
                // 같은 결과가 두 번 왔거나, 다시 보낸 뒤 늦게 온 결과다
                return;
            }
            for u in &r.usage {
                job.usage.push(UsageRow { stage: stage.clone(), cached: u.input_tokens == 0 && u.output_tokens == 0, usage: u.clone() });
            }
            let failed = matches!(r.outcome, TaskOutcome::Failed { .. });
            if failed && state.task.attempt < max {
                state.task.attempt += 1;
                Some(state.task.clone())
            } else {
                state.result = Some(r.clone());
                state.done = true;
                None
            }
        };
        match resend {
            Some(task) => {
                warn!(task_id = %task.task_id, attempt = task.attempt, "에이전트가 실패를 알려 다시 보냄");
                self.send_task(&task, ctx).await;
            }
            None => self.finish_if_complete(r.job_id).await,
        }
    }

    async fn on_task_timeout(&mut self, job_id: Uuid, task_id: Uuid, attempt: u32, ctx: &ActorContext<Self>) {
        let max = self.cfg.max_task_attempts;
        let resend = {
            let Some(job) = self.jobs.get_mut(&job_id) else { return };
            let Some(state) = job.tasks.iter_mut().find(|s| s.task.task_id == task_id) else { return };
            if state.done || state.task.attempt != attempt {
                return;
            }
            if attempt < max {
                state.task.attempt += 1;
                Some(state.task.clone())
            } else {
                state.done = true;
                None
            }
        };
        match resend {
            Some(task) => {
                warn!(task_id = %task_id, attempt = task.attempt, "응답이 없어 다시 보냄");
                self.send_task(&task, ctx).await;
            }
            None => {
                warn!(task_id = %task_id, "재시도 상한까지 응답 없음");
                self.finish_if_complete(job_id).await;
            }
        }
    }

    /// 모든 작업이 끝났으면 합쳐서 검토 대기로 둔다.
    async fn finish_if_complete(&mut self, job_id: Uuid) {
        let (result, usage_total, composition_ok) = {
            let Some(job) = self.jobs.get(&job_id) else { return };
            if job.status != JobStatus::Processing || job.tasks.iter().any(|t| !t.done) {
                return;
            }
            let warnings: Vec<Warning> = job
                .skipped
                .iter()
                .map(|s| Warning {
                    code: "task_skipped".into(),
                    severity: Severity::Info,
                    message: format!("{:?} 작업 생략: {}", s.agent, s.reason),
                    path: Some(format!("products[{}]", s.product_index)),
                })
                .collect();
            let finished: Vec<merge::Finished> =
                job.tasks.iter().map(|t| merge::Finished { task: &t.task, result: t.result.as_ref() }).collect();
            let result = merge::merge(&job.created, job.profile.as_ref().unwrap(), &finished, warnings);
            let composition_ok = job.tasks.iter().filter(|t| t.task.agent == AgentKind::Composition).all(|t| {
                matches!(t.result.as_ref().map(|r| &r.outcome), Some(TaskOutcome::Ok { .. }))
            });
            let total = job.usage.iter().fold((0u64, 0u64), |(i, o), r| (i + r.usage.input_tokens, o + r.usage.output_tokens));
            (result, total, composition_ok)
        };
        self.set_status(job_id, JobStatus::Merging, None).await;
        self.save(job_id, "result.json", &result).await;
        if let Some(job) = self.jobs.get(&job_id) {
            self.save(job_id, "usage.json", &job.usage).await;
        }
        info!(job_id = %job_id, input_tokens = usage_total.0, output_tokens = usage_total.1, warnings = result.warnings.len(), "합치기 완료");
        if composition_ok {
            self.set_status(job_id, JobStatus::PendingReview, None).await;
        } else {
            self.fail(job_id, "성분 작업이 실패해 결과가 비어 있음 (result.json에 경고)".into()).await;
        }
    }

    async fn set_status(&mut self, job_id: Uuid, status: JobStatus, detail: Option<String>) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.status = status;
        }
        info!(job_id = %job_id, ?status, detail = detail.as_deref().unwrap_or(""), "상태");
        let env = Envelope::new(SERVICE, topics::JOB_STATUS, job_id, JobStatusChanged { job_id, status, detail });
        if let Err(e) = self.bus.publish(topics::JOB_STATUS, &env).await {
            warn!(job_id = %job_id, error = %e, "상태를 보내지 못함");
        }
    }

    async fn fail(&mut self, job_id: Uuid, detail: String) {
        error!(job_id = %job_id, detail, "작업 실패");
        self.set_status(job_id, JobStatus::Failed, Some(detail)).await;
    }

    /// 확인용 파일을 `jobs/<job_id>/`에 남긴다. 실패해도 흐름은 멈추지 않는다.
    async fn save<T: Serialize>(&self, job_id: Uuid, name: &str, value: &T) {
        let key = format!("jobs/{job_id}/{name}");
        let saved = match serde_json::to_vec_pretty(value) {
            Ok(bytes) => self.storage.put(&key, &bytes).await,
            Err(e) => Err(e.into()),
        };
        if let Err(e) = saved {
            warn!(key, error = %e, "저장 실패");
        }
    }
}

/// 계획 한 줄 → 작업 메시지. 섹션 본문과 페이지 이미지 키는 프로필에서 골라 넣는다.
pub fn build_task(job_id: Uuid, planned: &PlannedTask, profile: &DocumentProfile, model: &ModelSpec) -> AgentTask {
    let product = &profile.products[planned.product_index as usize];
    let sections = product
        .sections
        .iter()
        .filter(|s| planned.sections.is_empty() || planned.sections.contains(&s.kind))
        .cloned()
        .collect();
    let page_images = profile.page_images.iter().filter(|i| planned.page_images.contains(&i.page)).cloned().collect();
    AgentTask {
        task_id: Uuid::new_v4(),
        job_id,
        agent: planned.agent,
        attempt: 1,
        model: model.clone(),
        prompt_version: prompt_version(planned.agent).into(),
        product_index: planned.product_index,
        language: profile.language,
        sections,
        page_images,
        note: planned.note.clone(),
        limits: TaskLimits { max_tokens: 16000, max_tool_rounds: 0 },
    }
}

/// 에이전트별 프롬프트 버전. 각 에이전트 서비스의 `PROMPT_VERSION`과 같아야 한다(다르면 에이전트가 경고를 남긴다).
pub fn prompt_version(agent: AgentKind) -> &'static str {
    match agent {
        AgentKind::Composition => "composition/1",
        AgentKind::Translate => "translate/1",
        AgentKind::Handling => "handling/1",
    }
}
