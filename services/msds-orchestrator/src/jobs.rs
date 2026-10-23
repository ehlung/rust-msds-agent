//! 오케스트레이터 액터. 모든 작업의 상태를 들고, 받은 메시지를 하나씩 처리해 다음 단계로 넘긴다.
//!
//! `received → ingesting → routing → processing → merging → pending_review` (실패하면 `failed`)
//!
//! 오래 걸리는 Claude 호출(경로 판단)은 액터 안에서 기다리지 않는다. 별도 태스크로 돌리고,
//! 끝나면 그 태스크가 액터에게 `RoutingDone` 메시지를 보낸다. 그동안 액터는 다른 작업의 메시지를 처리한다.
//!
//! 상태가 바뀔 때마다 PostgreSQL(`msds_jobs.status`·`state`)에 저장한다. 재시작하면 `Recover` 메시지로
//! 끝나지 않은 작업을 불러와 멈춘 단계부터 이어서 처리한다.

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
use pekko_agent_msds_io::{Bus, JobStore, MsdsLlm};
use serde::{Deserialize, Serialize};
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
    store: JobStore,
    llm: Arc<MsdsLlm>,
    cfg: Config,
    jobs: HashMap<Uuid, Job>,
}

struct Job {
    created: JobCreated,
    status: JobStatus,
    state: JobState,
}

/// DB(`msds_jobs.state`)에 저장하는 작업 내부 상태. 재시작하면 이것으로 이어서 처리한다.
#[derive(Default, Serialize, Deserialize)]
struct JobState {
    profile: Option<DocumentProfile>,
    routing_attempts: u32,
    /// 경로 판단 기록(회차별). 판단을 에이전트가 했다는 근거로 남긴다.
    routing: Vec<RoutingRecord>,
    skipped: Vec<SkippedTask>,
    tasks: Vec<TaskState>,
}

#[derive(Serialize, Deserialize)]
struct RoutingRecord {
    attempt: u32,
    prompt_version: String,
    model: ModelSpec,
    plan: RoutingPlan,
    /// 안전장치 검사에서 나온 문제. 비어 있으면 통과.
    problems: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct TaskState {
    task: AgentTask,
    result: Option<TaskResult>,
    done: bool,
}

pub enum OrchMsg {
    /// 시작할 때 한 번: 끝나지 않은 작업을 DB에서 불러와 이어서 처리한다. 답은 불러온 작업 수.
    Recover(ReplyTo<usize>),
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
            OrchMsg::Recover(reply) => {
                let n = self.recover(ctx).await;
                reply.reply(n);
            }
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
    pub fn new(bus: Bus, store: JobStore, llm: Arc<MsdsLlm>, cfg: Config) -> Self {
        Self { bus, store, llm, cfg, jobs: HashMap::new() }
    }

    async fn recover(&mut self, ctx: &ActorContext<Self>) -> usize {
        let unfinished = match self.store.unfinished().await {
            Ok(u) => u,
            Err(e) => {
                error!(error = %e, "끝나지 않은 작업을 불러오지 못함");
                return 0;
            }
        };
        let n = unfinished.len();
        for u in unfinished {
            let job_id = u.created.job_id;
            let state: JobState = u.state.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
            info!(job_id = %job_id, status = ?u.status, "이어서 처리");
            self.jobs.insert(job_id, Job { created: u.created, status: u.status, state });
            match u.status {
                JobStatus::Received | JobStatus::Ingesting => self.request_ingest(job_id).await,
                JobStatus::Routing => self.start_routing(job_id, None, ctx).await,
                JobStatus::Processing | JobStatus::Merging => {
                    // 끝나지 않은 작업은 제한 시간을 처음부터 다시 잰다. 결과가 Kafka에 남아 있으면 곧 도착한다.
                    let pending: Vec<AgentTask> =
                        self.jobs[&job_id].state.tasks.iter().filter(|t| !t.done).map(|t| t.task.clone()).collect();
                    for t in &pending {
                        self.arm_timeout(t, ctx);
                    }
                    if let Some(job) = self.jobs.get_mut(&job_id) {
                        job.status = JobStatus::Processing;
                    }
                    self.finish_if_complete(job_id).await;
                }
                _ => {}
            }
        }
        n
    }

    async fn on_job_created(&mut self, m: JobCreated) {
        let job_id = m.job_id;
        if self.jobs.contains_key(&job_id) {
            return;
        }
        // 업로드 API가 이미 만들었으면 그대로 둔다(개발용 submit은 여기서 처음 만든다)
        if let Err(e) = self.store.create(&m, None).await {
            error!(job_id = %job_id, error = %e, "작업을 DB에 만들지 못함");
        }
        self.jobs.insert(job_id, Job { created: m, status: JobStatus::Received, state: JobState::default() });
        self.request_ingest(job_id).await;
    }

    async fn request_ingest(&mut self, job_id: Uuid) {
        let Some(pdf_key) = self.jobs.get(&job_id).map(|j| j.created.pdf_key.clone()) else { return };
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
                job.state.profile = Some(profile);
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
        let Some(profile) = job.state.profile.clone() else {
            // 판정 결과 없이 경로 판단 단계에 있을 수는 없다 → 판정부터 다시
            return self.request_ingest(job_id).await;
        };
        job.state.routing_attempts += 1;
        let attempt = job.state.routing_attempts;
        self.set_status(job_id, JobStatus::Routing, Some(format!("경로 판단 {attempt}회차"))).await;
        self.persist(job_id).await;
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
        let Some(job) = self.jobs.get(&job_id) else { return };
        if job.status != JobStatus::Routing {
            return;
        }
        let attempt = job.state.routing_attempts;
        let profile = job.state.profile.as_ref().unwrap();
        let problems = routing::validate(&plan, profile, profile.products.len() * 3);
        info!(job_id = %job_id, verdict = ?plan.verdict, tasks = plan.plan.len(), summary = %plan.summary, problems = problems.len(), "경로 판단");
        self.record_usage(job_id, &format!("routing#{attempt}"), cached, &usage).await;
        let job = self.jobs.get_mut(&job_id).unwrap();
        job.state.routing.push(RoutingRecord {
            attempt,
            prompt_version: routing::PROMPT_VERSION.into(),
            model: self.cfg.model.clone(),
            plan: plan.clone(),
            problems: problems.clone(),
        });
        self.persist(job_id).await;

        if plan.verdict != Verdict::Msds {
            return self.fail(job_id, format!("처리하지 않는 문서 ({:?}): {}", plan.verdict, plan.summary)).await;
        }
        if !problems.is_empty() {
            warn!(job_id = %job_id, attempt, ?problems, "경로 판단이 안전장치에 걸림");
            if attempt < MAX_ROUTING_ATTEMPTS {
                return self.start_routing(job_id, Some((plan, problems)), ctx).await;
            }
            return self.fail(job_id, format!("경로 판단이 {attempt}번 모두 검사에 걸림: {}", problems.join("; "))).await;
        }
        self.dispatch(job_id, plan, ctx).await;
    }

    async fn dispatch(&mut self, job_id: Uuid, plan: RoutingPlan, ctx: &ActorContext<Self>) {
        let mut to_send = Vec::new();
        {
            let job = self.jobs.get_mut(&job_id).unwrap();
            let profile = job.state.profile.as_ref().unwrap();
            let mut skipped = plan.skipped.clone();
            let mut tasks = Vec::new();
            for planned in &plan.plan {
                if !self.cfg.enabled.contains(&planned.agent) {
                    skipped.push(SkippedTask {
                        agent: planned.agent,
                        product_index: planned.product_index,
                        reason: "에이전트 서비스가 아직 없음".into(),
                    });
                    continue;
                }
                let task = build_task(job_id, planned, profile, &self.cfg.model);
                tasks.push(TaskState { task: task.clone(), result: None, done: false });
                to_send.push(task);
            }
            job.state.skipped = skipped;
            job.state.tasks = tasks;
        }
        self.set_status(job_id, JobStatus::Processing, Some(format!("작업 {}개", to_send.len()))).await;
        // 보내기 전에 저장한다 → 보낸 뒤 꺼져도 재시작하면 어떤 작업을 기다리는지 안다
        self.persist(job_id).await;
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
        self.arm_timeout(task, ctx);
    }

    /// 제한 시간이 지나면 액터에게 `TaskTimeout`을 보낸다. 그때 결과가 와 있으면 무시된다.
    fn arm_timeout(&self, task: &AgentTask, ctx: &ActorContext<Self>) {
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
            let Some(state) = job.state.tasks.iter_mut().find(|s| s.task.task_id == r.task_id) else { return };
            if state.done {
                // 같은 결과가 두 번 왔거나, 다시 보낸 뒤 늦게 온 결과다
                return;
            }
            if matches!(r.outcome, TaskOutcome::Failed { .. }) && state.task.attempt < max {
                state.task.attempt += 1;
                Some(state.task.clone())
            } else {
                state.result = Some(r.clone());
                state.done = true;
                None
            }
        };
        let stage = format!("{:?}#{}", r.agent, r.attempt).to_lowercase();
        for u in &r.usage {
            self.record_usage(r.job_id, &stage, u.input_tokens == 0 && u.output_tokens == 0, u).await;
        }
        self.persist(r.job_id).await;
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
            let Some(state) = job.state.tasks.iter_mut().find(|s| s.task.task_id == task_id) else { return };
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
        self.persist(job_id).await;
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
        let (result, composition_ok) = {
            let Some(job) = self.jobs.get(&job_id) else { return };
            if job.status != JobStatus::Processing || job.state.tasks.iter().any(|t| !t.done) {
                return;
            }
            let warnings: Vec<Warning> = job
                .state
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
                job.state.tasks.iter().map(|t| merge::Finished { task: &t.task, result: t.result.as_ref() }).collect();
            let result = merge::merge(&job.created, job.state.profile.as_ref().unwrap(), &finished, warnings);
            let composition_ok = job.state.tasks.iter().filter(|t| t.task.agent == AgentKind::Composition).all(|t| {
                matches!(t.result.as_ref().map(|r| &r.outcome), Some(TaskOutcome::Ok { .. }))
            });
            (result, composition_ok)
        };
        self.set_status(job_id, JobStatus::Merging, None).await;
        let saved = match serde_json::to_value(&result) {
            Ok(v) => self.store.save_result(job_id, 1, &v, "agent").await,
            Err(e) => Err(e.into()),
        };
        if let Err(e) = saved {
            return self.fail(job_id, format!("결과를 저장하지 못함: {e}")).await;
        }
        let usage = self.store.usage_summary(job_id).await.unwrap_or_default();
        info!(
            job_id = %job_id,
            calls = usage.calls,
            input_tokens = usage.input_tokens,
            output_tokens = usage.output_tokens,
            warnings = result.warnings.len(),
            "합치기 완료"
        );
        if composition_ok {
            self.set_status(job_id, JobStatus::PendingReview, None).await;
        } else {
            self.fail(job_id, "성분 작업이 실패해 결과가 비어 있음 (결과의 warnings 참고)".into()).await;
        }
    }

    async fn set_status(&mut self, job_id: Uuid, status: JobStatus, detail: Option<String>) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.status = status;
        }
        info!(job_id = %job_id, ?status, detail = detail.as_deref().unwrap_or(""), "상태");
        if let Err(e) = self.store.set_status(job_id, status, detail.as_deref()).await {
            error!(job_id = %job_id, error = %e, "상태를 DB에 저장하지 못함");
        }
        let env = Envelope::new(SERVICE, topics::JOB_STATUS, job_id, JobStatusChanged { job_id, status, detail });
        if let Err(e) = self.bus.publish(topics::JOB_STATUS, &env).await {
            warn!(job_id = %job_id, error = %e, "상태를 보내지 못함");
        }
    }

    async fn fail(&mut self, job_id: Uuid, detail: String) {
        error!(job_id = %job_id, detail, "작업 실패");
        self.set_status(job_id, JobStatus::Failed, Some(detail)).await;
        self.persist(job_id).await;
    }

    /// 작업 내부 상태를 DB에 저장한다.
    async fn persist(&self, job_id: Uuid) {
        let Some(job) = self.jobs.get(&job_id) else { return };
        let saved = match serde_json::to_value(&job.state) {
            Ok(v) => self.store.save_state(job_id, &v).await,
            Err(e) => Err(e.into()),
        };
        if let Err(e) = saved {
            error!(job_id = %job_id, error = %e, "작업 상태를 DB에 저장하지 못함");
        }
    }

    async fn record_usage(&self, job_id: Uuid, stage: &str, cached: bool, usage: &LlmUsage) {
        if let Err(e) = self.store.record_usage(job_id, stage, cached, usage).await {
            error!(job_id = %job_id, error = %e, "사용량을 DB에 기록하지 못함");
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

#[cfg(test)]
mod tests {
    use super::*;
    use pekko_agent_msds::message::{Effort, TaskOutcome};
    use pekko_agent_msds::profile::{Language, TextLayer};

    #[test]
    fn job_state_survives_db_roundtrip() {
        let model = ModelSpec { name: "claude-sonnet-5-5".into(), effort: Effort::High };
        let profile = DocumentProfile {
            pages: 1,
            language: Language::En,
            text_layer: TextLayer::Ok,
            products: vec![],
            figures: vec![],
            page_images: vec![],
        };
        let task = AgentTask {
            task_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            agent: AgentKind::Composition,
            attempt: 2,
            model: model.clone(),
            prompt_version: "composition/1".into(),
            product_index: 0,
            language: Language::En,
            sections: vec![],
            page_images: vec![],
            note: String::new(),
            limits: TaskLimits { max_tokens: 1, max_tool_rounds: 0 },
        };
        let result = TaskResult {
            task_id: task.task_id,
            job_id: task.job_id,
            agent: AgentKind::Composition,
            attempt: 2,
            outcome: TaskOutcome::Ok { output: serde_json::json!({}) },
            warnings: vec![],
            usage: vec![],
            duration_ms: 1,
        };
        let state = JobState {
            profile: Some(profile),
            routing_attempts: 1,
            routing: vec![RoutingRecord {
                attempt: 1,
                prompt_version: "routing/1".into(),
                model,
                plan: RoutingPlan { verdict: Verdict::Msds, plan: vec![], skipped: vec![], summary: "s".into() },
                problems: vec![],
            }],
            skipped: vec![],
            tasks: vec![TaskState { task, result: Some(result), done: true }],
        };
        let back: JobState = serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
        assert_eq!(back.routing_attempts, 1);
        assert_eq!(back.tasks[0].task.attempt, 2);
        assert!(back.tasks[0].done);
        assert_eq!(back.routing[0].plan.summary, "s");
    }
}
