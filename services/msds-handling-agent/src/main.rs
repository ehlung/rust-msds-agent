//! msds-handling-agent — 취급 에이전트. 취급·저장과 개인보호구를 정리하고, 그림 후보 중 GHS 그림문자를 고른다.
//!
//! - 그림 후보를 페이지 이미지에서 잘라내는 일은 코드(`figure`), 무슨 그림인지 고르는 일은 Claude가 한다.
//! - 고른 그림(그림문자·취급 그림)만 저장소 `jobs/<job_id>/images/<후보 번호>.png`에 저장한다.
//! - 문서에 그림문자가 없으면 유해성 분류로 추정하고 `basis: inferred`로 표시한다.

mod extract;
mod figure;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use pekko_agent_msds::message::{AgentKind, AgentTask, TaskFigure};
use pekko_agent_msds::result::{Severity, Warning};
use pekko_agent_msds_io::agent::{self, Done, Failed, TaskHandler};
use pekko_agent_msds_io::storage::figure_image_key;
use pekko_agent_msds_io::{LocalStorage, MsdsLlm};
use tracing::{info, warn};

const SERVICE: &str = "msds-handling-agent";
/// 빈 응답일 때 다시 묻는 최대 횟수.
const MAX_EMPTY_RETRIES: u32 = 2;

struct HandlingAgent {
    llm: Arc<MsdsLlm>,
    storage: LocalStorage,
}

impl HandlingAgent {
    /// 그림 후보를 잘라낸다. 자르지 못한 후보는 빼고 경고를 남긴다(작업 전체를 실패로 만들지 않는다).
    async fn crop_figures<'a>(&self, task: &'a AgentTask, warnings: &mut Vec<Warning>) -> Vec<(&'a TaskFigure, Vec<u8>)> {
        let mut pages: HashMap<&str, Vec<u8>> = HashMap::new();
        let mut crops = Vec::new();
        for f in &task.figures {
            let key = f.page_image.key.as_str();
            if !pages.contains_key(key) {
                match self.storage.get(key).await {
                    Ok(png) => {
                        pages.insert(key, png);
                    }
                    Err(e) => {
                        warnings.push(crop_warning(task, f, format!("페이지 이미지를 읽지 못함: {e:#}")));
                        continue;
                    }
                }
            }
            match figure::crop(&pages[key], f.figure.bbox_pt, f.page_image.dpi) {
                Ok(png) => crops.push((f, png)),
                Err(e) => warnings.push(crop_warning(task, f, format!("{e:#}"))),
            }
        }
        crops
    }
}

fn crop_warning(task: &AgentTask, f: &TaskFigure, message: String) -> Warning {
    Warning {
        code: "figure_crop_failed".into(),
        severity: Severity::Warn,
        message: format!("{} ({}쪽): {message}", f.id, f.figure.page),
        path: Some(format!("products[{}].hazards.pictograms", task.product_index)),
    }
}

#[async_trait]
impl TaskHandler for HandlingAgent {
    fn kind(&self) -> AgentKind {
        AgentKind::Handling
    }

    fn prompt_version(&self) -> &'static str {
        extract::PROMPT_VERSION
    }

    async fn handle(&self, task: &AgentTask) -> Result<Done, Failed> {
        let mut pages = Vec::new();
        for img in &task.page_images {
            let png = self.storage.get(&img.key).await.map_err(|e| Failed::new("storage_read_failed", e, vec![]))?;
            pages.push((img.page, png));
        }
        let mut warnings = Vec::new();
        let crops = self.crop_figures(task, &mut warnings).await;

        // 모든 칸이 빈 응답은 기록하지 않는다 → 다음 실행에서도 새로 부른다
        let keep = |v: &serde_json::Value| !extract::looks_empty(v);
        let request = extract::build_request(task, &pages, &crops, None);
        let mut call =
            self.llm.call_json_keep_if("handling", &task.model, request, keep).await.map_err(|e| Failed::new("llm_failed", e, vec![]))?;
        let mut usage = vec![call.usage.clone()];
        // 입력에 내용이 있는데 응답이 모두 비었으면 다시 묻는다. 그래도 비면 사람이 보도록 경고를 남긴다.
        // 같은 요청이 어떤 때는 비고 어떤 때는 차서(10-26 실측) 확률적인 현상으로 보고, 회차를 적어 매번 새로 부른다.
        let mut tries = 0;
        while extract::looks_empty(&call.value) && extract::has_content(task) && tries < MAX_EMPTY_RETRIES {
            tries += 1;
            warn!(task_id = %task.task_id, tries, "응답이 모두 비어 다시 물음");
            let note = format!("{} (다시 묻기 {tries}회차)", extract::RETRY_EMPTY);
            let retry = extract::build_request(task, &pages, &crops, Some(&note));
            call = self
                .llm
                .call_json_keep_if("handling", &task.model, retry, keep)
                .await
                .map_err(|e| Failed::new("llm_failed", e, usage.clone()))?;
            usage.push(call.usage.clone());
        }
        if extract::looks_empty(&call.value) && extract::has_content(task) {
            warnings.push(Warning {
                code: "handling_empty".into(),
                severity: Severity::Error,
                message: format!("입력에 취급·보호구 내용이나 그림 후보가 있는데 {}번 모두 빈 응답이 옴", tries + 1),
                path: Some(format!("products[{}].handling", task.product_index)),
            });
        }
        let sent: Vec<&TaskFigure> = crops.iter().map(|(f, _)| *f).collect();
        let (output, more) =
            extract::convert(call.value, &format!("products[{}]", task.product_index), &sent, |id| figure_image_key(task.job_id, id))
                .map_err(|e| Failed::new("output_invalid", e, usage.clone()))?;
        warnings.extend(more);

        // 고른 그림만 저장한다. 기록된 응답을 다시 써도 같은 그림이 같은 키로 저장된다.
        for img in &output.images {
            let Some((_, png)) = crops.iter().find(|(f, _)| f.id == img.id) else { continue };
            self.storage.put(&img.key, png).await.map_err(|e| Failed::new("storage_write_failed", e, usage.clone()))?;
        }
        info!(
            task_id = %task.task_id,
            figures = task.figures.len(),
            images = output.images.len(),
            pictograms = output.pictograms.len(),
            warnings = warnings.len(),
            cached = call.cached,
            "취급 정리 완료"
        );
        let output = serde_json::to_value(output).map_err(|e| Failed::new("internal", e, usage.clone()))?;
        Ok(Done { output, warnings, usage })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?)).init();
    let storage = LocalStorage::new(std::env::var("MSDS_STORAGE_DIR").context("MSDS_STORAGE_DIR가 필요함")?)?;
    let llm = Arc::new(MsdsLlm::from_env(storage.clone())?);
    info!(mode = ?llm.mode(), "Claude 호출 모드");
    agent::serve(SERVICE, HandlingAgent { llm, storage }).await
}
