//! msds-composition-agent — 성분 에이전트. 제품 정보, 유해성 분류, 성분·CAS, 위험물질을 뽑는다.
//!
//! `msds.task.composition`을 받아 Claude를 부르고 `msds.task.result`로 결과를 보낸다.
//! Kafka 루프와 액터는 공통 틀(`pekko_agent_msds_io::agent`)이 맡고, 여기는 이 에이전트의 일만 정의한다.

mod extract;

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use pekko_agent_msds::message::{AgentKind, AgentTask};
use pekko_agent_msds_io::agent::{self, Done, Failed, TaskHandler};
use pekko_agent_msds_io::{LocalStorage, MsdsLlm};
use tracing::info;

const SERVICE: &str = "msds-composition-agent";

struct Composition {
    llm: Arc<MsdsLlm>,
    storage: LocalStorage,
}

#[async_trait]
impl TaskHandler for Composition {
    fn kind(&self) -> AgentKind {
        AgentKind::Composition
    }

    fn prompt_version(&self) -> &'static str {
        extract::PROMPT_VERSION
    }

    async fn handle(&self, task: &AgentTask) -> Result<Done, Failed> {
        let mut images = Vec::new();
        for img in &task.page_images {
            let png = self.storage.get(&img.key).await.map_err(|e| Failed::new("storage_read_failed", e, vec![]))?;
            images.push((img.page, png));
        }
        let request = extract::build_request(task, &images);
        let call = self.llm.call_json("composition", &task.model, request).await.map_err(|e| Failed::new("llm_failed", e, vec![]))?;
        let usage = vec![call.usage];
        let (output, warnings) = extract::convert(call.value, &format!("products[{}]", task.product_index))
            .map_err(|e| Failed::new("output_invalid", e, usage.clone()))?;
        info!(
            task_id = %task.task_id,
            components = output.components.len(),
            hazardous = output.hazardous_substances.len(),
            warnings = warnings.len(),
            cached = call.cached,
            "성분 추출 완료"
        );
        let output = serde_json::to_value(output).map_err(|e| Failed::new("internal", e, usage.clone()))?;
        Ok(Done { output, warnings, usage })
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

    let storage = LocalStorage::new(std::env::var("MSDS_STORAGE_DIR").context("MSDS_STORAGE_DIR가 필요함")?)?;
    let llm = Arc::new(MsdsLlm::from_env(storage.clone())?);
    info!(mode = ?llm.mode(), "Claude 호출 모드");
    agent::serve(SERVICE, Composition { llm, storage }).await
}
