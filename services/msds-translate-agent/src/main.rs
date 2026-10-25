//! msds-translate-agent — 번역 에이전트. 섹션 본문을 한국어로 옮긴다.
//!
//! - 섹션을 호출 단위로 묶는 일은 코드(`chunk`), 번역은 Claude가 한다. 묶음은 동시에 몇 개씩 부른다.
//! - 섹션 제목은 국내 MSDS 16개 항목 명칭을 코드가 붙인다(`SectionKind::title_ko`).
//! - 한국어 문서는 Claude를 부르지 않고 원문을 그대로 `ko`에 넣는다(오케스트레이터가 보통 작업을 만들지 않는다).

mod chunk;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use pekko_agent_llm::{ClaudeMessage, ContentBlock, LlmRequest};
use pekko_agent_msds::message::{AgentKind, AgentTask, LlmUsage};
use pekko_agent_msds::output::TranslateOutput;
use pekko_agent_msds::profile::Language;
use pekko_agent_msds::result::{Text, TranslatedSection};
use pekko_agent_msds_io::agent::{self, Done, Failed, TaskHandler};
use pekko_agent_msds_io::{LocalStorage, MsdsLlm};
use serde::Deserialize;
use serde_json::json;
use tracing::info;

use crate::chunk::{batches, pieces, Piece, CHUNK_CHARS};

const SERVICE: &str = "msds-translate-agent";
pub const PROMPT_VERSION: &str = "translate/1";
const SYSTEM_PROMPT: &str = include_str!("../prompts/translate.md");
/// 한 작업 안에서 동시에 부르는 Claude 호출 수.
const CONCURRENCY: usize = 4;

struct Translate {
    llm: Arc<MsdsLlm>,
}

#[derive(Deserialize)]
struct Reply {
    pieces: Vec<ReplyPiece>,
}

#[derive(Deserialize)]
struct ReplyPiece {
    id: usize,
    ko: String,
}

fn schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "pieces": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": { "id": { "type": "integer" }, "ko": { "type": "string" } },
                    "required": ["id", "ko"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["pieces"],
        "additionalProperties": false
    })
}

fn build_request(task: &AgentTask, batch: &[Piece]) -> LlmRequest {
    let mut text = String::from("아래 조각들을 한국어로 옮겨라.\n\n");
    for p in batch {
        let s = &task.sections[p.section];
        let kind = serde_json::to_value(s.kind).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default();
        text.push_str(&format!(
            "<piece id=\"{}\" section=\"{kind}\" title=\"{}\" part=\"{}/{}\">\n{}\n</piece>\n\n",
            p.id,
            s.title.replace('"', "'"),
            p.part.0,
            p.part.1,
            p.text.trim_end()
        ));
    }
    LlmRequest {
        system_prompt: SYSTEM_PROMPT.into(),
        messages: vec![ClaudeMessage { role: "user".into(), content: vec![ContentBlock::Text { text }] }],
        max_tokens: task.limits.max_tokens,
        output_schema: Some(schema()),
        ..LlmRequest::default()
    }
}

/// 묶음 하나를 번역한다. 받은 조각이 빠졌거나 남으면 실패로 본다.
async fn translate_batch(llm: &MsdsLlm, task: &AgentTask, batch: Vec<Piece>) -> Result<(Vec<(usize, String)>, LlmUsage), (anyhow::Error, Option<LlmUsage>)> {
    let call = llm.call_json("translate", &task.model, build_request(task, &batch)).await.map_err(|e| (e, None))?;
    let usage = call.usage.clone();
    let reply: Reply = serde_json::from_value(call.value).map_err(|e| (anyhow!(e), Some(usage.clone())))?;
    let mut got: HashMap<usize, String> = reply.pieces.into_iter().map(|p| (p.id, p.ko)).collect();
    let mut out = Vec::new();
    for p in &batch {
        match got.remove(&p.id) {
            Some(ko) if !ko.trim().is_empty() || p.text.trim().is_empty() => out.push((p.id, ko)),
            _ => return Err((anyhow!("조각 {}의 번역이 없음", p.id), Some(usage))),
        }
    }
    if !got.is_empty() {
        return Err((anyhow!("요청하지 않은 조각 번호가 돌아옴: {:?}", got.keys().collect::<Vec<_>>()), Some(usage)));
    }
    Ok((out, usage))
}

#[async_trait]
impl TaskHandler for Translate {
    fn kind(&self) -> AgentKind {
        AgentKind::Translate
    }

    fn prompt_version(&self) -> &'static str {
        PROMPT_VERSION
    }

    async fn handle(&self, task: &AgentTask) -> Result<Done, Failed> {
        let section = |s: &pekko_agent_msds::profile::SectionText, ko: String| TranslatedSection {
            kind: s.kind,
            number: s.number,
            title: Text { src: s.title.clone(), ko: s.kind.title_ko().into() },
            body: Text { src: s.text.clone(), ko },
            pages: s.pages.clone(),
        };
        if task.language == Language::Ko {
            let sections = task.sections.iter().map(|s| section(s, s.text.clone())).collect();
            let output = serde_json::to_value(TranslateOutput { sections }).map_err(|e| Failed::new("internal", e, vec![]))?;
            return Ok(Done { output, warnings: vec![], usage: vec![] });
        }

        let texts: Vec<&str> = task.sections.iter().map(|s| s.text.as_str()).collect();
        let all = pieces(&texts, CHUNK_CHARS);
        let groups = batches(&all, CHUNK_CHARS);
        info!(task_id = %task.task_id, sections = texts.len(), pieces = all.len(), calls = groups.len(), "번역 시작");

        let results: Vec<_> = stream::iter(groups.into_iter().map(|b| translate_batch(&self.llm, task, b)))
            .buffer_unordered(CONCURRENCY)
            .collect()
            .await;
        let mut usage = Vec::new();
        let mut ko: HashMap<usize, String> = HashMap::new();
        let mut first_err = None;
        for r in results {
            match r {
                Ok((parts, u)) => {
                    usage.push(u);
                    ko.extend(parts);
                }
                Err((e, u)) => {
                    usage.extend(u);
                    first_err.get_or_insert(e);
                }
            }
        }
        // 실패한 묶음이 있으면 작업 전체를 실패로 돌려준다. 다시 받으면 성공한 묶음은 기록된 응답을 쓴다.
        if let Some(e) = first_err {
            return Err(Failed::new("llm_failed", e, usage));
        }

        let mut sections = Vec::new();
        for (si, s) in task.sections.iter().enumerate() {
            let body: Vec<&str> = all.iter().filter(|p| p.section == si).map(|p| ko[&p.id].trim_end()).collect();
            sections.push(section(s, body.join("\n")));
        }
        info!(task_id = %task.task_id, sections = sections.len(), calls = usage.len(), "번역 완료");
        let output = serde_json::to_value(TranslateOutput { sections }).map_err(|e| Failed::new("internal", e, usage.clone()))?;
        Ok(Done { output, warnings: vec![], usage })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?)).init();
    let storage = LocalStorage::new(std::env::var("MSDS_STORAGE_DIR").context("MSDS_STORAGE_DIR가 필요함")?)?;
    let llm = Arc::new(MsdsLlm::from_env(storage)?);
    info!(mode = ?llm.mode(), "Claude 호출 모드");
    agent::serve(SERVICE, Translate { llm }).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_is_closed() {
        let s = schema();
        assert_eq!(s["additionalProperties"], json!(false));
        assert_eq!(s["properties"]["pieces"]["items"]["additionalProperties"], json!(false));
    }
}
