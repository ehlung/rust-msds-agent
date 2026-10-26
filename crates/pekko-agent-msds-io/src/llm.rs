//! MSDS 서비스의 Claude 호출.
//!
//! 응답을 파일 저장소(`llm-cache/<agent>/<요청 해시>.json`)에 기록하고, 같은 요청은 다시 보내지 않는다.
//! - 결과 캐시: 모델·effort·프롬프트·입력·스키마가 모두 같으면 기록된 응답을 쓴다(비용 0).
//! - 재생 모드(`MSDS_LLM_MODE=replay`): 기록이 없으면 API를 부르지 않고 실패한다.
//!   파이프라인·화면 개발 중에 실수로 비용이 나가지 않게 한다.

use anyhow::{anyhow, bail, Context};
use pekko_agent_llm::{Effort as LlmEffort, LlmConfig, LlmGateway, LlmRequest, LlmResponse};
use pekko_agent_msds::message::{Effort, LlmUsage, ModelSpec};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::info;

use crate::LocalStorage;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LlmMode {
    Live,
    Replay,
}

pub struct MsdsLlm {
    gateway: Option<LlmGateway>,
    storage: LocalStorage,
    mode: LlmMode,
}

/// 구조화 출력 한 번의 결과.
pub struct JsonCall {
    pub value: serde_json::Value,
    /// 이번에 실제로 쓴 토큰. 기록된 응답을 썼으면 0이다.
    pub usage: LlmUsage,
    pub cached: bool,
}

#[derive(Serialize, Deserialize)]
struct Recorded {
    model: String,
    stop_reason: String,
    text: String,
    usage: LlmUsage,
}

/// 긴 문서의 성분 추출·번역은 몇 분이 걸릴 수 있어 HTTP·서킷 브레이커 제한 시간을 늘린다.
const CALL_TIMEOUT_SECS: u64 = 600;
/// 프로세스가 켜져 있는 동안 쓸 수 있는 토큰 상한의 기본값 (`MSDS_TOKEN_BUDGET`).
const DEFAULT_TOKEN_BUDGET: u64 = 2_000_000;

impl MsdsLlm {
    pub fn from_env(storage: LocalStorage) -> anyhow::Result<Self> {
        let mode = match std::env::var("MSDS_LLM_MODE").as_deref() {
            Ok("replay") => LlmMode::Replay,
            Ok("live") | Err(_) => LlmMode::Live,
            Ok(other) => bail!("MSDS_LLM_MODE는 live 또는 replay: {other}"),
        };
        let gateway = match mode {
            LlmMode::Replay => None,
            LlmMode::Live => {
                let api_key = std::env::var("CLAUDE_API_KEY").context("CLAUDE_API_KEY가 필요함")?;
                let token_budget_daily = std::env::var("MSDS_TOKEN_BUDGET")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(DEFAULT_TOKEN_BUDGET);
                Some(LlmGateway::new(LlmConfig {
                    api_key,
                    timeout_secs: CALL_TIMEOUT_SECS,
                    call_timeout_secs: CALL_TIMEOUT_SECS,
                    max_retries: 3,
                    token_budget_daily,
                    ..LlmConfig::default()
                }))
            }
        };
        Ok(Self { gateway, storage, mode })
    }

    pub fn mode(&self) -> LlmMode {
        self.mode
    }

    /// 구조화 출력으로 호출한다. `request.output_schema`가 있어야 한다.
    /// 모델·effort는 `model`로 덮어쓴다(오케스트레이터가 정한 값).
    pub async fn call_json(&self, agent: &str, model: &ModelSpec, request: LlmRequest) -> anyhow::Result<JsonCall> {
        self.call_json_keep_if(agent, model, request, |_| true).await
    }

    /// `call_json`과 같지만, `keep`이 false인 응답은 기록하지 않는다.
    /// 쓸 수 없는 응답(예: 모든 칸이 빈 응답)을 기록하면 다음 실행도 그 응답을 그대로 쓰게 되기 때문이다.
    pub async fn call_json_keep_if(
        &self,
        agent: &str,
        model: &ModelSpec,
        mut request: LlmRequest,
        keep: impl Fn(&serde_json::Value) -> bool,
    ) -> anyhow::Result<JsonCall> {
        if request.output_schema.is_none() {
            bail!("output_schema가 없는 요청");
        }
        request.model = Some(model.name.clone());
        request.effort = Some(match model.effort {
            Effort::Medium => LlmEffort::Medium,
            Effort::High => LlmEffort::High,
        });
        let key = format!("llm-cache/{agent}/{}.json", request_hash(&request));

        if let Ok(bytes) = self.storage.get(&key).await {
            let rec: Recorded = serde_json::from_slice(&bytes).with_context(|| format!("기록을 읽지 못함: {key}"))?;
            let value = serde_json::from_str(&rec.text).with_context(|| format!("기록된 응답이 JSON이 아님: {key}"))?;
            info!(agent, key, "기록된 응답 사용 (호출 안 함)");
            let usage = LlmUsage { model: rec.model, effort: Some(model.effort), ..LlmUsage::default() };
            return Ok(JsonCall { value, usage, cached: true });
        }

        let gateway = match (&self.gateway, self.mode) {
            (Some(g), LlmMode::Live) => g,
            _ => bail!("재생 모드인데 기록된 응답이 없음: {key}"),
        };
        let response: LlmResponse = gateway.call(request).await.map_err(|e| anyhow!("Claude 호출 실패: {e}"))?;
        let usage = LlmUsage {
            model: model.name.clone(),
            effort: Some(model.effort),
            input_tokens: response.usage.input_tokens as u64,
            output_tokens: response.usage.output_tokens as u64,
            cache_read_tokens: response.usage.cache_read_input_tokens as u64,
            cache_write_tokens: response.usage.cache_creation_input_tokens as u64,
        };
        info!(
            agent,
            input_tokens = usage.input_tokens,
            output_tokens = usage.output_tokens,
            stop_reason = %response.stop_reason,
            "Claude 호출"
        );
        // 거절·잘림·JSON 오류는 기록하지 않는다 → 다시 시도하면 새로 호출한다
        let value: serde_json::Value = response.json().map_err(|e| anyhow!(e))?;
        if keep(&value) {
            let rec = Recorded { model: model.name.clone(), stop_reason: response.stop_reason.clone(), text: response.text(), usage: usage.clone() };
            self.storage.put(&key, &serde_json::to_vec_pretty(&rec)?).await?;
        } else {
            info!(agent, key, "쓸 수 없는 응답이라 기록하지 않음");
        }
        Ok(JsonCall { value, usage, cached: false })
    }
}

/// 응답을 바꿀 수 있는 값만 모아 해시한다.
fn request_hash(r: &LlmRequest) -> String {
    let canonical = serde_json::json!({
        "model": r.model,
        "effort": r.effort,
        "max_tokens": r.max_tokens,
        "system": r.system_prompt,
        "messages": r.messages,
        "schema": r.output_schema,
    });
    let digest = Sha256::digest(canonical.to_string().as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pekko_agent_llm::{ClaudeMessage, ContentBlock};

    fn req(text: &str) -> LlmRequest {
        LlmRequest {
            system_prompt: "s".into(),
            messages: vec![ClaudeMessage { role: "user".into(), content: vec![ContentBlock::Text { text: text.into() }] }],
            max_tokens: 100,
            output_schema: Some(serde_json::json!({"type": "object"})),
            ..LlmRequest::default()
        }
    }

    #[test]
    fn hash_changes_with_input_and_model() {
        let a = req("a");
        assert_eq!(request_hash(&a), request_hash(&req("a")));
        assert_ne!(request_hash(&a), request_hash(&req("b")));
        let mut m = req("a");
        m.model = Some("claude-opus-5-5".into());
        assert_ne!(request_hash(&a), request_hash(&m));
    }

    #[tokio::test]
    async fn replay_uses_record_and_never_calls_without_one() {
        let dir = std::env::temp_dir().join(format!("msds-llm-test-{}", std::process::id()));
        let storage = LocalStorage::new(&dir).unwrap();
        let llm = MsdsLlm { gateway: None, storage: storage.clone(), mode: LlmMode::Replay };
        let model = ModelSpec { name: "claude-sonnet-5-5".into(), effort: Effort::High };

        let err = llm.call_json("composition", &model, req("x")).await.err().unwrap();
        assert!(err.to_string().contains("재생 모드"), "{err}");

        // 같은 요청의 기록을 넣어 두면 그것을 쓴다
        let mut r = req("x");
        r.model = Some(model.name.clone());
        r.effort = Some(LlmEffort::High);
        let rec = Recorded { model: model.name.clone(), stop_reason: "end_turn".into(), text: r#"{"ok":true}"#.into(), usage: LlmUsage::default() };
        storage.put(&format!("llm-cache/composition/{}.json", request_hash(&r)), &serde_json::to_vec(&rec).unwrap()).await.unwrap();
        let got = llm.call_json("composition", &model, req("x")).await.unwrap();
        assert!(got.cached);
        assert_eq!(got.value, serde_json::json!({"ok": true}));
        assert_eq!(got.usage.input_tokens, 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
