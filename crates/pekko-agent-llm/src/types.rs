use serde::{Deserialize, Serialize};

/// Claude API request
#[derive(Debug, Serialize)]
pub struct ClaudeRequest {
    pub model: String,
    pub max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    pub messages: Vec<ClaudeMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ClaudeTool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    /// effort(생각의 깊이)와 구조화 출력(JSON 스키마).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_config: Option<OutputConfig>,
    /// 요청 최상위에 두면 마지막 캐시 가능 블록에 프롬프트 캐시가 자동으로 붙는다.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControl>,
}

/// 생각의 깊이. Sonnet 5.5는 생략하면 `high`다.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct OutputConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<Effort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<OutputFormat>,
}

/// 구조화 출력. 스키마의 모든 객체에 `additionalProperties: false`가 있어야 하고,
/// 숫자·길이 제한(`minimum`, `maxLength` 등)과 재귀 스키마는 쓸 수 없다.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "type", rename = "json_schema")]
pub struct OutputFormat {
    pub schema: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "type", rename = "ephemeral")]
pub struct CacheControl {
    /// `None`이면 기본 5분. `"1h"`면 1시간.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClaudeMessage {
    pub role: String,
    pub content: Vec<ContentBlock>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        is_error: Option<bool>,
    },
    /// 확장 사고 블록. 최신 모델(adaptive thinking 기본)은 표시 생략 시 빈 `thinking`과
    /// `signature`만 보냄. Tool 사용 루프에서 같은 모델로 이어갈 때 수정 없이 그대로 되돌려야 함.
    #[serde(rename = "thinking")]
    Thinking { thinking: String, signature: String },
    #[serde(rename = "redacted_thinking")]
    RedactedThinking { data: String },
    /// 이미지 입력 (페이지 렌더링 PNG 등).
    #[serde(rename = "image")]
    Image { source: ImageSource },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type", rename = "base64")]
pub struct ImageSource {
    /// 예: `image/png`
    pub media_type: String,
    /// base64로 인코딩한 이미지
    pub data: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClaudeTool {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// Claude API response
#[derive(Debug, Deserialize)]
pub struct ClaudeResponse {
    pub id: String,
    pub content: Vec<ContentBlock>,
    pub model: String,
    pub stop_reason: String,
    pub usage: ClaudeUsage,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ClaudeUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    /// 캐시에 새로 쓴 입력 토큰 (Claude만 보냄)
    #[serde(default)]
    pub cache_creation_input_tokens: u32,
    /// 캐시에서 읽은 입력 토큰 (Claude만 보냄)
    #[serde(default)]
    pub cache_read_input_tokens: u32,
}

impl ClaudeUsage {
    pub fn total(&self) -> u32 { self.input_tokens + self.output_tokens }
}

/// LLM request/response
#[derive(Debug, Clone, Default)]
pub struct LlmRequest {
    pub system_prompt: String,
    pub messages: Vec<ClaudeMessage>,
    pub tools: Vec<ClaudeTool>,
    pub max_tokens: u32,
    pub temperature: Option<f32>,
    /// true면 프롬프트 캐시를 켠다(요청 최상위 `cache_control`).
    pub cacheable: bool,
    /// 이 요청에만 쓸 모델. `None`이면 `LlmConfig.model`.
    pub model: Option<String>,
    /// `None`이면 보내지 않는다(모델 기본값).
    pub effort: Option<Effort>,
    /// 구조화 출력 스키마. 있으면 응답 텍스트가 이 스키마의 JSON이다.
    pub output_schema: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct LlmResponse {
    pub content: Vec<ContentBlock>,
    pub stop_reason: String,
    pub usage: ClaudeUsage,
}

impl LlmResponse {
    /// 텍스트 블록만 이어 붙인다(생각 블록은 뺀다).
    pub fn text(&self) -> String {
        self.content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    /// 구조화 출력을 타입으로 읽는다. 거절이나 `max_tokens`로 끊긴 응답은 스키마를 따르지 않을 수 있어 먼저 걸러낸다.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, String> {
        match self.stop_reason.as_str() {
            "refusal" => return Err("모델이 응답을 거절함 (stop_reason=refusal)".into()),
            "max_tokens" => return Err("max_tokens에 걸려 응답이 끊김".into()),
            _ => {}
        }
        serde_json::from_str(&self.text()).map_err(|e| format!("구조화 출력을 읽지 못함: {e}"))
    }
}

/// LLM configuration
#[derive(Debug, Clone)]
pub struct LlmConfig {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
    pub max_tokens: u32,
    /// `None`이면 요청에 temperature를 넣지 않음 (최신 Claude 모델은 지정 시 400 오류)
    pub temperature: Option<f32>,
    pub timeout_secs: u64,
    pub max_retries: u32,
    pub rate_limit_rpm: u32,
    pub token_budget_daily: u64,
    /// `LlmGateway` 서킷 브레이커의 호출 제한 시간. 긴 번역처럼 오래 걸리는 호출은 늘린다.
    pub call_timeout_secs: u64,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            base_url: "https://api.anthropic.com".into(),
            model: "claude-sonnet-5-5".into(),
            max_tokens: 4096,
            temperature: None,
            timeout_secs: 60,
            max_retries: 5,
            rate_limit_rpm: 1000,
            token_budget_daily: 1_000_000,
            call_timeout_secs: 30,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_sends_no_temperature() {
        // 최신 Claude 모델은 `temperature`를 거부(400: "temperature is deprecated")하므로
        // 명시적으로 설정하지 않으면 요청에 포함되지 않아야 함
        assert_eq!(LlmConfig::default().temperature, None);

        let req = ClaudeRequest {
            model: LlmConfig::default().model,
            max_tokens: 16,
            system: None,
            messages: vec![],
            tools: vec![],
            temperature: LlmConfig::default().temperature,
            output_config: None,
            cache_control: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert!(json.get("temperature").is_none(), "temperature must be omitted: {json}");
    }

    #[test]
    fn thinking_blocks_roundtrip_unchanged() {
        let raw = r#"[{"type":"thinking","thinking":"","signature":"sig=="},
                      {"type":"redacted_thinking","data":"enc"},
                      {"type":"tool_use","id":"t1","name":"coreos_robot","input":{"command":"status"}}]"#;
        let blocks: Vec<ContentBlock> = serde_json::from_str(raw).unwrap();
        let back = serde_json::to_value(&blocks).unwrap();
        let expected: serde_json::Value = serde_json::from_str(raw).unwrap();
        assert_eq!(back, expected, "thinking blocks must be echoed back exactly");
    }

    #[test]
    fn explicit_temperature_is_sent() {
        let req = ClaudeRequest {
            model: "m".into(),
            max_tokens: 16,
            system: None,
            messages: vec![],
            tools: vec![],
            temperature: Some(0.2),
            output_config: None,
            cache_control: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert!((json["temperature"].as_f64().unwrap() - 0.2).abs() < 1e-6);
    }
}
