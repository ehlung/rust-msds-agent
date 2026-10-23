use crate::types::*;
use reqwest::Client;
use std::time::Duration;
use tracing::{info, warn};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("API error: status={status}, body={body}")]
    ApiError { status: u16, body: String },
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Timeout")]
    Timeout,
}

/// 다시 시도하면 나을 수 있는 응답: 요청 한도(429), 일시적 서버 오류(500·502·503·504), 과부하(529)
fn is_retryable(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504 | 529)
}

/// Claude API Client
pub struct ClaudeClient {
    client: Client,
    config: LlmConfig,
}

impl ClaudeClient {
    pub fn new(config: LlmConfig) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .expect("Failed to build HTTP client");
        Self { client, config }
    }

    /// `LlmRequest`를 API 요청 본문으로 바꾼다. 요청에 모델이 있으면 설정의 기본 모델보다 우선한다.
    pub fn build_request(&self, request: &LlmRequest) -> ClaudeRequest {
        let output_config = (request.effort.is_some() || request.output_schema.is_some()).then(|| OutputConfig {
            effort: request.effort,
            format: request.output_schema.clone().map(|schema| OutputFormat { schema }),
        });
        ClaudeRequest {
            model: request.model.clone().unwrap_or_else(|| self.config.model.clone()),
            max_tokens: request.max_tokens,
            system: Some(request.system_prompt.clone()),
            messages: request.messages.clone(),
            tools: request.tools.clone(),
            temperature: request.temperature,
            output_config,
            cache_control: request.cacheable.then_some(CacheControl { ttl: None }),
        }
    }

    pub async fn send_message(
        &self,
        request: &LlmRequest,
    ) -> Result<LlmResponse, ClientError> {
        let claude_req = self.build_request(request);

        let mut retries = 0;
        loop {
            let resp = self
                .client
                .post(format!("{}/v1/messages", self.config.base_url))
                .header("x-api-key", &self.config.api_key)
                .header("anthropic-version", "2023-06-01")
                .header("content-type", "application/json")
                .json(&claude_req)
                .send()
                .await;

            match resp {
                Ok(response) if response.status().is_success() => {
                    let text = response.text().await
                        .map_err(|e| ClientError::ParseError(e.to_string()))?;
                    let body: ClaudeResponse = serde_json::from_str(&text).map_err(|e| {
                        // 파싱 실패 시 원인 파악을 위해 응답 본문 앞부분을 포함
                        let snippet: String = text.chars().take(800).collect();
                        ClientError::ParseError(format!("{e}; body: {snippet}"))
                    })?;

                    info!(
                        model = %body.model,
                        input_tokens = body.usage.input_tokens,
                        output_tokens = body.usage.output_tokens,
                        cache_read_tokens = body.usage.cache_read_input_tokens,
                        cache_write_tokens = body.usage.cache_creation_input_tokens,
                        stop_reason = %body.stop_reason,
                        "Claude API call succeeded"
                    );

                    return Ok(LlmResponse {
                        content: body.content,
                        stop_reason: body.stop_reason,
                        usage: body.usage,
                    });
                }
                Ok(response) if is_retryable(response.status().as_u16()) => {
                    let status = response.status().as_u16();
                    retries += 1;
                    if retries > self.config.max_retries {
                        let body = response.text().await.unwrap_or_default();
                        return Err(ClientError::ApiError { status, body });
                    }
                    warn!(retry = retries, status, "Retryable API error, retrying...");
                    tokio::time::sleep(Duration::from_secs(2u64.pow(retries))).await;
                }
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    return Err(ClientError::ApiError { status: status.as_u16(), body });
                }
                Err(e) if retries < self.config.max_retries => {
                    retries += 1;
                    warn!(retry = retries, error = %e, "Request failed, retrying...");
                    tokio::time::sleep(Duration::from_secs(2u64.pow(retries))).await;
                }
                Err(e) => {
                    return Err(ClientError::NetworkError(e.to_string()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn client(base_url: &str, max_retries: u32) -> ClaudeClient {
        ClaudeClient::new(LlmConfig {
            api_key: "test-key".into(),
            base_url: base_url.into(),
            timeout_secs: 10,
            max_retries,
            ..LlmConfig::default()
        })
    }

    fn user(text: &str) -> ClaudeMessage {
        ClaudeMessage { role: "user".into(), content: vec![ContentBlock::Text { text: text.into() }] }
    }

    #[test]
    fn defaults_send_no_new_fields() {
        // 기존 호출(EHS 기능)은 새 필드를 채우지 않으므로 요청 본문이 예전과 같아야 한다
        let c = client("http://unused", 0);
        let req = LlmRequest { system_prompt: "s".into(), messages: vec![user("hi")], max_tokens: 16, ..Default::default() };
        let body = serde_json::to_value(c.build_request(&req)).unwrap();
        assert_eq!(body["model"], "claude-sonnet-5-5");
        for absent in ["output_config", "cache_control", "temperature", "tools"] {
            assert!(body.get(absent).is_none(), "{absent} must be omitted: {body}");
        }
    }

    #[test]
    fn model_effort_schema_cache_and_image() {
        let c = client("http://unused", 0);
        let schema = json!({ "type": "object", "properties": { "ok": { "type": "boolean" } }, "required": ["ok"], "additionalProperties": false });
        let req = LlmRequest {
            system_prompt: "s".into(),
            messages: vec![ClaudeMessage {
                role: "user".into(),
                content: vec![
                    ContentBlock::Image { source: ImageSource { media_type: "image/png".into(), data: "iVBORw0K".into() } },
                    ContentBlock::Text { text: "이 그림은?".into() },
                ],
            }],
            max_tokens: 1024,
            cacheable: true,
            model: Some("claude-opus-5-5".into()),
            effort: Some(Effort::Medium),
            output_schema: Some(schema.clone()),
            ..Default::default()
        };
        let body = serde_json::to_value(c.build_request(&req)).unwrap();
        assert_eq!(body["model"], "claude-opus-5-5");
        assert_eq!(body["output_config"], json!({ "effort": "medium", "format": { "type": "json_schema", "schema": schema } }));
        assert_eq!(body["cache_control"], json!({ "type": "ephemeral" }));
        assert_eq!(
            body["messages"][0]["content"][0],
            json!({ "type": "image", "source": { "type": "base64", "media_type": "image/png", "data": "iVBORw0K" } })
        );
    }

    #[test]
    fn usage_without_cache_fields_defaults_to_zero() {
        let u: ClaudeUsage = serde_json::from_value(json!({ "input_tokens": 10, "output_tokens": 5 })).unwrap();
        assert_eq!((u.cache_read_input_tokens, u.cache_creation_input_tokens), (0, 0));
        let u: ClaudeUsage = serde_json::from_value(json!({ "input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": 900, "cache_creation_input_tokens": 100 })).unwrap();
        assert_eq!((u.cache_read_input_tokens, u.cache_creation_input_tokens), (900, 100));
    }

    #[test]
    fn json_rejects_refusal_and_truncation() {
        let resp = |stop: &str, text: &str| LlmResponse {
            content: vec![ContentBlock::Thinking { thinking: String::new(), signature: "s".into() }, ContentBlock::Text { text: text.into() }],
            stop_reason: stop.into(),
            usage: ClaudeUsage::default(),
        };
        assert_eq!(resp("end_turn", r#"{"ok":true}"#).json::<Value>().unwrap(), json!({ "ok": true }));
        assert!(resp("refusal", r#"{"ok":true}"#).json::<Value>().is_err());
        assert!(resp("max_tokens", r#"{"ok":tr"#).json::<Value>().is_err());
    }

    /// 응답을 순서대로 하나씩 돌려주는 가짜 Claude 서버. 받은 요청 본문을 모아 돌려준다.
    async fn fake_server(responses: Vec<(u16, Value)>) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let mut bodies = Vec::new();
            for (status, body) in responses {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let mut chunk = [0u8; 8192];
                // 헤더 끝까지 읽고, Content-Length만큼 본문을 더 읽는다
                let (head_end, len) = loop {
                    let n = sock.read(&mut chunk).await.unwrap();
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..i]).to_lowercase();
                        let len = head.lines().find_map(|l| l.strip_prefix("content-length:")).map(|v| v.trim().parse::<usize>().unwrap()).unwrap_or(0);
                        break (i + 4, len);
                    }
                };
                while buf.len() < head_end + len {
                    let n = sock.read(&mut chunk).await.unwrap();
                    buf.extend_from_slice(&chunk[..n]);
                }
                bodies.push(serde_json::from_slice(&buf[head_end..head_end + len]).unwrap());
                let out = body.to_string();
                let reply = format!("HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{out}", out.len());
                sock.write_all(reply.as_bytes()).await.unwrap();
            }
            bodies
        });
        (url, handle)
    }

    fn ok_body() -> Value {
        json!({
            "id": "msg_1", "model": "claude-sonnet-5-5", "stop_reason": "end_turn",
            "content": [{ "type": "thinking", "thinking": "", "signature": "sig" }, { "type": "text", "text": "{\"ok\":true}" }],
            "usage": { "input_tokens": 120, "output_tokens": 8, "cache_read_input_tokens": 100, "cache_creation_input_tokens": 0 }
        })
    }

    #[tokio::test]
    async fn retries_overloaded_then_succeeds() {
        let (url, server) = fake_server(vec![(529, json!({ "type": "error", "error": { "type": "overloaded_error" } })), (200, ok_body())]).await;
        let req = LlmRequest { system_prompt: "s".into(), messages: vec![user("hi")], max_tokens: 64, effort: Some(Effort::High), ..Default::default() };
        let resp = client(&url, 1).send_message(&req).await.unwrap();
        assert_eq!(resp.json::<Value>().unwrap(), json!({ "ok": true }));
        assert_eq!(resp.usage.cache_read_input_tokens, 100);
        let bodies = server.await.unwrap();
        assert_eq!(bodies.len(), 2, "529 뒤에 한 번 더 보내야 함");
        assert_eq!(bodies[1]["output_config"]["effort"], "high");
    }

    #[tokio::test]
    async fn bad_request_is_not_retried() {
        let (url, server) = fake_server(vec![(400, json!({ "type": "error", "error": { "type": "invalid_request_error" } }))]).await;
        let req = LlmRequest { system_prompt: "s".into(), messages: vec![user("hi")], max_tokens: 64, ..Default::default() };
        match client(&url, 3).send_message(&req).await {
            Err(ClientError::ApiError { status, .. }) => assert_eq!(status, 400),
            other => panic!("400이어야 함: {other:?}"),
        }
        assert_eq!(server.await.unwrap().len(), 1);
    }
}
