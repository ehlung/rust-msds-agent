//! 경로 판단: 오케스트레이터 에이전트(Claude)가 문서 프로필 요약을 보고 작업 계획을 낸다.
//! 코드는 판단을 대신하지 않고, 계획이 프로필 범위 안인지만 검사한다(설계 4-5).

use std::collections::HashSet;

use pekko_agent_llm::{ClaudeMessage, ContentBlock, LlmRequest};
use pekko_agent_msds::message::{AgentKind, RoutingPlan, Verdict};
use pekko_agent_msds::profile::DocumentProfile;
use pekko_agent_msds::SectionKind;
use serde_json::{json, Value};

pub const PROMPT_VERSION: &str = "routing/1";
const SYSTEM_PROMPT: &str = include_str!("../prompts/routing.md");
const MAX_TOKENS: u32 = 16000;

fn enum_values<T: serde::Serialize>(items: &[T]) -> Value {
    Value::Array(items.iter().map(|i| serde_json::to_value(i).unwrap()).collect())
}

/// `RoutingPlan`의 구조화 출력 스키마.
pub fn schema() -> Value {
    let agent = json!({ "type": "string", "enum": enum_values(&[AgentKind::Translate, AgentKind::Composition, AgentKind::Handling]) });
    json!({
        "type": "object",
        "properties": {
            "verdict": { "type": "string", "enum": enum_values(&[Verdict::Msds, Verdict::NotMsds, Verdict::Unsupported]) },
            "plan": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "agent": agent,
                        "product_index": { "type": "integer" },
                        "sections": { "type": "array", "items": { "type": "string", "enum": enum_values(&SectionKind::ALL) } },
                        "page_images": { "type": "array", "items": { "type": "integer" } },
                        "note": { "type": "string" }
                    },
                    "required": ["agent", "product_index", "sections", "page_images", "note"],
                    "additionalProperties": false
                }
            },
            "skipped": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "agent": agent,
                        "product_index": { "type": "integer" },
                        "reason": { "type": "string" }
                    },
                    "required": ["agent", "product_index", "reason"],
                    "additionalProperties": false
                }
            },
            "summary": { "type": "string" }
        },
        "required": ["verdict", "plan", "skipped", "summary"],
        "additionalProperties": false
    })
}

/// 판단에 필요한 것만 남긴 프로필 요약. 섹션 본문은 빼고 글자 수만 둔다(입력 토큰을 줄인다).
pub fn summary(p: &DocumentProfile) -> Value {
    json!({
        "pages": p.pages,
        "language": p.language,
        "text_layer": p.text_layer,
        "products": p.products.iter().map(|pr| json!({
            "index": pr.index,
            "name_guess": pr.name_guess,
            "pages": pr.pages,
            "format": pr.format,
            "cas_found": pr.cas_found,
            "sections": pr.sections.iter().map(|s| json!({
                "kind": s.kind, "number": s.number, "title": s.title, "pages": s.pages, "chars": s.text.chars().count()
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "figures": p.figures.iter().map(|f| json!({ "page": f.page, "source": f.source, "near_text": f.near_text })).collect::<Vec<_>>(),
        "page_images": p.page_images.iter().map(|i| i.page).collect::<Vec<_>>(),
    })
}

/// 안전장치 검사에 걸렸으면 `previous`에 직전 계획과 문제를 넣어 다시 묻는다.
pub fn build_request(profile: &DocumentProfile, previous: Option<(&RoutingPlan, &[String])>) -> LlmRequest {
    let mut text = format!("문서 프로필 요약:\n{}\n", serde_json::to_string_pretty(&summary(profile)).unwrap());
    if let Some((plan, problems)) = previous {
        text.push_str(&format!(
            "\n직전 계획이 검사에서 걸렸다. 아래 문제를 고친 계획을 다시 내라.\n직전 계획:\n{}\n문제:\n- {}\n",
            serde_json::to_string(plan).unwrap(),
            problems.join("\n- ")
        ));
    }
    LlmRequest {
        system_prompt: SYSTEM_PROMPT.into(),
        messages: vec![ClaudeMessage { role: "user".into(), content: vec![ContentBlock::Text { text }] }],
        max_tokens: MAX_TOKENS,
        output_schema: Some(schema()),
        ..LlmRequest::default()
    }
}

/// 계획이 프로필 범위 안인지 검사한다. 문제가 없으면 빈 배열.
pub fn validate(plan: &RoutingPlan, profile: &DocumentProfile, max_tasks: usize) -> Vec<String> {
    let mut problems = Vec::new();
    if plan.verdict != Verdict::Msds {
        return problems;
    }
    let n = profile.products.len() as u32;
    let image_pages: HashSet<u32> = profile.page_images.iter().map(|i| i.page).collect();
    let mut seen = HashSet::new();
    for (i, t) in plan.plan.iter().enumerate() {
        let Some(product) = profile.products.get(t.product_index as usize) else {
            problems.push(format!("plan[{i}]: 제품 번호 {}가 없음 (제품 {n}개)", t.product_index));
            continue;
        };
        if !seen.insert((t.agent, t.product_index)) {
            problems.push(format!("plan[{i}]: {:?} 작업이 제품 {}에 두 번 있음", t.agent, t.product_index));
        }
        let kinds: HashSet<SectionKind> = product.sections.iter().map(|s| s.kind).collect();
        for k in &t.sections {
            if !kinds.contains(k) {
                problems.push(format!("plan[{i}]: 제품 {}에 {:?} 섹션이 없음", t.product_index, k));
            }
        }
        for p in &t.page_images {
            if !image_pages.contains(p) {
                problems.push(format!("plan[{i}]: {p}쪽 이미지가 없음"));
            }
        }
    }
    for idx in 0..n {
        if !plan.plan.iter().any(|t| t.agent == AgentKind::Composition && t.product_index == idx) {
            problems.push(format!("제품 {idx}에 composition 작업이 없음"));
        }
    }
    if plan.plan.len() > max_tasks {
        problems.push(format!("작업이 {}개로 상한 {max_tasks}개를 넘음", plan.plan.len()));
    }
    problems
}

#[cfg(test)]
mod tests {
    use super::*;
    use pekko_agent_msds::message::PlannedTask;
    use pekko_agent_msds::profile::*;

    fn profile() -> DocumentProfile {
        let section = |kind, number| SectionText { kind, number, title: "T".into(), pages: vec![1], text: "본문".into() };
        DocumentProfile {
            pages: 4,
            language: Language::En,
            text_layer: TextLayer::Ok,
            products: vec![ProductProfile {
                index: 0,
                name_guess: Some("POLANE".into()),
                pages: [1, 4],
                format: DocFormat::LegacyUs,
                cas_found: 6,
                sections: vec![section(SectionKind::Identification, 1), section(SectionKind::Composition, 2)],
            }],
            figures: vec![],
            page_images: (1..=4).map(|page| PageImage { page, key: format!("jobs/x/pages/{page}.png"), dpi: 150.0 }).collect(),
        }
    }

    fn task(agent: AgentKind, product_index: u32, sections: Vec<SectionKind>, page_images: Vec<u32>) -> PlannedTask {
        PlannedTask { agent, product_index, sections, page_images, note: String::new() }
    }

    fn plan(tasks: Vec<PlannedTask>) -> RoutingPlan {
        RoutingPlan { verdict: Verdict::Msds, plan: tasks, skipped: vec![], summary: String::new() }
    }

    #[test]
    fn valid_plan_passes() {
        let p = plan(vec![task(AgentKind::Composition, 0, vec![SectionKind::Composition], vec![1])]);
        assert!(validate(&p, &profile(), 3).is_empty());
    }

    #[test]
    fn catches_out_of_range_and_missing_composition() {
        let p = plan(vec![
            task(AgentKind::Translate, 0, vec![SectionKind::Regulatory], vec![9]),
            task(AgentKind::Translate, 0, vec![], vec![]),
            task(AgentKind::Handling, 3, vec![], vec![]),
        ]);
        let problems = validate(&p, &profile(), 2);
        let joined = problems.join("\n");
        for needle in ["Regulatory 섹션이 없음", "9쪽 이미지가 없음", "두 번", "제품 번호 3", "composition 작업이 없음", "상한 2개"] {
            assert!(joined.contains(needle), "{needle} 없음:\n{joined}");
        }
    }

    #[test]
    fn not_msds_needs_no_tasks() {
        let p = RoutingPlan { verdict: Verdict::NotMsds, plan: vec![], skipped: vec![], summary: "견적서".into() };
        assert!(validate(&p, &profile(), 3).is_empty());
    }

    #[test]
    fn schema_matches_plan_type() {
        // 스키마대로 만든 예시가 RoutingPlan으로 읽혀야 한다
        let v = json!({
            "verdict": "msds",
            "plan": [{ "agent": "composition", "product_index": 0, "sections": ["identification", "exposure_ppe"], "page_images": [1], "note": "" }],
            "skipped": [{ "agent": "translate", "product_index": 0, "reason": "한글 문서" }],
            "summary": "s"
        });
        let p: RoutingPlan = serde_json::from_value(v).unwrap();
        assert_eq!(p.plan[0].sections[1], SectionKind::ExposurePpe);
        let s = schema();
        assert_eq!(s["properties"]["plan"]["items"]["properties"]["sections"]["items"]["enum"].as_array().unwrap().len(), 16);
    }
}
