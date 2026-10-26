//! 취급 에이전트의 Claude 요청과 응답 변환.
//!
//! Claude는 취급·보호구 내용을 정리하고, 그림 후보마다 종류와 GHS 코드를 고른다.
//! 그림문자 이름(`name_ko`)과 그림 파일 키처럼 정해진 값은 코드가 붙인다.

use std::collections::{HashMap, HashSet};

use anyhow::bail;
use pekko_agent_llm::{ClaudeMessage, ContentBlock, ImageSource, LlmRequest};
use pekko_agent_msds::ghs;
use pekko_agent_msds::message::{AgentTask, TaskFigure};
use pekko_agent_msds::output::HandlingOutput;
use pekko_agent_msds::result::{Basis, Handling, Image, ImageKind, Pictogram, Ppe, Severity, Text, Warning};
use serde::Deserialize;
use serde_json::{json, Value};

pub const PROMPT_VERSION: &str = "handling/1";
const SYSTEM_PROMPT: &str = include_str!("../prompts/handling.md");

/// 구조화 출력 스키마. 모든 객체에 `additionalProperties: false`, 모든 필드는 필수다.
pub fn schema() -> Value {
    let text = json!({ "$ref": "#/$defs/text" });
    let texts = json!({ "type": "array", "items": text });
    let codes = json!({ "type": "array", "items": { "type": "string", "enum": ghs::CODES } });
    let s = json!({ "type": "string" });
    json!({
        "type": "object",
        "properties": {
            "precautions": texts,
            "storage": texts,
            "ppe": {
                "type": "object",
                "properties": { "respiratory": text, "hand": text, "eye": text, "skin": text },
                "required": ["respiratory", "hand", "eye", "skin"],
                "additionalProperties": false
            },
            "pages": { "type": "array", "items": { "type": "integer" } },
            "figures": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": s,
                        "kind": { "type": "string", "enum": ["pictogram", "handling", "other"] },
                        "ghs_codes": codes
                    },
                    "required": ["id", "kind", "ghs_codes"],
                    "additionalProperties": false
                }
            },
            "inferred_pictograms": codes,
            "issues": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": { "code": s, "message": s, "path": s },
                    "required": ["code", "message", "path"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["precautions", "storage", "ppe", "pages", "figures", "inferred_pictograms", "issues"],
        "additionalProperties": false,
        "$defs": {
            "text": {
                "type": "object",
                "properties": { "src": { "type": "string" }, "ko": { "type": "string" } },
                "required": ["src", "ko"],
                "additionalProperties": false
            }
        }
    })
}

fn png_block(png: &[u8]) -> ContentBlock {
    use base64::Engine as _;
    ContentBlock::Image {
        source: ImageSource { media_type: "image/png".into(), data: base64::engine::general_purpose::STANDARD.encode(png) },
    }
}

/// 다시 물을 때 덧붙이는 말. 첫 응답이 모두 비어 있었을 때 쓴다(`looks_empty`).
pub const RETRY_EMPTY: &str = "직전 응답은 모든 칸이 비어 있었다. 입력 섹션에 취급·저장이나 보호구 내용이 있으면 규칙대로 다시 정리하라. \
그림 후보가 있으면 후보마다 판단하라. 내용이 정말 없다고 판단하면 그 이유를 issues에 적어라.";

/// 응답이 모든 칸에서 비었는지. 입력에 내용이 있는데 이렇게 오면 한 번 더 묻는다.
/// (2026-10-26 10건 실행에서 13번 중 2번, 정상 종료인데 메모도 없이 모두 빈 응답이 왔다.)
pub fn looks_empty(value: &Value) -> bool {
    let empty = |k: &str| value[k].as_array().is_some_and(|a| a.is_empty());
    let ppe_empty = ["respiratory", "hand", "eye", "skin"].iter().all(|k| value["ppe"][k]["src"].as_str().is_some_and(|s| s.trim().is_empty()));
    ["precautions", "storage", "figures", "inferred_pictograms", "issues"].iter().all(|k| empty(k)) && ppe_empty
}

/// 입력에 정리할 내용이 있는지(취급·보호구 섹션 본문이나 그림 후보).
pub fn has_content(task: &AgentTask) -> bool {
    use pekko_agent_msds::SectionKind;
    !task.figures.is_empty()
        || task
            .sections
            .iter()
            .any(|s| matches!(s.kind, SectionKind::HandlingStorage | SectionKind::ExposurePpe) && s.text.trim().len() > s.title.trim().len() + 20)
}

/// 작업 메시지 → Claude 요청. `pages`는 오케스트레이터가 고른 페이지 이미지, `crops`는 잘라낸 그림 후보다.
/// `retry`는 다시 물을 때 맨 끝에 덧붙이는 말이다(요청이 달라지므로 기록된 응답을 쓰지 않고 새로 부른다).
pub fn build_request(task: &AgentTask, pages: &[(u32, Vec<u8>)], crops: &[(&TaskFigure, Vec<u8>)], retry: Option<&str>) -> LlmRequest {
    let mut content = Vec::new();
    for (page, png) in pages {
        content.push(ContentBlock::Text { text: format!("페이지 {page} 이미지:") });
        content.push(png_block(png));
    }
    let note = if task.note.trim().is_empty() { "없음" } else { task.note.trim() };
    let mut text = format!("오케스트레이터 지시: {note}\n문서 언어: {:?}\n\n", task.language);
    for s in &task.sections {
        let pages: Vec<String> = s.pages.iter().map(u32::to_string).collect();
        text.push_str(&format!(
            "<section kind=\"{}\" number=\"{}\" pages=\"{}\" title=\"{}\">\n{}\n</section>\n\n",
            serde_json::to_value(s.kind).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default(),
            s.number,
            pages.join(","),
            s.title.replace('"', "'"),
            s.text
        ));
    }
    text.push_str(&format!("그림 후보 {}개:", crops.len()));
    content.push(ContentBlock::Text { text });
    for (f, png) in crops {
        let source = serde_json::to_value(f.figure.source).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default();
        let near = f.figure.near_text.as_deref().unwrap_or("").replace('"', "'");
        content.push(ContentBlock::Text {
            text: format!("<figure id=\"{}\" page=\"{}\" source=\"{source}\" near_text=\"{near}\"/>", f.id, f.figure.page),
        });
        content.push(png_block(png));
    }
    if let Some(r) = retry {
        content.push(ContentBlock::Text { text: r.into() });
    }
    LlmRequest {
        system_prompt: SYSTEM_PROMPT.into(),
        messages: vec![ClaudeMessage { role: "user".into(), content }],
        max_tokens: task.limits.max_tokens,
        output_schema: Some(schema()),
        ..LlmRequest::default()
    }
}

// ── Claude 응답 모양 (스키마와 같다) ───────────────────────────────────────────

#[derive(Deserialize)]
struct Raw {
    precautions: Vec<RawText>,
    storage: Vec<RawText>,
    ppe: RawPpe,
    pages: Vec<u32>,
    figures: Vec<RawFigure>,
    inferred_pictograms: Vec<String>,
    issues: Vec<RawIssue>,
}

#[derive(Deserialize)]
struct RawText {
    src: String,
    ko: String,
}

#[derive(Deserialize)]
struct RawPpe {
    respiratory: RawText,
    hand: RawText,
    eye: RawText,
    skin: RawText,
}

#[derive(Deserialize)]
struct RawFigure {
    id: String,
    kind: String,
    ghs_codes: Vec<String>,
}

#[derive(Deserialize)]
struct RawIssue {
    code: String,
    message: String,
    path: String,
}

fn opt_text(t: RawText) -> Option<Text> {
    (!t.src.trim().is_empty()).then_some(Text { src: t.src, ko: t.ko })
}

fn texts(v: Vec<RawText>) -> Vec<Text> {
    v.into_iter().filter_map(opt_text).collect()
}

/// 알려진 코드만 남기고 중복을 뺀다(순서 유지).
fn codes(v: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    v.into_iter().filter(|c| ghs::name_ko(c).is_some() && seen.insert(c.clone())).collect()
}

fn pictogram(code: &str, image_id: Option<String>, basis: Basis) -> Pictogram {
    Pictogram { ghs_code: code.into(), name_ko: ghs::name_ko(code).unwrap_or_default().into(), image_id, basis }
}

/// Claude 응답 → 결과 타입. `figures`는 Claude에게 보낸 후보, `key`는 그림 번호 → 저장소 키다.
/// 받지 않은 후보 번호가 돌아오면 실패로 본다(오케스트레이터가 다시 보낸다).
pub fn convert(
    value: Value,
    path_prefix: &str,
    figures: &[&TaskFigure],
    key: impl Fn(&str) -> String,
) -> anyhow::Result<(HandlingOutput, Vec<Warning>)> {
    let raw: Raw = serde_json::from_value(value)?;
    let mut warnings = Vec::new();
    let warn = |code: &str, severity, message: String, path: String| Warning {
        code: code.into(),
        severity,
        message,
        path: Some(path),
    };
    let pictogram_path = format!("{path_prefix}.hazards.pictograms");

    let by_id: HashMap<&str, &TaskFigure> = figures.iter().map(|f| (f.id.as_str(), *f)).collect();
    let mut judged = HashSet::new();
    let mut images = Vec::new();
    let mut pictograms: Vec<Pictogram> = Vec::new();
    let mut handling_ids = Vec::new();
    for rf in raw.figures {
        let Some(f) = by_id.get(rf.id.as_str()) else { bail!("받지 않은 그림 후보 번호: {}", rf.id) };
        if !judged.insert(rf.id.clone()) {
            continue;
        }
        let kind = match rf.kind.as_str() {
            "pictogram" => ImageKind::Pictogram,
            "handling" => ImageKind::Handling,
            _ => continue,
        };
        let codes = if kind == ImageKind::Pictogram { codes(rf.ghs_codes) } else { Vec::new() };
        if kind == ImageKind::Pictogram && codes.is_empty() {
            warnings.push(warn("pictogram_without_code", Severity::Warn, format!("{}: 그림문자로 골랐지만 GHS 코드가 없음", rf.id), pictogram_path.clone()));
        }
        // 같은 그림문자가 여러 그림에 있으면 처음 것만 쓴다
        for c in &codes {
            if !pictograms.iter().any(|p| p.ghs_code == *c) {
                pictograms.push(pictogram(c, Some(rf.id.clone()), Basis::Document));
            }
        }
        if kind == ImageKind::Handling {
            handling_ids.push(rf.id.clone());
        }
        images.push(Image {
            id: rf.id.clone(),
            kind,
            page: f.figure.page,
            bbox_pt: f.figure.bbox_pt,
            key: key(&rf.id),
            // 한 그림에 그림문자가 여러 개면 코드를 하나로 정할 수 없다 → 비우고, 그림문자 쪽에서 이 그림을 가리킨다
            ghs_code: (codes.len() == 1).then(|| codes[0].clone()),
            basis: Basis::Document,
        });
    }
    let unjudged: Vec<&str> = figures.iter().map(|f| f.id.as_str()).filter(|id| !judged.contains(*id)).collect();
    if !unjudged.is_empty() {
        warnings.push(warn("figure_unjudged", Severity::Info, format!("판단하지 않은 그림 후보: {}", unjudged.join(", ")), pictogram_path.clone()));
    }

    // 추정은 문서에 그림문자가 하나도 없을 때만 쓴다(설계 5-1: 문서에 있는 것과 추론한 것을 나눈다)
    let inferred = codes(raw.inferred_pictograms);
    if pictograms.is_empty() {
        pictograms = inferred.iter().map(|c| pictogram(c, None, Basis::Inferred)).collect();
    } else if !inferred.is_empty() {
        warnings.push(warn(
            "inferred_pictograms_ignored",
            Severity::Info,
            format!("문서에 그림문자가 있어 추정값을 쓰지 않음: {}", inferred.join(", ")),
            pictogram_path,
        ));
    }

    for issue in raw.issues {
        let path = if issue.path.trim().is_empty() { format!("{path_prefix}.handling") } else { format!("{path_prefix}.handling.{}", issue.path.trim()) };
        warnings.push(warn(&format!("agent:{}", issue.code), Severity::Info, issue.message, path));
    }

    let handling = Handling {
        precautions: texts(raw.precautions),
        storage: texts(raw.storage),
        ppe: Ppe {
            respiratory: opt_text(raw.ppe.respiratory),
            hand: opt_text(raw.ppe.hand),
            eye: opt_text(raw.ppe.eye),
            skin: opt_text(raw.ppe.skin),
        },
        image_ids: handling_ids,
        pages: raw.pages,
    };
    Ok((HandlingOutput { handling, pictograms, images }, warnings))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pekko_agent_msds::profile::{Figure, FigureSource, PageImage};

    fn fig(id: &str, page: u32) -> TaskFigure {
        TaskFigure {
            id: id.into(),
            figure: Figure { page, bbox_pt: [10.0, 20.0, 30.0, 30.0], source: FigureSource::ImageObject, near_text: None },
            page_image: PageImage { page, key: format!("jobs/x/pages/{page}.png"), dpi: 150.0 },
        }
    }

    fn empty_text() -> Value {
        json!({ "src": "", "ko": "" })
    }

    fn reply(figures: Value, inferred: Value) -> Value {
        json!({
            "precautions": [ { "src": "Avoid contact with eyes.", "ko": "눈에 닿지 않게 한다." }, { "src": " ", "ko": "" } ],
            "storage": [ { "src": "Keep container closed.", "ko": "용기를 닫아 둔다." } ],
            "ppe": { "respiratory": empty_text(), "hand": { "src": "Nitrile gloves", "ko": "니트릴 장갑" }, "eye": empty_text(), "skin": empty_text() },
            "pages": [3],
            "figures": figures,
            "inferred_pictograms": inferred,
            "issues": []
        })
    }

    fn key(id: &str) -> String {
        format!("jobs/j/images/{id}.png")
    }

    #[test]
    fn schema_objects_all_closed_and_required() {
        fn walk(v: &Value, path: &str) {
            if let Some(obj) = v.as_object() {
                if obj.get("type") == Some(&json!("object")) {
                    assert_eq!(obj.get("additionalProperties"), Some(&json!(false)), "{path}");
                    let props: Vec<&String> = obj["properties"].as_object().unwrap().keys().collect();
                    let req: Vec<&str> = obj["required"].as_array().unwrap().iter().map(|r| r.as_str().unwrap()).collect();
                    for p in props {
                        assert!(req.contains(&p.as_str()), "{path}.{p} 필수 아님");
                    }
                }
                for (k, child) in obj {
                    walk(child, &format!("{path}.{k}"));
                }
            } else if let Some(arr) = v.as_array() {
                for c in arr {
                    walk(c, path);
                }
            }
        }
        walk(&schema(), "$");
    }

    #[test]
    fn document_pictograms_from_chosen_figures() {
        let (a, b, c) = (fig("fig-0", 1), fig("fig-1", 1), fig("fig-2", 1));
        let figures = json!([
            { "id": "fig-0", "kind": "other", "ghs_codes": [] },
            { "id": "fig-1", "kind": "pictogram", "ghs_codes": ["GHS02", "GHS07"] },
            { "id": "fig-2", "kind": "pictogram", "ghs_codes": ["GHS07"] }
        ]);
        let (out, warnings) = convert(reply(figures, json!(["GHS08"])), "products[0]", &[&a, &b, &c], key).unwrap();
        // 로고(other)는 저장하지 않는다
        let ids: Vec<&str> = out.images.iter().map(|i| i.id.as_str()).collect();
        assert_eq!(ids, ["fig-1", "fig-2"]);
        assert_eq!(out.images[0].ghs_code, None);
        assert_eq!(out.images[1].ghs_code.as_deref(), Some("GHS07"));
        assert_eq!(out.images[0].key, "jobs/j/images/fig-1.png");
        // GHS07은 처음 나온 그림(fig-1)을 가리킨다
        let p: Vec<(&str, Option<&str>, Basis)> =
            out.pictograms.iter().map(|p| (p.ghs_code.as_str(), p.image_id.as_deref(), p.basis)).collect();
        assert_eq!(p, [("GHS02", Some("fig-1"), Basis::Document), ("GHS07", Some("fig-1"), Basis::Document)]);
        assert_eq!(out.pictograms[0].name_ko, "인화성(불꽃)");
        // 문서에 그림문자가 있으면 추정값은 쓰지 않고 경고만 남긴다
        assert!(warnings.iter().any(|w| w.code == "inferred_pictograms_ignored"));
        // 빈 항목은 빠지고, 없는 보호구는 None
        assert_eq!(out.handling.precautions.len(), 1);
        assert_eq!(out.handling.ppe.hand.as_ref().unwrap().ko, "니트릴 장갑");
        assert_eq!(out.handling.ppe.eye, None);
    }

    #[test]
    fn inferred_when_no_pictogram_figure() {
        let a = fig("fig-0", 1);
        let figures = json!([{ "id": "fig-0", "kind": "other", "ghs_codes": [] }]);
        let (out, _) = convert(reply(figures, json!(["GHS02", "GHS02", "GHS08"])), "products[0]", &[&a], key).unwrap();
        let p: Vec<(&str, Option<&str>, Basis)> =
            out.pictograms.iter().map(|p| (p.ghs_code.as_str(), p.image_id.as_deref(), p.basis)).collect();
        assert_eq!(p, [("GHS02", None, Basis::Inferred), ("GHS08", None, Basis::Inferred)]);
        assert!(out.images.is_empty());
    }

    #[test]
    fn detects_all_empty_reply() {
        let empty = json!({
            "precautions": [], "storage": [], "pages": [], "figures": [], "inferred_pictograms": [], "issues": [],
            "ppe": { "respiratory": empty_text(), "hand": empty_text(), "eye": empty_text(), "skin": empty_text() }
        });
        assert!(looks_empty(&empty));
        assert!(!looks_empty(&reply(json!([]), json!([]))));
        // 메모가 있으면 에이전트가 이유를 남긴 것이라 비었다고 보지 않는다
        let mut with_issue = empty.clone();
        with_issue["issues"] = json!([{ "code": "no_content", "message": "7·8절이 비어 있음", "path": "" }]);
        assert!(!looks_empty(&with_issue));
    }

    #[test]
    fn unknown_figure_id_fails_and_missing_one_warns() {
        let (a, b) = (fig("fig-0", 1), fig("fig-1", 1));
        let bad = json!([{ "id": "fig-9", "kind": "pictogram", "ghs_codes": ["GHS05"] }]);
        assert!(convert(reply(bad, json!([])), "products[0]", &[&a, &b], key).is_err());

        let partial = json!([{ "id": "fig-0", "kind": "pictogram", "ghs_codes": ["GHS05"] }]);
        let (_, warnings) = convert(reply(partial, json!([])), "products[0]", &[&a, &b], key).unwrap();
        let w = warnings.iter().find(|w| w.code == "figure_unjudged").unwrap();
        assert!(w.message.contains("fig-1"));
    }
}
