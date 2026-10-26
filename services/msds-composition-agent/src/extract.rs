//! 성분 에이전트의 Claude 요청과 응답 변환.
//!
//! Claude에게는 빈 값을 `""`로 받는다. 구조화 출력은 `null`을 허용하는 필드(union 타입) 수에 상한이 있어서,
//! 빈 문자열로 받은 뒤 여기서 `None`으로 바꾼다. 함량 범위와 CAS 검사처럼 정확히 잴 수 있는 것은 코드가 한다.

use pekko_agent_llm::{ClaudeMessage, ContentBlock, ImageSource, LlmRequest};
use pekko_agent_msds::cas;
use pekko_agent_msds::ghs::HazardClass;
use pekko_agent_msds::message::AgentTask;
use pekko_agent_msds::output::CompositionOutput;
use pekko_agent_msds::result::{
    CasCheck, CodedText, Component, Concentration, GhsClass, HazardousSubstance, Hazards, Identification, Manufacturer,
    Rating, Severity, Text, Warning,
};
use serde::Deserialize;
use serde_json::{json, Value};

pub const PROMPT_VERSION: &str = "composition/3";
const SYSTEM_PROMPT: &str = include_str!("../prompts/composition.md");

/// 구조화 출력 스키마. 모든 객체에 `additionalProperties: false`, 모든 필드는 필수다.
///
/// 원문·한국어 쌍(`text`)과 코드 문구(`coded`)는 `$defs`에 한 번만 정의하고 `$ref`로 쓴다.
/// 같은 객체를 자리마다 풀어 쓰면 API가 "compiled grammar is too large"(400)로 거절한다(2026-10-23 확인).
pub fn schema() -> Value {
    let text = json!({ "$ref": "#/$defs/text" });
    let texts = json!({ "type": "array", "items": text });
    let coded = json!({ "type": "array", "items": { "$ref": "#/$defs/coded" } });
    let pages = json!({ "type": "array", "items": { "type": "integer" } });
    let s = json!({ "type": "string" });
    // 유해성 항목 + 맞는 항목이 없을 때 쓰는 "unknown"
    let mut classes: Vec<Value> = HazardClass::ALL.iter().map(|c| serde_json::to_value(c).unwrap()).collect();
    classes.push(json!("unknown"));
    json!({
        "type": "object",
        "properties": {
            "identification": {
                "type": "object",
                "properties": {
                    "product_name": s, "product_code": s,
                    "manufacturer_name": s, "manufacturer_address": s, "emergency_phone": s,
                    "recommended_use": text, "revision_date": s, "pages": pages
                },
                "required": ["product_name", "product_code", "manufacturer_name", "manufacturer_address",
                             "emergency_phone", "recommended_use", "revision_date", "pages"],
                "additionalProperties": false
            },
            "hazards": {
                "type": "object",
                "properties": {
                    "classifications": { "type": "array", "items": { "$ref": "#/$defs/classification" } }, "signal_word": text,
                    "h_statements": coded, "p_statements": coded,
                    "other_ratings": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "system": s,
                                "values": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": { "name": s, "value": s },
                                        "required": ["name", "value"],
                                        "additionalProperties": false
                                    }
                                }
                            },
                            "required": ["system", "values"],
                            "additionalProperties": false
                        }
                    },
                    "pages": pages
                },
                "required": ["classifications", "signal_word", "h_statements", "p_statements", "other_ratings", "pages"],
                "additionalProperties": false
            },
            "components": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": text, "cas_no": s, "cas_withheld": { "type": "boolean" },
                        "concentration_raw": s, "unit": s,
                        "exposure_limits": texts, "regulatory_marks": texts, "pages": pages
                    },
                    "required": ["name", "cas_no", "cas_withheld", "concentration_raw", "unit",
                                 "exposure_limits", "regulatory_marks", "pages"],
                    "additionalProperties": false
                }
            },
            "hazardous_substances": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": { "component_index": { "type": "integer" }, "reasons": texts },
                    "required": ["component_index", "reasons"],
                    "additionalProperties": false
                }
            },
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
        "required": ["identification", "hazards", "components", "hazardous_substances", "issues"],
        "additionalProperties": false,
        "$defs": {
            "text": {
                "type": "object",
                "properties": { "src": { "type": "string" }, "ko": { "type": "string" } },
                "required": ["src", "ko"],
                "additionalProperties": false
            },
            "coded": {
                "type": "object",
                "properties": { "code": { "type": "string" }, "src": { "type": "string" }, "ko": { "type": "string" } },
                "required": ["code", "src", "ko"],
                "additionalProperties": false
            },
            "classification": {
                "type": "object",
                "properties": {
                    "src": { "type": "string" }, "ko": { "type": "string" },
                    "class": { "type": "string", "enum": classes }, "category": { "type": "string" }
                },
                "required": ["src", "ko", "class", "category"],
                "additionalProperties": false
            }
        }
    })
}

/// 작업 메시지 → Claude 요청. 페이지 이미지는 `(쪽 번호, PNG)`로 받는다.
pub fn build_request(task: &AgentTask, images: &[(u32, Vec<u8>)]) -> LlmRequest {
    use base64::Engine as _;
    let mut content = Vec::new();
    for (page, png) in images {
        content.push(ContentBlock::Text { text: format!("페이지 {page} 이미지:") });
        content.push(ContentBlock::Image {
            source: ImageSource {
                media_type: "image/png".into(),
                data: base64::engine::general_purpose::STANDARD.encode(png),
            },
        });
    }
    let mut text = String::new();
    let note = if task.note.trim().is_empty() { "없음" } else { task.note.trim() };
    text.push_str(&format!("오케스트레이터 지시: {note}\n문서 언어: {:?}\n\n", task.language));
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
    content.push(ContentBlock::Text { text });
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
    identification: RawIdentification,
    hazards: RawHazards,
    components: Vec<RawComponent>,
    hazardous_substances: Vec<RawHazardous>,
    issues: Vec<RawIssue>,
}

#[derive(Deserialize)]
struct RawText {
    src: String,
    ko: String,
}

#[derive(Deserialize)]
struct RawIdentification {
    product_name: String,
    product_code: String,
    manufacturer_name: String,
    manufacturer_address: String,
    emergency_phone: String,
    recommended_use: RawText,
    revision_date: String,
    pages: Vec<u32>,
}

#[derive(Deserialize)]
struct RawCoded {
    code: String,
    src: String,
    ko: String,
}

#[derive(Deserialize)]
struct RawRating {
    system: String,
    values: Vec<RawRatingValue>,
}

#[derive(Deserialize)]
struct RawRatingValue {
    name: String,
    value: String,
}

#[derive(Deserialize)]
struct RawHazards {
    classifications: Vec<RawClassification>,
    signal_word: RawText,
    h_statements: Vec<RawCoded>,
    p_statements: Vec<RawCoded>,
    other_ratings: Vec<RawRating>,
    pages: Vec<u32>,
}

#[derive(Deserialize)]
struct RawComponent {
    name: RawText,
    cas_no: String,
    cas_withheld: bool,
    concentration_raw: String,
    unit: String,
    exposure_limits: Vec<RawText>,
    regulatory_marks: Vec<RawText>,
    pages: Vec<u32>,
}

#[derive(Deserialize)]
struct RawHazardous {
    component_index: i64,
    reasons: Vec<RawText>,
}

#[derive(Deserialize)]
struct RawClassification {
    src: String,
    ko: String,
    /// 유해성 항목 이름 또는 "unknown"
    class: String,
    category: String,
}

#[derive(Deserialize)]
struct RawIssue {
    code: String,
    message: String,
    path: String,
}

fn opt(s: String) -> Option<String> {
    let t = s.trim();
    (!t.is_empty()).then(|| t.to_string())
}

fn text(t: RawText) -> Text {
    Text { src: t.src, ko: t.ko }
}

fn opt_text(t: RawText) -> Option<Text> {
    (!t.src.trim().is_empty()).then(|| text(t))
}

fn texts(v: Vec<RawText>) -> Vec<Text> {
    v.into_iter().filter(|t| !t.src.trim().is_empty()).map(text).collect()
}

fn coded(v: Vec<RawCoded>) -> Vec<CodedText> {
    v.into_iter().map(|c| CodedText { code: c.code, src: c.src, ko: c.ko }).collect()
}

/// Claude 응답 → 결과 타입. CAS 검사·함량 범위는 여기서 코드로 채우고, 문제는 `warnings`로 돌려준다.
/// `path_prefix`는 결과 JSON 안에서 이 제품의 위치(예: `products[0]`)다.
pub fn convert(value: Value, path_prefix: &str) -> anyhow::Result<(CompositionOutput, Vec<Warning>)> {
    let raw: Raw = serde_json::from_value(value)?;
    let mut warnings = Vec::new();

    let i = raw.identification;
    let identification = Identification {
        product_name: opt(i.product_name),
        product_code: opt(i.product_code),
        manufacturer: opt(i.manufacturer_name).map(|name| Manufacturer {
            name,
            address: opt(i.manufacturer_address),
            emergency_phone: opt(i.emergency_phone),
        }),
        recommended_use: opt_text(i.recommended_use),
        revision_date: opt(i.revision_date).filter(|d| is_iso_date(d)),
        pages: i.pages,
    };

    let h = raw.hazards;
    let mut ghs_classes = Vec::new();
    let mut classifications = Vec::new();
    for (n, c) in h.classifications.into_iter().enumerate() {
        if c.src.trim().is_empty() {
            continue;
        }
        // 항목·구분이 표(지침 별표 2)에 있는 조합일 때만 대조에 쓴다
        match serde_json::from_value::<HazardClass>(Value::String(c.class.clone())) {
            Ok(class) if class.h_code(&c.category).is_some() => ghs_classes.push(GhsClass { class, category: c.category.trim().into() }),
            Ok(_) => warnings.push(Warning {
                code: "ghs_category_unknown".into(),
                severity: Severity::Info,
                message: format!("분류의 구분을 표에서 찾지 못함: {} {:?} ({})", c.class, c.category, c.src),
                path: Some(format!("{path_prefix}.hazards.classifications[{n}]")),
            }),
            Err(_) => {}
        }
        classifications.push(Text { src: c.src, ko: c.ko });
    }
    let hazards = Hazards {
        classifications,
        ghs_classes,
        signal_word: opt_text(h.signal_word),
        h_statements: coded(h.h_statements),
        p_statements: coded(h.p_statements),
        pictograms: Vec::new(),
        other_ratings: h
            .other_ratings
            .into_iter()
            .map(|r| Rating { system: r.system, values: r.values.into_iter().map(|v| (v.name, v.value)).collect() })
            .collect(),
        pages: h.pages,
    };

    let components: Vec<Component> = raw
        .components
        .into_iter()
        .enumerate()
        .map(|(n, c)| {
            let cas_no = opt(c.cas_no);
            let cas_check = match &cas_no {
                Some(no) => cas::check_list(no),
                None if c.cas_withheld => CasCheck::Withheld,
                None => CasCheck::Absent,
            };
            let code = match cas_check {
                CasCheck::ChecksumFailed => Some(("cas_checksum_failed", "CAS 체크 디지트가 맞지 않음")),
                CasCheck::Malformed => Some(("cas_malformed", "CAS 번호 형식이 아님")),
                _ => None,
            };
            if let Some((code, message)) = code {
                warnings.push(Warning {
                    code: code.into(),
                    severity: Severity::Warn,
                    message: format!("{message}: {}", cas_no.as_deref().unwrap_or_default()),
                    path: Some(format!("{path_prefix}.components[{n}].cas_no")),
                });
            }
            Component {
                name: text(c.name),
                cas_no,
                cas_check,
                concentration: opt(c.concentration_raw).map(|raw| parse_concentration(raw, opt(c.unit))),
                exposure_limits: texts(c.exposure_limits),
                regulatory_marks: texts(c.regulatory_marks),
                pages: c.pages,
            }
        })
        .collect();

    let mut hazardous_substances = Vec::new();
    for hs in raw.hazardous_substances {
        if hs.component_index < 0 || hs.component_index as usize >= components.len() {
            warnings.push(Warning {
                code: "hazardous_index_out_of_range".into(),
                severity: Severity::Warn,
                message: format!("위험물질이 없는 성분 번호를 가리킴: {}", hs.component_index),
                path: Some(format!("{path_prefix}.hazardous_substances")),
            });
            continue;
        }
        hazardous_substances.push(HazardousSubstance { component_index: hs.component_index as u32, reasons: texts(hs.reasons) });
    }

    for issue in raw.issues {
        warnings.push(Warning {
            code: format!("agent:{}", issue.code),
            severity: Severity::Info,
            message: issue.message,
            path: opt(issue.path).map(|p| format!("{path_prefix}.{p}")),
        });
    }

    Ok((CompositionOutput { identification, hazards, components, hazardous_substances }, warnings))
}

fn is_iso_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10 && b[4] == b'-' && b[7] == b'-' && s.chars().enumerate().all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit())
}

/// 함량 원문에서 범위를 읽는다. `"10 ~ 12"` → 10~12, `"< 1"` → ~1, `"> 30"` → 30~, `"12"` → 12~12.
/// 숫자가 없으면(예: "Trade secret") 범위를 비운다.
pub fn parse_concentration(raw: String, unit: Option<String>) -> Concentration {
    let nums: Vec<f64> = number_tokens(&raw).iter().filter_map(|t| t.parse().ok()).collect();
    let lower = raw.contains('<') || raw.contains('≤') || raw.contains("미만") || raw.contains("이하") || raw.to_lowercase().contains("less than");
    let upper = raw.contains('>') || raw.contains('≥') || raw.contains("초과") || raw.contains("이상") || raw.to_lowercase().contains("greater than");
    let (min, max) = match (nums.as_slice(), lower, upper) {
        ([a, b, ..], _, _) => (Some(a.min(*b)), Some(a.max(*b))),
        ([a], true, false) => (None, Some(*a)),
        ([a], false, true) => (Some(*a), None),
        ([a], _, _) => (Some(*a), Some(*a)),
        ([], _, _) => (None, None),
    };
    let unit = unit.or_else(|| raw.contains('%').then(|| "%".to_string()));
    Concentration { raw, min, max, unit }
}

fn number_tokens(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() || (c == '.' && !cur.is_empty()) {
            cur.push(c);
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conc(raw: &str) -> (Option<f64>, Option<f64>) {
        let c = parse_concentration(raw.into(), None);
        (c.min, c.max)
    }

    #[test]
    fn concentration_ranges() {
        assert_eq!(conc("10 ~ 12"), (Some(10.0), Some(12.0)));
        assert_eq!(conc("10-20%"), (Some(10.0), Some(20.0)));
        assert_eq!(conc("< 1"), (None, Some(1.0)));
        assert_eq!(conc("0.1 미만"), (None, Some(0.1)));
        assert_eq!(conc("> 30"), (Some(30.0), None));
        assert_eq!(conc("12"), (Some(12.0), Some(12.0)));
        assert_eq!(conc("Trade secret"), (None, None));
        assert_eq!(parse_concentration("5 %".into(), None).unit.as_deref(), Some("%"));
        assert_eq!(parse_concentration("5".into(), Some("wt%".into())).unit.as_deref(), Some("wt%"));
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
    fn convert_fills_cas_check_and_empty_values() {
        let v = json!({
            "identification": {
                "product_name": "POLANE Reducer", "product_code": "R99KY29",
                "manufacturer_name": "THE SHERWIN-WILLIAMS COMPANY", "manufacturer_address": "", "emergency_phone": "",
                "recommended_use": { "src": "", "ko": "" }, "revision_date": "2015-11-29", "pages": [1]
            },
            "hazards": {
                "classifications": [
                    { "src": "Flam. Liq. 2", "ko": "인화성 액체 구분 2", "class": "flammable_liquids", "category": "2" },
                    { "src": "Odd 9", "ko": "이상한 분류", "class": "flammable_liquids", "category": "9" },
                    { "src": "Not GHS", "ko": "GHS 아님", "class": "unknown", "category": "" }
                ],
                "signal_word": { "src": "", "ko": "" },
                "h_statements": [], "p_statements": [],
                "other_ratings": [ { "system": "HMIS", "values": [ { "name": "health", "value": "2*" } ] } ],
                "pages": [1]
            },
            "components": [
                { "name": { "src": "Toluene", "ko": "톨루엔" }, "cas_no": "108-88-3", "cas_withheld": false,
                  "concentration_raw": "12", "unit": "wt%", "exposure_limits": [ { "src": "ACGIH TLV 20 PPM", "ko": "ACGIH TLV 20 ppm" } ],
                  "regulatory_marks": [], "pages": [1] },
                { "name": { "src": "X", "ko": "X" }, "cas_no": "108-88-4", "cas_withheld": false,
                  "concentration_raw": "", "unit": "", "exposure_limits": [], "regulatory_marks": [], "pages": [1] },
                { "name": { "src": "Y", "ko": "Y" }, "cas_no": "", "cas_withheld": true,
                  "concentration_raw": "", "unit": "", "exposure_limits": [], "regulatory_marks": [], "pages": [1] }
            ],
            "hazardous_substances": [
                { "component_index": 0, "reasons": [ { "src": "ACGIH TLV 20 PPM", "ko": "노출기준 설정 물질" } ] },
                { "component_index": 9, "reasons": [] }
            ],
            "issues": [ { "code": "column_shift", "message": "열이 어긋남", "path": "components[1]" } ]
        });
        let (out, warnings) = convert(v, "products[0]").unwrap();
        assert_eq!(out.identification.manufacturer.as_ref().unwrap().address, None);
        assert_eq!(out.identification.recommended_use, None);
        assert_eq!(out.hazards.signal_word, None);
        assert_eq!(out.hazards.other_ratings[0].values["health"], "2*");
        let checks: Vec<CasCheck> = out.components.iter().map(|c| c.cas_check).collect();
        assert_eq!(checks, [CasCheck::Valid, CasCheck::ChecksumFailed, CasCheck::Withheld]);
        assert_eq!(out.components[0].concentration.as_ref().unwrap().max, Some(12.0));
        assert_eq!(out.components[1].concentration, None);
        assert_eq!(out.hazardous_substances.len(), 1);
        let codes: Vec<&str> = warnings.iter().map(|w| w.code.as_str()).collect();
        assert_eq!(codes, ["ghs_category_unknown", "cas_checksum_failed", "hazardous_index_out_of_range", "agent:column_shift"]);
        assert_eq!(out.hazards.classifications.len(), 3);
        assert_eq!(out.hazards.ghs_classes, [GhsClass { class: HazardClass::FlammableLiquids, category: "2".into() }]);
        assert_eq!(warnings[1].path.as_deref(), Some("products[0].components[1].cas_no"));
        assert_eq!(warnings[3].path.as_deref(), Some("products[0].components[1]"));
    }
}
