//! 설계 문서(4·5절)의 JSON 예시가 타입으로 그대로 읽히고, 다시 쓰면 같은 JSON이 되는지 확인한다.
//! 문서와 코드 중 한쪽만 바뀌면 여기서 깨진다.

use pekko_agent_msds::message::*;
use pekko_agent_msds::profile::*;
use pekko_agent_msds::result::*;
use pekko_agent_msds::{topics, Envelope, SectionKind};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};

fn roundtrip<T: Serialize + DeserializeOwned>(v: Value) -> T {
    let typed: T = serde_json::from_value(v.clone()).expect("문서 예시를 타입으로 읽지 못함");
    assert_eq!(serde_json::to_value(&typed).unwrap(), v, "다시 쓴 JSON이 원래와 다름");
    typed
}

const JOB: &str = "6f1c2a3e-0000-4000-8000-000000000001";
const TASK: &str = "6f1c2a3e-0000-4000-8000-000000000002";

#[test]
fn document_profile() {
    let p: DocumentProfile = roundtrip(json!({
        "pages": 13,
        "language": "en",
        "text_layer": "ok",
        "products": [{
            "index": 0,
            "name_guess": "POLANE* CLEAR (Accelerated), Clear",
            "pages": [1, 4],
            "format": "legacy_us",
            "cas_found": 6,
            "sections": [{ "kind": "composition", "number": 2, "title": "COMPOSITION/INFORMATION ON INGREDIENTS", "pages": [1], "text": "12  108-88-3  Toluene" }]
        }],
        "figures": [{ "page": 1, "bbox_pt": [72.0, 400.0, 56.0, 56.0], "source": "image_object", "near_text": "Hazard pictograms" }],
        "page_images": [{ "page": 1, "key": "jobs/x/pages/1.png", "dpi": 150.0 }]
    }));
    assert_eq!(p.products[0].format, DocFormat::LegacyUs);
    assert_eq!(p.products[0].sections[0].kind, SectionKind::Composition);
}

#[test]
fn routing_plan() {
    let r: RoutingPlan = roundtrip(json!({
        "verdict": "msds",
        "plan": [
            { "agent": "composition", "product_index": 0, "sections": ["identification", "hazards", "composition", "regulatory"], "page_images": [1], "note": "옛 형식이라 성분 표의 열이 어긋남" },
            { "agent": "translate", "product_index": 0, "sections": [], "page_images": [], "note": "" },
            { "agent": "handling", "product_index": 0, "sections": ["handling_storage", "exposure_ppe"], "page_images": [], "note": "그림 후보 없음" }
        ],
        "skipped": [{ "agent": "translate", "product_index": 1, "reason": "한글 문서" }],
        "summary": "제품 3개가 든 옛 형식 MSDS"
    }));
    assert_eq!(r.plan[0].agent.task_topic(), topics::TASK_COMPOSITION);
}

#[test]
fn agent_task_in_envelope() {
    let e: Envelope<AgentTask> = roundtrip(json!({
        "event_id": TASK,
        "timestamp": "2026-10-23T01:00:00Z",
        "source_service": "msds-orchestrator",
        "event_type": "msds.task.composition",
        "tenant_id": "default",
        "correlation_id": JOB,
        "schema_version": 1,
        "payload": {
            "task_id": TASK,
            "job_id": JOB,
            "agent": "composition",
            "attempt": 1,
            "model": { "name": "claude-sonnet-5-5", "effort": "high" },
            "prompt_version": "composition/1",
            "product_index": 0,
            "language": "en",
            "sections": [{ "kind": "composition", "number": 2, "title": "COMPOSITION/INFORMATION ON INGREDIENTS", "pages": [1], "text": "..." }],
            "page_images": [{ "page": 1, "key": "jobs/x/pages/1.png", "dpi": 150.0 }],
            "figures": [],
            "note": "15절 SARA 표의 CAS로 교차 확인",
            "limits": { "max_tokens": 16000, "max_tool_rounds": 5 }
        }
    }));
    assert_eq!(e.key(), JOB);
}

/// 취급 작업에만 들어가는 그림 후보 (설계 문서 4-6).
#[test]
fn task_figure() {
    let f: TaskFigure = roundtrip(json!({
        "id": "fig-1",
        "page": 1,
        "bbox_pt": [72.0, 400.0, 56.0, 56.0],
        "source": "image_object",
        "near_text": "Hazard pictograms",
        "page_image": { "page": 1, "key": "jobs/x/pages/1.png", "dpi": 150.0 }
    }));
    assert_eq!(f.figure.source, FigureSource::ImageObject);
}

#[test]
fn task_result_ok_and_failed() {
    let ok: TaskResult = roundtrip(json!({
        "task_id": TASK, "job_id": JOB, "agent": "composition", "attempt": 1,
        "status": "ok",
        "output": { "components": [] },
        "warnings": [{ "code": "cas_checksum_failed", "severity": "warn", "message": "CAS 체크 디지트가 맞지 않음", "path": "products[0].components[2].cas_no" }],
        "usage": [{ "model": "claude-sonnet-5-5", "effort": "high", "input_tokens": 1200, "output_tokens": 800, "cache_read_tokens": 0, "cache_write_tokens": 0 }],
        "duration_ms": 4200
    }));
    assert!(matches!(ok.outcome, TaskOutcome::Ok { .. }));

    let failed: TaskResult = roundtrip(json!({
        "task_id": TASK, "job_id": JOB, "agent": "translate", "attempt": 2,
        "status": "failed",
        "error": { "code": "llm_timeout", "message": "호출 제한 시간 초과" },
        "warnings": [], "usage": [], "duration_ms": 60000
    }));
    assert!(matches!(failed.outcome, TaskOutcome::Failed { .. }));
}

#[test]
fn ingest_done_failed() {
    let d: IngestDone = roundtrip(json!({
        "job_id": JOB,
        "status": "failed",
        "error": { "code": "pdf_unreadable", "message": "PDF를 열 수 없음" }
    }));
    assert!(matches!(d.outcome, IngestOutcome::Failed { .. }));
}

/// 설계 문서 5-3의 POLANE Reducer 예시.
#[test]
fn polane_product() {
    let p: Product = roundtrip(json!({
        "index": 0,
        "pages": [1, 4],
        "format": "legacy_us",
        "identification": {
            "product_name": "POLANE Reducer",
            "product_code": "R99KY29",
            "manufacturer": { "name": "THE SHERWIN-WILLIAMS COMPANY", "address": "101 Prospect Avenue N.W. Cleveland, OH 44115", "emergency_phone": "(800) 424-9300" },
            "recommended_use": null,
            "revision_date": "2015-11-29",
            "pages": [1]
        },
        "hazards": {
            "classifications": [], "signal_word": null, "h_statements": [], "p_statements": [], "pictograms": [],
            "other_ratings": [{ "system": "HMIS", "values": { "flammability": "3", "health": "2*", "reactivity": "0" } }],
            "pages": [1, 2]
        },
        "components": [{
            "name": { "src": "Toluene", "ko": "톨루엔" },
            "cas_no": "108-88-3",
            "cas_check": "valid",
            "concentration": { "raw": "12", "min": 12.0, "max": 12.0, "unit": "wt%" },
            "exposure_limits": [{ "src": "ACGIH TLV 20 PPM", "ko": "ACGIH TLV 20 ppm" }],
            "regulatory_marks": [{ "src": "SARA 313", "ko": "SARA 313 보고 대상" }],
            "pages": [1, 4]
        }],
        "hazardous_substances": [{ "component_index": 0, "reasons": [{ "src": "ACGIH TLV 20 PPM", "ko": "노출기준 설정 물질" }] }],
        "handling": {
            "precautions": [{ "src": "Keep away from heat, sparks, and open flame.", "ko": "열·스파크·화염에서 멀리할 것" }],
            "storage": [{ "src": "Keep container closed when not in use.", "ko": "사용하지 않을 때는 용기를 닫아 둘 것" }],
            "ppe": {
                "respiratory": { "src": "organic vapor/particulate respirator approved by NIOSH/MSHA", "ko": "NIOSH/MSHA 인증 유기증기·입자 겸용 방독마스크" },
                "hand": { "src": "gloves recommended by glove supplier", "ko": "장갑 공급사가 권장하는 보호장갑" },
                "eye": { "src": "safety spectacles with unperforated sideshields", "ko": "측면 보호대가 있는 보안경" },
                "skin": null
            },
            "image_ids": [],
            "pages": [2, 3]
        },
        "sections": [{ "kind": "first_aid", "number": 4, "title": { "src": "FIRST AID MEASURES", "ko": "응급조치 요령" }, "body": { "src": "...", "ko": "..." }, "pages": [2] }]
    }));
    // 결과의 cas_check는 코드가 계산한 값과 같아야 한다
    assert_eq!(pekko_agent_msds::cas::check(p.components[0].cas_no.as_deref().unwrap()), p.components[0].cas_check);
}

#[test]
fn whole_result() {
    let r: MsdsResult = roundtrip(json!({
        "schema": SCHEMA,
        "job_id": JOB,
        "document": { "file_name": "R99KY29, POLANE ® Reducer.pdf", "sha256": "abc", "pages": 4, "language": "en", "text_layer": "ok" },
        "products": [],
        "images": [{ "id": "fig-1", "kind": "pictogram", "page": 1, "bbox_pt": [72.0, 400.0, 56.0, 56.0], "key": "jobs/x/images/fig-1.png", "ghs_code": "GHS02", "basis": "document" }],
        "warnings": [],
        "provenance": [{ "agent": "composition", "model": "claude-sonnet-5-5", "effort": "high", "prompt_version": "composition/1", "attempts": 1 }]
    }));
    assert_eq!(r.schema, "msds-result/1");
}

#[test]
fn every_section_kind_has_a_snake_case_name() {
    let names: Vec<String> = SectionKind::ALL.iter().map(|k| serde_json::to_value(k).unwrap().as_str().unwrap().to_string()).collect();
    assert_eq!(names.len(), 16);
    assert!(names.contains(&"handling_storage".to_string()));
    assert!(names.contains(&"exposure_ppe".to_string()));
}

#[test]
fn every_topic_is_listed_once() {
    let mut all = topics::ALL.to_vec();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), topics::ALL.len());
    for agent in [AgentKind::Translate, AgentKind::Composition, AgentKind::Handling] {
        assert!(topics::ALL.contains(&agent.task_topic()));
    }
}
