//! 결과를 코드로 검사한다. 오케스트레이터가 합칠 때와, 검토자가 고친 결과를 저장할 때(api-gateway)
//! 같은 코드를 쓴다. 규칙을 한 곳에만 두려는 것이다(설계 6절 5번).

use crate::result::{CasCheck, MsdsResult, Product, Severity, Warning};
use crate::{cas, ghs};

/// 이 모듈이 만드는 경고 코드. 다시 검사할 때 이 코드의 경고만 지우고 새로 만든다.
pub const CODES: &[&str] = &[
    "cas_checksum_failed",
    "cas_malformed",
    "classification_h_mismatch",
    "pictogram_missing",
    "pictogram_precedence",
    "pictogram_without_h",
];

/// 검토자가 고친 결과를 다시 검사한다.
///
/// - 코드가 정하는 값(CAS 검사 결과, 그림문자 이름)을 다시 계산한다. 검토자가 CAS 번호나 그림문자를 바꿨을 수 있다.
/// - 이 모듈의 경고는 지우고 지금 값으로 다시 만든다. 에이전트가 남긴 경고(메모, 실패 등)는 그대로 둔다.
/// - 표에 없는 그림문자 코드는 대조할 수 없어 오류로 돌려준다.
pub fn recheck(result: &mut MsdsResult) -> Result<(), String> {
    for (pi, p) in result.products.iter().enumerate() {
        if let Some(x) = p.hazards.pictograms.iter().find(|x| ghs::name_ko(&x.ghs_code).is_none()) {
            return Err(format!("products[{pi}]: 모르는 그림문자 코드 {:?}", x.ghs_code));
        }
    }
    result.warnings.retain(|w| !CODES.contains(&w.code.as_str()));
    let mut warnings = Vec::new();
    for (pi, p) in result.products.iter_mut().enumerate() {
        for x in &mut p.hazards.pictograms {
            x.name_ko = ghs::name_ko(&x.ghs_code).unwrap_or_default().into();
        }
        for (ci, c) in p.components.iter_mut().enumerate() {
            // 화면에서 칸을 비우면 빈 문자열로 온다 → 없음으로 본다
            if c.cas_no.as_deref().is_some_and(|no| no.trim().is_empty()) {
                c.cas_no = None;
            }
            c.cas_check = match c.cas_no.as_deref() {
                Some(no) => cas::check_list(no),
                None if c.cas_check == CasCheck::Withheld => CasCheck::Withheld,
                None => CasCheck::Absent,
            };
            warnings.extend(cas_warning(c.cas_no.as_deref(), c.cas_check, format!("products[{pi}].components[{ci}].cas_no")));
        }
    }
    pictograms(&result.products, &mut warnings);
    result.warnings.extend(warnings);
    Ok(())
}

/// CAS 검사 결과가 틀렸으면 경고 하나. 성분 에이전트와 [`recheck`]가 같은 문구를 쓴다.
pub fn cas_warning(cas_no: Option<&str>, check: CasCheck, path: String) -> Option<Warning> {
    let (code, message) = match check {
        CasCheck::ChecksumFailed => ("cas_checksum_failed", "CAS 체크 디지트가 맞지 않음"),
        CasCheck::Malformed => ("cas_malformed", "CAS 번호 형식이 아님"),
        _ => return None,
    };
    Some(Warning {
        code: code.into(),
        severity: Severity::Warn,
        message: format!("{message}: {}", cas_no.unwrap_or_default()),
        path: Some(path),
    })
}

/// 성분 에이전트가 뽑은 유해성 정보와 취급 에이전트가 고른 그림문자가 맞는지 코드로 비교한다(설계 5-5).
/// 근거는 문서의 H 문구와, 분류(`ghs_classes`)를 지침 표로 바꾼 H 코드 둘을 합친 것이다.
/// H 문구 없이 분류만 적은 문서(예: LOCTITE)도 대조하기 위해서다.
/// 고치지 않고 경고만 남긴다. 문서 자체가 기준과 다를 수도 있어서 판단은 검토하는 사람이 한다.
/// 근거가 둘 다 없는 제품(옛 형식 등)은 건너뛴다.
pub fn pictograms(products: &[Product], warnings: &mut Vec<Warning>) {
    for (pi, p) in products.iter().enumerate() {
        let stated: Vec<u16> = p.hazards.h_statements.iter().flat_map(|s| s.code.split('+')).filter_map(ghs::h_number).collect();
        let from_classes: Vec<u16> = p.hazards.ghs_classes.iter().filter_map(|c| c.class.h_code(&c.category)).collect();
        // 분류에는 있는데 H 문구에는 없는 것(예: ARALDITE 경화제의 H318) — 문서 안의 불일치라 안내로 남긴다.
        // 그림문자 대조가 목적이라 그림문자가 있는 코드만 본다(H402처럼 그림문자 없는 코드는 빼는 문서가 많다)
        if !stated.is_empty() {
            let missing: Vec<String> = from_classes
                .iter()
                .filter(|h| !stated.contains(h) && !ghs::pictograms_for_h(**h).is_empty())
                .map(|h| format!("H{h}"))
                .collect();
            if !missing.is_empty() {
                warnings.push(Warning {
                    code: "classification_h_mismatch".into(),
                    severity: Severity::Info,
                    message: format!("분류에는 해당하는데 H 문구에 없음: {}", missing.join(", ")),
                    path: Some(format!("products[{pi}].hazards.h_statements")),
                });
            }
        }
        let h: Vec<u16> = stated.iter().chain(&from_classes).copied().collect();
        if h.is_empty() {
            continue;
        }
        let expected = ghs::expected(&h);
        let got: Vec<&str> = p.hazards.pictograms.iter().map(|x| x.ghs_code.as_str()).collect();
        let path = Some(format!("products[{pi}].hazards.pictograms"));
        let name = |c: &str| ghs::name_ko(c).unwrap_or("");
        // 해당하는 그림문자가 4개를 넘으면 4개만 표시할 수 있다 → 그때 빠진 것은 안내로만 남긴다
        let capped = expected.required.len() > ghs::MAX_SHOWN && got.len() >= ghs::MAX_SHOWN;
        for (code, sources) in &expected.required {
            if !got.contains(code) {
                let hs: Vec<String> = sources.iter().map(|h| format!("H{h}")).collect();
                // 그림문자 이름이 받침으로 끝나기도 해서 조사를 붙이지 않는 모양으로 쓴다
                let mut message = format!("필요한 그림문자 없음: {code} {} ({})", name(code), hs.join("·"));
                if capped {
                    message.push_str(&format!(" (해당 그림문자가 {}개라 {}개만 표시했을 수 있음)", expected.required.len(), ghs::MAX_SHOWN));
                }
                warnings.push(Warning {
                    code: "pictogram_missing".into(),
                    severity: if capped { Severity::Info } else { Severity::Warn },
                    message,
                    path: path.clone(),
                });
            }
        }
        for code in &got {
            if expected.required.contains_key(code) {
                continue;
            }
            let (code_name, severity, message) = match expected.suppressed.get(code) {
                Some(reason) => ("pictogram_precedence", Severity::Info, format!("{code} {}: {reason} (문서에 표시되어 있음)", name(code))),
                None => ("pictogram_without_h", Severity::Warn, format!("{code} {}의 근거가 되는 H 코드가 없음", name(code))),
            };
            warnings.push(Warning { code: code_name.into(), severity, message, path: path.clone() });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::DocFormat;
    use crate::result::{Component, Text};

    fn product(ko: &str) -> Product {
        Product {
            index: 0,
            pages: [1, 1],
            format: DocFormat::LegacyUs,
            identification: Default::default(),
            hazards: Default::default(),
            components: vec![Component {
                name: Text { src: "n-Butyl Acetate".into(), ko: ko.into() },
                cas_no: Some("123-86-4".into()),
                cas_check: CasCheck::Valid,
                concentration: None,
                exposure_limits: vec![],
                regulatory_marks: vec![],
                pages: vec![],
            }],
            hazardous_substances: vec![],
            handling: Default::default(),
            sections: vec![],
        }
    }

    #[test]
    fn pictograms_checked_against_h_codes() {
        use crate::result::{Basis, CodedText, Pictogram};
        let mut p = product("x");
        let h = |code: &str| CodedText { code: code.into(), src: String::new(), ko: String::new() };
        let pic = |code: &str| Pictogram { ghs_code: code.into(), name_ko: String::new(), image_id: None, basis: Basis::Document };
        // HF: H310(급성 독성)이 있는데 GHS07도 표시, GHS08(H372)은 빠짐, GHS09는 근거 없음
        p.hazards.h_statements = vec![h("H290"), h("H310"), h("H314"), h("H317"), h("H372")];
        p.hazards.pictograms = vec![pic("GHS05"), pic("GHS06"), pic("GHS07"), pic("GHS09")];
        let mut warnings = vec![];
        pictograms(&[p.clone()], &mut warnings);
        let codes: Vec<&str> = warnings.iter().map(|w| w.code.as_str()).collect();
        assert_eq!(codes, ["pictogram_missing", "pictogram_precedence", "pictogram_without_h"]);
        assert!(warnings[0].message.contains("GHS08") && warnings[0].message.contains("H372"));
        assert!(warnings[1].message.contains("GHS07"));
        assert!(warnings[2].message.contains("GHS09"));

        // 합쳐진 코드(H300+H310)도 나눠 본다. 맞으면 경고 없음
        p.hazards.h_statements = vec![h("H300+H310"), h("H412")];
        p.hazards.pictograms = vec![pic("GHS06")];
        let mut warnings = vec![];
        pictograms(&[p.clone()], &mut warnings);
        assert!(warnings.is_empty(), "{warnings:?}");

        // 5개가 해당하는데 4개만 표시했으면 빠진 것은 안내(info)
        p.hazards.h_statements = vec![h("H225"), h("H290"), h("H301"), h("H351"), h("H410")];
        p.hazards.pictograms = vec![pic("GHS02"), pic("GHS05"), pic("GHS06"), pic("GHS08")];
        let mut warnings = vec![];
        pictograms(&[p.clone()], &mut warnings);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].severity, Severity::Info);
        assert!(warnings[0].message.contains("GHS09") && warnings[0].message.contains("4개만"));

        // H 문구 없이 분류만 있어도 대조한다(LOCTITE)
        use crate::ghs::HazardClass;
        use crate::result::GhsClass;
        let class = |class, category: &str| GhsClass { class, category: category.into() };
        p.hazards.h_statements = vec![];
        p.hazards.ghs_classes = vec![
            class(HazardClass::SkinCorrosionIrritation, "1B"),
            class(HazardClass::SeriousEyeDamageIrritation, "1"),
            class(HazardClass::SkinSensitization, "1"),
        ];
        p.hazards.pictograms = vec![pic("GHS05")];
        let mut warnings = vec![];
        pictograms(&[p.clone()], &mut warnings);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].code == "pictogram_missing" && warnings[0].message.contains("GHS07") && warnings[0].message.contains("H317"));

        // 분류에는 있는데 H 문구에 없으면 안내(ARALDITE 경화제: 분류 H318, H 문구 H314·H317)
        p.hazards.h_statements = vec![h("H314"), h("H317")];
        p.hazards.pictograms = vec![pic("GHS05"), pic("GHS07")];
        let mut warnings = vec![];
        pictograms(&[p.clone()], &mut warnings);
        let codes: Vec<&str> = warnings.iter().map(|w| w.code.as_str()).collect();
        assert_eq!(codes, ["classification_h_mismatch"]);
        assert!(warnings[0].message.contains("H318"));

        // 근거가 둘 다 없으면 비교하지 않는다
        p.hazards.ghs_classes = vec![];
        p.hazards.h_statements = vec![];
        let mut warnings = vec![];
        pictograms(&[p], &mut warnings);
        assert!(warnings.is_empty());
    }


    fn result_with(p: Product) -> MsdsResult {
        use crate::profile::{Language, TextLayer};
        use crate::result::DocumentInfo;
        MsdsResult {
            schema: crate::result::SCHEMA.into(),
            job_id: uuid::Uuid::nil(),
            document: DocumentInfo { file_name: "a.pdf".into(), sha256: "s".into(), pages: 1, language: Language::En, text_layer: TextLayer::Ok },
            products: vec![p],
            images: vec![],
            warnings: vec![],
            provenance: vec![],
        }
    }

    #[test]
    fn recheck_recomputes_code_values_and_keeps_agent_warnings() {
        use crate::result::{Basis, CodedText, Pictogram};
        let mut p = product("아세트산 n-뷰틸");
        // 검토자가 CAS를 틀리게 고치고, 그림문자를 이름 없이 넣었다
        p.components[0].cas_no = Some("123-86-5".into());
        p.hazards.h_statements = vec![CodedText { code: "H225".into(), src: String::new(), ko: String::new() }];
        p.hazards.pictograms = vec![Pictogram { ghs_code: "GHS07".into(), name_ko: String::new(), image_id: None, basis: Basis::Document }];
        let mut r = result_with(p);
        let old = |code: &str| Warning { code: code.into(), severity: Severity::Warn, message: "이전".into(), path: None };
        r.warnings = vec![old("agent:note"), old("pictogram_missing"), old("cas_malformed")];

        recheck(&mut r).unwrap();
        let p = &r.products[0];
        assert_eq!(p.components[0].cas_check, CasCheck::ChecksumFailed);
        assert_eq!(p.hazards.pictograms[0].name_ko, ghs::name_ko("GHS07").unwrap());
        // 에이전트 경고는 남고, 코드 경고는 지금 값으로 다시 생긴다
        let codes: Vec<&str> = r.warnings.iter().map(|w| w.code.as_str()).collect();
        assert_eq!(codes, ["agent:note", "cas_checksum_failed", "pictogram_missing", "pictogram_without_h"]);
        assert!(r.warnings[1..].iter().all(|w| w.message != "이전"));
    }

    #[test]
    fn recheck_handles_cleared_cas_and_unknown_codes() {
        use crate::result::{Basis, Pictogram};
        // 칸을 비우면 CAS 없음
        let mut p = product("x");
        p.components[0].cas_no = Some("  ".into());
        let mut r = result_with(p.clone());
        recheck(&mut r).unwrap();
        assert_eq!(r.products[0].components[0].cas_no, None);
        assert_eq!(r.products[0].components[0].cas_check, CasCheck::Absent);

        // 영업비밀 표시는 CAS가 없을 때 그대로 둔다
        p.components[0].cas_no = None;
        p.components[0].cas_check = CasCheck::Withheld;
        let mut r = result_with(p.clone());
        recheck(&mut r).unwrap();
        assert_eq!(r.products[0].components[0].cas_check, CasCheck::Withheld);

        p.hazards.pictograms = vec![Pictogram { ghs_code: "GHS10".into(), name_ko: String::new(), image_id: None, basis: Basis::Document }];
        let mut r = result_with(p);
        assert!(recheck(&mut r).unwrap_err().contains("GHS10"));
    }
}
