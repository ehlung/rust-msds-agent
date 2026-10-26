//! GHS 그림문자 코드와 한국어 이름, H 코드별로 표시해야 하는 그림문자.
//! 정해진 값이라 Claude에게 맡기지 않고 코드가 쓴다.
//!
//! 근거: KOSHA GUIDE W-14-2022 「경고표지 작성 지침」(고용노동부고시 제2020-130호 기준) 4장 (3) 우선순위,
//! <별표 2> 유해성·위험성별 경고표지 요소 할당. 2026-10-26 대조. 고시가 개정되면 다시 대조한다.

use std::collections::{BTreeMap, BTreeSet};

pub const CODES: [&str; 9] = ["GHS01", "GHS02", "GHS03", "GHS04", "GHS05", "GHS06", "GHS07", "GHS08", "GHS09"];

/// 그림문자의 한국어 이름(기호 모양). 모르는 코드면 `None`.
pub fn name_ko(code: &str) -> Option<&'static str> {
    Some(match code {
        "GHS01" => "폭발성(폭탄의 폭발)",
        "GHS02" => "인화성(불꽃)",
        "GHS03" => "산화성(원 위의 불꽃)",
        "GHS04" => "고압가스(가스 실린더)",
        "GHS05" => "부식성",
        "GHS06" => "급성 독성(해골과 X자형 뼈)",
        "GHS07" => "경고(감탄부호)",
        "GHS08" => "건강 유해성",
        "GHS09" => "수생환경 유해성",
        _ => return None,
    })
}

/// `"H225"`, `"H360FD"`, `"H361d"` → 225, 360, 361. 모양이 다르면 `None`.
pub fn h_number(code: &str) -> Option<u16> {
    let digits = code.trim().strip_prefix('H')?.get(..3)?;
    digits.chars().all(|c| c.is_ascii_digit()).then(|| digits.parse().ok()).flatten()
}

/// H 코드 하나가 요구하는 그림문자. 그림문자가 없는 코드(H227·H303·H412 등)와 모르는 코드는 빈 배열.
pub fn pictograms_for_h(h: u16) -> &'static [&'static str] {
    match h {
        // H205(폭발성 등급 1.5)는 폭탄 그림이 아니라 주황색 바탕에 숫자 1.5라 GHS01이 아니다
        200..=204 | 240 => &["GHS01"],
        241 => &["GHS01", "GHS02"],
        220 | 222..=226 | 228 | 232 | 242 | 250..=252 | 260 | 261 => &["GHS02"],
        270..=272 => &["GHS03"],
        280 | 281 => &["GHS04"],
        290 | 314 | 318 => &["GHS05"],
        300 | 301 | 310 | 311 | 330 | 331 => &["GHS06"],
        302 | 312 | 332 | 315 | 317 | 319 | 335 | 336 | 420 => &["GHS07"],
        // H305(흡인 유해성 구분 2)도 GHS08이다(EU CLP에는 구분 2가 없어 다르다)
        304 | 305 | 334 | 340 | 341 | 350 | 351 | 360 | 361 | 370..=373 => &["GHS08"],
        400 | 410 | 411 => &["GHS09"],
        _ => &[],
    }
}

/// H 코드 목록으로 정한 그림문자.
#[derive(Debug, Default, PartialEq)]
pub struct Expected {
    /// 표시해야 하는 그림문자와 그 근거 H 코드.
    pub required: BTreeMap<&'static str, Vec<u16>>,
    /// H 코드로는 해당하지만 우선순위 규칙 때문에 표시하지 않는 그림문자와 이유.
    pub suppressed: BTreeMap<&'static str, &'static str>,
}

/// 지침상 그림문자가 이보다 많이 해당하면 우선순위에 따라 이만큼만 표시할 수 있다.
pub const MAX_SHOWN: usize = 4;

/// H 코드 목록 → 표시해야 하는 그림문자. 우선순위 규칙을 적용한다.
/// - GHS06이 있으면 GHS07은 표시하지 않는다.
/// - GHS05가 있으면 피부·눈 자극(H315·H319) 때문의 GHS07은 표시하지 않는다.
/// - 호흡기 과민성(H334)으로 GHS08이 있으면 피부 과민성·피부·눈 자극(H317·H315·H319) 때문의 GHS07은 표시하지 않는다.
pub fn expected(h_codes: &[u16]) -> Expected {
    let set: BTreeSet<u16> = h_codes.iter().copied().collect();
    let mut required: BTreeMap<&'static str, Vec<u16>> = BTreeMap::new();
    for &h in &set {
        for &p in pictograms_for_h(h) {
            required.entry(p).or_default().push(h);
        }
    }
    let mut suppressed = BTreeMap::new();
    if let Some(sources) = required.get("GHS07").cloned() {
        let reason = if required.contains_key("GHS06") {
            Some("GHS06(급성 독성)이 있으면 GHS07은 표시하지 않음")
        } else {
            let corrosive = required.contains_key("GHS05");
            let resp_sens = set.contains(&334);
            let left: Vec<u16> = sources
                .iter()
                .copied()
                .filter(|h| !(corrosive && matches!(h, 315 | 319)) && !(resp_sens && matches!(h, 315 | 317 | 319)))
                .collect();
            if left.is_empty() {
                Some(if corrosive && !resp_sens {
                    "GHS05(부식성)가 있으면 피부·눈 자극 때문의 GHS07은 표시하지 않음"
                } else {
                    "호흡기 과민성(GHS08)이 있으면 피부 과민성·자극 때문의 GHS07은 표시하지 않음"
                })
            } else {
                required.insert("GHS07", left);
                None
            }
        };
        if let Some(r) = reason {
            required.remove("GHS07");
            suppressed.insert("GHS07", r);
        }
    }
    Expected { required, suppressed }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_has_a_name() {
        assert!(CODES.iter().all(|c| name_ko(c).is_some()));
        assert_eq!(name_ko("GHS10"), None);
    }

    #[test]
    fn parses_h_numbers() {
        assert_eq!(h_number("H225"), Some(225));
        assert_eq!(h_number("H360FD"), Some(360));
        assert_eq!(h_number(" H361d"), Some(361));
        assert_eq!(h_number("EUH066"), None);
        assert_eq!(h_number("H2"), None);
    }

    fn codes(e: &Expected) -> Vec<&str> {
        e.required.keys().copied().collect()
    }

    #[test]
    fn maps_and_applies_precedence() {
        // NYCOTE88: 인화성 액체, 눈·피부 자극, 피부 과민성, 생식독성, 장기 독성, 수생 만성 3(그림문자 없음)
        let e = expected(&[225, 319, 315, 317, 361, 373, 412]);
        assert_eq!(codes(&e), ["GHS02", "GHS07", "GHS08"]);
        // ARALDITE 경화제: 부식성이 있어도 피부 과민성(H317) 때문의 GHS07은 남는다
        assert_eq!(codes(&expected(&[314, 317])), ["GHS05", "GHS07"]);
        // 부식성 + 피부 자극만 → GHS07은 빠진다
        let e = expected(&[314, 315]);
        assert_eq!(codes(&e), ["GHS05"]);
        assert!(e.suppressed.contains_key("GHS07"));
        // 급성 독성(GHS06)이 있으면 GHS07은 어떤 이유든 빠진다
        let e = expected(&[310, 317]);
        assert_eq!(codes(&e), ["GHS06"]);
        assert!(e.suppressed["GHS07"].contains("GHS06"));
        // 호흡기 과민성이면 피부 과민성 때문의 GHS07은 빠진다
        assert_eq!(codes(&expected(&[334, 317])), ["GHS08"]);
        // 그림문자가 없는 코드만 있으면 비어 있다
        assert!(expected(&[412, 227]).required.is_empty());
        assert_eq!(expected(&[241]).required["GHS01"], [241]);
        // 지침 별표 2와 대조해 고친 두 곳
        assert!(expected(&[205]).required.is_empty());
        assert_eq!(codes(&expected(&[305])), ["GHS08"]);
    }
}
