//! GHS 그림문자 코드와 한국어 이름, H 코드별로 표시해야 하는 그림문자.
//! 정해진 값이라 Claude에게 맡기지 않고 코드가 쓴다.
//!
//! 근거: KOSHA GUIDE W-14-2022 「경고표지 작성 지침」(고용노동부고시 제2020-130호 기준) 4장 (3) 우선순위,
//! <별표 2> 유해성·위험성별 경고표지 요소 할당. 2026-10-26 대조. 고시가 개정되면 다시 대조한다.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

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

/// 유해성 항목(GHS 유해성 분류). 지침 4장 (1)의 항목이고, 급성 독성은 노출 경로별, 과민성은 호흡기·피부, 수생환경은 급성·만성으로 나눴다.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
#[serde(rename_all = "snake_case")]
pub enum HazardClass {
    Explosives,
    FlammableGases,
    Aerosols,
    OxidizingGases,
    GasesUnderPressure,
    FlammableLiquids,
    FlammableSolids,
    SelfReactive,
    PyrophoricLiquids,
    PyrophoricSolids,
    SelfHeating,
    WaterReactive,
    OxidizingLiquids,
    OxidizingSolids,
    OrganicPeroxides,
    CorrosiveToMetals,
    AcuteToxicityOral,
    AcuteToxicityDermal,
    AcuteToxicityInhalation,
    SkinCorrosionIrritation,
    SeriousEyeDamageIrritation,
    RespiratorySensitization,
    SkinSensitization,
    GermCellMutagenicity,
    Carcinogenicity,
    ReproductiveToxicity,
    StotSingle,
    StotRepeated,
    Aspiration,
    AquaticAcute,
    AquaticChronic,
    OzoneLayer,
}

impl HazardClass {
    pub const ALL: [HazardClass; 32] = {
        use HazardClass::*;
        [
            Explosives, FlammableGases, Aerosols, OxidizingGases, GasesUnderPressure, FlammableLiquids, FlammableSolids,
            SelfReactive, PyrophoricLiquids, PyrophoricSolids, SelfHeating, WaterReactive, OxidizingLiquids,
            OxidizingSolids, OrganicPeroxides, CorrosiveToMetals, AcuteToxicityOral, AcuteToxicityDermal,
            AcuteToxicityInhalation, SkinCorrosionIrritation, SeriousEyeDamageIrritation, RespiratorySensitization,
            SkinSensitization, GermCellMutagenicity, Carcinogenicity, ReproductiveToxicity, StotSingle, StotRepeated,
            Aspiration, AquaticAcute, AquaticChronic, OzoneLayer,
        ]
    };

    /// 항목·구분 → 그 구분의 H 코드(지침 <별표 2>). 구분 값은 `1`, `1A`, `2B`, `A`~`G`, `1.1`~`1.6`,
    /// `unstable`, `compressed`·`liquefied`·`refrigerated`·`dissolved`, `pyrophoric`, `lactation` 꼴이다(대소문자 무관).
    /// 표에 없는 조합이면 `None`.
    pub fn h_code(self, category: &str) -> Option<u16> {
        use HazardClass::*;
        let c = category.trim().to_ascii_uppercase();
        Some(match (self, c.as_str()) {
            (Explosives, "UNSTABLE") => 200,
            (Explosives, "1.1") => 201,
            (Explosives, "1.2") => 202,
            (Explosives, "1.3") => 203,
            (Explosives, "1.4") => 204,
            (Explosives, "1.5") => 205,
            (FlammableGases, "1" | "1A" | "1B") => 220,
            (FlammableGases, "2") => 221,
            (FlammableGases, "PYROPHORIC") => 232,
            (Aerosols, "1") => 222,
            (Aerosols, "2") => 223,
            (Aerosols, "3") => 229,
            (OxidizingGases, "1") => 270,
            (GasesUnderPressure, "COMPRESSED" | "LIQUEFIED" | "DISSOLVED") => 280,
            (GasesUnderPressure, "REFRIGERATED") => 281,
            (FlammableLiquids, "1") => 224,
            (FlammableLiquids, "2") => 225,
            (FlammableLiquids, "3") => 226,
            (FlammableLiquids, "4") => 227,
            (FlammableSolids, "1" | "2") => 228,
            (SelfReactive | OrganicPeroxides, "A") => 240,
            (SelfReactive | OrganicPeroxides, "B") => 241,
            (SelfReactive | OrganicPeroxides, "C" | "D" | "E" | "F") => 242,
            (PyrophoricLiquids | PyrophoricSolids, "1") => 250,
            (SelfHeating, "1") => 251,
            (SelfHeating, "2") => 252,
            (WaterReactive, "1") => 260,
            (WaterReactive, "2" | "3") => 261,
            (OxidizingLiquids | OxidizingSolids, "1") => 271,
            (OxidizingLiquids | OxidizingSolids, "2" | "3") => 272,
            (CorrosiveToMetals, "1") => 290,
            (AcuteToxicityOral, "1" | "2") => 300,
            (AcuteToxicityOral, "3") => 301,
            (AcuteToxicityOral, "4") => 302,
            (AcuteToxicityOral, "5") => 303,
            (AcuteToxicityDermal, "1" | "2") => 310,
            (AcuteToxicityDermal, "3") => 311,
            (AcuteToxicityDermal, "4") => 312,
            (AcuteToxicityDermal, "5") => 313,
            (AcuteToxicityInhalation, "1" | "2") => 330,
            (AcuteToxicityInhalation, "3") => 331,
            (AcuteToxicityInhalation, "4") => 332,
            (AcuteToxicityInhalation, "5") => 333,
            (SkinCorrosionIrritation, "1" | "1A" | "1B" | "1C") => 314,
            (SkinCorrosionIrritation, "2") => 315,
            // 지침 별표 2에는 없지만 GHS 구분이라 미국 문서에 나온다. 그림문자는 없다(H316·H401·H402)
            (SkinCorrosionIrritation, "3") => 316,
            (SeriousEyeDamageIrritation, "1") => 318,
            (SeriousEyeDamageIrritation, "2" | "2A") => 319,
            (SeriousEyeDamageIrritation, "2B") => 320,
            (RespiratorySensitization, "1" | "1A" | "1B") => 334,
            (SkinSensitization, "1" | "1A" | "1B") => 317,
            (GermCellMutagenicity, "1" | "1A" | "1B") => 340,
            (GermCellMutagenicity, "2") => 341,
            (Carcinogenicity, "1" | "1A" | "1B") => 350,
            (Carcinogenicity, "2") => 351,
            (ReproductiveToxicity, "1" | "1A" | "1B") => 360,
            (ReproductiveToxicity, "2") => 361,
            (ReproductiveToxicity, "LACTATION") => 362,
            (StotSingle, "1") => 370,
            (StotSingle, "2") => 371,
            // 구분 3은 호흡기 자극(H335)과 마취 작용(H336) 두 가지지만 그림문자는 둘 다 GHS07이다
            (StotSingle, "3") => 335,
            (StotRepeated, "1") => 372,
            (StotRepeated, "2") => 373,
            (Aspiration, "1") => 304,
            (Aspiration, "2") => 305,
            (AquaticAcute, "1") => 400,
            (AquaticAcute, "2") => 401,
            (AquaticAcute, "3") => 402,
            (AquaticChronic, "1") => 410,
            (AquaticChronic, "2") => 411,
            (AquaticChronic, "3") => 412,
            (AquaticChronic, "4") => 413,
            (OzoneLayer, "1") => 420,
            _ => return None,
        })
    }
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

    #[test]
    fn class_and_category_to_h_code() {
        use HazardClass::*;
        // LOCTITE: H 코드 없이 분류만 있는 문서
        assert_eq!(SkinCorrosionIrritation.h_code("1B"), Some(314));
        assert_eq!(SeriousEyeDamageIrritation.h_code("1"), Some(318));
        assert_eq!(SkinSensitization.h_code("1"), Some(317));
        assert_eq!(FlammableLiquids.h_code("2"), Some(225));
        assert_eq!(SeriousEyeDamageIrritation.h_code("2a"), Some(319));
        assert_eq!(OrganicPeroxides.h_code("E"), Some(242));
        assert_eq!(GasesUnderPressure.h_code("refrigerated"), Some(281));
        assert_eq!(Explosives.h_code("1.6"), None);
        assert_eq!(FlammableLiquids.h_code("5"), None);
        // ROYCO·NYCOTE88의 수생 급성 구분 3: 표에 있지만 그림문자는 없다
        assert_eq!(AquaticAcute.h_code("3"), Some(402));
        assert!(pictograms_for_h(402).is_empty());
        // 이름이 스키마 enum 값과 같은지
        assert_eq!(serde_json::to_value(StotSingle).unwrap(), "stot_single");
        assert_eq!(HazardClass::ALL.len(), 32);
    }
}
