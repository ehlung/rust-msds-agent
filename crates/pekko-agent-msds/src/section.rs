use serde::{Deserialize, Serialize};

/// MSDS 섹션 종류. 섹션 번호는 형식마다 뜻이 달라(옛 미국 형식은 2절이 성분) 번호 대신 종류로 묶는다.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
#[serde(rename_all = "snake_case")]
pub enum SectionKind {
    Identification,
    Hazards,
    Composition,
    FirstAid,
    Firefighting,
    AccidentalRelease,
    HandlingStorage,
    ExposurePpe,
    PhysicalChemical,
    StabilityReactivity,
    Toxicology,
    Ecology,
    Disposal,
    Transport,
    Regulatory,
    Other,
}

impl SectionKind {
    /// 국내 MSDS 16개 항목 명칭. 번역 결과의 섹션 제목(`title.ko`)으로 쓴다.
    /// 제목은 정해진 이름이라 Claude에게 번역시키지 않고 코드가 붙인다.
    pub fn title_ko(self) -> &'static str {
        match self {
            Self::Identification => "화학제품과 회사에 관한 정보",
            Self::Hazards => "유해성·위험성",
            Self::Composition => "구성성분의 명칭 및 함유량",
            Self::FirstAid => "응급조치 요령",
            Self::Firefighting => "폭발·화재 시 대처방법",
            Self::AccidentalRelease => "누출 사고 시 대처방법",
            Self::HandlingStorage => "취급 및 저장방법",
            Self::ExposurePpe => "노출방지 및 개인보호구",
            Self::PhysicalChemical => "물리화학적 특성",
            Self::StabilityReactivity => "안정성 및 반응성",
            Self::Toxicology => "독성에 관한 정보",
            Self::Ecology => "환경에 미치는 영향",
            Self::Disposal => "폐기 시 주의사항",
            Self::Transport => "운송에 필요한 정보",
            Self::Regulatory => "법적 규제현황",
            Self::Other => "그 밖의 참고사항",
        }
    }

    pub const ALL: [SectionKind; 16] = [
        Self::Identification,
        Self::Hazards,
        Self::Composition,
        Self::FirstAid,
        Self::Firefighting,
        Self::AccidentalRelease,
        Self::HandlingStorage,
        Self::ExposurePpe,
        Self::PhysicalChemical,
        Self::StabilityReactivity,
        Self::Toxicology,
        Self::Ecology,
        Self::Disposal,
        Self::Transport,
        Self::Regulatory,
        Self::Other,
    ];
}
