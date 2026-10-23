use serde::{Deserialize, Serialize};

/// MSDS 섹션 종류. 섹션 번호는 형식마다 뜻이 달라(옛 미국 형식은 2절이 성분) 번호 대신 종류로 묶는다.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
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
