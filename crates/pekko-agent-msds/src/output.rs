//! 에이전트별 결과(`TaskOutcome::Ok.output`)의 타입. 결과 스키마(5절) 중 그 에이전트가 맡은 부분이다.

use serde::{Deserialize, Serialize};

use crate::result::{Component, HazardousSubstance, Hazards, Identification, TranslatedSection};

/// 성분 에이전트의 결과 (제품 하나). `hazards.pictograms`는 취급 에이전트 몫이라 비워 둔다.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CompositionOutput {
    pub identification: Identification,
    pub hazards: Hazards,
    pub components: Vec<Component>,
    pub hazardous_substances: Vec<HazardousSubstance>,
}

/// 번역 에이전트의 결과 (제품 하나). 받은 섹션을 같은 순서로 원문·한국어를 함께 담는다.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TranslateOutput {
    pub sections: Vec<TranslatedSection>,
}
