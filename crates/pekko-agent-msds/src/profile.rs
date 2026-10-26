//! 문서 프로필: msds-ingest가 코드로 잰 문서 상태. 오케스트레이터가 경로를 판단할 때 보는 입력이다.

use serde::{Deserialize, Serialize};

use crate::section::SectionKind;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub struct DocumentProfile {
    pub pages: u32,
    pub language: Language,
    pub text_layer: TextLayer,
    pub products: Vec<ProductProfile>,
    pub figures: Vec<Figure>,
    pub page_images: Vec<PageImage>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
#[serde(rename_all = "snake_case")]
pub enum Language {
    En,
    Ko,
}

/// 텍스트 레이어 상태.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
#[serde(rename_all = "snake_case")]
pub enum TextLayer {
    /// 텍스트가 정상으로 읽힌다.
    Ok,
    /// 텍스트는 있지만 일부 글자가 다른 글자로 나온다(예: 전→젂).
    MappingErrors,
    /// 텍스트가 없다(스캔본).
    None,
}

/// 섹션 순서 형식.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
#[serde(rename_all = "snake_case")]
pub enum DocFormat {
    /// 2절 유해성, 3절 성분.
    Ghs,
    /// 2절 성분, 3절 유해성 (옛 미국 MSDS).
    LegacyUs,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub struct ProductProfile {
    pub index: u32,
    pub name_guess: Option<String>,
    /// 시작·끝 쪽 (1부터, 양 끝 포함).
    pub pages: [u32; 2],
    pub format: DocFormat,
    /// 성분 섹션에서 찾은 CAS 번호 형식 문자열 수.
    pub cas_found: u32,
    pub sections: Vec<SectionText>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub struct SectionText {
    pub kind: SectionKind,
    /// 문서에 적힌 섹션 번호.
    pub number: u32,
    pub title: String,
    pub pages: Vec<u32>,
    pub text: String,
}

/// 그림 후보. 고르는 일은 취급 에이전트가 한다.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub struct Figure {
    pub page: u32,
    /// PDF 좌표(pt)로 [왼쪽, 위, 너비, 높이]. 위는 페이지 위쪽에서 잰 거리.
    pub bbox_pt: [f32; 4],
    pub source: FigureSource,
    pub near_text: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
#[serde(rename_all = "snake_case")]
pub enum FigureSource {
    ImageObject,
    Vector,
    TextAnchor,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub struct PageImage {
    pub page: u32,
    /// 파일 저장소 키. 예: `jobs/<job_id>/pages/1.png`
    pub key: String,
    /// 렌더링 해상도. 그림 후보 좌표(pt)를 픽셀로 바꿀 때 쓴다(픽셀 = pt × dpi / 72).
    /// 이 필드가 생기기 전 프로필은 기본값 150으로 렌더링했다.
    #[serde(default = "default_dpi")]
    pub dpi: f32,
}

fn default_dpi() -> f32 {
    150.0
}
