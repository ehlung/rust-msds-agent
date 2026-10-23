//! 결과 JSON 스키마 `msds-result/1` (설계 문서 5절).
//!
//! 원칙: 원문과 한국어를 같이 둔다 / 근거 페이지를 남긴다 / 문서에 있는 것과 추론한 것을 나눈다 /
//! 없으면 비운다(지어내지 않는다) / 섹션 번호가 아니라 종류로 묶는다.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::profile::{DocFormat, Language, TextLayer};
use crate::section::SectionKind;

pub const SCHEMA: &str = "msds-result/1";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MsdsResult {
    pub schema: String,
    pub job_id: Uuid,
    pub document: DocumentInfo,
    pub products: Vec<Product>,
    pub images: Vec<Image>,
    pub warnings: Vec<Warning>,
    pub provenance: Vec<Provenance>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DocumentInfo {
    pub file_name: String,
    pub sha256: String,
    pub pages: u32,
    pub language: Language,
    pub text_layer: TextLayer,
}

/// 원문과 한국어. 한글 문서는 둘이 같다.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Text {
    pub src: String,
    pub ko: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Basis {
    /// 문서에 적혀 있다.
    Document,
    /// 문서에 없어 에이전트가 판단했다.
    Inferred,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Product {
    pub index: u32,
    pub pages: [u32; 2],
    pub format: DocFormat,
    pub identification: Identification,
    pub hazards: Hazards,
    pub components: Vec<Component>,
    pub hazardous_substances: Vec<HazardousSubstance>,
    pub handling: Handling,
    pub sections: Vec<TranslatedSection>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Identification {
    pub product_name: Option<String>,
    pub product_code: Option<String>,
    pub manufacturer: Option<Manufacturer>,
    pub recommended_use: Option<Text>,
    /// 문서에 적힌 날짜를 `YYYY-MM-DD`로. 읽을 수 없으면 비운다.
    pub revision_date: Option<String>,
    pub pages: Vec<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Manufacturer {
    pub name: String,
    pub address: Option<String>,
    pub emergency_phone: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Hazards {
    pub classifications: Vec<Text>,
    pub signal_word: Option<Text>,
    pub h_statements: Vec<CodedText>,
    pub p_statements: Vec<CodedText>,
    pub pictograms: Vec<Pictogram>,
    /// GHS 이전 등급(HMIS, NFPA 등).
    pub other_ratings: Vec<Rating>,
    pub pages: Vec<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodedText {
    pub code: String,
    pub src: String,
    pub ko: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Pictogram {
    /// `GHS01` ~ `GHS09`
    pub ghs_code: String,
    pub name_ko: String,
    pub image_id: Option<String>,
    pub basis: Basis,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Rating {
    pub system: String,
    pub values: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Component {
    pub name: Text,
    pub cas_no: Option<String>,
    pub cas_check: CasCheck,
    pub concentration: Option<Concentration>,
    pub exposure_limits: Vec<Text>,
    pub regulatory_marks: Vec<Text>,
    pub pages: Vec<u32>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CasCheck {
    Valid,
    /// 체크 디지트가 맞지 않는다.
    ChecksumFailed,
    /// CAS 번호 모양이 아니다.
    Malformed,
    /// 영업비밀 등으로 공개하지 않았다.
    Withheld,
    /// 문서에 CAS 번호가 없다.
    Absent,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Concentration {
    /// 원문 그대로 (예: `"10 ~ 12"`, `"< 1"`).
    pub raw: String,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub unit: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HazardousSubstance {
    pub component_index: u32,
    pub reasons: Vec<Text>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Handling {
    pub precautions: Vec<Text>,
    pub storage: Vec<Text>,
    pub ppe: Ppe,
    pub image_ids: Vec<String>,
    pub pages: Vec<u32>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Ppe {
    pub respiratory: Option<Text>,
    pub hand: Option<Text>,
    pub eye: Option<Text>,
    pub skin: Option<Text>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TranslatedSection {
    pub kind: SectionKind,
    pub number: u32,
    pub title: Text,
    pub body: Text,
    pub pages: Vec<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Image {
    pub id: String,
    pub kind: ImageKind,
    pub page: u32,
    /// PDF 좌표(pt)로 [왼쪽, 위, 너비, 높이].
    pub bbox_pt: [f32; 4],
    pub key: String,
    pub ghs_code: Option<String>,
    pub basis: Basis,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImageKind {
    Pictogram,
    Handling,
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Warning {
    pub code: String,
    pub severity: Severity,
    pub message: String,
    /// 결과 JSON 안의 위치 (예: `products[0].components[2].cas_no`).
    pub path: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Provenance {
    pub agent: crate::message::AgentKind,
    pub model: String,
    pub effort: crate::message::Effort,
    pub prompt_version: String,
    pub attempts: u32,
}
