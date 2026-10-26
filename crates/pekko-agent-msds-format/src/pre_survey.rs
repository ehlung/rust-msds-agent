//! 사전조사서: 성분 하나가 한 행인 표(설계 5-8).
//!
//! 고객 양식 샘플이 없어 공식 서식과 표준을 근거로 열을 정했다.
//! - 성분 열: MSDS 3항 「구성성분의 명칭 및 함유량」, 화학물질관리법 시행규칙 별지 제1호서식 「화학물질 확인명세서」
//!   (화학물질명·CAS No.·함량 또는 함량범위(%))
//! - 물질 하나가 한 행, 영업비밀 물질은 CAS를 비움: IPC-1752A·IEC 62474 성분 신고
//! - 국내 법규 해당 여부: 화평법(인체등유해성물질·허가·제한·금지), 화관법(사고대비물질), 산업안전보건법 대상.
//!   공식 목록과 대조하는 일은 `RegulationCheck`(구현: `kr_lists::KrLists`)가 맡고, 대조하지 않은 칸은 "미대조"로 둔다
//!   (빈칸이면 "해당 없음"으로 읽힐 수 있다).
//!
//! 결과에 없는 값은 지어내지 않는다(결과 원칙 4). 성분이 없는 제품도 한 행을 남겨 제품이 빠지지 않게 한다.

use pekko_agent_msds::result::{CasCheck, Component, MsdsResult, Product, Text};

use crate::table::{Column, Table};

/// 확인완료 정보. 사전조사서는 확인완료한 버전으로만 만든다(설계 3-3: 게시는 확인한 버전을 쓴다).
#[derive(Clone, Debug)]
pub struct Approval {
    pub version: i32,
    pub by: String,
    /// RFC 3339, UTC (예: `2026-10-26T07:40:17Z`). 서버마다 시간대가 달라도 같은 값이 되게 UTC로 둔다.
    pub at: String,
}

/// 성분마다 해당 여부를 보는 국내 규제 목록.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KrList {
    /// 화평법 제2조·제20조 인체등유해성물질 — 2025-08-07부터 종전 유독물질을 셋으로 나눴다
    AcuteToxic,
    ChronicToxic,
    EcoToxic,
    /// 화평법 제25조·제27조
    Permitted,
    Restricted,
    Prohibited,
    /// 화학물질관리법 제39조
    AccidentPrecaution,
    /// 산업안전보건법
    ManagedHazardous,
    WorkEnvMeasurement,
    SpecialHealthExam,
}

impl KrList {
    pub const ALL: [KrList; 10] = [
        KrList::AcuteToxic,
        KrList::ChronicToxic,
        KrList::EcoToxic,
        KrList::Permitted,
        KrList::Restricted,
        KrList::Prohibited,
        KrList::AccidentPrecaution,
        KrList::ManagedHazardous,
        KrList::WorkEnvMeasurement,
        KrList::SpecialHealthExam,
    ];

    pub fn column(self) -> Column {
        let (key, title) = match self {
            KrList::AcuteToxic => ("kr_acute_toxic", "화평법 인체급성유해성물질"),
            KrList::ChronicToxic => ("kr_chronic_toxic", "화평법 인체만성유해성물질"),
            KrList::EcoToxic => ("kr_eco_toxic", "화평법 생태유해성물질"),
            KrList::Permitted => ("kr_permitted", "화평법 허가물질"),
            KrList::Restricted => ("kr_restricted", "화평법 제한물질"),
            KrList::Prohibited => ("kr_prohibited", "화평법 금지물질"),
            KrList::AccidentPrecaution => ("kr_accident_precaution", "화관법 사고대비물질"),
            KrList::ManagedHazardous => ("kr_managed_hazardous", "산안법 관리대상유해물질"),
            KrList::WorkEnvMeasurement => ("kr_work_env_measurement", "산안법 작업환경측정 대상"),
            KrList::SpecialHealthExam => ("kr_special_health_exam", "산안법 특수건강진단 대상"),
        };
        Column { key, title }
    }

    pub fn from_key(key: &str) -> Option<KrList> {
        KrList::ALL.into_iter().find(|l| l.column().key == key)
    }
}

/// 성분 하나를 목록 하나와 대조한 결과.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Listing {
    /// CAS가 목록에 있고 이미 시행 중이다.
    Listed,
    /// CAS가 목록에 있지만 이 날짜(`YYYY-MM-DD`)부터 시행된다.
    ListedFrom(String),
    /// CAS가 목록에 없다. 목록에는 CAS 없이 묶음으로 적힌 항목("~ 및 그 화합물", "~ 등")도 있어,
    /// CAS가 없다는 것만으로 "해당 없음"이라고 단정하지 않는다.
    NotListedByCas,
    /// 대조하지 않았거나 할 수 없다(CAS 없음·비공개, 목록 없음).
    NotChecked,
}

/// 성분이 국내 규제 목록에 드는지.
/// 목록은 미리 메모리에 올려 두는 구현을 전제로 한다. 그래야 wasm32(공개 페이지)에서도 같은 코드가 돈다.
pub trait RegulationCheck {
    fn check(&self, component: &Component, list: KrList) -> Listing;

    /// 시행 여부를 판단한 날짜(`YYYY-MM-DD`). 대조하지 않으면 없다.
    fn as_of(&self) -> Option<&str> {
        None
    }
}

/// 대조하지 않는다. 모든 칸이 "미대조"다.
pub struct NotChecked;

impl RegulationCheck for NotChecked {
    fn check(&self, _: &Component, _: KrList) -> Listing {
        Listing::NotChecked
    }
}

const fn col(key: &'static str, title: &'static str) -> Column {
    Column { key, title }
}

/// 국내 법규 열 앞의 열. 순서가 곧 표의 열 순서다.
const BEFORE_KR: [Column; 21] = [
    col("file_name", "원본 파일"),
    col("product_no", "제품 번호"),
    col("product_name", "제품명"),
    col("product_code", "제품코드"),
    col("manufacturer", "제조사"),
    col("revision_date", "MSDS 개정일"),
    col("signal_word", "신호어"),
    col("pictograms", "그림문자"),
    col("h_codes", "H 코드"),
    col("component_no", "성분 번호"),
    col("name_ko", "물질명(국문)"),
    col("name_src", "물질명(원문)"),
    col("cas_no", "CAS No."),
    col("cas_check", "CAS 검사"),
    col("concentration_raw", "함유량(원문)"),
    col("concentration_min", "함유량 하한"),
    col("concentration_max", "함유량 상한"),
    col("concentration_unit", "함유량 단위"),
    col("exposure_limits", "노출기준(문서)"),
    col("regulatory_marks", "규제 표시(문서 15항)"),
    col("hazard_reasons", "위험물질 근거(문서)"),
];

const AFTER_KR: [Column; 5] = [
    col("kr_as_of", "법규 대조 기준일"),
    col("pages", "근거 쪽"),
    col("approved_version", "확인 버전"),
    col("approved_by", "확인자"),
    col("approved_at", "확인 시각(UTC)"),
];

pub fn columns() -> Vec<Column> {
    BEFORE_KR.into_iter().chain(KrList::ALL.map(KrList::column)).chain(AFTER_KR).collect()
}

/// 확인완료한 결과를 사전조사서 표로 바꾼다. 여러 제품이 든 문서는 모든 제품의 성분을 행으로 펼친다
/// (기존 기능은 첫 제품만 저장했다).
pub fn pre_survey(result: &MsdsResult, approval: &Approval, regulations: &dyn RegulationCheck) -> Table {
    let mut rows = Vec::new();
    for product in &result.products {
        let head = product_cells(&result.document.file_name, product);
        if product.components.is_empty() {
            // 성분·법규·기준일·근거 쪽 칸을 비운다
            let blank = BEFORE_KR.len() - head.len() + KrList::ALL.len() + 2;
            rows.push(head.into_iter().chain(std::iter::repeat_n(String::new(), blank)).chain(approval_cells(approval)).collect());
            continue;
        }
        for (i, component) in product.components.iter().enumerate() {
            let mut row = head.clone();
            row.extend(component_cells(product, i, component));
            row.extend(KrList::ALL.map(|list| kr_cell(regulations.check(component, list))));
            row.push(regulations.as_of().unwrap_or_default().to_string());
            row.push(join_pages(&component.pages));
            row.extend(approval_cells(approval));
            rows.push(row);
        }
    }
    Table { columns: columns(), rows }
}

fn product_cells(file_name: &str, p: &Product) -> Vec<String> {
    let id = &p.identification;
    let hz = &p.hazards;
    vec![
        file_name.to_string(),
        (p.index + 1).to_string(),
        id.product_name.clone().unwrap_or_default(),
        id.product_code.clone().unwrap_or_default(),
        id.manufacturer.as_ref().map(|m| m.name.clone()).unwrap_or_default(),
        id.revision_date.clone().unwrap_or_default(),
        hz.signal_word.as_ref().map(|t| t.ko.clone()).unwrap_or_default(),
        hz.pictograms.iter().map(|x| if x.name_ko.is_empty() { x.ghs_code.clone() } else { format!("{} {}", x.ghs_code, x.name_ko) }).collect::<Vec<_>>().join(", "),
        hz.h_statements.iter().map(|h| h.code.as_str()).filter(|c| !c.is_empty()).collect::<Vec<_>>().join(", "),
    ]
}

fn component_cells(p: &Product, i: usize, c: &Component) -> Vec<String> {
    let (cas, check) = match c.cas_check {
        CasCheck::Valid => (c.cas_no.clone().unwrap_or_default(), "정상"),
        // 틀린 번호도 그대로 보여 줘야 검토자가 원문과 대조할 수 있다
        CasCheck::ChecksumFailed => (c.cas_no.clone().unwrap_or_default(), "체크 디지트 불일치"),
        CasCheck::Malformed => (c.cas_no.clone().unwrap_or_default(), "형식 오류"),
        // 영업비밀 물질은 CAS를 비운다(IPC-1752A와 같은 처리). 대신 검사 칸에 이유를 둔다
        CasCheck::Withheld => (String::new(), "비공개"),
        CasCheck::Absent => (String::new(), "없음"),
    };
    let conc = c.concentration.as_ref();
    let reasons = p.hazardous_substances.iter().filter(|h| h.component_index as usize == i).flat_map(|h| &h.reasons);
    vec![
        (i + 1).to_string(),
        c.name.ko.clone(),
        c.name.src.clone(),
        cas,
        check.to_string(),
        conc.map(|x| x.raw.clone()).unwrap_or_default(),
        conc.and_then(|x| x.min).map(number).unwrap_or_default(),
        conc.and_then(|x| x.max).map(number).unwrap_or_default(),
        conc.and_then(|x| x.unit.clone()).unwrap_or_default(),
        join_ko(&c.exposure_limits),
        join_ko(&c.regulatory_marks),
        join_ko(reasons),
    ]
}

fn approval_cells(a: &Approval) -> [String; 3] {
    [a.version.to_string(), a.by.clone(), a.at.clone()]
}

fn kr_cell(listing: Listing) -> String {
    match listing {
        Listing::Listed => "해당".into(),
        Listing::ListedFrom(date) => format!("해당({date} 시행)"),
        Listing::NotListedByCas => "비해당(CAS 기준)".into(),
        Listing::NotChecked => "미대조".into(),
    }
}

/// `30.0` → `30`, `0.5` → `0.5`
fn number(v: f64) -> String {
    v.to_string()
}

fn join_ko<'a>(texts: impl IntoIterator<Item = &'a Text>) -> String {
    texts.into_iter().map(|t| if t.ko.is_empty() { t.src.as_str() } else { t.ko.as_str() }).collect::<Vec<_>>().join("; ")
}

fn join_pages(pages: &[u32]) -> String {
    pages.iter().map(u32::to_string).collect::<Vec<_>>().join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pekko_agent_msds::profile::{DocFormat, Language, TextLayer};
    use pekko_agent_msds::result::{Basis, CodedText, Concentration, DocumentInfo, HazardousSubstance, Hazards, Identification, Manufacturer, Pictogram, SCHEMA};

    fn text(src: &str, ko: &str) -> Text {
        Text { src: src.into(), ko: ko.into() }
    }

    fn component(name: (&str, &str), cas: Option<&str>, check: CasCheck, conc: Option<(&str, Option<f64>, Option<f64>)>) -> Component {
        Component {
            name: text(name.0, name.1),
            cas_no: cas.map(String::from),
            cas_check: check,
            concentration: conc.map(|(raw, min, max)| Concentration { raw: raw.into(), min, max, unit: Some("wt%".into()) }),
            exposure_limits: vec![text("ACGIH TLV 20 PPM", "ACGIH TLV 20 ppm")],
            regulatory_marks: vec![],
            pages: vec![1, 4],
        }
    }

    fn product(index: u32, name: &str, components: Vec<Component>) -> Product {
        Product {
            index,
            pages: [1, 4],
            format: DocFormat::Ghs,
            identification: Identification {
                product_name: Some(name.into()),
                manufacturer: Some(Manufacturer { name: "ACME, Inc.".into(), address: None, emergency_phone: None }),
                revision_date: Some("2015-11-29".into()),
                ..Default::default()
            },
            hazards: Hazards {
                signal_word: Some(text("Danger", "위험")),
                h_statements: vec![CodedText { code: "H225".into(), src: String::new(), ko: String::new() }, CodedText { code: "H361".into(), src: String::new(), ko: String::new() }],
                pictograms: vec![Pictogram { ghs_code: "GHS02".into(), name_ko: "인화성(불꽃)".into(), image_id: None, basis: Basis::Document }],
                ..Default::default()
            },
            components,
            hazardous_substances: vec![HazardousSubstance { component_index: 0, reasons: vec![text("SARA 313", "SARA 313 보고 대상")] }],
            handling: Default::default(),
            sections: vec![],
        }
    }

    /// 제품 2개: 성분 2개(하나는 영업비밀) + 성분 없는 제품
    fn sample() -> MsdsResult {
        MsdsResult {
            schema: SCHEMA.into(),
            job_id: uuid::Uuid::nil(),
            document: DocumentInfo { file_name: "sample.pdf".into(), sha256: "0".into(), pages: 4, language: Language::En, text_layer: TextLayer::Ok },
            products: vec![
                product(
                    0,
                    "CLEAR",
                    vec![
                        component(("Toluene", "톨루엔"), Some("108-88-3"), CasCheck::Valid, Some(("10 ~ 30", Some(10.0), Some(30.0)))),
                        component(("Trade secret", "영업비밀"), Some("Proprietary"), CasCheck::Withheld, Some(("< 0.5", None, Some(0.5)))),
                    ],
                ),
                product(1, "Catalyst", vec![]),
            ],
            images: vec![],
            warnings: vec![],
            provenance: vec![],
        }
    }

    fn approval() -> Approval {
        Approval { version: 2, by: "admin".into(), at: "2026-10-26T07:40:17Z".into() }
    }

    #[test]
    fn one_row_per_component_across_products() {
        let t = pre_survey(&sample(), &approval(), &NotChecked);
        assert_eq!(t.rows.len(), 3, "제품 1의 성분 2개 + 성분 없는 제품 2");
        assert!(t.rows.iter().all(|r| r.len() == t.columns.len()));
        assert_eq!(t.cell(0, "product_no"), Some("1"));
        assert_eq!(t.cell(0, "pictograms"), Some("GHS02 인화성(불꽃)"));
        assert_eq!(t.cell(0, "h_codes"), Some("H225, H361"));
        assert_eq!(t.cell(0, "name_ko"), Some("톨루엔"));
        assert_eq!(t.cell(0, "cas_no"), Some("108-88-3"));
        assert_eq!(t.cell(0, "concentration_min"), Some("10"));
        assert_eq!(t.cell(0, "concentration_max"), Some("30"));
        assert_eq!(t.cell(0, "hazard_reasons"), Some("SARA 313 보고 대상"));
        assert_eq!(t.cell(0, "pages"), Some("1, 4"));
        // 근거는 해당 성분에만 붙는다
        assert_eq!(t.cell(1, "hazard_reasons"), Some(""));
        // 성분 없는 제품도 한 행 남는다
        assert_eq!(t.cell(2, "product_name"), Some("Catalyst"));
        assert_eq!(t.cell(2, "name_ko"), Some(""));
        assert_eq!(t.cell(2, "kr_acute_toxic"), Some(""));
        assert_eq!(t.cell(2, "approved_version"), Some("2"));
    }

    #[test]
    fn withheld_cas_is_blank() {
        let t = pre_survey(&sample(), &approval(), &NotChecked);
        assert_eq!(t.cell(1, "cas_no"), Some(""));
        assert_eq!(t.cell(1, "cas_check"), Some("비공개"));
        assert_eq!(t.cell(1, "concentration_min"), Some(""));
        assert_eq!(t.cell(1, "concentration_max"), Some("0.5"));
    }

    #[test]
    fn regulations_default_to_not_checked() {
        let t = pre_survey(&sample(), &approval(), &NotChecked);
        for list in KrList::ALL {
            assert_eq!(t.cell(0, list.column().key), Some("미대조"));
        }
    }

    #[test]
    fn regulation_checker_fills_cells() {
        struct Toluene;
        impl RegulationCheck for Toluene {
            fn check(&self, c: &Component, list: KrList) -> Listing {
                match (c.cas_no.as_deref().filter(|_| c.cas_check == CasCheck::Valid), list) {
                    (None, _) => Listing::NotChecked,
                    (Some("108-88-3"), KrList::ManagedHazardous) => Listing::Listed,
                    (Some("108-88-3"), KrList::EcoToxic) => Listing::ListedFrom("2026-12-19".into()),
                    _ => Listing::NotListedByCas,
                }
            }
            fn as_of(&self) -> Option<&str> {
                Some("2026-10-26")
            }
        }
        let t = pre_survey(&sample(), &approval(), &Toluene);
        assert_eq!(t.cell(0, "kr_managed_hazardous"), Some("해당"));
        assert_eq!(t.cell(0, "kr_eco_toxic"), Some("해당(2026-12-19 시행)"));
        assert_eq!(t.cell(0, "kr_acute_toxic"), Some("비해당(CAS 기준)"));
        assert_eq!(t.cell(0, "kr_as_of"), Some("2026-10-26"));
        assert_eq!(t.cell(1, "kr_managed_hazardous"), Some("미대조"), "CAS가 없으면 대조할 수 없다");
        // 성분 없는 제품은 기준일도 비운다
        assert_eq!(t.cell(2, "kr_as_of"), Some(""));
    }

    #[test]
    fn column_keys_are_unique() {
        let cols = columns();
        let mut keys: Vec<_> = cols.iter().map(|c| c.key).collect();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), cols.len());
    }

    #[test]
    fn csv_keeps_commas_inside_cells() {
        let csv = pre_survey(&sample(), &approval(), &NotChecked).to_csv();
        let first = csv.split("\r\n").nth(1).unwrap();
        assert!(first.starts_with("sample.pdf,1,CLEAR,,\"ACME, Inc.\",2015-11-29,위험,GHS02 인화성(불꽃),\"H225, H361\",1,톨루엔,Toluene,108-88-3,"), "{first}");
    }
}
