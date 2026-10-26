//! 국내 규제 목록 대조(설계 5-8). 공식 고시·별표에서 뽑은 CAS 목록(`data/kr_lists.tsv`)을 프로그램에 넣어 두고 CAS로 찾는다.
//!
//! 파일을 읽지 않고 `include_str!`로 넣었다. 그래서 gateway와 공개 페이지(wasm32)가 같은 목록·같은 코드로 대조한다.
//! 데이터는 `examples/build_kr_lists.rs`가 원문에서 만든다. 직접 고치지 않는다.

use std::collections::HashMap;
use std::sync::OnceLock;

use pekko_agent_msds::result::{CasCheck, Component};

use crate::pre_survey::{KrList, Listing, RegulationCheck};

const DATA: &str = include_str!("../data/kr_lists.tsv");

/// 데이터 파일 머리의 출처 한 줄.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    pub lists: Vec<KrList>,
    pub basis: String,
    pub url: String,
    pub sha256: String,
}

#[derive(Debug, Default)]
struct Data {
    /// (목록, CAS) → 시행일. 빈 값이면 이미 시행 중.
    entries: HashMap<(KrList, String), String>,
    sources: Vec<Source>,
}

fn parse(text: &str) -> Result<Data, String> {
    let mut data = Data::default();
    let mut header_seen = false;
    for (n, line) in text.lines().enumerate() {
        let f: Vec<&str> = line.split('\t').collect();
        let bad = |what: &str| format!("kr_lists.tsv {}행: {what}", n + 1);
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if f[0] == "@source" {
            let [_, lists, basis, url, sha] = f[..] else { return Err(bad("출처 칸 수")) };
            let lists = lists.split(',').map(|k| KrList::from_key(k).ok_or_else(|| bad(&format!("모르는 목록 {k}")))).collect::<Result<_, _>>()?;
            let sha256 = sha.strip_prefix("sha256=").ok_or_else(|| bad("sha256"))?.to_string();
            data.sources.push(Source { lists, basis: basis.into(), url: url.into(), sha256 });
        } else if f[0] == "list" {
            header_seen = true;
        } else {
            let [list, cas, from, _name] = f[..] else { return Err(bad("칸 수")) };
            if !header_seen {
                return Err(bad("머리글 전에 항목이 있음"));
            }
            let list = KrList::from_key(list).ok_or_else(|| bad(&format!("모르는 목록 {list}")))?;
            data.entries.insert((list, cas.to_string()), from.to_string());
        }
    }
    Ok(data)
}

fn embedded() -> &'static Data {
    static DATA_PARSED: OnceLock<Data> = OnceLock::new();
    // 저장소에 넣은 파일이라 형식은 테스트가 보장한다
    DATA_PARSED.get_or_init(|| parse(DATA).expect("data/kr_lists.tsv 형식 오류"))
}

/// 저장소에 넣어 둔 공식 목록으로 대조한다. `as_of`(`YYYY-MM-DD`, 한국 날짜) 이후에 시행되는 항목은 "시행 예정"으로 본다.
pub struct KrLists {
    data: &'static Data,
    as_of: String,
}

impl KrLists {
    pub fn embedded(as_of: &str) -> Self {
        Self { data: embedded(), as_of: as_of.to_string() }
    }

    /// 목록마다 근거(고시·별표)와 원문 주소.
    pub fn sources(&self) -> &[Source] {
        &self.data.sources
    }

    /// 출처가 있는 목록만 대조한다. 허가물질처럼 목록을 찾지 못한 것은 "미대조"로 둔다.
    fn has_list(&self, list: KrList) -> bool {
        self.data.sources.iter().any(|s| s.lists.contains(&list))
    }

    fn lookup(&self, list: KrList, cas: &str) -> Option<Listing> {
        let from = self.data.entries.get(&(list, cas.to_string()))?;
        Some(if from.is_empty() || from.as_str() <= self.as_of.as_str() { Listing::Listed } else { Listing::ListedFrom(from.clone()) })
    }
}

impl RegulationCheck for KrLists {
    fn check(&self, component: &Component, list: KrList) -> Listing {
        let Some(cas_no) = component.cas_no.as_deref() else { return Listing::NotChecked };
        if !self.has_list(list) {
            return Listing::NotChecked;
        }
        // 한 칸에 CAS가 여럿이면(예: 이성질체) 하나라도 목록에 있으면 해당이다
        let found: Vec<Listing> = cas_no.split([',', ';', '/']).map(str::trim).filter_map(|c| self.lookup(list, c)).collect();
        if found.contains(&Listing::Listed) {
            return Listing::Listed;
        }
        if let Some(from) = found.iter().filter_map(|l| if let Listing::ListedFrom(d) = l { Some(d) } else { None }).min() {
            return Listing::ListedFrom(from.clone());
        }
        match component.cas_check {
            // 맞는 CAS인데 목록에 없다
            CasCheck::Valid => Listing::NotListedByCas,
            // 틀리거나 비공개인 CAS로는 "없다"고 말할 수 없다. 고시 원문에도 체크 디지트가 틀린 CAS가 있어(테스트 참고)
            // 위에서 글자 그대로 찾아본 것이다
            CasCheck::ChecksumFailed | CasCheck::Malformed | CasCheck::Withheld | CasCheck::Absent => Listing::NotChecked,
        }
    }

    fn as_of(&self) -> Option<&str> {
        Some(&self.as_of)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pekko_agent_msds::cas;
    use pekko_agent_msds::result::Text;

    fn component(cas_no: Option<&str>, check: CasCheck) -> Component {
        Component {
            name: Text { src: String::new(), ko: String::new() },
            cas_no: cas_no.map(String::from),
            cas_check: check,
            concentration: None,
            exposure_limits: vec![],
            regulatory_marks: vec![],
            pages: vec![],
        }
    }

    fn valid(cas: &str) -> Component {
        component(Some(cas), CasCheck::Valid)
    }

    fn count(list: KrList) -> usize {
        embedded().entries.keys().filter(|(l, _)| *l == list).count()
    }

    /// 원문에서 뽑은 수. 다시 만들어 수가 바뀌면 원문이 바뀐 것이니 확인하고 고친다(설계 5-8).
    #[test]
    fn embedded_counts_match_sources() {
        assert_eq!(count(KrList::AcuteToxic), 1024);
        assert_eq!(count(KrList::ChronicToxic), 305);
        assert_eq!(count(KrList::EcoToxic), 732);
        assert_eq!(count(KrList::Permitted), 0);
        assert_eq!(count(KrList::Restricted), 107);
        assert_eq!(count(KrList::Prohibited), 84);
        assert_eq!(count(KrList::AccidentPrecaution), 103);
        assert_eq!(count(KrList::ManagedHazardous), 180);
        assert_eq!(count(KrList::WorkEnvMeasurement), 191);
        assert_eq!(count(KrList::SpecialHealthExam), 166);
        assert_eq!(embedded().sources.len(), 10);
        assert!(embedded().sources.iter().all(|s| s.url.starts_with("https://www.law.go.kr/") && s.sha256.len() == 64));
    }

    /// PDF에서 글자를 뽑다 CAS가 갈라지거나 숫자가 섞이면 체크 디지트가 깨진다(예: HF 문서의 `7664 -39 -3`).
    /// 예외는 고시 엑셀 CAS 칸에 원문 그대로 틀리게 적힌 하나뿐이다(2018-1-822, 2026-5호·2026-16호 모두).
    #[test]
    fn every_cas_passes_check_digit_except_official_typos() {
        const OFFICIAL_TYPOS: [&str; 1] = ["57235-57-7"];
        let mut bad: Vec<&str> = embedded().entries.keys().map(|(_, c)| c.as_str()).filter(|c| cas::check(c) != CasCheck::Valid).collect();
        bad.sort();
        bad.dedup();
        assert_eq!(bad, OFFICIAL_TYPOS);
    }

    #[test]
    fn listed_and_not_listed() {
        let lists = KrLists::embedded("2026-10-26");
        let toluene = valid("108-88-3");
        assert_eq!(lists.check(&toluene, KrList::ManagedHazardous), Listing::Listed);
        assert_eq!(lists.check(&toluene, KrList::WorkEnvMeasurement), Listing::Listed);
        assert_eq!(lists.check(&toluene, KrList::SpecialHealthExam), Listing::Listed);
        assert_eq!(lists.check(&toluene, KrList::AccidentPrecaution), Listing::Listed);
        assert_eq!(lists.check(&toluene, KrList::Prohibited), Listing::NotListedByCas);
        // 불산: 인체급성유해성물질
        assert_eq!(lists.check(&valid("7664-39-3"), KrList::AcuteToxic), Listing::Listed);
        assert_eq!(lists.as_of(), Some("2026-10-26"));
    }

    #[test]
    fn future_entries_follow_as_of() {
        // 2026-16호(2026-12-19 시행)에서 생태유해성이 더해진 물질
        let lithium_salt = valid("1217886-69-5");
        let before = KrLists::embedded("2026-10-26");
        assert_eq!(before.check(&lithium_salt, KrList::AcuteToxic), Listing::Listed);
        assert_eq!(before.check(&lithium_salt, KrList::EcoToxic), Listing::ListedFrom("2026-12-19".into()));
        assert_eq!(KrLists::embedded("2026-12-19").check(&lithium_salt, KrList::EcoToxic), Listing::Listed);
        // 2026-5호 부칙 제1조 단서: 차아염소산 나트륨은 2028-01-01부터
        let hypochlorite = valid("7681-52-9");
        assert_eq!(before.check(&hypochlorite, KrList::AcuteToxic), Listing::ListedFrom("2028-01-01".into()));
        assert_eq!(KrLists::embedded("2028-01-01").check(&hypochlorite, KrList::AcuteToxic), Listing::Listed);
    }

    #[test]
    fn cannot_say_no_without_a_valid_cas_or_a_list() {
        let lists = KrLists::embedded("2026-10-26");
        assert_eq!(lists.check(&component(None, CasCheck::Absent), KrList::ManagedHazardous), Listing::NotChecked);
        assert_eq!(lists.check(&component(Some("Proprietary"), CasCheck::Withheld), KrList::ManagedHazardous), Listing::NotChecked);
        assert_eq!(lists.check(&component(Some("108-88-4"), CasCheck::ChecksumFailed), KrList::ManagedHazardous), Listing::NotChecked);
        // 허가물질은 목록이 없다
        assert_eq!(lists.check(&valid("108-88-3"), KrList::Permitted), Listing::NotChecked);
        // 고시에 틀리게 적힌 CAS도 글자 그대로면 찾는다
        assert_eq!(lists.check(&component(Some("57235-57-7"), CasCheck::ChecksumFailed), KrList::AcuteToxic), Listing::Listed);
    }

    #[test]
    fn several_cas_in_one_cell() {
        let lists = KrLists::embedded("2026-10-26");
        assert_eq!(lists.check(&valid("50-00-0, 108-88-3"), KrList::ManagedHazardous), Listing::Listed);
        assert_eq!(lists.check(&valid("7732-18-5"), KrList::ManagedHazardous), Listing::NotListedByCas, "물");
    }

    #[test]
    fn parse_rejects_broken_lines() {
        assert!(parse("list\tcas\tfrom\tname\nkr_unknown\t1-2-3\t\t\n").is_err());
        assert!(parse("kr_restricted\t50-00-0\t\t\n").is_err(), "머리글 전 항목");
        assert!(parse("@source\tkr_restricted\t근거\thttps://x\tnosha\n").is_err());
    }
}
