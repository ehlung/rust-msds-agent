//! 국내 규제 목록 데이터(`data/kr_lists.tsv`)를 공식 원문에서 만든다(설계 5-8).
//!
//! ```bash
//! cargo run -p pekko-agent-msds-format --example build_kr_lists -- <작업 폴더>
//! ```
//!
//! 작업 폴더에 원문이 없으면 국가법령정보센터에서 받는다(`curl`). PDF는 `pdftotext`(poppler)로 글자를 뽑는다.
//! 출처는 아래 `SOURCES` 한 곳에만 적는다. 고시가 바뀌면 여기를 고치고 다시 돌린다.
//! 대조는 CAS로만 한다. 이름은 사람이 읽기 위한 참고값이고, 엑셀 원문(인체등유해성물질)에만 넣는다.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use calamine::{open_workbook, Reader, Xlsx};
use pekko_agent_msds_format::KrList;
use regex::Regex;
use sha2::{Digest, Sha256};

const DOWNLOAD: &str = "https://www.law.go.kr/LSW/flDownload.do?flSeq=";

enum Kind {
    /// 인체등유해성물질 지정고시 첨부 엑셀. `from`이 비면 이미 시행 중인 판이다.
    Hazard { from: &'static str },
    /// 별표 PDF. 글자에서 CAS를 모두 뽑는다.
    Pdf(KrList),
}

struct Source {
    file: &'static str,
    fl_seq: u64,
    basis: &'static str,
    kind: Kind,
}

const SOURCES: &[Source] = &[
    Source {
        file: "hazard_2026-5.xlsx",
        fl_seq: 162837753,
        basis: "화평법 제2조제6호·제6의2호·제6의3호, 제20조 — 인체급성유해성물질, 인체만성유해성물질 및 생태유해성물질의 지정고시(화학물질안전원고시 제2026-5호, 시행 2026-07-01) 별표 첨부",
        kind: Kind::Hazard { from: "" },
    },
    Source {
        file: "hazard_2026-16.xlsx",
        fl_seq: 169398093,
        basis: "같은 고시 화학물질안전원고시 제2026-16호(고시 2026-09-18, 시행 2026-12-19) 별표 첨부 — 2026-5호에 새로 더해진 것만 시행일을 붙인다",
        kind: Kind::Hazard { from: "2026-12-19" },
    },
    Source {
        file: "restricted_byl2.pdf",
        fl_seq: 158305577,
        basis: "화평법 제27조 — 제한물질·금지물질의 지정(기후에너지환경부고시 제2025-28호, 시행 2026-01-01) 별표 2 제한물질",
        kind: Kind::Pdf(KrList::Restricted),
    },
    Source {
        file: "restricted_byl3.pdf",
        fl_seq: 158305589,
        basis: "같은 고시 별표 3 총칭으로 지정된 제한물질의 구체적 목록",
        kind: Kind::Pdf(KrList::Restricted),
    },
    Source {
        file: "prohibited_byl4.pdf",
        fl_seq: 158305599,
        basis: "같은 고시 별표 4 금지물질",
        kind: Kind::Pdf(KrList::Prohibited),
    },
    Source {
        file: "prohibited_byl5.pdf",
        fl_seq: 158305607,
        basis: "같은 고시 별표 5 총칭으로 지정된 금지물질의 구체적 목록",
        kind: Kind::Pdf(KrList::Prohibited),
    },
    Source {
        file: "accident_byl1.pdf",
        fl_seq: 162231931,
        basis: "화관법 제2조제6호, 제39조 — 사고대비물질의 지정(화학물질안전원고시 제2026-2호, 시행 2026-03-10) 별표 1",
        kind: Kind::Pdf(KrList::AccidentPrecaution),
    },
    Source {
        file: "managed_byl12.pdf",
        fl_seq: 164209729,
        basis: "산업안전보건기준에 관한 규칙 별표 12 관리대상 유해물질의 종류(개정 2022-10-18, 규칙 시행 2026-03-02 판)",
        kind: Kind::Pdf(KrList::ManagedHazardous),
    },
    Source {
        file: "workenv_byl21.pdf",
        fl_seq: 167463135,
        basis: "산업안전보건법 시행규칙 별표 21 작업환경측정 대상 유해인자(규칙 시행 2026-08-01 판)",
        kind: Kind::Pdf(KrList::WorkEnvMeasurement),
    },
    Source {
        file: "health_byl22.pdf",
        fl_seq: 167463161,
        basis: "산업안전보건법 시행규칙 별표 22 특수건강진단 대상 유해인자(개정 2021-11-19, 규칙 시행 2026-08-01 판)",
        kind: Kind::Pdf(KrList::SpecialHealthExam),
    },
];

/// 고시 부칙으로 시행일이 따로 정해진 항목. (고유번호, 시행일)
/// 2026-5호 부칙 제1조 단서: 고유번호 2026-1-1354 차아염소산 나트륨은 2028-01-01부터 시행.
const HAZARD_FROM_OVERRIDES: &[(&str, &str)] = &[("2026-1-1354", "2028-01-01")];

/// (목록, CAS) → (시행일, 이름). 시행일이 빈 값이면 이미 시행 중.
type Entries = BTreeMap<(&'static str, String), (String, String)>;

fn main() {
    let dir = PathBuf::from(std::env::args().nth(1).expect("작업 폴더를 인자로 준다"));
    std::fs::create_dir_all(&dir).unwrap();
    let cas_re = Regex::new(r"\d+(?:-\d+)+").unwrap();

    let mut entries = Entries::new();
    let mut header = vec![
        "# 국내 규제 목록 — 사전조사서의 법규 해당 여부 대조용(설계 5-8). 직접 고치지 않는다.".to_string(),
        "# 만든 방법: cargo run -p pekko-agent-msds-format --example build_kr_lists -- <작업 폴더>".to_string(),
        "# 법령·고시는 저작권 보호 대상이 아니다(저작권법 제7조). 대조는 CAS로만 하고, 이름은 참고값이다.".to_string(),
        "# 허가물질은 지정 고시를 찾지 못해 목록이 없다(대조하지 않음).".to_string(),
    ];
    for s in SOURCES {
        let path = fetch(&dir, s);
        let sha = Sha256::digest(std::fs::read(&path).unwrap()).iter().map(|b| format!("{b:02x}")).collect::<String>();
        let lists: Vec<&str> = match &s.kind {
            Kind::Hazard { from } => {
                read_hazard(&path, from, &cas_re, &mut entries);
                vec![KrList::AcuteToxic.column().key, KrList::ChronicToxic.column().key, KrList::EcoToxic.column().key]
            }
            Kind::Pdf(list) => {
                let text = pdf_text(&path);
                let key = list.column().key;
                for cas in cas_tokens(&cas_re, &text) {
                    entries.entry((key, cas)).or_insert((String::new(), String::new()));
                }
                vec![key]
            }
        };
        header.push(format!("@source\t{}\t{}\t{DOWNLOAD}{}\tsha256={sha}", lists.join(","), s.basis, s.fl_seq));
    }

    let mut out = header.join("\n");
    out.push_str("\nlist\tcas\tfrom\tname\n");
    for ((list, cas), (from, name)) in &entries {
        out.push_str(&format!("{list}\t{cas}\t{from}\t{name}\n"));
    }
    let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/kr_lists.tsv");
    std::fs::write(&target, out).unwrap();
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    entries.keys().for_each(|(l, _)| *counts.entry(l).or_default() += 1);
    println!("{} 에 썼다: {counts:?}", target.display());
}

/// 작업 폴더에 없으면 받는다.
fn fetch(dir: &Path, s: &Source) -> PathBuf {
    let path = dir.join(s.file);
    if !path.exists() {
        let ok = Command::new("curl").args(["-sSfL", "-m", "60", "-o"]).arg(&path).arg(format!("{DOWNLOAD}{}", s.fl_seq)).status().unwrap();
        assert!(ok.success(), "{} 을 받지 못함", s.file);
    }
    path
}

fn pdf_text(path: &Path) -> String {
    let out = Command::new("pdftotext").args(["-enc", "UTF-8", "-layout"]).arg(path).arg("-").output().expect("pdftotext(poppler)가 필요하다");
    assert!(out.status.success(), "pdftotext 실패: {}", path.display());
    String::from_utf8(out.stdout).unwrap()
}

/// 열: 고유번호 | 소번호 | 국문명 | 영문명 | CAS No. | 급성·만성·생태 함량 기준 | 유해성물질(예: "급성/생태", "삭제") | 종전 함량.
/// 소번호 행은 고유번호가 비어 있고, 구분이 비면 위 고유번호 행의 구분을 이어받는다.
fn read_hazard(path: &Path, from: &str, cas_re: &Regex, entries: &mut Entries) {
    let mut book: Xlsx<_> = open_workbook(path).unwrap();
    // 판마다 시트 이름이 다르다(2026-5호 "유해성물질 지정", 2026-16호 "인체등유해성물질 지정")
    let name = book.sheet_names().into_iter().find(|n| n.ends_with("유해성물질 지정")).expect("지정 목록 시트가 없음");
    let sheet = book.worksheet_range(&name).unwrap();
    let rows: Vec<Vec<String>> = sheet.rows().map(|r| r.iter().map(|c| c.to_string().trim().to_string()).collect()).collect();
    let start = rows.iter().position(|r| r.first().is_some_and(|c| c == "고유번호")).expect("머리글 행이 없음") + 1;
    let (mut id, mut parent_class) = (String::new(), String::new());
    for r in &rows[start..] {
        let cell = |i: usize| r.get(i).map(String::as_str).unwrap_or("");
        if !cell(0).is_empty() {
            id = cell(0).to_string();
            parent_class = cell(8).to_string();
        }
        let class = if cell(8).is_empty() { parent_class.as_str() } else { cell(8) };
        if class == "삭제" {
            continue;
        }
        let from = HAZARD_FROM_OVERRIDES.iter().find(|(i, _)| *i == id).map(|(_, d)| *d).unwrap_or(from);
        let lists = [("급성", KrList::AcuteToxic), ("만성", KrList::ChronicToxic), ("생태", KrList::EcoToxic)];
        // 이름에 줄바꿈·탭이 섞이면 TSV가 깨진다
        let name = cell(2).split_whitespace().collect::<Vec<_>>().join(" ");
        for cas in cas_tokens(cas_re, cell(4)) {
            for (_, list) in lists.iter().filter(|(word, _)| class.contains(word)) {
                // 앞 판에 이미 있으면 그 시행일을 둔다(빈 값 = 이미 시행 중)
                entries.entry((list.column().key, cas.clone())).or_insert((from.to_string(), name.clone()));
            }
        }
    }
}

/// 숫자·하이픈 덩어리를 통째로 잘라 CAS 모양(2~7자리-2자리-1자리)만 남긴다.
/// 앞뒤 한 글자를 함께 맞추는 정규식은 `111-30-8;100-37-8`의 두 번째를 놓치고,
/// 부분 일치는 EU Index 번호 `603-073-00-2`의 일부를 CAS로 잘못 잡는다.
fn cas_tokens(re: &Regex, text: &str) -> Vec<String> {
    re.find_iter(text)
        .map(|m| m.as_str())
        .filter(|t| {
            let p: Vec<&str> = t.split('-').collect();
            p.len() == 3 && (2..=7).contains(&p[0].len()) && p[1].len() == 2 && p[2].len() == 1
        })
        .map(String::from)
        .collect()
}
