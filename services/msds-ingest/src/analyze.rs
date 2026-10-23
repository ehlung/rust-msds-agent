//! 문서 판정. pdfium에서 꺼낸 쪽 데이터만 보고 언어·텍스트 상태·섹션·제품 경계·그림 후보를 정한다.
//! LLM을 쓰지 않으며, 판단이 필요한 일(어느 에이전트를 부를지 등)은 오케스트레이터에게 넘긴다.

use std::collections::HashMap;
use std::sync::LazyLock;

use pekko_agent_msds::profile::{DocFormat, Figure, FigureSource, Language, ProductProfile, SectionText, TextLayer};
use pekko_agent_msds::SectionKind;
use regex::Regex;

use crate::pdf::{Line, PageData, Rect};

/// 쪽 데이터 → 문서 프로필 재료. 페이지 이미지 키는 렌더링한 뒤 호출하는 쪽이 채운다.
pub struct Analysis {
    pub language: Language,
    pub text_layer: TextLayer,
    pub products: Vec<ProductProfile>,
    pub figures: Vec<Figure>,
}

pub fn analyze(pages: &[PageData]) -> Analysis {
    let pages = strip_running_lines(pages);
    let headings = find_headings(&pages);
    Analysis {
        language: language(&pages),
        text_layer: text_layer(&pages),
        products: split_products(&pages, &headings),
        figures: figures(&pages),
    }
}

// ── 언어 · 텍스트 상태 ────────────────────────────────────────────────────────

fn is_hangul(c: char) -> bool {
    ('\u{AC00}'..='\u{D7A3}').contains(&c)
}

fn all_text(pages: &[PageData]) -> impl Iterator<Item = char> + '_ {
    pages.iter().flat_map(|p| p.lines.iter()).flat_map(|l| l.text.chars())
}

/// 한글이 글자(공백 제외)의 20% 이상이면 한글 문서. 한글 MSDS도 물질명·CAS가 영문이라 기준을 낮게 둔다.
pub fn language(pages: &[PageData]) -> Language {
    let (mut hangul, mut letters) = (0usize, 0usize);
    for c in all_text(pages).filter(|c| c.is_alphabetic()) {
        letters += 1;
        if is_hangul(c) {
            hangul += 1;
        }
    }
    if letters > 0 && hangul * 5 >= letters {
        Language::Ko
    } else {
        Language::En
    }
}

/// KS X 1001(완성형 2,350자)에 있는 한글 음절인가.
///
/// 한글 문서는 거의 이 안의 글자만 쓴다. PDF 폰트의 글자 대응표가 틀리면 "전"이 "젂"처럼
/// 이 밖의 글자로 나오므로, 이 밖의 글자 수로 대응 오류를 잰다.
pub fn is_ksx1001_hangul(c: char) -> bool {
    let mut buf = [0u8; 4];
    let (bytes, _, had_errors) = encoding_rs::EUC_KR.encode(c.encode_utf8(&mut buf));
    // encoding_rs의 EUC-KR은 확장 완성형(CP949)이라 11,172자를 모두 인코딩한다. KS X 1001 한글 영역은 B0A1–C8FE.
    !had_errors && bytes.len() == 2 && (0xB0..=0xC8).contains(&bytes[0]) && bytes[1] >= 0xA1
}

pub fn text_layer(pages: &[PageData]) -> TextLayer {
    let chars: usize = all_text(pages).filter(|c| !c.is_whitespace()).count();
    if pages.is_empty() || chars / pages.len() < 50 {
        return TextLayer::None;
    }
    let hangul: Vec<char> = all_text(pages).filter(|c| is_hangul(*c)).collect();
    let odd = hangul.iter().filter(|c| !is_ksx1001_hangul(**c)).count();
    // 5자 이상이면서 한글의 0.2% 이상일 때만 오류로 본다(드문 글자 한두 개는 정상 문서에도 있다)
    if odd >= 5 && odd * 500 >= hangul.len() {
        TextLayer::MappingErrors
    } else {
        TextLayer::Ok
    }
}

// ── 머리말·꼬리말 제거 ────────────────────────────────────────────────────────

/// 숫자를 지우고 공백을 없앤 비교용 문자열. "page 2 of 4"와 "page 3 of 4"를 같은 줄로 본다.
fn shape(text: &str) -> String {
    text.chars().filter(|c| !c.is_whitespace() && !c.is_ascii_digit()).collect::<String>().to_lowercase()
}

/// 쪽 위·아래 10% 안에서 절반 이상의 쪽에 반복되는 줄(머리말·꼬리말·쪽 번호)을 지운다.
/// 섹션 제목이 머리말로 반복되는 문서(NYCOTE88)도 있어, 섹션 제목 모양이면 남긴다.
fn strip_running_lines(pages: &[PageData]) -> Vec<PageData> {
    if pages.len() < 3 {
        return pages.to_vec();
    }
    let edge = |height: f32, l: &Line| l.top < height * 0.10 || l.top > height * 0.90;
    let mut count: HashMap<String, usize> = HashMap::new();
    for p in pages {
        let mut seen = std::collections::HashSet::new();
        for l in p.lines.iter().filter(|l| edge(p.height, l)) {
            let s = shape(&l.text);
            if !s.is_empty() && seen.insert(s.clone()) {
                *count.entry(s).or_default() += 1;
            }
        }
    }
    let threshold = (pages.len() / 2).max(2);
    pages
        .iter()
        .map(|p| {
            let mut p = p.clone();
            let height = p.height;
            p.lines.retain(|l| {
                !(edge(height, l) && count.get(&shape(&l.text)).copied().unwrap_or(0) >= threshold && parse_heading(&l.text).is_none())
            });
            p
        })
        .collect()
}

// ── 섹션 제목 ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct Heading {
    pub number: u32,
    pub kind: SectionKind,
    pub title: String,
    /// 쪽 번호(1부터)와 그 쪽 안의 줄 번호.
    pub page: u32,
    pub line: usize,
}

static SECTION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^section(\d{1,2})(?:[^0-9a-z가-힣]|$)").unwrap());
static NUMBERED_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\d{1,2})[.:)]").unwrap());

/// 제목 글자(공백 없앤 소문자)에 든 낱말로 섹션 종류를 정한다. 앞에 있는 것부터 본다.
/// "Hazards identification"이 identification으로 잡히지 않게 hazards를 먼저 둔다.
const KIND_KEYWORDS: &[(SectionKind, &[&str])] = &[
    (SectionKind::Composition, &["composition", "ingredient", "구성성분"]),
    (SectionKind::Hazards, &["hazard", "유해성", "위험성"]),
    (SectionKind::Identification, &["identification", "companyinformation", "제품과회사", "화학제품"]),
    (SectionKind::FirstAid, &["firstaid", "응급조치"]),
    (SectionKind::AccidentalRelease, &["accidentalrelease", "누출"]),
    (SectionKind::Firefighting, &["fire", "화재", "폭발"]),
    (SectionKind::HandlingStorage, &["handling", "취급"]),
    (SectionKind::ExposurePpe, &["exposurecontrol", "personalprotection", "노출방지", "보호구"]),
    // NO.6 원문 제목이 "물리화확적 특성"(오타)이라 "물리"만 본다
    (SectionKind::PhysicalChemical, &["physical", "물리"]),
    (SectionKind::StabilityReactivity, &["stability", "reactivity", "안정성", "반응성"]),
    (SectionKind::Toxicology, &["toxicolog", "독성"]),
    (SectionKind::Ecology, &["ecolog", "환경"]),
    (SectionKind::Disposal, &["disposal", "폐기"]),
    (SectionKind::Transport, &["transport", "운송"]),
    (SectionKind::Regulatory, &["regulatory", "규제"]),
    (SectionKind::Other, &["otherinformation", "additionalinformation", "기타", "그밖", "참고"]),
];

fn kind_of(compact_title: &str) -> Option<SectionKind> {
    KIND_KEYWORDS.iter().find(|(_, words)| words.iter().any(|w| compact_title.contains(w))).map(|(k, _)| *k)
}

/// 한 줄이 섹션 제목 모양이면 (번호, 종류)를 돌려준다.
/// "SECTION 2 — COMPOSITION…", "Section 7. Handling", "1. 화학제품과…", "1  .  화  학…"(글자 사이를 띄운 제목)을 받는다.
pub fn parse_heading(text: &str) -> Option<(u32, SectionKind)> {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect::<String>().to_lowercase();
    if compact.chars().count() > 90 {
        return None;
    }
    let (number, rest) = if let Some(c) = SECTION_RE.captures(&compact) {
        (c[1].parse().ok()?, &compact[c.get(1)?.end()..])
    } else {
        let c = NUMBERED_RE.captures(&compact)?;
        (c[1].parse().ok()?, &compact[c.get(0)?.end()..])
    };
    if !(1..=16).contains(&number) {
        return None;
    }
    Some((number, kind_of(rest)?))
}

/// 번호가 앞 제목보다 1~3 크거나, 1로 돌아갈 때(다음 제품)만 제목으로 받는다.
/// 같은 번호가 다시 나오면 쪽 머리에 반복된 제목이라 건너뛴다.
pub fn find_headings(pages: &[PageData]) -> Vec<Heading> {
    let mut out: Vec<Heading> = Vec::new();
    for p in pages {
        for (i, l) in p.lines.iter().enumerate() {
            let Some((number, kind)) = parse_heading(&l.text) else { continue };
            let last = out.last().map(|h| h.number).unwrap_or(0);
            let accept = (number > last && number <= last + 3) || (number == 1 && (last == 0 || last >= 8));
            if accept {
                out.push(Heading { number, kind, title: l.text.trim().to_string(), page: p.number, line: i });
            }
        }
    }
    out
}

// ── 제품 · 섹션 나누기 ───────────────────────────────────────────────────────

/// CAS 번호 모양. 한글 바로 옆에 붙은 경우(`불화수소7664-39-3`)도 세려고 `\b` 대신 숫자·하이픈이 아닌 글자로 경계를 본다.
static CAS_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?:^|[^0-9-])(\d{2,7}-\d{2}-\d)(?:$|[^0-9-])").unwrap());

fn count_cas(text: &str) -> u32 {
    // 정규식이 뒤쪽 경계 글자까지 먹으므로, 붙어 있는 다음 번호를 놓치지 않게 번호 끝부터 다시 찾는다
    let mut n = 0;
    let mut rest = text;
    while let Some(c) = CAS_RE.captures(rest) {
        n += 1;
        rest = &rest[c.get(1).unwrap().end()..];
    }
    n
}

pub fn split_products(pages: &[PageData], headings: &[Heading]) -> Vec<ProductProfile> {
    // 1번 제목마다 제품이 하나 시작한다
    let starts: Vec<usize> = headings.iter().enumerate().filter(|(_, h)| h.number == 1).map(|(i, _)| i).collect();
    if starts.is_empty() {
        return Vec::new();
    }
    let last_page = pages.last().map(|p| p.number).unwrap_or(1);
    let mut products = Vec::new();
    for (pi, &start) in starts.iter().enumerate() {
        let end = starts.get(pi + 1).copied().unwrap_or(headings.len());
        let hs = &headings[start..end];
        let mut sections: Vec<SectionText> = Vec::new();
        for (si, h) in hs.iter().enumerate() {
            // 다음 제목 직전까지. 마지막 섹션은 다음 제품 시작(또는 문서 끝) 직전까지
            let until = hs.get(si + 1).or_else(|| headings.get(end)).map(|n| (n.page, n.line));
            let (text, section_pages) = collect(pages, (h.page, h.line), until);
            sections.push(SectionText { kind: h.kind, number: h.number, title: h.title.clone(), pages: section_pages, text });
        }
        // 첫 제품의 1번 제목 앞(문서 제목 영역)은 제품 식별 섹션 앞에 붙인다
        if pi == 0 {
            let (pre, _) = collect(pages, (1, 0), Some((hs[0].page, hs[0].line)));
            if !pre.trim().is_empty() {
                sections[0].text = format!("{pre}\n{}", sections[0].text);
            }
        }
        let first = hs[0].page;
        let last = headings.get(end).map(|n| if n.line == 0 { n.page - 1 } else { n.page }).unwrap_or(last_page).max(first);
        let format = match hs.iter().find(|h| h.number == 2).map(|h| h.kind) {
            Some(SectionKind::Composition) => DocFormat::LegacyUs,
            _ => DocFormat::Ghs,
        };
        let cas_found = sections
            .iter()
            .filter(|s| s.kind == SectionKind::Composition)
            .map(|s| count_cas(&s.text))
            .sum();
        let name_guess = sections.iter().find(|s| s.kind == SectionKind::Identification).and_then(|s| guess_name(&s.text));
        products.push(ProductProfile { index: pi as u32, name_guess, pages: [first, last], format, cas_found, sections });
    }
    products
}

/// (쪽, 줄)부터 (쪽, 줄) 직전까지의 줄을 모은다.
fn collect(pages: &[PageData], from: (u32, usize), until: Option<(u32, usize)>) -> (String, Vec<u32>) {
    let mut text = String::new();
    let mut used = Vec::new();
    for p in pages.iter().filter(|p| p.number >= from.0) {
        for (i, l) in p.lines.iter().enumerate() {
            if (p.number, i) < from {
                continue;
            }
            if let Some(u) = until {
                if (p.number, i) >= u {
                    return (text.trim_end().to_string(), used);
                }
            }
            text.push_str(&l.text);
            text.push('\n');
            if used.last() != Some(&p.number) {
                used.push(p.number);
            }
        }
    }
    (text.trim_end().to_string(), used)
}

/// 제품명 라벨. 글자 사이를 띄운 문서(`제 품 명`)도 있어 글자 사이 공백을 허용한다.
static NAME_LABEL_RE: LazyLock<Regex> = LazyLock::new(|| {
    let labels = ["product name", "product identifier", "trade name", "제품명"];
    let alts: Vec<String> = labels
        .iter()
        .map(|l| l.chars().filter(|c| !c.is_whitespace()).map(|c| regex::escape(&c.to_string())).collect::<Vec<_>>().join(r"\s*"))
        .collect();
    Regex::new(&format!(r"(?i)(?:{})\s*[:：]?\s*", alts.join("|"))).unwrap()
});

/// 제품명 추정. 판정 참고용이라 못 찾으면 비운다(제품명은 성분 에이전트가 확정한다).
fn guess_name(text: &str) -> Option<String> {
    // "Product identifier : Trade name : X"처럼 라벨이 이어지면 맨 앞 라벨들을 모두 뗀다
    fn strip_labels(mut s: &str) -> &str {
        while let Some(m) = NAME_LABEL_RE.find(s).filter(|m| m.start() == 0) {
            s = &s[m.end()..];
        }
        s.trim()
    }
    let lines: Vec<&str> = text.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        let Some(m) = NAME_LABEL_RE.find(line) else { continue };
        // 라벨만 있는 줄이면 다음 줄이 값이다
        let value = match strip_labels(&line[m.start()..]) {
            "" => lines.get(i + 1).map(|s| strip_labels(s.trim())).unwrap_or(""),
            v => v,
        };
        if !value.is_empty() && value.chars().count() <= 120 {
            return Some(value.to_string());
        }
    }
    None
}

// ── 그림 후보 ────────────────────────────────────────────────────────────────

const ANCHOR_WORDS: &[&str] = &["pictogram", "그림문자", "hazardsymbol", "symbol"];

pub fn figures(pages: &[PageData]) -> Vec<Figure> {
    // 여러 쪽의 같은 자리에 반복되는 이미지는 회사 로고로 보고 뺀다
    let key = |r: &Rect| (r.left.round() as i32, r.top.round() as i32, r.width.round() as i32, r.height.round() as i32);
    let mut repeats: HashMap<(i32, i32, i32, i32), usize> = HashMap::new();
    for p in pages {
        for r in &p.images {
            *repeats.entry(key(r)).or_default() += 1;
        }
    }
    let logo = |r: &Rect| pages.len() >= 3 && repeats.get(&key(r)).copied().unwrap_or(0) >= (pages.len() / 2).max(2);

    let mut out = Vec::new();
    for p in pages {
        let images: Vec<Rect> = p.images.iter().filter(|r| !logo(r)).copied().collect();
        let mut found = Vec::new();
        for (rects, source) in [(merge(&images), FigureSource::ImageObject), (merge(&p.red_shapes), FigureSource::Vector)] {
            for r in rects.into_iter().filter(|r| r.width >= 15.0 && r.height >= 15.0 && r.width <= 300.0 && r.height <= 300.0) {
                found.push(r);
                out.push(Figure { page: p.number, bbox_pt: bbox(&r), source, near_text: near_text(p, &r) });
            }
        }
        // 그림문자 라벨이 있는데 그 근처에 찾은 그림이 없으면 라벨 위치를 후보로 넘긴다
        for l in &p.lines {
            let compact: String = l.text.chars().filter(|c| !c.is_whitespace()).collect::<String>().to_lowercase();
            if !ANCHOR_WORDS.iter().any(|w| compact.contains(w)) {
                continue;
            }
            let r = Rect { left: l.left, top: l.top, width: (p.width - l.left).max(1.0), height: l.height.max(1.0) };
            let near = found.iter().any(|f| (f.top - l.top).abs() < 150.0);
            if !near {
                out.push(Figure { page: p.number, bbox_pt: bbox(&r), source: FigureSource::TextAnchor, near_text: Some(l.text.clone()) });
            }
        }
    }
    out
}

fn bbox(r: &Rect) -> [f32; 4] {
    [r.left.round(), r.top.round(), r.width.round(), r.height.round()]
}

/// 한 그림의 조각을 하나로 합친다. 그림 하나가 가는 조각 수백 개로 나뉜 문서(NO.6)와
/// 테두리·안쪽 그림이 따로 그려진 문서를 한 그림으로 묶는다.
///
/// 나란히 붙은 그림문자(HF는 70pt 그림 4개가 빈틈없이 이어짐)는 따로 두어야 하므로,
/// 실제로 겹치거나 한쪽이 가는 조각(짧은 변 8pt 미만)일 때만 합친다.
fn same_figure(a: &Rect, b: &Rect) -> bool {
    let sliver = |r: &Rect| r.width.min(r.height) < 8.0;
    a.touches(b, -0.5) || ((sliver(a) || sliver(b)) && a.touches(b, 1.5))
}

pub fn merge(rects: &[Rect]) -> Vec<Rect> {
    let mut groups: Vec<Rect> = Vec::new();
    for r in rects {
        let mut cur = *r;
        loop {
            let before = groups.len();
            groups.retain(|g| {
                if same_figure(g, &cur) {
                    cur = cur.union(g);
                    false
                } else {
                    true
                }
            });
            if groups.len() == before {
                break;
            }
        }
        groups.push(cur);
    }
    groups
}

/// 그림과 같은 높이에 있거나 바로 위에 있는 줄 중 가장 가까운 것.
fn near_text(p: &PageData, r: &Rect) -> Option<String> {
    p.lines
        .iter()
        .filter(|l| l.top <= r.bottom() && l.top >= r.top - 40.0)
        .min_by(|a, b| (r.top - a.top).abs().total_cmp(&(r.top - b.top).abs()))
        .map(|l| l.text.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(text: &str, top: f32) -> Line {
        Line { text: text.to_string(), top, left: 50.0, height: 10.0 }
    }

    fn page(number: u32, lines: &[(&str, f32)]) -> PageData {
        PageData { number, width: 612.0, height: 792.0, lines: lines.iter().map(|(t, y)| line(t, *y)).collect(), ..Default::default() }
    }

    #[test]
    fn headings_in_test_pdf_styles() {
        let cases = [
            ("SECTION 2 -- COMPOSITION/INFORMATION ON INGREDIENTS", 2, SectionKind::Composition),
            ("SECTION 3 — HAZARDS IDENTIFICATION", 3, SectionKind::Hazards),
            ("SECTION 1: Identification of the substance/mixture and of the company/undertaking", 1, SectionKind::Identification),
            ("Section 7. Handling and storage", 7, SectionKind::HandlingStorage),
            ("Section 8 : Exposure Controls, Personal Protection", 8, SectionKind::ExposurePpe),
            ("4. FIRST AID MEASURES", 4, SectionKind::FirstAid),
            ("Section 5 – 폭발ᆞ화재시 대처방법", 5, SectionKind::Firefighting),
            ("1  .  화  학  제  품  과  회  사  에  관  한  정  보", 1, SectionKind::Identification),
            ("3.구성성분의 명칭 및 함유량", 3, SectionKind::Composition),
            ("15. 법적 규제현황", 15, SectionKind::Regulatory),
            ("Section 16 : Additional Information", 16, SectionKind::Other),
            ("Section 1 : Chemical Product And Company Information", 1, SectionKind::Identification),
            ("9 . 물 리 화확 적 특 성", 9, SectionKind::PhysicalChemical),
            ("1 6 . 그 밖 에 참 고 자 료", 16, SectionKind::Other),
            ("16. 그 밖의 참고사항", 16, SectionKind::Other),
        ];
        for (text, n, k) in cases {
            assert_eq!(parse_heading(text), Some((n, k)), "{text}");
        }
    }

    #[test]
    fn body_lines_are_not_headings() {
        for text in ["1. Flush eyes with large amounts of water", "2 mm", "12  108-88-3  Toluene", "page 1 of 4", "SECTION 2 continued below"] {
            assert_eq!(parse_heading(text), None, "{text}");
        }
    }

    #[test]
    fn ksx1001_detects_mapping_errors() {
        for c in ['한', '전', '안', '관', '물', '질'] {
            assert!(is_ksx1001_hangul(c), "{c}");
        }
        // Hydrofluoric acid 문서에서 나온 글자
        for c in ['핚', '젂'] {
            assert!(!is_ksx1001_hangul(c), "{c}");
        }
    }

    #[test]
    fn two_products_and_repeated_heading() {
        let mut pages = Vec::new();
        let mut lines = vec![("SAFETY DATA SHEET", 40.0), ("Product name : RESIN", 60.0)];
        let titles = [
            "1. Identification", "2. Hazards identification", "3. Composition/information on ingredients",
            "4. First aid measures", "5. Firefighting measures", "6. Accidental release measures",
            "7. Handling and storage", "8. Exposure controls/personal protection",
        ];
        for (i, t) in titles.iter().enumerate() {
            lines.push((t, 100.0 + i as f32 * 60.0));
        }
        pages.push(page(1, &lines));
        // 2쪽: 머리에 8절 제목 반복(건너뛰어야 함) + 두 번째 제품
        pages.push(page(2, &[("8. Exposure controls/personal protection", 80.0), ("1. Identification", 200.0), ("Product name : HARDENER", 220.0), ("2. Hazards identification", 300.0), ("3. Composition", 400.0), ("CAS 108-88-3", 420.0)]));
        let hs = find_headings(&pages);
        assert_eq!(hs.iter().map(|h| h.number).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3]);
        let products = split_products(&pages, &hs);
        assert_eq!(products.len(), 2);
        assert_eq!(products[0].name_guess.as_deref(), Some("RESIN"));
        assert_eq!(products[1].name_guess.as_deref(), Some("HARDENER"));
        assert_eq!(products[0].pages, [1, 2]);
        assert_eq!(products[1].pages, [2, 2]);
        assert_eq!(products[1].cas_found, 1);
        assert_eq!(products[0].format, DocFormat::Ghs);
    }

    #[test]
    fn counts_cas_next_to_hangul_and_adjacent() {
        assert_eq!(count_cas("불화수소7664-39-3 49"), 1);
        assert_eq!(count_cas("12  108-88-3  Toluene\n1 100-41-4 Ethylbenzene"), 2);
        assert_eq!(count_cas("7664-39-3,7732-18-5"), 2);
        // 날짜·전화번호·EC 번호 일부는 세지 않는다
        assert_eq!(count_cas("27.09.2021 (800) 424-9300 216-823-55"), 0);
    }

    #[test]
    fn guesses_name_from_label_styles() {
        assert_eq!(guess_name("가.제품명 불화수소(Hydrofluoric acid)").as_deref(), Some("불화수소(Hydrofluoric acid)"));
        assert_eq!(guess_name("1.1 Product identifier : Trade name : ARALDITE 2011 RESIN").as_deref(), Some("ARALDITE 2011 RESIN"));
        assert_eq!(guess_name("PRODUCT NAME\nPOLANE Reducer").as_deref(), Some("POLANE Reducer"));
        assert_eq!(guess_name("1.1 Product identifier\nTrade name : ARALDITE 2011 HARDENER").as_deref(), Some("ARALDITE 2011 HARDENER"));
        assert_eq!(guess_name("가 . 제 품 명 터치크린골드U").as_deref(), Some("터치크린골드U"));
        assert_eq!(guess_name("no label here"), None);
    }

    #[test]
    fn keeps_adjacent_pictograms_apart() {
        // Hydrofluoric acid: 70pt 그림문자 4개가 빈틈없이 이어져 있다
        let row: Vec<Rect> = [232.0, 302.0, 373.0, 443.0].iter().map(|x| Rect { left: *x, top: 399.0, width: 70.0, height: 70.0 }).collect();
        assert_eq!(merge(&row).len(), 4);
    }

    #[test]
    fn merges_pieces_into_one_figure() {
        let pieces: Vec<Rect> = (0..40).map(|i| Rect { left: 100.0 + i as f32, top: 265.0, width: 1.5, height: 40.0 }).collect();
        let merged = merge(&pieces);
        assert_eq!(merged.len(), 1);
        assert!(merged[0].width >= 40.0);
    }
}
