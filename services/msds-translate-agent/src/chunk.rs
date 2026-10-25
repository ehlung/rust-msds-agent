//! 번역할 섹션을 Claude 호출 단위로 묶는다. 판단이 필요 없는 일이라 코드가 한다(측정은 코드, 판단은 에이전트).
//!
//! - 섹션이 기본 단위다. 짧은 섹션은 한 호출에 여러 개 묶는다.
//! - 섹션 하나가 상한보다 길 때만 줄 단위로 나눈다. 가능하면 문장이 끝나는 줄(`.`, `:`, `;`)에서 자른다.
//!
//! 상한은 원문 글자 수로 잰다. 한 호출의 출력(번역 + 생각)이 작업의 `max_tokens`(16,000) 안에 들도록 정했다.

/// 한 호출에 보낼 원문 글자 수 상한.
pub const CHUNK_CHARS: usize = 6000;

/// 번역할 조각. 섹션 하나이거나 긴 섹션의 일부다.
#[derive(Clone, Debug, PartialEq)]
pub struct Piece {
    pub id: usize,
    /// 작업 메시지의 `sections` 안 순서.
    pub section: usize,
    /// 1부터. 나뉘지 않은 섹션은 (1, 1).
    pub part: (usize, usize),
    pub text: String,
}

/// 섹션 본문들을 조각으로 나눈다.
pub fn pieces(sections: &[&str], limit: usize) -> Vec<Piece> {
    let mut out = Vec::new();
    for (si, text) in sections.iter().enumerate() {
        let parts = split_long(text, limit);
        let n = parts.len();
        for (pi, part) in parts.into_iter().enumerate() {
            out.push(Piece { id: out.len(), section: si, part: (pi + 1, n), text: part });
        }
    }
    out
}

/// 조각을 호출 단위로 묶는다. 한 묶음의 글자 수 합이 `limit`를 넘지 않게 앞에서부터 채운다.
pub fn batches(pieces: &[Piece], limit: usize) -> Vec<Vec<Piece>> {
    let mut out: Vec<Vec<Piece>> = Vec::new();
    let mut size = 0;
    for p in pieces {
        let len = p.text.chars().count();
        if out.is_empty() || size + len > limit {
            out.push(Vec::new());
            size = 0;
        }
        out.last_mut().unwrap().push(p.clone());
        size += len;
    }
    out
}

fn split_long(text: &str, limit: usize) -> Vec<String> {
    if text.chars().count() <= limit {
        return vec![text.to_string()];
    }
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut cur_len = 0;
    for line in text.lines() {
        let len = line.chars().count() + 1;
        // 상한을 넘기 전에, 문장이 끝난 줄이 상한의 60%를 넘었으면 거기서 자른다
        if cur_len + len > limit && !cur.is_empty() {
            parts.push(std::mem::take(&mut cur));
            cur_len = 0;
        }
        cur.push_str(line);
        cur.push('\n');
        cur_len += len;
        let sentence_end = line.trim_end().ends_with(['.', ':', ';']);
        if sentence_end && cur_len >= limit * 6 / 10 {
            parts.push(std::mem::take(&mut cur));
            cur_len = 0;
        }
    }
    if !cur.trim().is_empty() {
        parts.push(cur);
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_sections_stay_whole_and_are_batched_together() {
        let ps = pieces(&["a".repeat(100).as_str(), "b".repeat(200).as_str(), "c".repeat(5900).as_str()], 6000);
        assert_eq!(ps.len(), 3);
        assert!(ps.iter().all(|p| p.part == (1, 1)));
        let bs = batches(&ps, 6000);
        assert_eq!(bs.iter().map(|b| b.len()).collect::<Vec<_>>(), [2, 1]);
    }

    #[test]
    fn long_section_splits_at_sentence_end_without_losing_text() {
        let line = "Acute toxicity data for the mixture is listed below.";
        let text: String = (0..400).map(|i| format!("{line} {i}\n{}\n", "LD50 Oral rat 2000 mg/kg")).collect();
        let ps = pieces(&[text.as_str()], 6000);
        assert!(ps.len() > 1);
        assert!(ps.iter().all(|p| p.text.chars().count() <= 6000));
        assert_eq!(ps[0].part, (1, ps.len()));
        // 이어 붙이면 원문과 같다(줄 끝 처리 외에는 잃는 글자가 없다)
        let joined: String = ps.iter().map(|p| p.text.as_str()).collect();
        assert_eq!(joined.trim_end(), text.trim_end());
        // 첫 조각은 문장이 끝나는 줄에서 끝난다
        assert!(ps[0].text.trim_end().ends_with('.') || ps[0].text.lines().last().unwrap().starts_with("LD50"));
    }

    #[test]
    fn piece_ids_are_sequential_across_sections() {
        let long = "x.\n".repeat(4000);
        let ps = pieces(&["short", long.as_str(), "tail"], 6000);
        assert_eq!(ps.iter().map(|p| p.id).collect::<Vec<_>>(), (0..ps.len()).collect::<Vec<_>>());
        assert_eq!(ps.first().unwrap().section, 0);
        assert_eq!(ps.last().unwrap().section, 2);
    }
}
