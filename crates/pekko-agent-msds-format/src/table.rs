//! 형식에 상관없는 표. 변환기는 표를 만들고, CSV·엑셀·HTML은 표에서 만든다.
//!
//! MSDS 타입에 의존하지 않는다. 다른 에이전트의 결과도 같은 표로 내보낼 수 있게 하려는 것이다.

/// 열. `key`는 프로그램용 이름(바뀌지 않음), `title`은 사람이 보는 머리글.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Column {
    pub key: &'static str,
    pub title: &'static str,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Table {
    pub columns: Vec<Column>,
    /// 행마다 칸 수가 `columns`와 같다.
    pub rows: Vec<Vec<String>>,
}

/// 엑셀은 BOM이 없는 UTF-8 CSV를 시스템 인코딩(한국어 Windows는 CP949)으로 읽어 한글이 깨진다.
pub const UTF8_BOM: &str = "\u{feff}";

impl Table {
    /// RFC 4180 CSV. 줄바꿈은 CRLF, 쉼표·따옴표·줄바꿈이 든 칸은 따옴표로 감싼다.
    /// 엑셀이 수식으로 실행할 수 있는 칸은 앞에 `'`를 붙인다(`neutralize_formula`).
    pub fn to_csv(&self) -> String {
        let mut out = String::new();
        let titles = self.columns.iter().map(|c| c.title.to_string()).collect::<Vec<_>>();
        for row in std::iter::once(&titles).chain(&self.rows) {
            let cells = row.iter().map(|c| csv_cell(&neutralize_formula(c))).collect::<Vec<_>>();
            out.push_str(&cells.join(","));
            out.push_str("\r\n");
        }
        out
    }

    /// 열 이름으로 칸을 찾는다(테스트·화면용).
    pub fn cell(&self, row: usize, key: &str) -> Option<&str> {
        let col = self.columns.iter().position(|c| c.key == key)?;
        self.rows.get(row)?.get(col).map(String::as_str)
    }
}

fn csv_cell(value: &str) -> String {
    if value.contains([',', '"', '\r', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// 결과의 글은 업로드된 PDF에서 왔다. 칸이 `=`·`+`·`@`·탭·CR로 시작하면 엑셀이 수식으로 실행할 수 있어
/// 앞에 `'`를 붙인다(OWASP CSV Injection). `-`로 시작해도 수식이 되지만, `-20`이나 `-` 같은 값은 그대로 둔다.
fn neutralize_formula(value: &str) -> std::borrow::Cow<'_, str> {
    let risky = match value.chars().next() {
        Some('=' | '+' | '@' | '\t' | '\r') => true,
        Some('-') => value.len() > 1 && value[1..].trim().parse::<f64>().is_err(),
        _ => false,
    };
    if risky {
        format!("'{value}").into()
    } else {
        value.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(rows: Vec<Vec<&str>>) -> Table {
        Table {
            columns: vec![Column { key: "a", title: "가" }, Column { key: "b", title: "나" }],
            rows: rows.into_iter().map(|r| r.into_iter().map(String::from).collect()).collect(),
        }
    }

    #[test]
    fn csv_quotes_only_when_needed() {
        let t = table(vec![vec!["톨루엔", "1, 4"], vec!["say \"hi\"", "두\n줄"]]);
        assert_eq!(t.to_csv(), "가,나\r\n톨루엔,\"1, 4\"\r\n\"say \"\"hi\"\"\",\"두\n줄\"\r\n");
    }

    #[test]
    fn csv_neutralizes_formulas() {
        let t = table(vec![vec!["=HYPERLINK(\"x\")", "+1"], vec!["@SUM(A1)", "-2+3"], vec!["-20", "-"]]);
        let csv = t.to_csv();
        let lines: Vec<&str> = csv.split("\r\n").collect();
        assert_eq!(lines[1], "\"'=HYPERLINK(\"\"x\"\")\",'+1");
        assert_eq!(lines[2], "'@SUM(A1),'-2+3");
        // 숫자와 빈 값 표시는 그대로
        assert_eq!(lines[3], "-20,-");
    }

    #[test]
    fn cell_by_key() {
        let t = table(vec![vec!["x", "y"]]);
        assert_eq!(t.cell(0, "b"), Some("y"));
        assert_eq!(t.cell(0, "z"), None);
        assert_eq!(t.cell(1, "a"), None);
    }
}
