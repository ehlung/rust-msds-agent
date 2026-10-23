//! pdfium으로 PDF를 읽어 판정에 필요한 값만 꺼낸다. 판정 자체는 `analyze`가 한다.

use std::io::Cursor;

use anyhow::Context;
use pdfium_render::prelude::*;

/// 위쪽 기준 좌표(pt)의 사각형. PDF는 아래쪽이 0이라 꺼낼 때 뒤집는다.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rect {
    pub left: f32,
    pub top: f32,
    pub width: f32,
    pub height: f32,
}

impl Rect {
    pub fn right(&self) -> f32 {
        self.left + self.width
    }
    pub fn bottom(&self) -> f32 {
        self.top + self.height
    }
    /// 두 사각형이 `gap` 안으로 붙어 있거나 겹치는가.
    pub fn touches(&self, o: &Rect, gap: f32) -> bool {
        self.left <= o.right() + gap && o.left <= self.right() + gap && self.top <= o.bottom() + gap && o.top <= self.bottom() + gap
    }
    pub fn union(&self, o: &Rect) -> Rect {
        let left = self.left.min(o.left);
        let top = self.top.min(o.top);
        Rect { left, top, width: self.right().max(o.right()) - left, height: self.bottom().max(o.bottom()) - top }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Line {
    pub text: String,
    pub top: f32,
    pub left: f32,
    pub height: f32,
}

#[derive(Clone, Debug, Default)]
pub struct PageData {
    /// 1부터.
    pub number: u32,
    pub width: f32,
    pub height: f32,
    pub lines: Vec<Line>,
    pub images: Vec<Rect>,
    /// 빨간 선이나 면으로 그린 도형. GHS 그림문자 테두리를 벡터로 그린 문서가 있다.
    pub red_shapes: Vec<Rect>,
}

pub fn bind(lib_dir: &str) -> anyhow::Result<Pdfium> {
    let bindings = Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path(lib_dir))
        .with_context(|| format!("pdfium 라이브러리를 불러올 수 없음 (PDFIUM_LIB_DIR={lib_dir})"))?;
    Ok(Pdfium::new(bindings))
}

pub fn read_pages(pdfium: &Pdfium, pdf: &[u8]) -> anyhow::Result<Vec<PageData>> {
    let doc = pdfium.load_pdf_from_byte_slice(pdf, None).context("PDF를 열 수 없음")?;
    let mut pages = Vec::new();
    for (i, page) in doc.pages().iter().enumerate() {
        let height = page.height().value;
        let mut data = PageData { number: i as u32 + 1, width: page.width().value, height, ..Default::default() };
        data.lines = lines(&page, height)?;
        for obj in page.objects().iter() {
            let Ok(b) = obj.bounds() else { continue };
            let rect = Rect {
                left: b.left().value,
                top: height - b.top().value,
                width: b.width().value,
                height: b.height().value,
            };
            match obj.object_type() {
                PdfPageObjectType::Image => data.images.push(rect),
                PdfPageObjectType::Path if is_red(&obj) => data.red_shapes.push(rect),
                _ => {}
            }
        }
        pages.push(data);
    }
    Ok(pages)
}

fn is_red(obj: &PdfPageObject) -> bool {
    let red = |c: PdfColor| c.red() >= 180 && c.green() <= 90 && c.blue() <= 90 && c.alpha() > 0;
    obj.stroke_color().map(red).unwrap_or(false) || obj.fill_color().map(red).unwrap_or(false)
}

/// pdfium은 텍스트를 PDF 안에 저장된 순서로 준다. 보이는 순서로 읽으려면 조각을 위치로 정렬해 줄을 다시 만든다.
fn lines(page: &PdfPage, height: f32) -> anyhow::Result<Vec<Line>> {
    struct Seg {
        mid: f32,
        top: f32,
        left: f32,
        right: f32,
        h: f32,
        text: String,
    }
    let text = page.text()?;
    let mut segs: Vec<Seg> = text
        .segments()
        .iter()
        .filter_map(|s| {
            let t = s.text();
            if t.trim().is_empty() {
                return None;
            }
            let b = s.bounds();
            let top = height - b.top().value;
            let h = b.height().value.max(1.0);
            Some(Seg { mid: top + h / 2.0, top, left: b.left().value, right: b.right().value, h, text: t })
        })
        .collect();
    segs.sort_by(|a, b| a.mid.total_cmp(&b.mid).then(a.left.total_cmp(&b.left)));

    // 세로 중심이 글자 높이의 절반 안이면 같은 줄로 본다
    let mut rows: Vec<Vec<Seg>> = Vec::new();
    for s in segs {
        match rows.last_mut() {
            Some(row) if (row[0].mid - s.mid).abs() <= row[0].h.max(s.h) * 0.5 => row.push(s),
            _ => rows.push(vec![s]),
        }
    }
    Ok(rows
        .into_iter()
        .map(|mut row| {
            row.sort_by(|a, b| a.left.total_cmp(&b.left));
            let mut out = String::new();
            let mut prev: Option<(f32, f32)> = None; // (오른쪽 끝, 높이)
            for s in &row {
                if let Some((right, h)) = prev {
                    // 조각 사이가 글자 높이의 0.25배보다 좁으면 붙이고, 넓으면 칸을 띄운다.
                    // 하이픈 조각은 높이가 0.6pt 정도라 앞뒤 중 큰 높이를 기준으로 쓴다(CAS `7664-39-3`이 갈라지지 않게).
                    if s.left - right > h.max(s.h) * 0.25 {
                        out.push(' ');
                    }
                }
                out.push_str(s.text.trim());
                prev = Some((s.right, s.h));
            }
            Line {
                text: collapse_letter_spacing(&out),
                top: row.iter().map(|s| s.top).fold(f32::MAX, f32::min),
                left: row[0].left,
                height: row.iter().map(|s| s.h).fold(0.0, f32::max),
            }
        })
        .collect())
}

/// 글자 사이를 띄워 조판한 한글 줄(`터 치 크 린 골 드U`)의 칸을 지운다.
/// 단어 4개 이상 중 60% 이상이 한 글자짜리 한글일 때만 그렇게 본다.
pub fn collapse_letter_spacing(text: &str) -> String {
    let tokens: Vec<&str> = text.split_whitespace().collect();
    let single_hangul = tokens
        .iter()
        .filter(|t| t.chars().count() == 1 && t.chars().all(|c| ('\u{AC00}'..='\u{D7A3}').contains(&c)))
        .count();
    if tokens.len() >= 4 && single_hangul * 10 >= tokens.len() * 6 {
        tokens.concat()
    } else {
        text.to_string()
    }
}

/// 한 쪽을 PNG로 렌더링한다. `dpi` 150이면 A4가 약 1240×1754px.
pub fn render_png(pdfium: &Pdfium, pdf: &[u8], page_number: u32, dpi: f32) -> anyhow::Result<Vec<u8>> {
    let doc = pdfium.load_pdf_from_byte_slice(pdf, None)?;
    let page = doc.pages().get((page_number - 1) as i32)?;
    let px = (page.width().value / 72.0 * dpi).round() as i32;
    let bitmap = page.render_with_config(&PdfRenderConfig::new().set_target_width(px))?;
    let mut buf = Cursor::new(Vec::new());
    bitmap.as_image()?.write_to(&mut buf, image::ImageFormat::Png)?;
    Ok(buf.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collapses_only_letter_spaced_hangul() {
        assert_eq!(collapse_letter_spacing("터 치 크 린 골 드U"), "터치크린골드U");
        assert_eq!(collapse_letter_spacing("1 . 화 학 제 품 과 회 사"), "1.화학제품과회사");
        // 보통 문장은 그대로
        assert_eq!(collapse_letter_spacing("이 물질은 피부에 자극을 일으킴"), "이 물질은 피부에 자극을 일으킴");
        assert_eq!(collapse_letter_spacing("12 108-88-3 Toluene"), "12 108-88-3 Toluene");
    }
}
