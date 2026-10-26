//! 그림 후보 자르기. 좌표대로 자르기만 하고, 무슨 그림인지는 판단하지 않는다(그건 Claude가 한다).

use std::io::Cursor;

use anyhow::bail;
use image::imageops::FilterType;
use image::ImageFormat;

/// 후보 영역 둘레에 더 붙이는 여백(pt). 찾은 영역이 테두리를 살짝 자르는 경우가 있다.
const PAD_PT: f32 = 2.0;
/// 잘라낸 그림의 짧은 변이 이보다 작으면 키운다. 31pt 그림은 150dpi에서 약 65px라 Claude가 알아보기 어렵다.
const MIN_SIDE: u32 = 160;

/// 페이지 PNG에서 `bbox_pt`([왼쪽, 위, 너비, 높이], pt) 영역을 잘라 PNG로 돌려준다. 픽셀 = pt × dpi / 72.
pub fn crop(page_png: &[u8], bbox_pt: [f32; 4], dpi: f32) -> anyhow::Result<Vec<u8>> {
    let img = image::load_from_memory_with_format(page_png, ImageFormat::Png)?;
    let s = dpi / 72.0;
    let [x, y, w, h] = bbox_pt;
    let x0 = ((x - PAD_PT) * s).floor().max(0.0) as u32;
    let y0 = ((y - PAD_PT) * s).floor().max(0.0) as u32;
    let x1 = (((x + w + PAD_PT) * s).ceil().max(0.0) as u32).min(img.width());
    let y1 = (((y + h + PAD_PT) * s).ceil().max(0.0) as u32).min(img.height());
    if x1 <= x0 || y1 <= y0 {
        bail!("그림 영역이 페이지 밖: {bbox_pt:?}");
    }
    let mut part = img.crop_imm(x0, y0, x1 - x0, y1 - y0);
    let short = part.width().min(part.height());
    if short < MIN_SIDE {
        let k = MIN_SIDE.div_ceil(short);
        part = part.resize_exact(part.width() * k, part.height() * k, FilterType::CatmullRom);
    }
    let mut out = Cursor::new(Vec::new());
    part.write_to(&mut out, ImageFormat::Png)?;
    Ok(out.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{GenericImageView, Rgb, RgbImage};

    fn page_png() -> Vec<u8> {
        // 72dpi(1pt = 1px) 200×100 흰 페이지, (50,20)부터 40×30 검은 사각형
        let mut img = RgbImage::from_pixel(200, 100, Rgb([255, 255, 255]));
        for px in 50..90 {
            for py in 20..50 {
                img.put_pixel(px, py, Rgb([0, 0, 0]));
            }
        }
        let mut out = Cursor::new(Vec::new());
        img.write_to(&mut out, ImageFormat::Png).unwrap();
        out.into_inner()
    }

    #[test]
    fn crops_bbox_with_padding_and_upscales_small() {
        let png = crop(&page_png(), [50.0, 20.0, 40.0, 30.0], 72.0).unwrap();
        let img = image::load_from_memory(&png).unwrap();
        // 여백 2pt씩 → 44×34, 짧은 변 34 < 160 → 5배
        assert_eq!(img.dimensions(), (44 * 5, 34 * 5));
        // 가운데는 검은 사각형
        assert_eq!(img.get_pixel(110, 85).0[0], 0);
    }

    #[test]
    fn scales_points_by_dpi() {
        // 144dpi면 1pt = 2px. 페이지 이미지는 72dpi 그대로 두고 좌표만 절반으로 준다.
        let png = crop(&page_png(), [25.0, 10.0, 20.0, 15.0], 144.0).unwrap();
        let img = image::load_from_memory(&png).unwrap();
        // 여백 포함 (23~47pt)×(8~27pt) → 46~94px × 16~54px → 48×38, 5배
        assert_eq!(img.dimensions(), (48 * 5, 38 * 5));
    }

    #[test]
    fn rejects_bbox_outside_page() {
        assert!(crop(&page_png(), [500.0, 500.0, 10.0, 10.0], 72.0).is_err());
    }
}
