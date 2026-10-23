//! CAS 번호 체크 디지트 검사. 결과의 `cas_check`는 LLM이 아니라 이 코드로 채운다.

use crate::result::CasCheck;

/// `"108-88-3"` 같은 CAS 번호의 형식과 체크 디지트를 검사한다.
///
/// 형식: 숫자 2~7자리 - 숫자 2자리 - 체크 디지트 1자리.
/// 체크 디지트 = (체크 디지트를 뺀 숫자를 오른쪽부터 1, 2, 3… 배 해서 더한 값) mod 10.
pub fn check(cas: &str) -> CasCheck {
    let parts: Vec<&str> = cas.trim().split('-').collect();
    let well_formed = parts.len() == 3
        && (2..=7).contains(&parts[0].len())
        && parts[1].len() == 2
        && parts[2].len() == 1
        && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_digit()));
    if !well_formed {
        return CasCheck::Malformed;
    }
    let body: Vec<u32> = parts[0]
        .chars()
        .chain(parts[1].chars())
        .map(|c| c.to_digit(10).unwrap())
        .collect();
    let sum: u32 = body.iter().rev().enumerate().map(|(i, d)| (i as u32 + 1) * d).sum();
    let check_digit = parts[2].chars().next().unwrap().to_digit(10).unwrap();
    if sum % 10 == check_digit {
        CasCheck::Valid
    } else {
        CasCheck::ChecksumFailed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_numbers_from_test_pdfs() {
        // POLANE 2절, NO.6 3절, Hydrofluoric acid
        for cas in ["108-88-3", "100-41-4", "1330-20-7", "78-93-3", "108-94-1", "123-86-4", "7732-18-5", "7664-39-3"] {
            assert_eq!(check(cas), CasCheck::Valid, "{cas}");
        }
    }

    #[test]
    fn wrong_check_digit() {
        assert_eq!(check("108-88-4"), CasCheck::ChecksumFailed);
    }

    #[test]
    fn malformed() {
        for cas in ["", "비공개", "1-88-3", "108-8-3", "108-88-33", "108 88 3", "12345678-88-3"] {
            assert_eq!(check(cas), CasCheck::Malformed, "{cas:?}");
        }
    }
}
