//! GHS 그림문자 코드와 한국어 이름. 정해진 이름이라 Claude에게 쓰게 하지 않고 코드가 붙인다.

pub const CODES: [&str; 9] = ["GHS01", "GHS02", "GHS03", "GHS04", "GHS05", "GHS06", "GHS07", "GHS08", "GHS09"];

/// 그림문자의 한국어 이름(기호 모양). 모르는 코드면 `None`.
pub fn name_ko(code: &str) -> Option<&'static str> {
    Some(match code {
        "GHS01" => "폭발성(폭탄의 폭발)",
        "GHS02" => "인화성(불꽃)",
        "GHS03" => "산화성(원 위의 불꽃)",
        "GHS04" => "고압가스(가스 실린더)",
        "GHS05" => "부식성",
        "GHS06" => "급성 독성(해골과 X자형 뼈)",
        "GHS07" => "경고(감탄부호)",
        "GHS08" => "건강 유해성",
        "GHS09" => "수생환경 유해성",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_has_a_name() {
        assert!(CODES.iter().all(|c| name_ko(c).is_some()));
        assert_eq!(name_ko("GHS10"), None);
    }
}
