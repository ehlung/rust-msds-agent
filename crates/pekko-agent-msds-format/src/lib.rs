//! 확인완료된 결과(`msds-result/1`)를 고객 형식으로 바꾸는 변환기(설계 5-8).
//!
//! 결과 JSON은 하나로 두고 형식마다 변환기만 둔다(시작 전 정리 결정 4). 판단이 필요 없는 일이라 LLM을 쓰지 않는다.
//! 파일·네트워크를 쓰지 않는 순수 함수라 gateway(내려받기), publisher, 공개 페이지(Workers, wasm32)가 같은 코드를 쓴다.

pub mod kr_lists;
pub mod pre_survey;
pub mod table;

pub use kr_lists::KrLists;
pub use pre_survey::{pre_survey, Approval, KrList, Listing, NotChecked, RegulationCheck};
pub use table::{Column, Table};
