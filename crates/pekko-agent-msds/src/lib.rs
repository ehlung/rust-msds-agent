//! MSDS 서비스들이 주고받는 메시지와 결과 스키마.
//!
//! 형식의 근거와 설명은 설계 문서(「MSDS 에이전틱 AI 과제 - 설계」 4·5절)에 있다.
//! 이 crate는 타입만 정의하고 I/O를 하지 않는다.

pub mod cas;
pub mod envelope;
pub mod message;
pub mod profile;
pub mod result;
pub mod section;
pub mod topics;

pub use envelope::Envelope;
pub use section::SectionKind;
