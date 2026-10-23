//! MSDS 서비스들이 바깥과 연결하는 부분: Kafka(`bus`)와 파일 저장소(`storage`).
//! 메시지 모양은 `pekko-agent-msds`에 있고, 여기는 보내고 받고 저장하는 일만 한다.

pub mod bus;
pub mod storage;

pub use bus::Bus;
pub use storage::LocalStorage;
