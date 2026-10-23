//! MSDS 서비스들이 바깥과 연결하는 부분: Kafka(`bus`), 파일 저장소(`storage`), Claude 호출(`llm`),
//! 작업 상태 저장소(`job_store`, PostgreSQL).
//! 메시지 모양은 `pekko-agent-msds`에 있고, 여기는 보내고 받고 저장하는 일만 한다.

pub mod bus;
pub mod job_store;
pub mod llm;
pub mod storage;

pub use bus::Bus;
pub use job_store::JobStore;
pub use llm::MsdsLlm;
pub use storage::LocalStorage;
