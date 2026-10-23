//! Kafka 토픽 이름. 브로커의 토픽 자동 생성을 꺼 두었으므로 서비스가 시작할 때 [`ALL`]을 만든다.

pub const JOB_CREATED: &str = "msds.job.created";
pub const INGEST_REQUEST: &str = "msds.ingest.request";
pub const INGEST_DONE: &str = "msds.ingest.done";
pub const TASK_TRANSLATE: &str = "msds.task.translate";
pub const TASK_COMPOSITION: &str = "msds.task.composition";
pub const TASK_HANDLING: &str = "msds.task.handling";
pub const TASK_RESULT: &str = "msds.task.result";
pub const JOB_STATUS: &str = "msds.job.status";
pub const PUBLISH_REQUEST: &str = "msds.publish.request";
pub const PUBLISH_DONE: &str = "msds.publish.done";

pub const ALL: &[&str] = &[
    JOB_CREATED,
    INGEST_REQUEST,
    INGEST_DONE,
    TASK_TRANSLATE,
    TASK_COMPOSITION,
    TASK_HANDLING,
    TASK_RESULT,
    JOB_STATUS,
    PUBLISH_REQUEST,
    PUBLISH_DONE,
];
