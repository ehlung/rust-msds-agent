//! 작업 상태 저장소 (PostgreSQL). api-gateway와 msds-orchestrator만 쓴다(설계 2-2).
//!
//! - gateway: 업로드할 때 작업을 만들고(`create`), 상태·결과를 읽는다.
//! - orchestrator: 상태를 바꾸고, 재시작해도 이어서 처리할 수 있게 내부 상태(`state`)를 저장한다.
//!
//! 상태는 컬럼 하나(`status`)로 둔다. 기존 기능처럼 컬럼의 NULL 조합으로 상태를 추측하지 않는다(설계 3-1).

use chrono::{DateTime, Utc};
use pekko_agent_msds::message::{JobCreated, JobStatus, LlmUsage};
use serde::Serialize;
use sqlx::{PgPool, Row};
use uuid::Uuid;

#[derive(Clone)]
pub struct JobStore {
    pool: PgPool,
}

/// 화면·API에 보여 줄 작업 한 줄.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub struct JobRow {
    pub job_id: Uuid,
    pub file_name: String,
    pub sha256: String,
    pub status: JobStatus,
    pub detail: Option<String>,
    pub created_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// 확인완료한 결과 버전. 게시는 최신 버전이 아니라 이 버전을 쓴다.
    pub approved_version: Option<i32>,
    pub approved_by: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
}

/// 재시작할 때 이어서 처리할 작업.
pub struct Unfinished {
    pub created: JobCreated,
    pub status: JobStatus,
    pub state: Option<serde_json::Value>,
}

/// 작업 하나의 Claude 사용량 합계.
#[derive(Clone, Debug, Default, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS), ts(export))]
pub struct UsageSummary {
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub calls: i64,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub cached_calls: i64,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub input_tokens: i64,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub output_tokens: i64,
}

/// 테이블 생성 잠금 번호. 다른 잠금과 겹치지 않을 아무 값이다.
const MIGRATE_LOCK_ID: i64 = 0x4d53_4453_4a4f_4253; // "MSDSJOBS"

/// 끝난 상태. 이 상태의 작업은 재시작해도 다시 처리하지 않는다.
const FINISHED: &[JobStatus] = &[JobStatus::PendingReview, JobStatus::Approved, JobStatus::Published, JobStatus::Failed];

pub fn status_str(s: JobStatus) -> String {
    serde_json::to_value(s).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default()
}

fn parse_status(s: &str) -> anyhow::Result<JobStatus> {
    Ok(serde_json::from_value(serde_json::Value::String(s.into()))?)
}

impl JobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        Ok(Self::new(PgPool::connect(database_url).await?))
    }

    /// 테이블이 없으면 만든다. gateway와 orchestrator 중 먼저 뜬 쪽이 만든다.
    ///
    /// 두 서비스가 동시에 `CREATE TABLE IF NOT EXISTS`를 실행하면 PostgreSQL에서 중복 키 오류가 난다
    /// (2026-10-23 실제로 남). 그래서 트랜잭션 잠금(advisory lock)을 잡고 한 쪽씩 실행한다.
    pub async fn migrate(&self) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(MIGRATE_LOCK_ID).execute(&mut *tx).await?;
        for sql in [
            r#"CREATE TABLE IF NOT EXISTS msds_jobs (
                job_id      UUID PRIMARY KEY,
                tenant_id   TEXT NOT NULL DEFAULT 'default',
                file_name   TEXT NOT NULL,
                sha256      TEXT NOT NULL,
                pdf_key     TEXT NOT NULL,
                status      TEXT NOT NULL,
                detail      TEXT,
                created_by  TEXT,
                state       JSONB,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
            "CREATE INDEX IF NOT EXISTS msds_jobs_status_idx ON msds_jobs (status)",
            // 확인완료(2026-10-26 추가). 이미 만든 테이블에도 붙도록 따로 더한다
            "ALTER TABLE msds_jobs ADD COLUMN IF NOT EXISTS approved_version INT",
            "ALTER TABLE msds_jobs ADD COLUMN IF NOT EXISTS approved_by TEXT",
            "ALTER TABLE msds_jobs ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ",
            r#"CREATE TABLE IF NOT EXISTS msds_results (
                job_id      UUID NOT NULL REFERENCES msds_jobs (job_id),
                version     INT NOT NULL,
                result      JSONB NOT NULL,
                created_by  TEXT NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (job_id, version)
            )"#,
            r#"CREATE TABLE IF NOT EXISTS msds_llm_usage (
                id                  BIGSERIAL PRIMARY KEY,
                job_id              UUID NOT NULL REFERENCES msds_jobs (job_id),
                stage               TEXT NOT NULL,
                cached              BOOLEAN NOT NULL,
                model               TEXT NOT NULL,
                effort              TEXT,
                input_tokens        BIGINT NOT NULL,
                output_tokens       BIGINT NOT NULL,
                cache_read_tokens   BIGINT NOT NULL,
                cache_write_tokens  BIGINT NOT NULL,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
            "CREATE INDEX IF NOT EXISTS msds_llm_usage_job_idx ON msds_llm_usage (job_id)",
        ] {
            sqlx::query(sql).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// 작업을 만든다. 이미 있으면 그대로 두고 `false`를 돌려준다(같은 메시지가 두 번 와도 안전).
    pub async fn create(&self, job: &JobCreated, created_by: Option<&str>) -> anyhow::Result<bool> {
        let r = sqlx::query(
            "INSERT INTO msds_jobs (job_id, file_name, sha256, pdf_key, status, created_by)
             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (job_id) DO NOTHING",
        )
        .bind(job.job_id)
        .bind(&job.file_name)
        .bind(&job.sha256)
        .bind(&job.pdf_key)
        .bind(status_str(JobStatus::Received))
        .bind(created_by)
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected() == 1)
    }

    pub async fn set_status(&self, job_id: Uuid, status: JobStatus, detail: Option<&str>) -> anyhow::Result<()> {
        sqlx::query("UPDATE msds_jobs SET status = $2, detail = $3, updated_at = now() WHERE job_id = $1")
            .bind(job_id)
            .bind(status_str(status))
            .bind(detail)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 오케스트레이터의 내부 상태(프로필, 경로 판단, 작업 목록)를 저장한다.
    pub async fn save_state(&self, job_id: Uuid, state: &serde_json::Value) -> anyhow::Result<()> {
        sqlx::query("UPDATE msds_jobs SET state = $2, updated_at = now() WHERE job_id = $1")
            .bind(job_id)
            .bind(state)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 끝나지 않은 작업. 오케스트레이터가 재시작할 때 이어서 처리한다.
    pub async fn unfinished(&self) -> anyhow::Result<Vec<Unfinished>> {
        let finished: Vec<String> = FINISHED.iter().map(|s| status_str(*s)).collect();
        let rows = sqlx::query(
            "SELECT job_id, file_name, sha256, pdf_key, status, state FROM msds_jobs
             WHERE status <> ALL($1) ORDER BY created_at",
        )
        .bind(&finished)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                Ok(Unfinished {
                    created: JobCreated {
                        job_id: r.try_get("job_id")?,
                        pdf_key: r.try_get("pdf_key")?,
                        file_name: r.try_get("file_name")?,
                        sha256: r.try_get("sha256")?,
                    },
                    status: parse_status(r.try_get("status")?)?,
                    state: r.try_get("state")?,
                })
            })
            .collect()
    }

    pub async fn get(&self, job_id: Uuid) -> anyhow::Result<Option<JobRow>> {
        let row = sqlx::query(
            "SELECT job_id, file_name, sha256, status, detail, created_by, created_at, updated_at,
                    approved_version, approved_by, approved_at
             FROM msds_jobs WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| job_row(&r)).transpose()
    }

    /// 최근 작업부터.
    pub async fn list(&self, limit: i64) -> anyhow::Result<Vec<JobRow>> {
        let rows = sqlx::query(
            "SELECT job_id, file_name, sha256, status, detail, created_by, created_at, updated_at,
                    approved_version, approved_by, approved_at
             FROM msds_jobs ORDER BY created_at DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(job_row).collect()
    }

    /// 결과를 새 버전으로 저장한다. 1은 에이전트 결과, 사람이 고친 값은 2부터(설계 5-5).
    pub async fn save_result(&self, job_id: Uuid, version: i32, result: &serde_json::Value, created_by: &str) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO msds_results (job_id, version, result, created_by) VALUES ($1, $2, $3, $4)
             ON CONFLICT (job_id, version) DO UPDATE SET result = EXCLUDED.result, created_by = EXCLUDED.created_by, created_at = now()",
        )
        .bind(job_id)
        .bind(version)
        .bind(result)
        .bind(created_by)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 검토자가 고친 결과를 `base` 다음 버전으로 저장한다(설계 5-5).
    /// 검토 대기 중이고 `base`가 지금 최신 버전일 때만 저장하고 `true`를 돌려준다.
    /// 그 사이 다른 사람이 먼저 저장했거나 확인완료됐으면 저장하지 않고 `false`.
    pub async fn save_review(&self, job_id: Uuid, base: i32, result: &serde_json::Value, created_by: &str) -> anyhow::Result<bool> {
        let r = sqlx::query(
            "INSERT INTO msds_results (job_id, version, result, created_by)
             SELECT $1, $2 + 1, $3, $4
             WHERE EXISTS (SELECT 1 FROM msds_jobs WHERE job_id = $1 AND status = $5)
               AND (SELECT max(version) FROM msds_results WHERE job_id = $1) = $2
             ON CONFLICT (job_id, version) DO NOTHING",
        )
        .bind(job_id)
        .bind(base)
        .bind(result)
        .bind(created_by)
        .bind(status_str(JobStatus::PendingReview))
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected() == 1)
    }

    /// 확인완료. 검토 대기 중이고 `version`이 최신 버전일 때만 바꾸고 `true`를 돌려준다.
    /// 화면에 보이던 버전과 확인한 버전이 같다는 것을 보장하려고 버전을 받는다.
    pub async fn approve(&self, job_id: Uuid, version: i32, approved_by: &str) -> anyhow::Result<bool> {
        let r = sqlx::query(
            "UPDATE msds_jobs SET status = $4, detail = NULL, approved_version = $2, approved_by = $3,
                    approved_at = now(), updated_at = now()
             WHERE job_id = $1 AND status = $5
               AND (SELECT max(version) FROM msds_results WHERE job_id = $1) = $2",
        )
        .bind(job_id)
        .bind(version)
        .bind(approved_by)
        .bind(status_str(JobStatus::Approved))
        .bind(status_str(JobStatus::PendingReview))
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected() == 1)
    }

    /// 정해진 버전의 결과. 처음 버전(에이전트 결과)과 비교할 때 쓴다.
    pub async fn result_version(&self, job_id: Uuid, version: i32) -> anyhow::Result<Option<serde_json::Value>> {
        let row = sqlx::query("SELECT result FROM msds_results WHERE job_id = $1 AND version = $2")
            .bind(job_id)
            .bind(version)
            .fetch_optional(&self.pool)
            .await?;
        Ok(match row {
            Some(r) => Some(r.try_get("result")?),
            None => None,
        })
    }

    /// 가장 최근 버전의 결과.
    pub async fn latest_result(&self, job_id: Uuid) -> anyhow::Result<Option<(i32, serde_json::Value)>> {
        let row = sqlx::query("SELECT version, result FROM msds_results WHERE job_id = $1 ORDER BY version DESC LIMIT 1")
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(match row {
            Some(r) => Some((r.try_get("version")?, r.try_get("result")?)),
            None => None,
        })
    }

    pub async fn record_usage(&self, job_id: Uuid, stage: &str, cached: bool, u: &LlmUsage) -> anyhow::Result<()> {
        let effort = u.effort.and_then(|e| serde_json::to_value(e).ok()).and_then(|v| v.as_str().map(String::from));
        sqlx::query(
            "INSERT INTO msds_llm_usage (job_id, stage, cached, model, effort, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(job_id)
        .bind(stage)
        .bind(cached)
        .bind(&u.model)
        .bind(effort)
        .bind(u.input_tokens as i64)
        .bind(u.output_tokens as i64)
        .bind(u.cache_read_tokens as i64)
        .bind(u.cache_write_tokens as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn usage_summary(&self, job_id: Uuid) -> anyhow::Result<UsageSummary> {
        let r = sqlx::query(
            "SELECT count(*) AS calls,
                    count(*) FILTER (WHERE cached) AS cached_calls,
                    coalesce(sum(input_tokens), 0)::BIGINT AS input_tokens,
                    coalesce(sum(output_tokens), 0)::BIGINT AS output_tokens
             FROM msds_llm_usage WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(UsageSummary {
            calls: r.try_get("calls")?,
            cached_calls: r.try_get("cached_calls")?,
            input_tokens: r.try_get("input_tokens")?,
            output_tokens: r.try_get("output_tokens")?,
        })
    }
}

fn job_row(r: &sqlx::postgres::PgRow) -> anyhow::Result<JobRow> {
    Ok(JobRow {
        job_id: r.try_get("job_id")?,
        file_name: r.try_get("file_name")?,
        sha256: r.try_get("sha256")?,
        status: parse_status(r.try_get("status")?)?,
        detail: r.try_get("detail")?,
        created_by: r.try_get("created_by")?,
        created_at: r.try_get("created_at")?,
        updated_at: r.try_get("updated_at")?,
        approved_version: r.try_get("approved_version")?,
        approved_by: r.try_get("approved_by")?,
        approved_at: r.try_get("approved_at")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_text_matches_message_format() {
        assert_eq!(status_str(JobStatus::PendingReview), "pending_review");
        assert_eq!(parse_status("pending_review").unwrap(), JobStatus::PendingReview);
        assert!(parse_status("done").is_err());
    }
}
