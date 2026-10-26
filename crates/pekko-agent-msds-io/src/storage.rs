use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use uuid::Uuid;

/// 로컬 폴더를 쓰는 파일 저장소. 키는 `jobs/<job_id>/pages/1.png` 같은 상대 경로다.
///
/// 키는 메시지로 건너오므로 그대로 경로로 쓰지 않고 검사한다(`..`이나 절대 경로로 저장소 밖을 가리키지 못하게).
#[derive(Clone, Debug)]
pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub fn new(root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).with_context(|| format!("저장소 폴더를 만들 수 없음: {}", root.display()))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path(&self, key: &str) -> anyhow::Result<PathBuf> {
        validate_key(key)?;
        Ok(self.root.join(key))
    }

    pub async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        let path = self.path(key)?;
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        // 다 쓴 뒤 이름을 바꿔, 읽는 쪽이 반쯤 쓴 파일을 보지 않게 한다
        let tmp = path.with_extension("part");
        tokio::fs::write(&tmp, bytes).await?;
        tokio::fs::rename(&tmp, &path).await?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let path = self.path(key)?;
        tokio::fs::read(&path).await.with_context(|| format!("저장소에서 읽기 실패: {key}"))
    }
}

pub fn job_pdf_key(job_id: Uuid) -> String {
    format!("jobs/{job_id}/source.pdf")
}

pub fn page_image_key(job_id: Uuid, page: u32) -> String {
    format!("jobs/{job_id}/pages/{page}.png")
}

/// 취급 에이전트가 고른 그림. `id`는 그림 후보 번호(`fig-3`)다.
pub fn figure_image_key(job_id: Uuid, id: &str) -> String {
    format!("jobs/{job_id}/images/{id}.png")
}

fn validate_key(key: &str) -> anyhow::Result<()> {
    let ok = !key.is_empty()
        && key.len() <= 512
        && !key.starts_with('/')
        && key.split('/').all(|part| !part.is_empty() && part != "." && part != "..")
        && key.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '-' | '_' | '.'));
    if !ok {
        bail!("허용되지 않는 저장소 키: {key:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_normal_keys() {
        for k in ["jobs/6f1c2a3e-0000-4000-8000-000000000001/source.pdf", "jobs/x/pages/12.png"] {
            assert!(validate_key(k).is_ok(), "{k}");
        }
    }

    #[test]
    fn rejects_escaping_keys() {
        for k in ["", "/etc/passwd", "../x", "jobs/../../x", "jobs//x", "jobs/./x", "jobs/x y.png", "jobs/한글.png", "jobs\\x"] {
            assert!(validate_key(k).is_err(), "{k:?}");
        }
    }

    #[tokio::test]
    async fn put_then_get() {
        let dir = std::env::temp_dir().join(format!("msds-storage-test-{}", std::process::id()));
        let s = LocalStorage::new(&dir).unwrap();
        s.put("jobs/a/pages/1.png", b"png").await.unwrap();
        assert_eq!(s.get("jobs/a/pages/1.png").await.unwrap(), b"png");
        assert!(s.get("../outside").await.is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
