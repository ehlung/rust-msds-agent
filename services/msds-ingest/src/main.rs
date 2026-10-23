//! msds-ingest — 업로드된 PDF를 처음 여는 서비스. LLM 없이 코드로 문서 상태를 잰다.
//!
//! - 서비스: `msds.ingest.request`를 받아 문서 프로필을 만들고 `msds.ingest.done`으로 보낸다.
//! - 개발용: `msds-ingest analyze <pdf>`는 Kafka 없이 프로필 JSON을 출력한다.

mod analyze;
mod pdf;

use std::time::Instant;

use anyhow::Context;
use pekko_agent_msds::message::{ErrorInfo, IngestDone, IngestOutcome, IngestRequest};
use pekko_agent_msds::profile::{DocumentProfile, PageImage};
use pekko_agent_msds::{topics, Envelope};
use pekko_agent_msds_io::bus::{commit, decode};
use pekko_agent_msds_io::storage::page_image_key;
use pekko_agent_msds_io::{Bus, LocalStorage};
use tracing::{error, info, warn};
use uuid::Uuid;

const SERVICE: &str = "msds-ingest";
const DEFAULT_DPI: f32 = 150.0;

/// pdfium과 렌더링 설정. pdfium 라이브러리는 프로세스당 한 번만 불러올 수 있어 시작할 때 하나 만들어 같이 쓴다.
struct Engine {
    pdfium: pdfium_render::prelude::Pdfium,
    dpi: f32,
}

impl Engine {
    fn from_env() -> anyhow::Result<Self> {
        let dir = std::env::var("PDFIUM_LIB_DIR").unwrap_or_else(|_| "./".into());
        Ok(Self {
            pdfium: pdf::bind(&dir)?,
            dpi: std::env::var("MSDS_RENDER_DPI").ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_DPI),
        })
    }
}

/// 렌더링한 페이지 이미지(쪽 번호, PNG).
type Rendered = Vec<(u32, Vec<u8>)>;

/// PDF → 프로필. 페이지 이미지 키는 `job_id`가 있을 때만 채운다.
fn build_profile(engine: &Engine, pdf_bytes: &[u8], job_id: Option<Uuid>, render: bool) -> anyhow::Result<(DocumentProfile, Rendered)> {
    let pages = pdf::read_pages(&engine.pdfium, pdf_bytes)?;
    let a = analyze::analyze(&pages);
    let mut rendered = Vec::new();
    if render {
        for p in &pages {
            rendered.push((p.number, pdf::render_png(&engine.pdfium, pdf_bytes, p.number, engine.dpi)?));
        }
    }
    let page_images = match job_id {
        Some(id) if render => pages.iter().map(|p| PageImage { page: p.number, key: page_image_key(id, p.number) }).collect(),
        _ => Vec::new(),
    };
    let profile = DocumentProfile {
        pages: pages.len() as u32,
        language: a.language,
        text_layer: a.text_layer,
        products: a.products,
        figures: a.figures,
        page_images,
    };
    Ok((profile, rendered))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?)).init();
    // 시작할 때 pdfium을 불러서 설정 오류를 바로 드러낸다
    let engine = Engine::from_env()?;

    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("analyze") => analyze_cli(&engine, &args[2..]),
        Some(other) => anyhow::bail!("알 수 없는 명령: {other} (사용: msds-ingest [analyze <pdf> [--render <폴더>]])"),
        None => serve(engine).await,
    }
}

/// 개발용: PDF 하나를 분석해 프로필 JSON을 출력한다. `--render <폴더>`면 페이지 PNG도 저장한다.
fn analyze_cli(engine: &Engine, args: &[String]) -> anyhow::Result<()> {
    let path = args.first().context("PDF 경로가 필요함")?;
    let render_dir = args.iter().position(|a| a == "--render").and_then(|i| args.get(i + 1));
    let bytes = std::fs::read(path).with_context(|| format!("읽기 실패: {path}"))?;
    let (profile, rendered) = build_profile(engine, &bytes, None, render_dir.is_some())?;
    if let Some(dir) = render_dir {
        std::fs::create_dir_all(dir)?;
        for (n, png) in rendered {
            std::fs::write(format!("{dir}/{n}.png"), png)?;
        }
    }
    println!("{}", serde_json::to_string_pretty(&profile)?);
    Ok(())
}

async fn serve(engine: Engine) -> anyhow::Result<()> {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let storage = LocalStorage::new(std::env::var("MSDS_STORAGE_DIR").context("MSDS_STORAGE_DIR가 필요함")?)?;

    let bus = Bus::new(&brokers, SERVICE)?;
    bus.ensure_topics(topics::ALL, 3).await?;
    let consumer = bus.consumer(SERVICE, &[topics::INGEST_REQUEST])?;
    info!(brokers, storage = %storage.root().display(), "msds-ingest 시작");

    let engine = std::sync::Arc::new(engine);
    loop {
        let msg = tokio::select! {
            m = consumer.recv() => m,
            _ = tokio::signal::ctrl_c() => { info!("종료"); return Ok(()); }
        };
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "받기 실패");
                continue;
            }
        };
        match decode::<IngestRequest>(&msg) {
            Ok(env) => {
                let req = env.payload;
                let outcome = handle(engine.clone(), &storage, &req).await;
                let done = Envelope::new(SERVICE, topics::INGEST_DONE, req.job_id, IngestDone { job_id: req.job_id, outcome });
                if let Err(e) = bus.publish(topics::INGEST_DONE, &done).await {
                    // 결과를 못 보냈으면 커밋하지 않는다 → 재시작하면 같은 요청을 다시 처리한다
                    error!(job_id = %req.job_id, error = %e, "결과 보내기 실패");
                    continue;
                }
            }
            // 읽을 수 없는 메시지는 다시 받아도 같으므로 기록만 하고 넘어간다
            Err(e) => error!(error = %e, "요청을 읽지 못해 건너뜀"),
        }
        commit(&consumer, &msg)?;
    }
}

async fn handle(engine: std::sync::Arc<Engine>, storage: &LocalStorage, req: &IngestRequest) -> IngestOutcome {
    let started = Instant::now();
    let fail = |code: &str, e: anyhow::Error| {
        error!(job_id = %req.job_id, code, error = %format!("{e:#}"), "판정 실패");
        IngestOutcome::Failed { error: ErrorInfo { code: code.into(), message: format!("{e:#}") } }
    };
    let bytes = match storage.get(&req.pdf_key).await {
        Ok(b) => b,
        Err(e) => return fail("storage_read_failed", e),
    };
    let job_id = req.job_id;
    // pdfium 호출은 블로킹이라 별도 스레드에서 돌린다
    let result = tokio::task::spawn_blocking(move || build_profile(&engine, &bytes, Some(job_id), true)).await;
    let (profile, rendered) = match result {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return fail("pdf_unreadable", e),
        Err(e) => return fail("internal", e.into()),
    };
    for (n, png) in rendered {
        if let Err(e) = storage.put(&page_image_key(job_id, n), &png).await {
            return fail("storage_write_failed", e);
        }
    }
    info!(
        job_id = %job_id,
        pages = profile.pages,
        products = profile.products.len(),
        figures = profile.figures.len(),
        text_layer = ?profile.text_layer,
        ms = started.elapsed().as_millis() as u64,
        "판정 완료"
    );
    IngestOutcome::Ok { profile }
}
