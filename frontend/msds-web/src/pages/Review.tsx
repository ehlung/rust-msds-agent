import { useEffect, useState, type ReactNode } from "react";
import { ApiError, approve, downloadPreSurvey, fileUrl, getJob, getResult, saveResult } from "../api";
import { STATUS_KO } from "../status";
import type { JobDetail, MsdsResult } from "../types";
import type { Product } from "../types/generated/Product";
import type { Text } from "../types/generated/Text";

const GHS_CODES = ["GHS01", "GHS02", "GHS03", "GHS04", "GHS05", "GHS06", "GHS07", "GHS08", "GHS09"];

// 검토 화면: 왼쪽 원문 페이지, 오른쪽 결과. 고친 값은 저장할 때 서버가 코드 검사를 다시 돌려 경고를 바꿔 준다(설계 3-3)
export function Review({ jobId, back }: { jobId: string; back: () => void }) {
  const [job, setJob] = useState<JobDetail | null>(null);
  const [version, setVersion] = useState(0);
  const [result, setResult] = useState<MsdsResult | null>(null);
  const [dirty, setDirty] = useState(false);
  const [productIdx, setProductIdx] = useState(0);
  const [page, setPage] = useState(1);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<{ error: boolean; text: string } | null>(null);

  useEffect(() => {
    getJob(jobId).then(setJob, (e: Error) => setMessage({ error: true, text: e.message }));
    getResult(jobId).then(
      (r) => {
        setVersion(r.version);
        setResult(r.result);
        setPage(r.result.products[0]?.pages[0] ?? 1);
      },
      (e: Error) => setMessage({ error: true, text: e.message }),
    );
  }, [jobId]);

  if (!result) return <section className="card">{message ? <p className="error">{message.text}</p> : <p>불러오는 중…</p>}</section>;

  const editable = job?.status === "pending_review";
  const product = result.products[productIdx];
  // 결과를 고친다. 원본을 건드리지 않게 복사본을 고쳐 바꿔 끼운다
  const edit = (f: (p: Product) => void) => {
    const next = structuredClone(result);
    f(next.products[productIdx]);
    setResult(next);
    setDirty(true);
  };

  async function run(action: () => Promise<void>) {
    setBusy(true);
    setMessage(null);
    try {
      await action();
    } catch (e) {
      const conflict = e instanceof ApiError && e.status === 409;
      setMessage({ error: true, text: conflict ? `${(e as Error).message} (새로고침해서 최신 버전을 불러오세요)` : (e as Error).message });
    } finally {
      setBusy(false);
    }
  }

  const save = () =>
    run(async () => {
      const r = await saveResult(jobId, version, result);
      setVersion(r.version);
      setResult(r.result);
      setDirty(false);
      setMessage({ error: false, text: `버전 ${r.version}으로 저장했습니다. 경고를 다시 확인하세요.` });
    });

  const confirm = () =>
    run(async () => {
      const row = await approve(jobId, version);
      setJob((j) => (j ? { ...j, ...row } : j));
      setMessage({ error: false, text: `버전 ${version}을 확인완료했습니다.` });
    });

  const warnings = result.warnings.filter((w) => !w.path || w.path.startsWith(`products[${productIdx}]`));
  const imageOf = (id: string | null) => result.images.find((i) => i.id === id);

  return (
    <section>
      <div className="card toolbar">
        <button className="link" onClick={back}>
          ← 진행 상태
        </button>
        <strong>{result.document.file_name}</strong>
        <span className={`badge ${job?.status ?? ""}`}>{job ? STATUS_KO[job.status] : ""}</span>
        <span className="muted">버전 {version}</span>
        <span className="spacer" />
        {editable && (
          <>
            <button disabled={busy || !dirty} onClick={save}>
              저장
            </button>
            <button disabled={busy || dirty} onClick={confirm} title={dirty ? "고친 내용을 먼저 저장하세요" : ""}>
              확인완료
            </button>
          </>
        )}
        {job?.status === "approved" && (
          <>
            <span className="muted">확인완료: {job.approved_by} · 버전 {job.approved_version}</span>
            <button disabled={busy} onClick={() => run(() => downloadPreSurvey(jobId))}>
              사전조사서 내려받기
            </button>
          </>
        )}
      </div>
      {message && <p className={message.error ? "error" : "ok"}>{message.text}</p>}

      {result.products.length > 1 && (
        <div className="tabs">
          {result.products.map((p, i) => (
            <button key={i} className={i === productIdx ? "" : "secondary"} onClick={() => (setProductIdx(i), setPage(p.pages[0]))}>
              제품 {i + 1}: {p.identification.product_name ?? "(이름 없음)"}
            </button>
          ))}
        </div>
      )}

      <div className="review">
        <PageView jobId={jobId} page={page} pages={result.document.pages} setPage={setPage} />

        <div className="fields">
          <Block title={`경고 ${warnings.length}개`}>
            {warnings.length === 0 && <p className="muted">없음</p>}
            <ul className="warnings">
              {warnings.map((w, i) => (
                <li key={i} className={w.severity}>
                  <b>{w.code}</b> {w.message}
                  {w.path && <span className="muted"> · {w.path}</span>}
                </li>
              ))}
            </ul>
          </Block>

          <Block title="제품 정보" pages={product.identification.pages} setPage={setPage}>
            <Field label="제품명" value={product.identification.product_name ?? ""} disabled={!editable} onChange={(v) => edit((p) => (p.identification.product_name = v || null))} />
            <Field label="개정일" value={product.identification.revision_date ?? ""} disabled={!editable} onChange={(v) => edit((p) => (p.identification.revision_date = v || null))} />
          </Block>

          <Block title="유해성" pages={product.hazards.pages} setPage={setPage}>
            <Field label="신호어" value={product.hazards.signal_word?.ko ?? ""} disabled={!editable} onChange={(v) => edit((p) => (p.hazards.signal_word = v ? { src: p.hazards.signal_word?.src ?? "", ko: v } : null))} />
            <h4>그림문자</h4>
            <div className="pictograms">
              {product.hazards.pictograms.map((x, i) => (
                <span key={i} className="pictogram">
                  <FigureImg jobId={jobId} imageKey={imageOf(x.image_id)?.id} />
                  {x.ghs_code} {x.name_ko}
                  {editable && (
                    <button className="link" onClick={() => edit((p) => p.hazards.pictograms.splice(i, 1))}>
                      ✕
                    </button>
                  )}
                </span>
              ))}
              {editable && (
                <select
                  value=""
                  onChange={(e) =>
                    e.target.value && edit((p) => p.hazards.pictograms.push({ ghs_code: e.target.value, name_ko: "", image_id: null, basis: "inferred" }))
                  }
                >
                  <option value="">+ 추가</option>
                  {GHS_CODES.filter((c) => !product.hazards.pictograms.some((x) => x.ghs_code === c)).map((c) => (
                    <option key={c}>{c}</option>
                  ))}
                </select>
              )}
            </div>
            <h4>H 문구</h4>
            <table>
              <tbody>
                {product.hazards.h_statements.map((h, i) => (
                  <tr key={i}>
                    <td className="code">
                      <input value={h.code} disabled={!editable} onChange={(e) => edit((p) => (p.hazards.h_statements[i].code = e.target.value))} />
                    </td>
                    <td>
                      <input value={h.ko} disabled={!editable} onChange={(e) => edit((p) => (p.hazards.h_statements[i].ko = e.target.value))} title={h.src} />
                    </td>
                    <td>
                      {editable && (
                        <button className="link" onClick={() => edit((p) => p.hazards.h_statements.splice(i, 1))}>
                          ✕
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {editable && (
              <button className="link" onClick={() => edit((p) => p.hazards.h_statements.push({ code: "", src: "", ko: "" }))}>
                + H 문구
              </button>
            )}
          </Block>

          <Block title={`성분 ${product.components.length}개`} pages={product.components.flatMap((c) => c.pages)} setPage={setPage}>
            <table>
              <thead>
                <tr>
                  <th>물질명</th>
                  <th>CAS</th>
                  <th>함량</th>
                </tr>
              </thead>
              <tbody>
                {product.components.map((c, i) => (
                  <tr key={i}>
                    <td>
                      <input value={c.name.ko} title={c.name.src} disabled={!editable} onChange={(e) => edit((p) => (p.components[i].name.ko = e.target.value))} />
                    </td>
                    <td className="code">
                      <input value={c.cas_no ?? ""} disabled={!editable} onChange={(e) => edit((p) => (p.components[i].cas_no = e.target.value))} />
                      {c.cas_check !== "valid" && <span className={`badge ${c.cas_check}`}>{c.cas_check}</span>}
                    </td>
                    <td className="code">{c.concentration?.raw ?? ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Block>

          <Block title="취급·보호구" pages={product.handling.pages} setPage={setPage}>
            <TextList label="취급" items={product.handling.precautions} disabled={!editable} onChange={(i, v) => edit((p) => (p.handling.precautions[i].ko = v))} />
            <TextList label="저장" items={product.handling.storage} disabled={!editable} onChange={(i, v) => edit((p) => (p.handling.storage[i].ko = v))} />
            {(["respiratory", "hand", "eye", "skin"] as const).map((k) => (
              <Field
                key={k}
                label={{ respiratory: "호흡", hand: "손", eye: "눈", skin: "피부" }[k]}
                value={product.handling.ppe[k]?.ko ?? ""}
                disabled={!editable}
                onChange={(v) => edit((p) => (p.handling.ppe[k] = v ? { src: p.handling.ppe[k]?.src ?? "", ko: v } : null))}
              />
            ))}
          </Block>

          <Block title={`번역 ${product.sections.length}개 섹션`}>
            {product.sections.map((s, i) => (
              <details key={i}>
                <summary onClick={() => s.pages[0] && setPage(s.pages[0])}>
                  {s.number}. {s.title.ko}
                </summary>
                <textarea
                  value={s.body.ko}
                  disabled={!editable}
                  rows={Math.min(20, s.body.ko.split("\n").length + 1)}
                  onChange={(e) => edit((p) => (p.sections[i].body.ko = e.target.value))}
                />
              </details>
            ))}
          </Block>
        </div>
      </div>
    </section>
  );
}

function Block({ title, pages, setPage, children }: { title: string; pages?: number[]; setPage?: (p: number) => void; children: ReactNode }) {
  const first = pages && pages.length ? Math.min(...pages) : null;
  return (
    <div className="card">
      <h3>
        {title}
        {first && setPage && (
          <button className="link page-link" onClick={() => setPage(first)}>
            원문 {first}쪽
          </button>
        )}
      </h3>
      {children}
    </div>
  );
}

function Field({ label, value, disabled, onChange }: { label: string; value: string; disabled: boolean; onChange: (v: string) => void }) {
  return (
    <label className="field">
      <span>{label}</span>
      <input value={value} disabled={disabled} onChange={(e) => onChange(e.target.value)} />
    </label>
  );
}

function TextList({ label, items, disabled, onChange }: { label: string; items: Text[]; disabled: boolean; onChange: (i: number, v: string) => void }) {
  return (
    <>
      <h4>{label}</h4>
      {items.map((t, i) => (
        <input key={i} className="wide" value={t.ko} title={t.src} disabled={disabled} onChange={(e) => onChange(i, e.target.value)} />
      ))}
    </>
  );
}

// 페이지 이미지는 인증이 필요해서 fetch로 받아 blob URL로 띄운다
function PageView({ jobId, page, pages, setPage }: { jobId: string; page: number; pages: number; setPage: (p: number) => void }) {
  const [url, setUrl] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let revoked: string | null = null;
    setError(null);
    fileUrl(jobId, `pages/${page}.png`).then(
      (u) => {
        revoked = u;
        setUrl(u);
      },
      (e: Error) => setError(e.message),
    );
    return () => {
      if (revoked) URL.revokeObjectURL(revoked);
    };
  }, [jobId, page]);
  return (
    <div className="card page-view">
      <div className="toolbar">
        <button className="secondary" disabled={page <= 1} onClick={() => setPage(page - 1)}>
          ‹
        </button>
        <span>
          원문 {page} / {pages}쪽
        </span>
        <button className="secondary" disabled={page >= pages} onClick={() => setPage(page + 1)}>
          ›
        </button>
      </div>
      {error ? <p className="error">{error}</p> : url && <img src={url} alt={`${page}쪽`} />}
    </div>
  );
}

function FigureImg({ jobId, imageKey }: { jobId: string; imageKey?: string }) {
  const [url, setUrl] = useState<string | null>(null);
  useEffect(() => {
    if (!imageKey) return;
    let u: string | null = null;
    fileUrl(jobId, `images/${imageKey}.png`).then((x) => {
      u = x;
      setUrl(x);
    }, () => setUrl(null));
    return () => {
      if (u) URL.revokeObjectURL(u);
    };
  }, [jobId, imageKey]);
  return url ? <img className="figure" src={url} alt={imageKey} /> : null;
}
