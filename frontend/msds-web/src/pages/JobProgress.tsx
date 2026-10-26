import { useEffect, useState } from "react";
import { getJob, watchJob } from "../api";
import { STATUS_KO, STEPS } from "../status";
import type { JobDetail, JobStatusChanged } from "../types";

// 작업 하나의 진행 상태. SSE로 상태가 바뀔 때마다 상세(사용량 등)를 다시 읽는다
export function JobProgress({ jobId, back, review }: { jobId: string; back: () => void; review: () => void }) {
  const [job, setJob] = useState<JobDetail | null>(null);
  const [live, setLive] = useState<JobStatusChanged | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const stop = watchJob(
      jobId,
      (s) => {
        setLive(s);
        getJob(jobId).then(setJob, (e: Error) => setError(e.message));
      },
      (e) => setError(e.message),
    );
    return stop;
  }, [jobId]);

  const status = live?.status ?? job?.status;
  const current = status ? STEPS.indexOf(status) : -1;

  return (
    <section className="card">
      <button className="link" onClick={back}>
        ← 목록
      </button>
      <h2>{job?.file_name ?? "불러오는 중…"}</h2>
      <ol className="steps">
        {STEPS.map((s, i) => (
          <li key={s} className={i < current ? "done" : i === current ? "now" : ""}>
            {STATUS_KO[s]}
          </li>
        ))}
      </ol>
      {status === "failed" && <p className="error">실패: {live?.detail ?? job?.detail}</p>}
      {status !== "failed" && live?.detail && <p>{live.detail}</p>}
      {(status === "pending_review" || status === "approved" || status === "published") && (
        <button onClick={review}>{status === "pending_review" ? "검토하기" : "결과 보기"}</button>
      )}
      {job && (
        <p className="muted">
          Claude 호출 {job.usage.calls}번(기록 사용 {job.usage.cached_calls}번) · 입력 {job.usage.input_tokens.toLocaleString()} / 출력{" "}
          {job.usage.output_tokens.toLocaleString()} 토큰
        </p>
      )}
      {error && <p className="error">{error}</p>}
    </section>
  );
}
