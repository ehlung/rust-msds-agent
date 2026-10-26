import { useEffect, useState } from "react";
import { listJobs, uploadPdf } from "../api";
import { STATUS_KO } from "../status";
import type { JobRow } from "../types";

export function Jobs({ open }: { open: (jobId: string) => void }) {
  const [jobs, setJobs] = useState<JobRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);

  useEffect(() => {
    listJobs().then(setJobs, (e: Error) => setError(e.message));
  }, []);

  async function upload(file: File) {
    setUploading(true);
    setError(null);
    try {
      const job = await uploadPdf(file);
      open(job.job_id);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setUploading(false);
    }
  }

  return (
    <section>
      <div className="card">
        <h2>PDF 올리기</h2>
        <input
          type="file"
          accept="application/pdf"
          disabled={uploading}
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (f) upload(f);
          }}
        />
        {uploading && <p>올리는 중…</p>}
        {error && <p className="error">{error}</p>}
      </div>

      <div className="card">
        <h2>작업 목록</h2>
        {jobs === null ? (
          <p>불러오는 중…</p>
        ) : jobs.length === 0 ? (
          <p>아직 작업이 없습니다.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>파일</th>
                <th>상태</th>
                <th>올린 시각</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((j) => (
                <tr key={j.job_id} onClick={() => open(j.job_id)} className="clickable">
                  <td>{j.file_name}</td>
                  <td>
                    <span className={`badge ${j.status}`}>{STATUS_KO[j.status]}</span>
                  </td>
                  <td>{new Date(j.created_at).toLocaleString("ko-KR")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}
