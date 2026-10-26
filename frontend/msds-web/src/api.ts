// gateway 호출. 토큰은 탭을 닫으면 사라지게 sessionStorage에 둔다.
import type { JobDetail, JobRow, JobStatusChanged, MsdsResult, ResultResponse } from "./types";

const TOKEN_KEY = "msds-token";

export function getToken(): string | null {
  try {
    return sessionStorage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
}

export function setToken(token: string | null) {
  try {
    if (token) sessionStorage.setItem(TOKEN_KEY, token);
    else sessionStorage.removeItem(TOKEN_KEY);
  } catch {
    // 저장소를 못 쓰는 환경이면 새로고침 때 다시 로그인한다
  }
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers = new Headers(init.headers);
  if (token) headers.set("authorization", `Bearer ${token}`);
  const res = await fetch(path, { ...init, headers });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    if (res.status === 401) setToken(null);
    throw new ApiError(res.status, body?.error ?? `${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export async function login(apiKey: string): Promise<void> {
  const r = await request<{ token: string }>("/api/auth/token", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ api_key: apiKey }),
  });
  setToken(r.token);
}

export const listJobs = () => request<JobRow[]>("/api/msds/jobs?limit=100");
export const getJob = (id: string) => request<JobDetail>(`/api/msds/jobs/${id}`);

export const getResult = (id: string) => request<ResultResponse>(`/api/msds/jobs/${id}/result`);

export const saveResult = (id: string, baseVersion: number, result: MsdsResult) =>
  request<ResultResponse>(`/api/msds/jobs/${id}/result`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ base_version: baseVersion, result }),
  });

export const approve = (id: string, version: number) =>
  request<JobRow>(`/api/msds/jobs/${id}/approve`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ version }),
  });

/** 작업 파일(페이지 이미지·그림)을 받아 blob URL로. `<img src>`는 Authorization 헤더를 못 붙인다(설계 3-3). */
export async function fileUrl(id: string, path: string): Promise<string> {
  const res = await fetch(`/api/msds/jobs/${id}/files/${path}`, { headers: { authorization: `Bearer ${getToken() ?? ""}` } });
  if (!res.ok) throw new ApiError(res.status, `파일을 받지 못함: ${path}`);
  return URL.createObjectURL(await res.blob());
}

/** 확인완료한 버전의 사전조사서(CSV)를 내려받는다. 링크로는 Authorization 헤더를 못 붙여 fetch로 받는다. */
export async function downloadPreSurvey(id: string): Promise<void> {
  const res = await fetch(`/api/msds/jobs/${id}/export/pre-survey.csv`, { headers: { authorization: `Bearer ${getToken() ?? ""}` } });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new ApiError(res.status, body?.error ?? `사전조사서를 받지 못함 (${res.status})`);
  }
  // 서버가 정한 한글 파일 이름(filename*)을 그대로 쓴다
  const encoded = /filename\*=UTF-8''([^;]+)/.exec(res.headers.get("content-disposition") ?? "")?.[1];
  const a = document.createElement("a");
  a.href = URL.createObjectURL(await res.blob());
  a.download = encoded ? decodeURIComponent(encoded) : "pre-survey.csv";
  a.click();
  // 내려받기가 시작된 뒤에 놓아준다
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

export function uploadPdf(file: File): Promise<JobRow> {
  const form = new FormData();
  form.append("file", file, file.name);
  return request<JobRow>("/api/msds/jobs", { method: "POST", body: form });
}

/**
 * 진행 상태 SSE를 받는다. EventSource는 Authorization 헤더를 못 붙여서 fetch 스트림으로 읽는다(설계 3-3).
 * 돌려준 함수를 부르면 연결을 끊는다.
 */
export function watchJob(id: string, onStatus: (s: JobStatusChanged) => void, onError: (e: Error) => void): () => void {
  const ctrl = new AbortController();
  (async () => {
    const res = await fetch(`/api/msds/jobs/${id}/events`, {
      headers: { authorization: `Bearer ${getToken() ?? ""}` },
      signal: ctrl.signal,
    });
    if (!res.ok || !res.body) throw new ApiError(res.status, `진행 상태를 받지 못함 (${res.status})`);
    const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
    let buf = "";
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += value;
      // 이벤트는 빈 줄로 나뉜다. data 줄만 쓴다(keep-alive 주석 줄은 버린다)
      let end;
      while ((end = buf.indexOf("\n\n")) >= 0) {
        const block = buf.slice(0, end);
        buf = buf.slice(end + 2);
        const data = block
          .split("\n")
          .filter((l) => l.startsWith("data:"))
          .map((l) => l.slice(5).trimStart())
          .join("\n");
        if (data) onStatus(JSON.parse(data) as JobStatusChanged);
      }
    }
  })().catch((e: unknown) => {
    if (!ctrl.signal.aborted) onError(e instanceof Error ? e : new Error(String(e)));
  });
  return () => ctrl.abort();
}
