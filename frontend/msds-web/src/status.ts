import type { JobStatus } from "./types";

// 작업 상태 순서(설계 3-1). failed는 어느 단계에서든 될 수 있어 순서에 넣지 않는다
export const STEPS: JobStatus[] = ["received", "ingesting", "routing", "processing", "merging", "pending_review", "approved", "published"];

export const STATUS_KO: Record<JobStatus, string> = {
  received: "접수",
  ingesting: "문서 판정",
  routing: "경로 판단",
  processing: "에이전트 분석",
  merging: "합치기",
  pending_review: "검토 대기",
  approved: "확인완료",
  published: "게시",
  failed: "실패",
};
