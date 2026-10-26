// Rust 타입에서 만든 타입(generated/)을 다시 내보낸다. generated/는 직접 고치지 않는다(scripts/gen-ts.sh).
export type { JobRow } from "./generated/JobRow";
export type { JobStatus } from "./generated/JobStatus";
export type { JobStatusChanged } from "./generated/JobStatusChanged";
export type { MsdsResult } from "./generated/MsdsResult";
export type { UsageSummary } from "./generated/UsageSummary";

import type { JobRow } from "./generated/JobRow";
import type { MsdsResult } from "./generated/MsdsResult";
import type { UsageSummary } from "./generated/UsageSummary";

// gateway 응답 모양(services/api-gateway/src/msds.rs). 안의 타입은 모두 생성된 것이다
export type JobDetail = JobRow & { usage: UsageSummary };
export type ResultResponse = { version: number; result: MsdsResult };
