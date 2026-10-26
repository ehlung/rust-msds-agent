#!/bin/bash
# 화면(frontend/msds-web)이 쓰는 TypeScript 타입을 Rust 타입에서 만든다(ts-rs).
# Rust의 결과·메시지 타입을 바꾸면 이 스크립트를 다시 돌린다. CI는 생성본이 최신인지 확인한다.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT="$PWD/frontend/msds-web/src/types/generated"
rm -rf "$OUT"
TS_RS_EXPORT_DIR="$OUT" cargo test -q -p pekko-agent-msds -p pekko-agent-msds-io --features ts export_bindings > /dev/null
ls "$OUT" | wc -l | xargs echo "생성한 타입 파일:"
