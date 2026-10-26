# msds-web

MSDS 업로드·진행 상태·검토 화면 (React + TypeScript, Vite).

```bash
npm install
npm run dev      # http://localhost:5173, /api는 로컬 api-gateway(8080)로 넘긴다
npm run build    # 타입 검사 + 빌드
```

- 로그인은 gateway의 API 키(`.env`의 `ADMIN_API_KEY` 등)로 토큰을 받는다.
- `src/types/generated/`는 Rust 타입에서 만든 것이라 직접 고치지 않는다. Rust의 결과·메시지 타입을 바꾸면 저장소 루트에서 `scripts/gen-ts.sh`를 다시 돌린다. CI가 생성본이 최신인지 확인한다.
- 화면에 WASM을 쓰지 않는 이유와 타입을 생성하는 이유는 설계 문서 6절 5번에 있다.
