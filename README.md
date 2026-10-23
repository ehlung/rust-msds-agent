# rust-msds-agent

영문 MSDS(물질안전보건자료) PDF를 올리면 여러 에이전트가 번역·위험물질 정리·취급 이미지 발췌를 나눠 처리하고, 사람이 확인한 결과만 웹에 게시하는 Rust 멀티 에이전트 시스템입니다.

> 작업 중입니다.

## 출처

[yhc007/pekko-agent](https://github.com/yhc007/pekko-agent)(MIT) `db20850`을 기반으로 개선합니다. 첫 커밋이 원본 그대로이며 `baseline` 태그가 붙어 있습니다.

- 원본 대비 변경: `git diff baseline`
- 원본 README: `git show baseline:README.md`
- 액터 crate 3개는 [daseul312/rust-pekko](https://github.com/daseul312/rust-pekko)(Apache-2.0)에서 `vendor/rust-pekko/`로 복사했습니다.

## 받기

액터 crate는 `vendor/rust-pekko/`에 포함되어 있어 이 저장소만 clone하면 됩니다.

```bash
git clone https://github.com/ehlung/rust-msds-agent.git
```
