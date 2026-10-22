# rust-msds-agent

영문 MSDS(물질안전보건자료) PDF를 올리면 여러 에이전트가 번역·위험물질 정리·취급 이미지 발췌를 나눠 처리하고, 사람이 확인한 결과만 웹에 게시하는 Rust 멀티 에이전트 시스템입니다.

> 작업 중입니다.

## 출처

[yhc007/pekko-agent](https://github.com/yhc007/pekko-agent)(MIT) `db20850`을 기반으로 개선합니다. 첫 커밋이 원본 그대로이며 `baseline` 태그가 붙어 있습니다.

- 원본 대비 변경: `git diff baseline`
- 원본 README: `git show baseline:README.md`

## 빌드에 필요한 폴더

`Cargo.toml`이 액터 프레임워크를 옆 폴더에서 찾으므로 아래 구조로 clone합니다.

```bash
git clone https://github.com/ehlung/rust-msds-agent.git
git clone https://github.com/daseul312/rust-pekko.git
git clone https://github.com/yhc007/coredb.git rust-pekko/coredb
```
