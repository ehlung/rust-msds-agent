# 구구단 함수형 Actor 예제

이 예제는 함수형 프로그래밍 스타일로 구구단을 계산하는 액터를 보여줍니다.

## 실행 방법

```bash
cargo run --example functional_multiplication_table
```

## 주요 기능

### 1. 개별 곱셈 계산
```rust
MultiplicationMessage::Calculate { a: 2, b: 3 }
// 결과: 2 × 3 = 6
```

### 2. 특정 구구단 계산
```rust
MultiplicationMessage::CalculateTable { number: 2 }
// 2단 전체 계산 (2×1부터 2×9까지)
```

### 3. 모든 구구단 계산
```rust
MultiplicationMessage::CalculateAllTables
// 1단부터 9단까지 모든 구구단 계산
```

### 4. 결과 조회
```rust
MultiplicationMessage::GetResult { a: 5, b: 7 }
// 5 × 7 = 35 조회
```

### 5. 구구단 조회
```rust
MultiplicationMessage::GetTable { number: 2 }
// 2단 전체 조회
```

### 6. 통계 정보
```rust
MultiplicationMessage::GetStatistics
// 총 계산 횟수, 계산된 구구단 수 등
```

### 7. 초기화
```rust
MultiplicationMessage::Reset
// 모든 결과 초기화
```

## 함수형 프로그래밍 특징

### 1. 불변성 (Immutability)
```rust
let state = MultiplicationTableState::new();
let (new_state, _) = actor.receive(state, msg, &ctx);

// 원본 state는 변경되지 않음
assert_eq!(state.calculation_count, 0);
assert_eq!(new_state.calculation_count, 1);
```

### 2. 순수 함수 (Pure Functions)
```rust
// 같은 입력에 대해 항상 같은 출력
let state1 = CounterState { count: 5 };
let state2 = CounterState { count: 5 };
let msg = CounterMessage::Increment;

let (result1, _) = actor.receive(state1, msg.clone(), &ctx);
let (result2, _) = actor.receive(state2, msg, &ctx);

assert_eq!(result1, result2); // 항상 같음
```

### 3. 상태 전이 (State Transition)
```rust
// 상태는 새로 생성되어 반환됨
let state = MultiplicationTableState::new();
let new_state = state.add_result(2, 3, 6);

// 원본은 변경되지 않음
assert_ne!(state, new_state);
```

## 예제 출력

```
=== 함수형 프로그래밍 스타일 구구단 액터 예제 ===

구구단 액터 초기화
초기 상태: 계산 횟수 = 0

--- 예제 1: 개별 곱셈 계산 ---
  계산: 2 × 3 = 6
  계산: 4 × 5 = 20
  계산: 7 × 8 = 56

--- 예제 2: 2단 계산 ---
=== 2단 계산 시작 ===
  2 × 1 = 2
  2 × 2 = 4
  ...
  2 × 9 = 18
=== 2단 계산 완료 ===

--- 최종 통계 ---
총 계산 횟수: 102
계산된 구구단 수: 9
저장된 결과 수: 81
계산된 구구단: [1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## 학습 포인트

1. **불변 상태**: 상태는 절대 변경되지 않고, 항상 새 상태를 반환
2. **순수 함수**: 같은 입력에 대해 항상 같은 출력 보장
3. **명시적 부수효과**: Effect로 부수효과를 명시적으로 표현
4. **테스트 용이성**: 순수 함수이므로 단위 테스트가 쉬움
5. **합성 가능성**: 작은 함수들을 조합하여 복잡한 동작 구현
