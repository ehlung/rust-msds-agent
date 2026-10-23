# Functional Programming Style Actors

이 문서는 pekko-actor의 함수형 프로그래밍 스타일 Actor 사용법을 설명합니다.

## 개요

함수형 프로그래밍 스타일 Actor는 다음과 같은 특징을 가집니다:

1. **불변성 (Immutability)**: 상태는 불변이며, 새로운 상태를 반환합니다
2. **순수 함수 (Pure Functions)**: 같은 입력에 대해 항상 같은 출력을 반환합니다
3. **명시적 부수효과 (Explicit Side Effects)**: 부수효과는 Effect로 명시적으로 표현됩니다
4. **합성 가능성 (Composability)**: 작은 함수들을 조합하여 복잡한 동작을 만들 수 있습니다

## 기본 사용법

### 1. FunctionalActor Trait 구현

```rust
use pekko_actor::{FunctionalActor, FunctionalActorContext, Effect};

#[derive(Debug, Clone, PartialEq)]
struct CounterState {
    count: u32,
}

enum CounterMessage {
    Increment,
    Decrement,
    Reset,
}

struct CounterActor;

impl FunctionalActor for CounterActor {
    type State = CounterState;
    type Message = CounterMessage;
    
    fn init(_ctx: &FunctionalActorContext) -> Self::State {
        CounterState { count: 0 }
    }
    
    fn receive(
        state: Self::State,
        msg: Self::Message,
        _ctx: &FunctionalActorContext,
    ) -> (Self::State, Effect) {
        match msg {
            CounterMessage::Increment => (
                CounterState { count: state.count + 1 },
                Effect::None,
            ),
            CounterMessage::Decrement => (
                CounterState { count: state.count.saturating_sub(1) },
                Effect::None,
            ),
            CounterMessage::Reset => (
                CounterState { count: 0 },
                Effect::None,
            ),
        }
    }
}
```

### 2. Effect 사용

Effect는 부수효과를 명시적으로 표현합니다:

```rust
fn receive(
    state: Self::State,
    msg: Self::Message,
    ctx: &FunctionalActorContext,
) -> (Self::State, Effect) {
    match msg {
        Message::SendToOther { target, data } => (
            state,
            Effect::send(target, data), // 메시지 전송
        ),
        Message::SpawnChild { name } => (
            state,
            Effect::spawn(name), // 자식 액터 생성
        ),
        Message::MultipleActions => (
            new_state,
            Effect::batch(vec![
                Effect::send("actor1".to_string(), msg1),
                Effect::send("actor2".to_string(), msg2),
            ]),
        ),
        _ => (state, Effect::None),
    }
}
```

### 3. State Reducer 패턴

StateReducer trait을 사용하면 Redux 스타일의 reducer 패턴을 구현할 수 있습니다:

```rust
use pekko_actor::StateReducer;

impl StateReducer for CounterActor {
    type State = CounterState;
    type Action = CounterMessage;
    
    fn reduce(state: Self::State, action: Self::Action) -> Self::State {
        match action {
            CounterMessage::Increment => CounterState { count: state.count + 1 },
            CounterMessage::Decrement => CounterState { count: state.count.saturating_sub(1) },
            CounterMessage::Reset => CounterState { count: 0 },
        }
    }
    
    fn initial_state() -> Self::State {
        CounterState { count: 0 }
    }
}
```

## 장점

### 1. 테스트 용이성

순수 함수이므로 테스트가 쉽습니다:

```rust
#[test]
fn test_counter_increment() {
    let state = CounterState { count: 5 };
    let msg = CounterMessage::Increment;
    let ctx = create_test_context();
    
    let (new_state, _) = CounterActor::receive(state, msg, &ctx);
    
    assert_eq!(new_state.count, 6);
    // 원본 상태는 변경되지 않음
}
```

### 2. 불변성 보장

상태가 불변이므로 동시성 문제가 줄어듭니다:

```rust
let state1 = CounterState { count: 10 };
let (new_state, _) = CounterActor::receive(state1.clone(), msg, &ctx);

// state1은 여전히 count: 10
// new_state는 count: 11
assert_ne!(state1, new_state);
```

### 3. 순수성 (Purity)

같은 입력에 대해 항상 같은 출력을 보장합니다:

```rust
let state = CounterState { count: 5 };
let msg = CounterMessage::Increment;

let (result1, _) = CounterActor::receive(state.clone(), msg.clone(), &ctx);
let (result2, _) = CounterActor::receive(state, msg, &ctx);

assert_eq!(result1, result2); // 항상 같음
```

## 기존 Actor와의 비교

### 객체지향 스타일 (기존)
```rust
impl Actor for Counter {
    fn receive(&mut self, msg: Message, _ctx: &mut ActorContext<Self>) -> ... {
        match msg {
            Message::Increment => {
                self.count += 1; // 상태 직접 변경
            }
        }
    }
}
```

### 함수형 스타일 (새로운)
```rust
impl FunctionalActor for CounterActor {
    fn receive(
        state: Self::State,
        msg: Self::Message,
        _ctx: &FunctionalActorContext,
    ) -> (Self::State, Effect) {
        match msg {
            Message::Increment => (
                CounterState { count: state.count + 1 }, // 새 상태 반환
                Effect::None,
            )
        }
    }
}
```

## 마이그레이션 가이드

기존 Actor를 FunctionalActor로 마이그레이션하려면:

1. 상태를 별도의 구조체로 분리
2. `receive` 메소드를 순수 함수로 변경
3. 부수효과를 Effect로 변환
4. `apply_effect`에서 실제 부수효과 처리

## 예제

더 많은 예제는 `examples/functional_counter.rs`를 참고하세요.

```bash
cargo run --example functional_counter
```
