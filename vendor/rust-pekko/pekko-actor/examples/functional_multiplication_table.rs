//! 구구단 (Multiplication Table) 함수형 Actor 예제
//! 
//! 이 예제는 함수형 프로그래밍 스타일로 구구단을 계산하는 액터를 보여줍니다.
//! - 불변 상태로 구구단 결과를 저장
//! - 순수 함수로 곱셈 계산
//! - Effect로 결과 출력

use pekko_actor::{
    FunctionalActor, FunctionalActorContext, Effect,
    ActorSystem, ActorPath, ActorAddress,
};
use std::sync::Arc;
use std::collections::HashMap;

/// 구구단 액터의 상태
#[derive(Debug, Clone, PartialEq)]
struct MultiplicationTableState {
    /// 계산된 곱셈 결과를 저장 (예: (2, 3) => 6)
    results: HashMap<(u32, u32), u32>,
    /// 계산된 구구단 목록 (예: 2단, 3단 등)
    calculated_tables: Vec<u32>,
    /// 총 계산 횟수
    calculation_count: u64,
}

impl MultiplicationTableState {
    fn new() -> Self {
        Self {
            results: HashMap::new(),
            calculated_tables: Vec::new(),
            calculation_count: 0,
        }
    }
    
    /// 곱셈 결과를 추가 (불변 방식 - 새 상태 반환)
    fn add_result(mut self, a: u32, b: u32, result: u32) -> Self {
        self.results.insert((a, b), result);
        self.calculation_count += 1;
        self
    }
    
    /// 구구단을 추가
    fn add_table(mut self, number: u32) -> Self {
        if !self.calculated_tables.contains(&number) {
            self.calculated_tables.push(number);
            self.calculated_tables.sort();
        }
        self
    }
    
    /// 특정 곱셈 결과 조회
    fn get_result(&self, a: u32, b: u32) -> Option<u32> {
        self.results.get(&(a, b)).copied()
    }
    
    /// 특정 숫자의 구구단 조회
    fn get_table(&self, number: u32) -> Vec<(u32, u32, u32)> {
        (1..=9)
            .filter_map(|i| {
                self.get_result(number, i)
                    .map(|result| (number, i, result))
            })
            .collect()
    }
}

/// 구구단 메시지 타입
#[derive(Debug, Clone)]
enum MultiplicationMessage {
    /// 특정 곱셈 계산 (예: 2 * 3)
    Calculate { a: u32, b: u32 },
    /// 특정 숫자의 구구단 전체 계산 (예: 2단)
    CalculateTable { number: u32 },
    /// 1부터 9까지 모든 구구단 계산
    CalculateAllTables,
    /// 특정 곱셈 결과 조회
    GetResult { a: u32, b: u32 },
    /// 특정 숫자의 구구단 조회
    GetTable { number: u32 },
    /// 계산된 모든 구구단 목록 조회
    ListCalculatedTables,
    /// 통계 정보 조회
    GetStatistics,
    /// 모든 결과 초기화
    Reset,
}

/// 구구단 함수형 액터
struct MultiplicationTableActor;

impl FunctionalActor for MultiplicationTableActor {
    type State = MultiplicationTableState;
    type Message = MultiplicationMessage;
    
    fn init(_ctx: &FunctionalActorContext) -> Self::State {
        println!("구구단 액터 초기화");
        MultiplicationTableState::new()
    }
    
    fn receive(
        state: Self::State,
        msg: Self::Message,
        _ctx: &FunctionalActorContext,
    ) -> (Self::State, Effect) {
        match msg {
            MultiplicationMessage::Calculate { a, b } => {
                // 순수 함수: 곱셈 계산
                let result = a * b;
                let new_state = state.add_result(a, b, result);
                
                println!("  계산: {} × {} = {}", a, b, result);
                
                (new_state, Effect::None)
            }
            
            MultiplicationMessage::CalculateTable { number } => {
                // 특정 숫자의 구구단 전체 계산
                println!("\n=== {}단 계산 시작 ===", number);
                
                let mut new_state = state.add_table(number);
                
                // 1부터 9까지 곱셈 계산
                for i in 1..=9 {
                    let result = number * i;
                    new_state = new_state.add_result(number, i, result);
                    println!("  {} × {} = {}", number, i, result);
                }
                
                println!("=== {}단 계산 완료 ===\n", number);
                
                (new_state, Effect::None)
            }
            
            MultiplicationMessage::CalculateAllTables => {
                // 1단부터 9단까지 모든 구구단 계산
                println!("\n=== 모든 구구단 계산 시작 ===");
                
                let mut new_state = state;
                
                for number in 1..=9 {
                    new_state = new_state.add_table(number);
                    for i in 1..=9 {
                        let result = number * i;
                        new_state = new_state.add_result(number, i, result);
                    }
                }
                
                println!("=== 모든 구구단 계산 완료 ===\n");
                
                (new_state, Effect::None)
            }
            
            MultiplicationMessage::GetResult { a, b } => {
                // 특정 곱셈 결과 조회
                match state.get_result(a, b) {
                    Some(result) => {
                        println!("조회 결과: {} × {} = {}", a, b, result);
                    }
                    None => {
                        println!("조회 결과: {} × {} 는 아직 계산되지 않았습니다", a, b);
                    }
                }
                
                (state, Effect::None)
            }
            
            MultiplicationMessage::GetTable { number } => {
                // 특정 숫자의 구구단 조회
                let table = state.get_table(number);
                
                if table.is_empty() {
                    println!("{}단은 아직 계산되지 않았습니다", number);
                } else {
                    println!("\n=== {}단 ===", number);
                    for (a, b, result) in table {
                        println!("  {} × {} = {}", a, b, result);
                    }
                    println!();
                }
                
                (state, Effect::None)
            }
            
            MultiplicationMessage::ListCalculatedTables => {
                // 계산된 구구단 목록 출력
                if state.calculated_tables.is_empty() {
                    println!("계산된 구구단이 없습니다");
                } else {
                    println!("계산된 구구단: {:?}", state.calculated_tables);
                }
                
                (state, Effect::None)
            }
            
            MultiplicationMessage::GetStatistics => {
                // 통계 정보 출력
                println!("\n=== 통계 정보 ===");
                println!("총 계산 횟수: {}", state.calculation_count);
                println!("계산된 구구단 수: {}", state.calculated_tables.len());
                println!("저장된 결과 수: {}", state.results.len());
                println!("계산된 구구단: {:?}", state.calculated_tables);
                println!();
                
                (state, Effect::None)
            }
            
            MultiplicationMessage::Reset => {
                // 모든 결과 초기화
                println!("모든 결과 초기화");
                (MultiplicationTableState::new(), Effect::None)
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 함수형 프로그래밍 스타일 구구단 액터 예제 ===\n");
    
    // 액터 시스템 생성
    let system = Arc::new(ActorSystem::new("multiplication-system"));
    let address = ActorAddress::local("multiplication-system");
    let path = ActorPath::new(address, vec!["user".to_string(), "multiplication".to_string()]);
    let ctx = FunctionalActorContext::new(system.clone(), path);
    
    // 초기 상태 생성
    let mut state = MultiplicationTableActor::init(&ctx);
    println!("초기 상태: 계산 횟수 = {}\n", state.calculation_count);
    
    // 예제 1: 개별 곱셈 계산
    println!("--- 예제 1: 개별 곱셈 계산 ---");
    let messages1 = vec![
        MultiplicationMessage::Calculate { a: 2, b: 3 },
        MultiplicationMessage::Calculate { a: 4, b: 5 },
        MultiplicationMessage::Calculate { a: 7, b: 8 },
    ];
    
    for msg in messages1 {
        let (new_state, _) = MultiplicationTableActor::receive(state.clone(), msg, &ctx);
        state = new_state;
    }
    
    println!("\n현재 계산 횟수: {}\n", state.calculation_count);
    
    // 예제 2: 특정 구구단 계산 (2단)
    println!("--- 예제 2: 2단 계산 ---");
    let (new_state, _) = MultiplicationTableActor::receive(
        state.clone(),
        MultiplicationMessage::CalculateTable { number: 2 },
        &ctx,
    );
    state = new_state;
    
    // 예제 3: 다른 구구단 계산 (5단)
    println!("--- 예제 3: 5단 계산 ---");
    let (new_state, _) = MultiplicationTableActor::receive(
        state.clone(),
        MultiplicationMessage::CalculateTable { number: 5 },
        &ctx,
    );
    state = new_state;
    
    // 예제 4: 결과 조회
    println!("--- 예제 4: 결과 조회 ---");
    let queries = vec![
        MultiplicationMessage::GetResult { a: 2, b: 3 },
        MultiplicationMessage::GetResult { a: 5, b: 7 },
        MultiplicationMessage::GetResult { a: 9, b: 9 },
    ];
    
    for query in queries {
        let (new_state, _) = MultiplicationTableActor::receive(state.clone(), query, &ctx);
        state = new_state;
    }
    
    // 예제 5: 구구단 조회
    println!("--- 예제 5: 구구단 조회 ---");
    let (new_state, _) = MultiplicationTableActor::receive(
        state.clone(),
        MultiplicationMessage::GetTable { number: 2 },
        &ctx,
    );
    state = new_state;
    
    // 예제 5-1: 계산된 구구단 목록 조회
    println!("--- 예제 5-1: 계산된 구구단 목록 ---");
    let (new_state, _) = MultiplicationTableActor::receive(
        state.clone(),
        MultiplicationMessage::ListCalculatedTables,
        &ctx,
    );
    state = new_state;
    
    // 예제 6: 통계 정보
    println!("--- 예제 6: 통계 정보 ---");
    let (new_state, _) = MultiplicationTableActor::receive(
        state.clone(),
        MultiplicationMessage::GetStatistics,
        &ctx,
    );
    state = new_state;
    
    // 예제 7: 모든 구구단 계산
    println!("--- 예제 7: 모든 구구단 계산 (1단~9단) ---");
    let (new_state, _) = MultiplicationTableActor::receive(
        state.clone(),
        MultiplicationMessage::CalculateAllTables,
        &ctx,
    );
    state = new_state;
    
    // 최종 통계
    println!("--- 최종 통계 ---");
    let (state_after_all, _) = MultiplicationTableActor::receive(
        state.clone(),
        MultiplicationMessage::GetStatistics,
        &ctx,
    );
    
    // 예제 8: Reset 기능 테스트
    println!("--- 예제 8: Reset (초기화) ---");
    let (reset_state, _) = MultiplicationTableActor::receive(
        state_after_all,
        MultiplicationMessage::Reset,
        &ctx,
    );
    
    // Reset 후 통계 확인
    let (_final_state, _) = MultiplicationTableActor::receive(
        reset_state.clone(),
        MultiplicationMessage::GetStatistics,
        &ctx,
    );
    
    // 불변성 검증: 상태가 변경되지 않았는지 확인
    println!("\n=== 불변성 검증 ===");
    let test_state = MultiplicationTableState::new();
    let test_msg = MultiplicationMessage::Calculate { a: 3, b: 4 };
    
    let (result1, _) = MultiplicationTableActor::receive(test_state.clone(), test_msg.clone(), &ctx);
    let (result2, _) = MultiplicationTableActor::receive(test_state, test_msg, &ctx);
    
    // 같은 입력에 대해 같은 출력 (순수성)
    assert_eq!(result1.calculation_count, result2.calculation_count);
    assert_eq!(result1.get_result(3, 4), result2.get_result(3, 4));
    println!("✓ 순수성 검증 통과: 같은 입력에 대해 같은 출력");
    
    println!("\n=== 예제 완료 ===");
    println!("함수형 프로그래밍의 특징:");
    println!("1. 상태는 불변 - 새 상태를 반환");
    println!("2. 순수 함수 - 같은 입력에 대해 항상 같은 출력");
    println!("3. 부수효과 분리 - Effect로 명시적 관리");
    println!("4. 테스트 용이 - 순수 함수로 쉽게 테스트 가능");
    
    Ok(())
}
