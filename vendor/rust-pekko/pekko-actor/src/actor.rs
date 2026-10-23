//! Core actor trait and related types

use std::error::Error;
use async_trait::async_trait;
use crate::actor_context::ActorContext;

/// Core trait that all actors must implement
#[async_trait]
pub trait Actor: Send + Sync + 'static {
    /// The message type this actor can receive
    type Message: Send + 'static;

    /// Handle an incoming message
    async fn receive(&mut self, msg: Self::Message, ctx: &mut ActorContext<Self>);

    /// Called before the actor starts processing messages
    async fn pre_start(&mut self) {
        // Default implementation does nothing
    }

    /// Called after the actor has stopped processing messages
    async fn post_stop(&mut self) {
        // Default implementation does nothing
    }

    /// Called before the actor is restarted due to failure
    async fn pre_restart(&mut self, _error: &dyn Error) {
        // Default: call post_stop
    }

    /// Called after the actor has been restarted
    async fn post_restart(&mut self) {
        // Default: call pre_start
    }
}

/// Error type for actor operations
#[derive(Debug, thiserror::Error)]
pub enum ActorError {
    #[error("Actor failed: {0}")]
    Failure(String),
    #[error("Actor stopped")]
    Stopped,
}

/// Trait for actors that need to handle actor lifecycle events
#[async_trait]
pub trait ActorLifecycle: Send + Sync {
    async fn on_failure(&mut self, error: ActorError);
    async fn on_child_terminated(&mut self, child_name: String);
}

/// Common actor behaviors
pub trait ActorBehavior<A: Actor> {
    /// Become a different behavior (stack-based)
    fn change_behavior(&mut self, behavior: Box<dyn Fn(&mut A, A::Message) + Send + Sync>);
    
    /// Unbecome the current behavior
    fn revert_behavior(&mut self);
    
    /// Check if there are stacked behaviors
    fn has_stacked_behaviors(&self) -> bool;
}

/// Props for actor creation
pub struct Props<A: Actor> {
    // 액터 팩토리 함수 (선택적)
    factory: Option<Box<dyn Fn() -> A + Send + Sync>>,
    // 액터 설정 (선택적)
    config: Option<ActorConfig>,
}

impl<A: Actor> Props<A> {
    pub fn new() -> Self {
        Self {
            factory: None,
            config: None,
        }
    }
    
    /// 액터 팩토리 함수로 Props 생성
    pub fn from_factory<F>(factory: F) -> Self 
    where 
        F: Fn() -> A + Send + Sync + 'static 
    {
        Self {
            factory: Some(Box::new(factory)),
            config: None,
        }
    }
    
    /// 액터 인스턴스로 Props 생성 (Clone이 필요한 경우)
    pub fn from_instance(actor: A) -> Self 
    where 
        A: Clone 
    {
        Self {
            factory: Some(Box::new(move || actor.clone())),
            config: None,
        }
    }
    
    /// 액터 팩토리 함수 실행
    pub fn create_actor(&self) -> Option<A> {
        self.factory.as_ref().map(|f| f())
    }
    
    /// 설정 가져오기
    pub fn config(&self) -> &Option<ActorConfig> {
        &self.config
    }
    
    /// 설정 설정
    pub fn with_config(mut self, config: ActorConfig) -> Self {
        self.config = Some(config);
        self
    }
}

/// 액터 설정 (향후 확장 가능)
#[derive(Debug, Clone)]
pub struct ActorConfig {
    pub dispatcher: Option<String>,
    pub mailbox: Option<String>,
    pub supervision_strategy: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)] // Clone 트레이트 추가
    struct TestActor {
        count: u32,
    }

    #[derive(Debug)]
    enum TestMessage {
        Increment,
        GetCount,
    }

    #[async_trait]
    impl Actor for TestActor {
        type Message = TestMessage;

        async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
            match msg {
                TestMessage::Increment => {
                    self.count += 1;
                }
                TestMessage::GetCount => {
                    // In a real implementation, we would send a response
                    println!("Current count: {}", self.count);
                }
            }
        }
    }

    #[test]
    fn test_actor_trait_compiles() {
        let _actor = TestActor { count: 0 };
        // This test just ensures the trait implementation compiles
    }

    #[test]
    fn test_props_creation() {
        // Test Props::new()
        let props = Props::<TestActor>::new();
        assert!(props.factory.is_none());
        assert!(props.config.is_none());

        // Test Props::from_factory()
        let props = Props::from_factory(|| TestActor { count: 42 });
        assert!(props.factory.is_some());
        assert!(props.config.is_none());
        
        // Test actor creation from factory
        let actor = props.create_actor().unwrap();
        assert_eq!(actor.count, 42);

        // Test Props::from_instance (Clone이 필요한 경우)
        let actor = TestActor { count: 100 };
        let props = Props::from_instance(actor);
        assert!(props.factory.is_some());
        assert!(props.config.is_none());
        
        // Test actor creation from instance
        let actor = props.create_actor().unwrap();
        assert_eq!(actor.count, 100);
    }

    #[test]
    fn test_actor_config() {
        let config = ActorConfig {
            dispatcher: Some("default".to_string()),
            mailbox: Some("unbounded".to_string()),
            supervision_strategy: Some("restart".to_string()),
        };

        assert_eq!(config.dispatcher, Some("default".to_string()));
        assert_eq!(config.mailbox, Some("unbounded".to_string()));
        assert_eq!(config.supervision_strategy, Some("restart".to_string()));
    }
    
    #[test]
    fn test_props_with_config() {
        let config = ActorConfig {
            dispatcher: Some("test-dispatcher".to_string()),
            mailbox: Some("test-mailbox".to_string()),
            supervision_strategy: Some("test-strategy".to_string()),
        };

        let props = Props::from_factory(|| TestActor { count: 0 })
            .with_config(config.clone());

        assert!(props.config().is_some());
        assert_eq!(props.config().as_ref().unwrap().dispatcher, Some("test-dispatcher".to_string()));
    }
}