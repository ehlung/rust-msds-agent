//! Integration tests for Ask pattern
//! 
//! These tests demonstrate the full ask pattern working with actual actors.

#[cfg(test)]
mod tests {
    use crate::actor::Actor;
    use crate::actor_context::ActorContext;
    use crate::actor_system::ActorSystem;
    use crate::ask::ReplyTo;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration;
    
    // ========================================
    // Example 1: Simple Calculator Actor
    // ========================================
    
    /// A simple calculator actor that responds to queries
    #[derive(Clone)]
    struct CalculatorActor {
        value: i64,
    }
    
    /// Calculator messages
    #[derive(Debug)]
    enum CalculatorMessage {
        Add(i64),
        Subtract(i64),
        Multiply(i64),
        GetValue { reply_to: ReplyTo<i64> },
        Reset,
    }
    
    #[async_trait]
    impl Actor for CalculatorActor {
        type Message = CalculatorMessage;
        
        async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
            match msg {
                CalculatorMessage::Add(n) => {
                    self.value += n;
                    tracing::debug!("Added {}, new value: {}", n, self.value);
                }
                CalculatorMessage::Subtract(n) => {
                    self.value -= n;
                    tracing::debug!("Subtracted {}, new value: {}", n, self.value);
                }
                CalculatorMessage::Multiply(n) => {
                    self.value *= n;
                    tracing::debug!("Multiplied by {}, new value: {}", n, self.value);
                }
                CalculatorMessage::GetValue { reply_to } => {
                    tracing::debug!("Replying with value: {}", self.value);
                    reply_to.reply(self.value);
                }
                CalculatorMessage::Reset => {
                    self.value = 0;
                    tracing::debug!("Reset to 0");
                }
            }
        }
    }
    
    #[tokio::test]
    async fn test_calculator_ask_pattern() {
        let system = Arc::new(ActorSystem::new("test-system"));
        
        let calc = CalculatorActor { value: 0 };
        let calc_ref = system.spawn(calc, "calculator").await.unwrap();
        
        // Tell: Add some values
        calc_ref.tell(CalculatorMessage::Add(10)).unwrap();
        calc_ref.tell(CalculatorMessage::Add(5)).unwrap();
        calc_ref.tell(CalculatorMessage::Multiply(2)).unwrap();
        
        // Give actor time to process
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Ask: Get the current value
        let result = calc_ref
            .ask(|reply_to| CalculatorMessage::GetValue { reply_to })
            .await;
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 30); // (0 + 10 + 5) * 2 = 30
    }
    
    // ========================================
    // Example 2: Key-Value Store Actor
    // ========================================
    
    use std::collections::HashMap;
    
    #[derive(Clone)]
    struct KVStoreActor {
        data: HashMap<String, String>,
    }
    
    #[derive(Debug)]
    enum KVMessage {
        Get { key: String, reply_to: ReplyTo<Option<String>> },
        Set { key: String, value: String },
        Delete { key: String, reply_to: ReplyTo<bool> },
        Count { reply_to: ReplyTo<usize> },
    }
    
    #[async_trait]
    impl Actor for KVStoreActor {
        type Message = KVMessage;
        
        async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
            match msg {
                KVMessage::Get { key, reply_to } => {
                    let value = self.data.get(&key).cloned();
                    reply_to.reply(value);
                }
                KVMessage::Set { key, value } => {
                    self.data.insert(key, value);
                }
                KVMessage::Delete { key, reply_to } => {
                    let existed = self.data.remove(&key).is_some();
                    reply_to.reply(existed);
                }
                KVMessage::Count { reply_to } => {
                    reply_to.reply(self.data.len());
                }
            }
        }
    }
    
    #[tokio::test]
    async fn test_kvstore_ask_pattern() {
        let system = Arc::new(ActorSystem::new("test-system"));
        
        let store = KVStoreActor { data: HashMap::new() };
        let store_ref = system.spawn(store, "kvstore").await.unwrap();
        
        // Set some values
        store_ref.tell(KVMessage::Set { 
            key: "name".to_string(), 
            value: "Paul".to_string() 
        }).unwrap();
        store_ref.tell(KVMessage::Set { 
            key: "lang".to_string(), 
            value: "Rust".to_string() 
        }).unwrap();
        
        // Wait for processing
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Ask: Get a value
        let name = store_ref
            .ask(|reply_to| KVMessage::Get { key: "name".to_string(), reply_to })
            .await
            .unwrap();
        assert_eq!(name, Some("Paul".to_string()));
        
        // Ask: Get count
        let count = store_ref
            .ask(|reply_to| KVMessage::Count { reply_to })
            .await
            .unwrap();
        assert_eq!(count, 2);
        
        // Ask: Delete a key
        let deleted = store_ref
            .ask(|reply_to| KVMessage::Delete { key: "lang".to_string(), reply_to })
            .await
            .unwrap();
        assert!(deleted);
        
        // Verify deletion
        let count = store_ref
            .ask(|reply_to| KVMessage::Count { reply_to })
            .await
            .unwrap();
        assert_eq!(count, 1);
    }
    
    // ========================================
    // Example 3: Timeout Test
    // ========================================
    
    #[derive(Clone)]
    struct SlowActor;
    
    #[derive(Debug)]
    enum SlowMessage {
        SlowOperation { delay_ms: u64, reply_to: ReplyTo<String> },
    }
    
    #[async_trait]
    impl Actor for SlowActor {
        type Message = SlowMessage;
        
        async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
            match msg {
                SlowMessage::SlowOperation { delay_ms, reply_to } => {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    reply_to.reply("completed".to_string());
                }
            }
        }
    }
    
    #[tokio::test]
    async fn test_ask_timeout() {
        let system = Arc::new(ActorSystem::new("test-system"));
        
        let actor = SlowActor;
        let actor_ref = system.spawn(actor, "slow-actor").await.unwrap();
        
        // This should timeout (100ms delay, 50ms timeout)
        let result = actor_ref
            .ask_with_timeout(
                |reply_to| SlowMessage::SlowOperation { delay_ms: 100, reply_to },
                Duration::from_millis(50)
            )
            .await;
        
        assert!(result.is_err());
    }
    
    #[tokio::test]
    async fn test_ask_success_within_timeout() {
        let system = Arc::new(ActorSystem::new("test-system"));
        
        let actor = SlowActor;
        let actor_ref = system.spawn(actor, "slow-actor").await.unwrap();
        
        // This should succeed (50ms delay, 200ms timeout)
        let result = actor_ref
            .ask_with_timeout(
                |reply_to| SlowMessage::SlowOperation { delay_ms: 50, reply_to },
                Duration::from_millis(200)
            )
            .await;
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "completed");
    }
}
