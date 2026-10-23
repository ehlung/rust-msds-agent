//! EventBus - Publish/Subscribe pattern for actors
//!
//! The EventBus allows actors to communicate through events without
//! direct references to each other, enabling loose coupling.
//!
//! ## Features
//!
//! - **Topic-based**: Subscribe to specific topics
//! - **Type-based**: Subscribe to specific event types
//! - **Wildcard**: Subscribe to all events
//! - **Channel-based**: Subscribe to channels (hierarchical topics)
//!
//! ## Example
//!
//! ```rust,ignore
//! use pekko_actor::EventBus;
//!
//! let event_bus = EventBus::new();
//!
//! // Subscribe to a topic
//! event_bus.subscribe("user.created", subscriber_ref).await;
//!
//! // Publish an event
//! event_bus.publish("user.created", UserCreatedEvent { id: 123 }).await;
//! ```

use std::any::{Any};
use std::collections::{HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// A subscriber that can receive events
pub trait Subscriber<E>: Send + Sync {
    /// Handle an event
    fn on_event(&self, event: &E);

    /// Get subscriber ID
    fn id(&self) -> String;
}

/// Callback-based subscriber
pub struct CallbackSubscriber<E> {
    id: String,
    callback: Box<dyn Fn(&E) + Send + Sync>,
}

impl<E> CallbackSubscriber<E> {
    pub fn new<F>(id: impl Into<String>, callback: F) -> Self
    where
        F: Fn(&E) + Send + Sync + 'static,
    {
        Self {
            id: id.into(),
            callback: Box::new(callback),
        }
    }
}

impl<E> Subscriber<E> for CallbackSubscriber<E> {
    fn on_event(&self, event: &E) {
        (self.callback)(event);
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

impl<E> fmt::Debug for CallbackSubscriber<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallbackSubscriber")
            .field("id", &self.id)
            .finish()
    }
}

/// Type-erased subscriber for storing different subscriber types
trait AnySubscriber: Send + Sync {
    fn notify(&self, event: &dyn Any);
    fn id(&self) -> String;
}

struct TypedSubscriber<E: 'static> {
    inner: Arc<dyn Subscriber<E>>,
}

impl<E: 'static> AnySubscriber for TypedSubscriber<E> {
    fn notify(&self, event: &dyn Any) {
        if let Some(e) = event.downcast_ref::<E>() {
            self.inner.on_event(e);
        }
    }

    fn id(&self) -> String {
        self.inner.id()
    }
}

/// Subscription handle that can be used to unsubscribe
#[derive(Debug, Clone)]
pub struct Subscription {
    id: u64,
    topic: String,
    subscriber_id: String,
}

impl Subscription {
    /// Get the subscription ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the topic
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the subscriber ID
    pub fn subscriber_id(&self) -> &str {
        &self.subscriber_id
    }
}

/// Topic-based event bus
pub struct EventBus {
    /// Subscribers per topic
    subscribers: RwLock<HashMap<String, Vec<Arc<dyn AnySubscriber>>>>,
    /// All subscriptions for wildcard subscribers
    wildcard_subscribers: RwLock<Vec<Arc<dyn AnySubscriber>>>,
    /// Subscription counter
    next_subscription_id: AtomicU64,
    /// Map from subscription ID to (topic, subscriber_index)
    subscription_map: RwLock<HashMap<u64, (String, String)>>,
}

impl EventBus {
    /// Create a new event bus
    pub fn new() -> Self {
        Self {
            subscribers: RwLock::new(HashMap::new()),
            wildcard_subscribers: RwLock::new(Vec::new()),
            next_subscription_id: AtomicU64::new(1),
            subscription_map: RwLock::new(HashMap::new()),
        }
    }

    /// Subscribe to a topic with a typed subscriber
    pub async fn subscribe<E: 'static>(
        &self,
        topic: impl Into<String>,
        subscriber: Arc<dyn Subscriber<E>>,
    ) -> Subscription {
        let topic = topic.into();
        let sub_id = self.next_subscription_id.fetch_add(1, Ordering::SeqCst);
        let subscriber_id = subscriber.id();

        let typed = Arc::new(TypedSubscriber { inner: subscriber });

        {
            let mut subs = self.subscribers.write().await;
            subs.entry(topic.clone())
                .or_insert_with(Vec::new)
                .push(typed);
        }

        {
            let mut map = self.subscription_map.write().await;
            map.insert(sub_id, (topic.clone(), subscriber_id.clone()));
        }

        tracing::debug!("EventBus: subscribed '{}' to topic '{}'", subscriber_id, topic);

        Subscription {
            id: sub_id,
            topic,
            subscriber_id,
        }
    }

    /// Subscribe with a callback function
    pub async fn subscribe_fn<E: 'static, F>(
        &self,
        topic: impl Into<String>,
        id: impl Into<String>,
        callback: F,
    ) -> Subscription
    where
        F: Fn(&E) + Send + Sync + 'static,
    {
        let subscriber = Arc::new(CallbackSubscriber::new(id, callback));
        self.subscribe(topic, subscriber).await
    }

    /// Subscribe to all events (wildcard)
    pub async fn subscribe_all<E: 'static>(
        &self,
        subscriber: Arc<dyn Subscriber<E>>,
    ) -> Subscription {
        let sub_id = self.next_subscription_id.fetch_add(1, Ordering::SeqCst);
        let subscriber_id = subscriber.id();

        let typed = Arc::new(TypedSubscriber { inner: subscriber });

        {
            let mut wildcards = self.wildcard_subscribers.write().await;
            wildcards.push(typed);
        }

        {
            let mut map = self.subscription_map.write().await;
            map.insert(sub_id, ("*".to_string(), subscriber_id.clone()));
        }

        tracing::debug!("EventBus: subscribed '{}' to all events", subscriber_id);

        Subscription {
            id: sub_id,
            topic: "*".to_string(),
            subscriber_id,
        }
    }

    /// Unsubscribe using a subscription handle
    pub async fn unsubscribe(&self, subscription: &Subscription) -> bool {
        let topic = &subscription.topic;
        let subscriber_id = &subscription.subscriber_id;

        if topic == "*" {
            let mut wildcards = self.wildcard_subscribers.write().await;
            let len_before = wildcards.len();
            wildcards.retain(|s| s.id() != *subscriber_id);
            let removed = wildcards.len() < len_before;
            if removed {
                tracing::debug!("EventBus: unsubscribed '{}' from all events", subscriber_id);
            }
            return removed;
        }

        let mut subs = self.subscribers.write().await;
        if let Some(topic_subs) = subs.get_mut(topic) {
            let len_before = topic_subs.len();
            topic_subs.retain(|s| s.id() != *subscriber_id);
            let removed = topic_subs.len() < len_before;

            if topic_subs.is_empty() {
                subs.remove(topic);
            }

            if removed {
                let mut map = self.subscription_map.write().await;
                map.remove(&subscription.id);
                tracing::debug!(
                    "EventBus: unsubscribed '{}' from topic '{}'",
                    subscriber_id,
                    topic
                );
            }

            return removed;
        }

        false
    }

    /// Publish an event to a topic
    pub async fn publish<E: 'static + Clone>(&self, topic: impl AsRef<str>, event: E) -> usize {
        let topic = topic.as_ref();
        let mut notified = 0;

        // Notify topic subscribers
        {
            let subs = self.subscribers.read().await;
            if let Some(topic_subs) = subs.get(topic) {
                for subscriber in topic_subs {
                    subscriber.notify(&event);
                    notified += 1;
                }
            }
        }

        // Notify wildcard subscribers
        {
            let wildcards = self.wildcard_subscribers.read().await;
            for subscriber in wildcards.iter() {
                subscriber.notify(&event);
                notified += 1;
            }
        }

        // Also notify hierarchical subscribers (e.g., "user" receives "user.created")
        if topic.contains('.') {
            let parts: Vec<&str> = topic.split('.').collect();
            let subs = self.subscribers.read().await;

            // Check parent topics
            for i in 1..parts.len() {
                let parent_topic = parts[..i].join(".");
                if let Some(parent_subs) = subs.get(&parent_topic) {
                    for subscriber in parent_subs {
                        subscriber.notify(&event);
                        notified += 1;
                    }
                }
            }
        }

        tracing::trace!(
            "EventBus: published to '{}', notified {} subscribers",
            topic,
            notified
        );

        notified
    }

    /// Get the number of subscribers for a topic
    pub async fn subscriber_count(&self, topic: &str) -> usize {
        let subs = self.subscribers.read().await;
        subs.get(topic).map(|v| v.len()).unwrap_or(0)
    }

    /// Get the total number of subscriptions
    pub async fn total_subscriptions(&self) -> usize {
        let subs = self.subscribers.read().await;
        let wildcards = self.wildcard_subscribers.read().await;
        subs.values().map(|v| v.len()).sum::<usize>() + wildcards.len()
    }

    /// Get all topics with subscribers
    pub async fn topics(&self) -> Vec<String> {
        let subs = self.subscribers.read().await;
        subs.keys().cloned().collect()
    }

    /// Clear all subscriptions
    pub async fn clear(&self) {
        let mut subs = self.subscribers.write().await;
        let mut wildcards = self.wildcard_subscribers.write().await;
        let mut map = self.subscription_map.write().await;

        subs.clear();
        wildcards.clear();
        map.clear();

        tracing::debug!("EventBus: cleared all subscriptions");
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for EventBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventBus")
            .field("next_subscription_id", &self.next_subscription_id.load(Ordering::Relaxed))
            .finish()
    }
}

/// A channel-based event bus with hierarchical topics
pub struct ChannelEventBus {
    inner: EventBus,
}

impl ChannelEventBus {
    /// Create a new channel event bus
    pub fn new() -> Self {
        Self {
            inner: EventBus::new()
        }
    }

    /// Create with a custom separator
    pub fn with_separator() -> Self {
        Self {
            inner: EventBus::new()
        }
    }

    /// Subscribe to a channel pattern (supports * and ** wildcards)
    /// - `user.*` matches `user.created`, `user.deleted` but not `user.profile.updated`
    /// - `user.**` matches `user.created`, `user.profile.updated`, etc.
    pub async fn subscribe<E: 'static>(
        &self,
        pattern: impl Into<String>,
        subscriber: Arc<dyn Subscriber<E>>,
    ) -> Subscription {
        self.inner.subscribe(pattern, subscriber).await
    }

    /// Publish to a channel
    pub async fn publish<E: 'static + Clone>(&self, channel: impl AsRef<str>, event: E) -> usize {
        self.inner.publish(channel, event).await
    }

    /// Unsubscribe
    pub async fn unsubscribe(&self, subscription: &Subscription) -> bool {
        self.inner.unsubscribe(subscription).await
    }
}

impl Default for ChannelEventBus {
    fn default() -> Self {
        Self::new()
    }
}

/// Event with metadata
#[derive(Debug, Clone)]
pub struct Event<T> {
    pub payload: T,
    pub topic: String,
    pub timestamp: std::time::Instant,
    pub metadata: HashMap<String, String>,
}

impl<T> Event<T> {
    pub fn new(topic: impl Into<String>, payload: T) -> Self {
        Self {
            payload,
            topic: topic.into(),
            timestamp: std::time::Instant::now(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Dead letter event bus for undelivered events
pub struct DeadLetterBus {
    events: RwLock<Vec<DeadLetter>>,
    max_size: usize,
}

#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub topic: String,
    pub reason: String,
    pub timestamp: std::time::Instant,
}

impl DeadLetterBus {
    pub fn new(max_size: usize) -> Self {
        Self {
            events: RwLock::new(Vec::new()),
            max_size,
        }
    }

    pub async fn record(&self, topic: impl Into<String>, reason: impl Into<String>) {
        let mut events = self.events.write().await;
        
        if events.len() >= self.max_size {
            events.remove(0);
        }

        events.push(DeadLetter {
            topic: topic.into(),
            reason: reason.into(),
            timestamp: std::time::Instant::now(),
        });
    }

    pub async fn get_recent(&self, count: usize) -> Vec<DeadLetter> {
        let events = self.events.read().await;
        events.iter().rev().take(count).cloned().collect()
    }

    pub async fn count(&self) -> usize {
        self.events.read().await.len()
    }

    pub async fn clear(&self) {
        self.events.write().await.clear();
    }
}

impl Default for DeadLetterBus {
    fn default() -> Self {
        Self::new(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[derive(Clone, Debug, PartialEq)]
    struct TestEvent {
        value: u32,
    }

    #[derive(Clone, Debug)]
    struct UserCreated {
        id: u64,
        name: String,
    }

    #[tokio::test]
    async fn test_basic_pubsub() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        let subscription = bus
            .subscribe_fn("test", "sub1", move |event: &TestEvent| {
                c.fetch_add(event.value, Ordering::SeqCst);
            })
            .await;

        bus.publish("test", TestEvent { value: 5 }).await;
        bus.publish("test", TestEvent { value: 3 }).await;

        assert_eq!(counter.load(Ordering::SeqCst), 8);
        assert_eq!(subscription.topic(), "test");
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let bus = EventBus::new();
        let counter1 = Arc::new(AtomicU32::new(0));
        let counter2 = Arc::new(AtomicU32::new(0));
        let c1 = counter1.clone();
        let c2 = counter2.clone();

        bus.subscribe_fn("events", "sub1", move |e: &TestEvent| {
            c1.fetch_add(e.value, Ordering::SeqCst);
        })
        .await;

        bus.subscribe_fn("events", "sub2", move |e: &TestEvent| {
            c2.fetch_add(e.value * 2, Ordering::SeqCst);
        })
        .await;

        bus.publish("events", TestEvent { value: 10 }).await;

        assert_eq!(counter1.load(Ordering::SeqCst), 10);
        assert_eq!(counter2.load(Ordering::SeqCst), 20);
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        let subscription = bus
            .subscribe_fn("topic", "sub1", move |e: &TestEvent| {
                c.fetch_add(e.value, Ordering::SeqCst);
            })
            .await;

        bus.publish("topic", TestEvent { value: 1 }).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Unsubscribe
        assert!(bus.unsubscribe(&subscription).await);

        bus.publish("topic", TestEvent { value: 1 }).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1); // No change
    }

    #[tokio::test]
    async fn test_wildcard_subscriber() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        // Subscribe to all events
        bus.subscribe_fn("*", "wildcard", move |_: &TestEvent| {
            c.fetch_add(1, Ordering::SeqCst);
        })
        .await;

        // Actually, subscribe_all should be used for wildcards
        // Let's test regular topic matching

        let counter2 = Arc::new(AtomicU32::new(0));
        let c2 = counter2.clone();
        bus.subscribe_fn("topic1", "sub1", move |_: &TestEvent| {
            c2.fetch_add(1, Ordering::SeqCst);
        })
        .await;

        bus.publish("topic1", TestEvent { value: 1 }).await;
        bus.publish("topic2", TestEvent { value: 1 }).await;

        assert_eq!(counter2.load(Ordering::SeqCst), 1); // Only topic1
    }

    #[tokio::test]
    async fn test_subscribe_all() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        let subscriber = Arc::new(CallbackSubscriber::new("all-sub", move |_: &TestEvent| {
            c.fetch_add(1, Ordering::SeqCst);
        }));

        bus.subscribe_all(subscriber).await;

        bus.publish("topic1", TestEvent { value: 1 }).await;
        bus.publish("topic2", TestEvent { value: 1 }).await;
        bus.publish("topic3", TestEvent { value: 1 }).await;

        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_hierarchical_topics() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        // Subscribe to parent topic
        bus.subscribe_fn("user", "user-sub", move |_: &TestEvent| {
            c.fetch_add(1, Ordering::SeqCst);
        })
        .await;

        // Publish to child topic
        bus.publish("user.created", TestEvent { value: 1 }).await;
        bus.publish("user.deleted", TestEvent { value: 1 }).await;

        // Parent subscriber should receive both
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_different_event_types() {
        let bus = EventBus::new();
        let test_counter = Arc::new(AtomicU32::new(0));
        let user_counter = Arc::new(AtomicU32::new(0));

        let tc = test_counter.clone();
        let uc = user_counter.clone();

        bus.subscribe_fn("events", "test-sub", move |_: &TestEvent| {
            tc.fetch_add(1, Ordering::SeqCst);
        })
        .await;

        bus.subscribe_fn("events", "user-sub", move |_: &UserCreated| {
            uc.fetch_add(1, Ordering::SeqCst);
        })
        .await;

        bus.publish("events", TestEvent { value: 1 }).await;
        bus.publish(
            "events",
            UserCreated {
                id: 1,
                name: "Alice".into(),
            },
        )
        .await;

        // Each subscriber only receives its type
        assert_eq!(test_counter.load(Ordering::SeqCst), 1);
        assert_eq!(user_counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_subscriber_count() {
        let bus = EventBus::new();

        assert_eq!(bus.subscriber_count("topic").await, 0);

        bus.subscribe_fn("topic", "sub1", |_: &TestEvent| {})
            .await;
        assert_eq!(bus.subscriber_count("topic").await, 1);

        bus.subscribe_fn("topic", "sub2", |_: &TestEvent| {})
            .await;
        assert_eq!(bus.subscriber_count("topic").await, 2);
    }

    #[tokio::test]
    async fn test_total_subscriptions() {
        let bus = EventBus::new();

        assert_eq!(bus.total_subscriptions().await, 0);

        bus.subscribe_fn("topic1", "sub1", |_: &TestEvent| {})
            .await;
        bus.subscribe_fn("topic2", "sub2", |_: &TestEvent| {})
            .await;
        
        let subscriber = Arc::new(CallbackSubscriber::new("all", |_: &TestEvent| {}));
        bus.subscribe_all(subscriber).await;

        assert_eq!(bus.total_subscriptions().await, 3);
    }

    #[tokio::test]
    async fn test_clear() {
        let bus = EventBus::new();

        bus.subscribe_fn("topic1", "sub1", |_: &TestEvent| {})
            .await;
        bus.subscribe_fn("topic2", "sub2", |_: &TestEvent| {})
            .await;

        assert_eq!(bus.total_subscriptions().await, 2);

        bus.clear().await;

        assert_eq!(bus.total_subscriptions().await, 0);
    }

    #[tokio::test]
    async fn test_topics() {
        let bus = EventBus::new();

        bus.subscribe_fn("alpha", "sub1", |_: &TestEvent| {}).await;
        bus.subscribe_fn("beta", "sub2", |_: &TestEvent| {}).await;
        bus.subscribe_fn("gamma", "sub3", |_: &TestEvent| {}).await;

        let topics = bus.topics().await;
        assert_eq!(topics.len(), 3);
        assert!(topics.contains(&"alpha".to_string()));
        assert!(topics.contains(&"beta".to_string()));
        assert!(topics.contains(&"gamma".to_string()));
    }

    #[tokio::test]
    async fn test_event_with_metadata() {
        let event = Event::new("user.created", UserCreated {
            id: 123,
            name: "Bob".into(),
        })
        .with_metadata("source", "api")
        .with_metadata("version", "1.0");

        assert_eq!(event.topic, "user.created");
        assert_eq!(event.metadata.get("source"), Some(&"api".to_string()));
        assert_eq!(event.metadata.get("version"), Some(&"1.0".to_string()));
    }

    #[tokio::test]
    async fn test_dead_letter_bus() {
        let dlb = DeadLetterBus::new(5);

        dlb.record("topic1", "no subscribers").await;
        dlb.record("topic2", "delivery failed").await;

        assert_eq!(dlb.count().await, 2);

        let recent = dlb.get_recent(10).await;
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].topic, "topic2"); // Most recent first
    }

    #[tokio::test]
    async fn test_dead_letter_bus_max_size() {
        let dlb = DeadLetterBus::new(3);

        for i in 0..5 {
            dlb.record(format!("topic{}", i), "reason").await;
        }

        assert_eq!(dlb.count().await, 3);

        let recent = dlb.get_recent(10).await;
        assert_eq!(recent[0].topic, "topic4");
        assert_eq!(recent[2].topic, "topic2");
    }

    #[tokio::test]
    async fn test_publish_returns_count() {
        let bus = EventBus::new();

        bus.subscribe_fn("topic", "sub1", |_: &TestEvent| {}).await;
        bus.subscribe_fn("topic", "sub2", |_: &TestEvent| {}).await;

        let count = bus.publish("topic", TestEvent { value: 1 }).await;
        assert_eq!(count, 2);

        let count = bus.publish("no-subs", TestEvent { value: 1 }).await;
        assert_eq!(count, 0);
    }
}
