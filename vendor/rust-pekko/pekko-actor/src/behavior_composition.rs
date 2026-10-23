// behavior_composition.rs - Behavior composition operators
// OrElse, And, Intercept, TransformMessages for composing actor behaviors

use std::fmt;

// ============================================================================
// MessageHandler - Core trait for composable behaviors
// ============================================================================

/// Result of handling a message in a composed behavior
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleResult {
    /// Message was handled successfully
    Handled,
    /// Message was not handled (try next in chain)
    Unhandled,
    /// Message was handled but behavior should stop
    Stop,
    /// Message was handled with a named transition
    Transition(String),
}

impl HandleResult {
    pub fn is_handled(&self) -> bool {
        matches!(self, HandleResult::Handled | HandleResult::Stop | HandleResult::Transition(_))
    }

    pub fn is_unhandled(&self) -> bool {
        matches!(self, HandleResult::Unhandled)
    }

    pub fn is_stop(&self) -> bool {
        matches!(self, HandleResult::Stop)
    }
}

// ============================================================================
// BehaviorComposer - Functional behavior composition
// ============================================================================

/// A composable message handler function
pub struct HandlerFn<M> {
    name: String,
    handler: Box<dyn Fn(&M) -> HandleResult + Send + Sync>,
}

impl<M> HandlerFn<M> {
    pub fn new<F>(name: &str, f: F) -> Self
    where
        F: Fn(&M) -> HandleResult + Send + Sync + 'static,
    {
        Self {
            name: name.to_string(),
            handler: Box::new(f),
        }
    }

    pub fn handle(&self, msg: &M) -> HandleResult {
        (self.handler)(msg)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<M> fmt::Debug for HandlerFn<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandlerFn")
            .field("name", &self.name)
            .finish()
    }
}

// ============================================================================
// OrElse - Try first handler, fall back to second if unhandled
// ============================================================================

/// Composes two handlers: tries the first, falls back to second if Unhandled
pub struct OrElse<M> {
    primary: HandlerFn<M>,
    fallback: HandlerFn<M>,
    stats: CompositionStats,
}

impl<M> OrElse<M> {
    pub fn new(primary: HandlerFn<M>, fallback: HandlerFn<M>) -> Self {
        Self {
            primary,
            fallback,
            stats: CompositionStats::default(),
        }
    }

    pub fn handle(&mut self, msg: &M) -> HandleResult {
        self.stats.total_messages += 1;
        let result = self.primary.handle(msg);
        if result.is_unhandled() {
            self.stats.fallback_count += 1;
            self.fallback.handle(msg)
        } else {
            self.stats.primary_count += 1;
            result
        }
    }

    pub fn stats(&self) -> &CompositionStats {
        &self.stats
    }

    pub fn primary_name(&self) -> &str {
        self.primary.name()
    }

    pub fn fallback_name(&self) -> &str {
        self.fallback.name()
    }
}

impl<M> fmt::Debug for OrElse<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OrElse({} | {})",
            self.primary.name(),
            self.fallback.name()
        )
    }
}

// ============================================================================
// AndThen - Run both handlers, combine results
// ============================================================================

/// Strategy for combining results from And composition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombineStrategy {
    /// Both must handle (AND semantics)
    Both,
    /// Either can handle (OR semantics)
    Either,
    /// First result takes priority
    FirstWins,
    /// Second result takes priority
    SecondWins,
}

/// Composes two handlers: runs both and combines results
pub struct AndThen<M> {
    first: HandlerFn<M>,
    second: HandlerFn<M>,
    strategy: CombineStrategy,
    stats: CompositionStats,
}

impl<M> AndThen<M> {
    pub fn new(first: HandlerFn<M>, second: HandlerFn<M>, strategy: CombineStrategy) -> Self {
        Self {
            first,
            second,
            strategy,
            stats: CompositionStats::default(),
        }
    }

    pub fn handle(&mut self, msg: &M) -> HandleResult {
        self.stats.total_messages += 1;
        let r1 = self.first.handle(msg);
        let r2 = self.second.handle(msg);

        let result = match self.strategy {
            CombineStrategy::Both => {
                if r1.is_handled() && r2.is_handled() {
                    // If either stops, stop
                    if r1.is_stop() || r2.is_stop() {
                        HandleResult::Stop
                    } else {
                        HandleResult::Handled
                    }
                } else {
                    HandleResult::Unhandled
                }
            }
            CombineStrategy::Either => {
                if r1.is_handled() || r2.is_handled() {
                    if r1.is_stop() || r2.is_stop() {
                        HandleResult::Stop
                    } else {
                        HandleResult::Handled
                    }
                } else {
                    HandleResult::Unhandled
                }
            }
            CombineStrategy::FirstWins => r1,
            CombineStrategy::SecondWins => r2,
        };

        if result.is_handled() {
            self.stats.primary_count += 1;
        }
        result
    }

    pub fn stats(&self) -> &CompositionStats {
        &self.stats
    }
}

impl<M> fmt::Debug for AndThen<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AndThen({} & {}, {:?})",
            self.first.name(),
            self.second.name(),
            self.strategy
        )
    }
}

// ============================================================================
// Intercept - Pre/post processing around a handler
// ============================================================================

/// Intercept result determines whether to proceed with the inner handler
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterceptDecision {
    /// Continue to the inner handler
    Continue,
    /// Skip the inner handler, use this result
    ShortCircuit(HandleResult),
    /// Transform the message (conceptually — we pass through)
    ContinueModified,
}

/// Wraps a handler with pre/post interceptors
pub struct Intercept<M> {
    inner: HandlerFn<M>,
    pre_hook: Option<Box<dyn Fn(&M) -> InterceptDecision + Send + Sync>>,
    post_hook: Option<Box<dyn Fn(&M, &HandleResult) -> HandleResult + Send + Sync>>,
    name: String,
    stats: InterceptStats,
}

/// Statistics for intercept operations
#[derive(Debug, Clone, Default)]
pub struct InterceptStats {
    pub total_messages: u64,
    pub pre_short_circuits: u64,
    pub post_modifications: u64,
    pub inner_handled: u64,
}

impl<M> Intercept<M> {
    pub fn new(inner: HandlerFn<M>) -> Self {
        let name = format!("Intercept({})", inner.name());
        Self {
            inner,
            pre_hook: None,
            post_hook: None,
            name,
            stats: InterceptStats::default(),
        }
    }

    pub fn with_pre_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&M) -> InterceptDecision + Send + Sync + 'static,
    {
        self.pre_hook = Some(Box::new(hook));
        self
    }

    pub fn with_post_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&M, &HandleResult) -> HandleResult + Send + Sync + 'static,
    {
        self.post_hook = Some(Box::new(hook));
        self
    }

    pub fn handle(&mut self, msg: &M) -> HandleResult {
        self.stats.total_messages += 1;

        // Pre-hook
        if let Some(ref pre) = self.pre_hook {
            match pre(msg) {
                InterceptDecision::ShortCircuit(result) => {
                    self.stats.pre_short_circuits += 1;
                    return result;
                }
                InterceptDecision::Continue | InterceptDecision::ContinueModified => {}
            }
        }

        // Inner handler
        let result = self.inner.handle(msg);
        if result.is_handled() {
            self.stats.inner_handled += 1;
        }

        // Post-hook
        if let Some(ref post) = self.post_hook {
            let modified = post(msg, &result);
            if modified != result {
                self.stats.post_modifications += 1;
            }
            modified
        } else {
            result
        }
    }

    pub fn stats(&self) -> &InterceptStats {
        &self.stats
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<M> fmt::Debug for Intercept<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

// ============================================================================
// HandlerChain - Chain of handlers with fallthrough
// ============================================================================

/// A chain of handlers tried in sequence until one handles the message
pub struct HandlerChain<M> {
    handlers: Vec<HandlerFn<M>>,
    default_result: HandleResult,
    stats: ChainStats,
}

/// Statistics for chain handling
#[derive(Debug, Clone, Default)]
pub struct ChainStats {
    pub total_messages: u64,
    pub handler_hits: Vec<u64>,
    pub default_count: u64,
}

impl<M> HandlerChain<M> {
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
            default_result: HandleResult::Unhandled,
            stats: ChainStats::default(),
        }
    }

    pub fn with_default(mut self, result: HandleResult) -> Self {
        self.default_result = result;
        self
    }

    pub fn add(mut self, handler: HandlerFn<M>) -> Self {
        self.stats.handler_hits.push(0);
        self.handlers.push(handler);
        self
    }

    pub fn handle(&mut self, msg: &M) -> HandleResult {
        self.stats.total_messages += 1;

        for (i, handler) in self.handlers.iter().enumerate() {
            let result = handler.handle(msg);
            if result.is_handled() {
                self.stats.handler_hits[i] += 1;
                return result;
            }
        }

        self.stats.default_count += 1;
        self.default_result.clone()
    }

    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    pub fn handler_names(&self) -> Vec<&str> {
        self.handlers.iter().map(|h| h.name()).collect()
    }

    pub fn stats(&self) -> &ChainStats {
        &self.stats
    }
}

impl<M> Default for HandlerChain<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> fmt::Debug for HandlerChain<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HandlerChain({:?})", self.handler_names())
    }
}

// ============================================================================
// MessageTransform - Transform messages between types
// ============================================================================

/// Transforms messages from one type to another before handling
pub struct MessageTransform<A, B> {
    transform: Box<dyn Fn(&A) -> Option<B> + Send + Sync>,
    handler: HandlerFn<B>,
    name: String,
    stats: TransformStats,
}

/// Statistics for message transforms
#[derive(Debug, Clone, Default)]
pub struct TransformStats {
    pub total_input: u64,
    pub successful_transforms: u64,
    pub failed_transforms: u64,
    pub handled_after_transform: u64,
}

impl<A, B> MessageTransform<A, B> {
    pub fn new<F>(name: &str, transform: F, handler: HandlerFn<B>) -> Self
    where
        F: Fn(&A) -> Option<B> + Send + Sync + 'static,
    {
        Self {
            transform: Box::new(transform),
            handler,
            name: name.to_string(),
            stats: TransformStats::default(),
        }
    }

    pub fn handle(&mut self, msg: &A) -> HandleResult {
        self.stats.total_input += 1;
        match (self.transform)(msg) {
            Some(transformed) => {
                self.stats.successful_transforms += 1;
                let result = self.handler.handle(&transformed);
                if result.is_handled() {
                    self.stats.handled_after_transform += 1;
                }
                result
            }
            None => {
                self.stats.failed_transforms += 1;
                HandleResult::Unhandled
            }
        }
    }

    pub fn stats(&self) -> &TransformStats {
        &self.stats
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<A, B> fmt::Debug for MessageTransform<A, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MessageTransform({})", self.name)
    }
}

// ============================================================================
// BehaviorGuard - Conditional behavior activation
// ============================================================================

/// A guarded behavior that only activates when the guard condition is met
pub struct BehaviorGuard<M> {
    guard: Box<dyn Fn(&M) -> bool + Send + Sync>,
    handler: HandlerFn<M>,
    name: String,
    stats: GuardStats,
}

/// Statistics for guarded behaviors
#[derive(Debug, Clone, Default)]
pub struct GuardStats {
    pub total_checked: u64,
    pub guard_passed: u64,
    pub guard_rejected: u64,
    pub handled: u64,
}

impl<M> BehaviorGuard<M> {
    pub fn new<G>(name: &str, guard: G, handler: HandlerFn<M>) -> Self
    where
        G: Fn(&M) -> bool + Send + Sync + 'static,
    {
        Self {
            guard: Box::new(guard),
            handler,
            name: name.to_string(),
            stats: GuardStats::default(),
        }
    }

    pub fn handle(&mut self, msg: &M) -> HandleResult {
        self.stats.total_checked += 1;
        if (self.guard)(msg) {
            self.stats.guard_passed += 1;
            let result = self.handler.handle(msg);
            if result.is_handled() {
                self.stats.handled += 1;
            }
            result
        } else {
            self.stats.guard_rejected += 1;
            HandleResult::Unhandled
        }
    }

    pub fn stats(&self) -> &GuardStats {
        &self.stats
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<M> fmt::Debug for BehaviorGuard<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BehaviorGuard({})", self.name)
    }
}

// ============================================================================
// CompositionStats - Shared stats for composed behaviors
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct CompositionStats {
    pub total_messages: u64,
    pub primary_count: u64,
    pub fallback_count: u64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handler(name: &str, handles: bool) -> HandlerFn<String> {
        let handles = handles;
        HandlerFn::new(name, move |_msg: &String| {
            if handles {
                HandleResult::Handled
            } else {
                HandleResult::Unhandled
            }
        })
    }

    fn make_prefix_handler(name: &str, prefix: &str) -> HandlerFn<String> {
        let prefix = prefix.to_string();
        HandlerFn::new(name, move |msg: &String| {
            if msg.starts_with(&prefix) {
                HandleResult::Handled
            } else {
                HandleResult::Unhandled
            }
        })
    }

    // -- HandleResult tests --

    #[test]
    fn test_handle_result_handled() {
        assert!(HandleResult::Handled.is_handled());
        assert!(!HandleResult::Handled.is_unhandled());
        assert!(!HandleResult::Handled.is_stop());
    }

    #[test]
    fn test_handle_result_unhandled() {
        assert!(!HandleResult::Unhandled.is_handled());
        assert!(HandleResult::Unhandled.is_unhandled());
    }

    #[test]
    fn test_handle_result_stop() {
        assert!(HandleResult::Stop.is_handled());
        assert!(HandleResult::Stop.is_stop());
    }

    #[test]
    fn test_handle_result_transition() {
        let t = HandleResult::Transition("next".to_string());
        assert!(t.is_handled());
        assert!(!t.is_stop());
    }

    // -- HandlerFn tests --

    #[test]
    fn test_handler_fn_basic() {
        let h = make_handler("test", true);
        assert_eq!(h.name(), "test");
        assert_eq!(h.handle(&"msg".to_string()), HandleResult::Handled);
    }

    #[test]
    fn test_handler_fn_unhandled() {
        let h = make_handler("skip", false);
        assert_eq!(h.handle(&"msg".to_string()), HandleResult::Unhandled);
    }

    #[test]
    fn test_handler_fn_debug() {
        let h = make_handler("test", true);
        let debug = format!("{:?}", h);
        assert!(debug.contains("test"));
    }

    // -- OrElse tests --

    #[test]
    fn test_or_else_primary_handles() {
        let mut comp = OrElse::new(make_handler("primary", true), make_handler("fallback", true));
        let result = comp.handle(&"msg".to_string());
        assert_eq!(result, HandleResult::Handled);
        assert_eq!(comp.stats().primary_count, 1);
        assert_eq!(comp.stats().fallback_count, 0);
    }

    #[test]
    fn test_or_else_fallback() {
        let mut comp = OrElse::new(
            make_handler("primary", false),
            make_handler("fallback", true),
        );
        let result = comp.handle(&"msg".to_string());
        assert_eq!(result, HandleResult::Handled);
        assert_eq!(comp.stats().fallback_count, 1);
    }

    #[test]
    fn test_or_else_both_unhandled() {
        let mut comp = OrElse::new(
            make_handler("primary", false),
            make_handler("fallback", false),
        );
        let result = comp.handle(&"msg".to_string());
        assert_eq!(result, HandleResult::Unhandled);
    }

    #[test]
    fn test_or_else_names() {
        let comp = OrElse::new(make_handler("a", true), make_handler("b", true));
        assert_eq!(comp.primary_name(), "a");
        assert_eq!(comp.fallback_name(), "b");
    }

    #[test]
    fn test_or_else_debug() {
        let comp = OrElse::new(make_handler("a", true), make_handler("b", true));
        let debug = format!("{:?}", comp);
        assert!(debug.contains("OrElse"));
    }

    #[test]
    fn test_or_else_with_prefix_handlers() {
        let mut comp = OrElse::new(
            make_prefix_handler("cmd", "cmd:"),
            make_prefix_handler("event", "evt:"),
        );
        assert_eq!(
            comp.handle(&"cmd:start".to_string()),
            HandleResult::Handled
        );
        assert_eq!(
            comp.handle(&"evt:click".to_string()),
            HandleResult::Handled
        );
        assert_eq!(
            comp.handle(&"other".to_string()),
            HandleResult::Unhandled
        );
    }

    // -- AndThen tests --

    #[test]
    fn test_and_then_both_strategy() {
        let mut comp = AndThen::new(
            make_handler("a", true),
            make_handler("b", true),
            CombineStrategy::Both,
        );
        assert_eq!(comp.handle(&"msg".to_string()), HandleResult::Handled);
    }

    #[test]
    fn test_and_then_both_one_fails() {
        let mut comp = AndThen::new(
            make_handler("a", true),
            make_handler("b", false),
            CombineStrategy::Both,
        );
        assert_eq!(comp.handle(&"msg".to_string()), HandleResult::Unhandled);
    }

    #[test]
    fn test_and_then_either_strategy() {
        let mut comp = AndThen::new(
            make_handler("a", true),
            make_handler("b", false),
            CombineStrategy::Either,
        );
        assert_eq!(comp.handle(&"msg".to_string()), HandleResult::Handled);
    }

    #[test]
    fn test_and_then_first_wins() {
        let mut comp = AndThen::new(
            make_handler("a", true),
            make_handler("b", false),
            CombineStrategy::FirstWins,
        );
        assert_eq!(comp.handle(&"msg".to_string()), HandleResult::Handled);
    }

    #[test]
    fn test_and_then_second_wins() {
        let mut comp = AndThen::new(
            make_handler("a", true),
            make_handler("b", false),
            CombineStrategy::SecondWins,
        );
        assert_eq!(comp.handle(&"msg".to_string()), HandleResult::Unhandled);
    }

    #[test]
    fn test_and_then_debug() {
        let comp = AndThen::new(
            make_handler("a", true),
            make_handler("b", true),
            CombineStrategy::Both,
        );
        let debug = format!("{:?}", comp);
        assert!(debug.contains("AndThen"));
    }

    // -- Intercept tests --

    #[test]
    fn test_intercept_no_hooks() {
        let mut intercept = Intercept::new(make_handler("inner", true));
        assert_eq!(intercept.handle(&"msg".to_string()), HandleResult::Handled);
    }

    #[test]
    fn test_intercept_pre_hook_continue() {
        let mut intercept = Intercept::new(make_handler("inner", true))
            .with_pre_hook(|_msg: &String| InterceptDecision::Continue);
        assert_eq!(intercept.handle(&"msg".to_string()), HandleResult::Handled);
        assert_eq!(intercept.stats().inner_handled, 1);
    }

    #[test]
    fn test_intercept_pre_hook_short_circuit() {
        let mut intercept = Intercept::new(make_handler("inner", true)).with_pre_hook(
            |_msg: &String| InterceptDecision::ShortCircuit(HandleResult::Stop),
        );
        assert_eq!(intercept.handle(&"msg".to_string()), HandleResult::Stop);
        assert_eq!(intercept.stats().pre_short_circuits, 1);
        assert_eq!(intercept.stats().inner_handled, 0);
    }

    #[test]
    fn test_intercept_post_hook() {
        let mut intercept = Intercept::new(make_handler("inner", true))
            .with_post_hook(|_msg: &String, _result: &HandleResult| HandleResult::Stop);
        assert_eq!(intercept.handle(&"msg".to_string()), HandleResult::Stop);
        assert_eq!(intercept.stats().post_modifications, 1);
    }

    #[test]
    fn test_intercept_name() {
        let intercept = Intercept::new(make_handler("inner", true));
        assert!(intercept.name().contains("inner"));
    }

    // -- HandlerChain tests --

    #[test]
    fn test_chain_first_handles() {
        let mut chain = HandlerChain::new()
            .add(make_handler("first", true))
            .add(make_handler("second", true));
        assert_eq!(chain.handle(&"msg".to_string()), HandleResult::Handled);
        assert_eq!(chain.stats().handler_hits[0], 1);
        assert_eq!(chain.stats().handler_hits[1], 0);
    }

    #[test]
    fn test_chain_fallthrough() {
        let mut chain = HandlerChain::new()
            .add(make_handler("first", false))
            .add(make_handler("second", true));
        assert_eq!(chain.handle(&"msg".to_string()), HandleResult::Handled);
        assert_eq!(chain.stats().handler_hits[0], 0);
        assert_eq!(chain.stats().handler_hits[1], 1);
    }

    #[test]
    fn test_chain_default_result() {
        let mut chain = HandlerChain::new()
            .with_default(HandleResult::Stop)
            .add(make_handler("first", false));
        assert_eq!(chain.handle(&"msg".to_string()), HandleResult::Stop);
        assert_eq!(chain.stats().default_count, 1);
    }

    #[test]
    fn test_chain_empty() {
        let mut chain: HandlerChain<String> = HandlerChain::new();
        assert!(chain.is_empty());
        assert_eq!(chain.handle(&"msg".to_string()), HandleResult::Unhandled);
    }

    #[test]
    fn test_chain_len() {
        let chain = HandlerChain::<String>::new()
            .add(make_handler("a", true))
            .add(make_handler("b", true));
        assert_eq!(chain.len(), 2);
    }

    #[test]
    fn test_chain_handler_names() {
        let chain = HandlerChain::<String>::new()
            .add(make_handler("alpha", true))
            .add(make_handler("beta", true));
        assert_eq!(chain.handler_names(), vec!["alpha", "beta"]);
    }

    #[test]
    fn test_chain_debug() {
        let chain = HandlerChain::<String>::new().add(make_handler("a", true));
        let debug = format!("{:?}", chain);
        assert!(debug.contains("HandlerChain"));
    }

    #[test]
    fn test_chain_with_prefix_handlers() {
        let mut chain = HandlerChain::new()
            .add(make_prefix_handler("cmd", "cmd:"))
            .add(make_prefix_handler("evt", "evt:"))
            .add(make_prefix_handler("sys", "sys:"));
        assert_eq!(
            chain.handle(&"evt:update".to_string()),
            HandleResult::Handled
        );
        assert_eq!(chain.stats().handler_hits, vec![0, 1, 0]);
    }

    // -- MessageTransform tests --

    #[test]
    fn test_transform_success() {
        let handler = HandlerFn::new("len_handler", |len: &usize| {
            if *len > 3 {
                HandleResult::Handled
            } else {
                HandleResult::Unhandled
            }
        });
        let mut transform: MessageTransform<String, usize> =
            MessageTransform::new("str_to_len", |s: &String| Some(s.len()), handler);
        assert_eq!(
            transform.handle(&"hello".to_string()),
            HandleResult::Handled
        );
        assert_eq!(transform.stats().successful_transforms, 1);
    }

    #[test]
    fn test_transform_failure() {
        let handler = HandlerFn::new("handler", |_: &usize| HandleResult::Handled);
        let mut transform: MessageTransform<String, usize> =
            MessageTransform::new("filter", |s: &String| {
                if s.starts_with("ok:") {
                    Some(s.len())
                } else {
                    None
                }
            }, handler);
        assert_eq!(
            transform.handle(&"bad:msg".to_string()),
            HandleResult::Unhandled
        );
        assert_eq!(transform.stats().failed_transforms, 1);
    }

    #[test]
    fn test_transform_name() {
        let handler = HandlerFn::new("h", |_: &usize| HandleResult::Handled);
        let transform: MessageTransform<String, usize> =
            MessageTransform::new("my_transform", |_: &String| Some(0), handler);
        assert_eq!(transform.name(), "my_transform");
    }

    #[test]
    fn test_transform_debug() {
        let handler = HandlerFn::new("h", |_: &usize| HandleResult::Handled);
        let transform: MessageTransform<String, usize> =
            MessageTransform::new("t", |_: &String| Some(0), handler);
        let debug = format!("{:?}", transform);
        assert!(debug.contains("MessageTransform"));
    }

    // -- BehaviorGuard tests --

    #[test]
    fn test_guard_passes() {
        let mut guard = BehaviorGuard::new(
            "positive",
            |msg: &String| msg.starts_with("ok"),
            make_handler("inner", true),
        );
        assert_eq!(guard.handle(&"ok:go".to_string()), HandleResult::Handled);
        assert_eq!(guard.stats().guard_passed, 1);
    }

    #[test]
    fn test_guard_rejects() {
        let mut guard = BehaviorGuard::new(
            "positive",
            |msg: &String| msg.starts_with("ok"),
            make_handler("inner", true),
        );
        assert_eq!(
            guard.handle(&"bad:no".to_string()),
            HandleResult::Unhandled
        );
        assert_eq!(guard.stats().guard_rejected, 1);
    }

    #[test]
    fn test_guard_passes_but_inner_unhandled() {
        let mut guard = BehaviorGuard::new(
            "always",
            |_msg: &String| true,
            make_handler("skip", false),
        );
        assert_eq!(
            guard.handle(&"msg".to_string()),
            HandleResult::Unhandled
        );
        assert_eq!(guard.stats().guard_passed, 1);
        assert_eq!(guard.stats().handled, 0);
    }

    #[test]
    fn test_guard_name() {
        let guard = BehaviorGuard::new("my_guard", |_: &String| true, make_handler("h", true));
        assert_eq!(guard.name(), "my_guard");
    }

    #[test]
    fn test_guard_debug() {
        let guard = BehaviorGuard::new("g", |_: &String| true, make_handler("h", true));
        let debug = format!("{:?}", guard);
        assert!(debug.contains("BehaviorGuard"));
    }

    // -- Integration tests --

    #[test]
    fn test_chain_with_guards() {
        let mut chain = HandlerChain::new()
            .add(make_prefix_handler("cmd", "cmd:"))
            .add(make_prefix_handler("evt", "evt:"));

        // cmd messages handled by first
        assert_eq!(
            chain.handle(&"cmd:start".to_string()),
            HandleResult::Handled
        );
        // evt messages handled by second
        assert_eq!(
            chain.handle(&"evt:click".to_string()),
            HandleResult::Handled
        );
        // unknown messages unhandled
        assert_eq!(
            chain.handle(&"unknown".to_string()),
            HandleResult::Unhandled
        );

        assert_eq!(chain.stats().total_messages, 3);
    }

    #[test]
    fn test_or_else_stats_tracking() {
        let mut comp = OrElse::new(
            make_prefix_handler("cmd", "cmd:"),
            make_prefix_handler("evt", "evt:"),
        );
        comp.handle(&"cmd:a".to_string());
        comp.handle(&"cmd:b".to_string());
        comp.handle(&"evt:c".to_string());
        assert_eq!(comp.stats().total_messages, 3);
        assert_eq!(comp.stats().primary_count, 2);
        assert_eq!(comp.stats().fallback_count, 1);
    }
}
