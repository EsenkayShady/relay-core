use crate::models::{AgentError, ScanResult, ScanTask};
use async_trait::async_trait;

/// Optional lifecycle hooks for observability and instrumentation.
///
/// Implement this trait to attach metrics, distributed traces, structured
/// audit logs, or any other side-effects to agent lifecycle events.
///
/// All methods have empty default implementations — only override what you need.
///
/// # Example
/// ```ignore
/// pub struct MetricsHook {
///     tasks_completed: Arc<AtomicU64>,
/// }
///
/// #[async_trait]
/// impl AgentHooks for MetricsHook {
///     async fn on_task_complete(&self, result: &ScanResult) {
///         self.tasks_completed.fetch_add(1, Ordering::Relaxed);
///     }
/// }
/// ```
#[async_trait]
pub trait AgentHooks: Send + Sync {
    /// Called just before a task is handed to the engine.
    async fn on_task_start(&self, _task: &ScanTask) {}

    /// Called after a task finishes — regardless of success, failure, or timeout.
    ///
    /// Check `result.status` to distinguish outcomes.
    async fn on_task_complete(&self, _result: &ScanResult) {}

    /// Called when a task produces an error before a result can be formed.
    async fn on_task_error(&self, _task_id: &str, _error: &AgentError) {}

    /// Called after each successful heartbeat delivery.
    async fn on_heartbeat(&self, _agent_id: &str) {}

    /// Called when the agent reconnects to the task queue after a failure.
    async fn on_reconnect(&self, _reason: &str) {}

    /// Called once when the agent begins graceful shutdown.
    async fn on_shutdown(&self) {}
}

/// Default no-op implementation used when no hooks are provided.
pub struct NoOpHooks;

#[async_trait]
impl AgentHooks for NoOpHooks {}
