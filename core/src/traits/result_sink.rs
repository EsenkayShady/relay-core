use crate::models::{AgentError, ScanResult};
use async_trait::async_trait;

/// Trait for result delivery implementations.
///
/// Implement this to route completed task results to your backend: NATS,
/// gRPC, HTTP webhooks, S3, a database, or anything else.
///
/// # Contract
/// - `publish_result()` is retried by the agent (up to `max_retries`) on
///   failure with exponential backoff. Implementations do not need to retry
///   internally.
/// - `publish_heartbeat()` is best-effort — errors are logged but don't
///   stop the agent.
/// - Both methods may be called from the agent's main loop; they share the
///   same `&mut self` so no concurrent calls occur.
///
/// # Example
/// ```ignore
/// pub struct StdoutSink;
///
/// #[async_trait]
/// impl ResultSink for StdoutSink {
///     async fn publish_result(&mut self, result: ScanResult) -> Result<(), AgentError> {
///         println!("{}", serde_json::to_string(&result).unwrap());
///         Ok(())
///     }
///
///     async fn publish_heartbeat(&mut self, agent_id: &str) -> Result<(), AgentError> {
///         println!("heartbeat: {}", agent_id);
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait ResultSink: Send + Sync {
    /// Deliver a completed task result.
    ///
    /// On `Err`, the agent retries up to `AgentConfig::max_retries` times
    /// with exponential backoff. After exhausting retries the task is nack'd.
    async fn publish_result(&mut self, result: ScanResult) -> Result<(), AgentError>;

    /// Send a liveness heartbeat.
    ///
    /// Called at intervals configured by `AgentConfig::heartbeat_interval_secs`.
    /// Errors are logged but do not stop the agent.
    async fn publish_heartbeat(&mut self, agent_id: &str) -> Result<(), AgentError>;

    /// Verify the sink connection is healthy.
    ///
    /// Called once on startup. Return `Err` to abort the agent before it
    /// begins processing tasks.
    async fn health_check(&self) -> Result<(), AgentError> {
        Ok(())
    }
}
