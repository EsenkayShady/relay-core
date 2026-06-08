use crate::models::{AgentError, ScanResult, ScanTask};
use async_trait::async_trait;
use std::collections::HashMap;

/// Trait for implementing task execution logic.
///
/// Implement this to plug in your own engine — port probing, certificate
/// checking, DNS enumeration, or any other network inspection logic.
///
/// # Contract
/// - `execute()` must be safe to call concurrently from multiple tokio tasks.
/// - The agent enforces `task.timeout_secs`; the engine should also respect it
///   internally when possible.
/// - Returning `Err` marks the task as failed; the agent will nack it.
/// - Implementations should be idempotent — the same `task.id` may be retried.
///
/// # Example
/// ```ignore
/// pub struct MyEngine;
///
/// #[async_trait]
/// impl ScanEngine for MyEngine {
///     async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
///         Ok(ScanResult::success(task.id.clone(), "my-agent".into()))
///     }
/// }
/// ```
#[async_trait]
pub trait ScanEngine: Send + Sync {
    /// Execute a task and return a result with findings.
    ///
    /// The agent enforces `task.timeout_secs` via `tokio::time::timeout`.
    /// If this method does not return before that deadline, the task is
    /// force-cancelled and a timeout result is published automatically.
    async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError>;

    /// Verify the engine is operational.
    ///
    /// Called once on startup. Return `Err` to abort the agent before it
    /// begins processing tasks.
    async fn health_check(&self) -> Result<(), AgentError> {
        Ok(())
    }

    /// Describe engine capabilities and version.
    ///
    /// Returned values appear in agent metadata and logs.
    /// Example: `{"version": "1.0", "capabilities": "port-scan,ssl-check"}`
    fn metadata(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}
