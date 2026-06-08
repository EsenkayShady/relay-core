use crate::models::{AgentError, ScanTask};
use async_trait::async_trait;

/// Trait for task queue implementations.
///
/// Implement this to connect the agent to any task source: NATS, gRPC streams,
/// HTTP polling, Kafka, database polling, or a simple in-memory queue.
///
/// # Contract
/// - `get_next_task()` blocks/awaits until a task is available.
/// - `acknowledge_task()` and `nack_task()` are optional — default to no-ops.
/// - Errors from `get_next_task()` are treated as transient; the agent backs
///   off and retries.
///
/// # Example
/// ```ignore
/// pub struct MyQueue { items: Vec<ScanTask> }
///
/// #[async_trait]
/// impl TaskQueue for MyQueue {
///     async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
///         self.items.pop().ok_or_else(|| AgentError::TaskQueueError("empty".into()))
///     }
/// }
/// ```
#[async_trait]
pub trait TaskQueue: Send + Sync {
    /// Return the next available task.
    ///
    /// Should block/await until a task arrives. The agent will call this in
    /// a loop; errors trigger exponential backoff before the next call.
    async fn get_next_task(&mut self) -> Result<ScanTask, AgentError>;

    /// Confirm that a task completed successfully.
    ///
    /// Call after the result has been delivered. The default no-op is fine
    /// for queues that do not support explicit acknowledgment.
    async fn acknowledge_task(&mut self, _task_id: String) -> Result<(), AgentError> {
        Ok(())
    }

    /// Signal that a task failed and should be retried or dead-lettered.
    ///
    /// The `reason` string is passed to the queue so it can record or route
    /// the failure. The default no-op is fine for simple queues.
    async fn nack_task(&mut self, _task_id: String, _reason: String) -> Result<(), AgentError> {
        Ok(())
    }

    /// Verify the queue connection is healthy.
    ///
    /// Called once on startup. Return `Err` to abort the agent before it
    /// begins processing tasks.
    async fn health_check(&self) -> Result<(), AgentError> {
        Ok(())
    }
}
