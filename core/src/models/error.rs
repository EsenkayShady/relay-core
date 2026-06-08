use thiserror::Error;

/// Errors that can occur during agent operation.
#[derive(Debug, Error)]
pub enum AgentError {
    #[error("Task queue error: {0}")]
    TaskQueueError(String),

    #[error("Execution engine error: {0}")]
    ScanEngineError(String),

    #[error("Result sink error: {0}")]
    ResultSinkError(String),

    #[error("Execution timeout")]
    ScanTimeout,

    #[error("Invalid configuration: {0}")]
    ConfigInvalid(String),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Shutdown requested")]
    ShutdownRequested,

    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl From<String> for AgentError {
    fn from(s: String) -> Self {
        AgentError::Unknown(s)
    }
}

impl From<&str> for AgentError {
    fn from(s: &str) -> Self {
        AgentError::Unknown(s.to_string())
    }
}
