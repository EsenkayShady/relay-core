use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Outcome of a completed task.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum ResultStatus {
    /// Task completed successfully.
    Success,
    /// Task failed with an error.
    Failed,
    /// Task exceeded its timeout.
    Timeout,
    /// Task was cancelled by the agent.
    Cancelled,
}

/// A single finding produced by a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    /// Unique finding identifier.
    pub id: String,

    /// Human-readable title.
    pub title: String,

    /// Severity: "CRITICAL", "HIGH", "MEDIUM", "LOW", or "INFO".
    pub severity: String,

    /// Finding-specific detail as flexible JSON.
    pub data: serde_json::Value,

    /// Optional external references (CVEs, docs, advisories).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<String>>,
}

impl Finding {
    /// Create a new finding with empty data.
    pub fn new(id: String, title: String, severity: String) -> Self {
        Self {
            id,
            title,
            severity,
            data: serde_json::json!({}),
            references: None,
        }
    }

    /// Attach structured detail data.
    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = data;
        self
    }

    /// Attach reference URLs or identifiers.
    pub fn with_references(mut self, refs: Vec<String>) -> Self {
        self.references = Some(refs);
        self
    }
}

/// The complete result of a finished task.
///
/// # Example (JSON)
/// ```json
/// {
///   "task_id": "task-123",
///   "agent_id": "agent-us-east-01",
///   "status": "SUCCESS",
///   "findings": [],
///   "duration_ms": 2340,
///   "executed_at": "2024-01-15T10:30:45.123Z"
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResult {
    /// ID of the task this result belongs to.
    pub task_id: String,

    /// ID of the agent that executed the task.
    pub agent_id: String,

    /// Whether execution succeeded, failed, or timed out.
    pub status: ResultStatus,

    /// Findings produced during execution.
    #[serde(default)]
    pub findings: Vec<Finding>,

    /// Wall-clock execution time in milliseconds.
    pub duration_ms: u64,

    /// UTC timestamp of when execution occurred.
    pub executed_at: DateTime<Utc>,

    /// Error description when status is Failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Custom metadata from the engine (e.g., version, region).
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl ScanResult {
    /// Create a successful result with no findings.
    pub fn success(task_id: String, agent_id: String) -> Self {
        Self {
            task_id,
            agent_id,
            status: ResultStatus::Success,
            findings: Vec::new(),
            duration_ms: 0,
            executed_at: Utc::now(),
            error: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a failed result with an error description.
    pub fn failed(task_id: String, agent_id: String, error: String) -> Self {
        Self {
            task_id,
            agent_id,
            status: ResultStatus::Failed,
            findings: Vec::new(),
            duration_ms: 0,
            executed_at: Utc::now(),
            error: Some(error),
            metadata: HashMap::new(),
        }
    }

    /// Create a timeout result.
    pub fn timeout(task_id: String, agent_id: String, duration_ms: u64) -> Self {
        Self {
            task_id,
            agent_id,
            status: ResultStatus::Timeout,
            findings: Vec::new(),
            duration_ms,
            executed_at: Utc::now(),
            error: Some("Execution exceeded timeout".to_string()),
            metadata: HashMap::new(),
        }
    }

    /// Add a finding to this result.
    pub fn add_finding(mut self, finding: Finding) -> Self {
        self.findings.push(finding);
        self
    }

    /// Set execution duration.
    pub fn with_duration(mut self, ms: u64) -> Self {
        self.duration_ms = ms;
        self
    }

    /// Attach a metadata key-value pair.
    pub fn add_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_result_has_correct_status() {
        let r = ScanResult::success("t1".into(), "a1".into());
        assert_eq!(r.status, ResultStatus::Success);
        assert!(r.error.is_none());
    }

    #[test]
    fn failed_result_has_error() {
        let r = ScanResult::failed("t1".into(), "a1".into(), "oops".into());
        assert_eq!(r.status, ResultStatus::Failed);
        assert_eq!(r.error.unwrap(), "oops");
    }

    #[test]
    fn timeout_result_has_duration() {
        let r = ScanResult::timeout("t1".into(), "a1".into(), 5000);
        assert_eq!(r.status, ResultStatus::Timeout);
        assert_eq!(r.duration_ms, 5000);
    }

    #[test]
    fn add_finding_appends() {
        let r = ScanResult::success("t1".into(), "a1".into()).add_finding(Finding::new(
            "f1".into(),
            "Open port".into(),
            "INFO".into(),
        ));
        assert_eq!(r.findings.len(), 1);
    }

    #[test]
    fn serialization_roundtrip() {
        let r = ScanResult::success("t1".into(), "a1".into());
        let json = serde_json::to_string(&r).unwrap();
        let back: ScanResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_id, r.task_id);
        assert_eq!(back.status, ResultStatus::Success);
    }
}
