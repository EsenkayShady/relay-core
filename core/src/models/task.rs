use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A task to be executed by the agent.
///
/// # Example (JSON)
/// ```json
/// {
///   "id": "task-123",
///   "target": "example.com",
///   "scan_type": "ssl-check",
///   "params": { "ports": [443] },
///   "priority": 100,
///   "timeout_secs": 300,
///   "tags": { "customer": "acme" }
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanTask {
    /// Unique task identifier.
    pub id: String,

    /// Target (IP address, domain, CIDR range, URL, etc.).
    pub target: String,

    /// Task type — interpreted by the engine ("port-scan", "ssl-check", etc.).
    pub scan_type: String,

    /// Engine-specific parameters as flexible JSON.
    pub params: serde_json::Value,

    /// Execution priority (0–255, higher = more urgent). Default: 128.
    #[serde(default = "default_priority")]
    pub priority: u8,

    /// Maximum allowed execution time in seconds.
    pub timeout_secs: u64,

    /// Optional routing hint to a specific agent instance or region.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_selector: Option<String>,

    /// Arbitrary key-value metadata.
    #[serde(default)]
    pub tags: HashMap<String, String>,

    /// ISO 8601 creation timestamp (set by the task queue).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

fn default_priority() -> u8 {
    128
}

impl ScanTask {
    /// Create a minimal task with default settings.
    pub fn new(id: String, target: String, scan_type: String) -> Self {
        Self {
            id,
            target,
            scan_type,
            params: serde_json::json!({}),
            priority: 128,
            timeout_secs: 300,
            agent_selector: None,
            tags: HashMap::new(),
            created_at: None,
        }
    }

    /// Set engine-specific parameters.
    pub fn with_params(mut self, params: serde_json::Value) -> Self {
        self.params = params;
        self
    }

    /// Override the execution timeout.
    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    /// Set execution priority.
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Route this task to a specific agent or region.
    pub fn with_agent_selector(mut self, selector: String) -> Self {
        self.agent_selector = Some(selector);
        self
    }

    /// Attach a key-value tag.
    pub fn add_tag(mut self, key: String, value: String) -> Self {
        self.tags.insert(key, value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_task_has_defaults() {
        let task = ScanTask::new("t1".into(), "example.com".into(), "port-scan".into());
        assert_eq!(task.priority, 128);
        assert_eq!(task.timeout_secs, 300);
        assert!(task.agent_selector.is_none());
        assert!(task.tags.is_empty());
    }

    #[test]
    fn builder_methods_work() {
        let task = ScanTask::new("t1".into(), "example.com".into(), "port-scan".into())
            .with_timeout(60)
            .with_priority(200)
            .with_agent_selector("region:us-east".into())
            .add_tag("customer".into(), "acme".into());

        assert_eq!(task.timeout_secs, 60);
        assert_eq!(task.priority, 200);
        assert_eq!(task.agent_selector.unwrap(), "region:us-east");
        assert_eq!(task.tags["customer"], "acme");
    }

    #[test]
    fn serialization_roundtrip() {
        let task = ScanTask::new("t1".into(), "1.2.3.4".into(), "port-scan".into());
        let json = serde_json::to_string(&task).unwrap();
        let back: ScanTask = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, task.id);
        assert_eq!(back.target, task.target);
    }
}
