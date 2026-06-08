use crate::models::AgentError;
use std::collections::HashMap;

/// Agent configuration.
///
/// # Validation Rules
/// - `agent_id`: Non-empty, alphanumeric + hyphens/underscores
/// - `timeout_secs`: 1–3600
/// - `max_concurrent`: 1–1000
/// - `max_retries`: 0–10
/// - `heartbeat_interval_secs`: 10–300
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Unique agent identifier.
    pub agent_id: String,

    /// Default timeout per task (seconds).
    pub timeout_secs: u64,

    /// Maximum number of tasks to run concurrently.
    pub max_concurrent: usize,

    /// Maximum retry attempts for failed result delivery.
    pub max_retries: u32,

    /// How often to send a heartbeat (seconds).
    pub heartbeat_interval_secs: u64,

    /// Custom metadata — used for routing, labelling, or filtering.
    pub metadata: HashMap<String, String>,
}

impl AgentConfig {
    /// Create a new config with sensible defaults.
    pub fn new(agent_id: String) -> Self {
        Self {
            agent_id,
            timeout_secs: 300,
            max_concurrent: 10,
            max_retries: 3,
            heartbeat_interval_secs: 30,
            metadata: HashMap::new(),
        }
    }

    /// Validate the configuration, returning an error if any field is out of range.
    pub fn validate(&self) -> Result<(), AgentError> {
        if self.agent_id.is_empty() {
            return Err(AgentError::ConfigInvalid(
                "agent_id cannot be empty".to_string(),
            ));
        }

        if !self
            .agent_id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(AgentError::ConfigInvalid(
                "agent_id must be alphanumeric with hyphens/underscores only".to_string(),
            ));
        }

        if self.timeout_secs == 0 || self.timeout_secs > 3600 {
            return Err(AgentError::ConfigInvalid(
                "timeout_secs must be between 1 and 3600".to_string(),
            ));
        }

        if self.max_concurrent == 0 || self.max_concurrent > 1000 {
            return Err(AgentError::ConfigInvalid(
                "max_concurrent must be between 1 and 1000".to_string(),
            ));
        }

        if self.max_retries > 10 {
            return Err(AgentError::ConfigInvalid(
                "max_retries must be between 0 and 10".to_string(),
            ));
        }

        if self.heartbeat_interval_secs < 10 || self.heartbeat_interval_secs > 300 {
            return Err(AgentError::ConfigInvalid(
                "heartbeat_interval_secs must be between 10 and 300".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_config_passes() {
        let config = AgentConfig::new("agent-01".into());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn empty_agent_id_fails() {
        let config = AgentConfig::new("".into());
        assert!(config.validate().is_err());
    }

    #[test]
    fn invalid_agent_id_chars_fail() {
        let config = AgentConfig::new("agent 01".into());
        assert!(config.validate().is_err());
    }

    #[test]
    fn zero_timeout_fails() {
        let mut config = AgentConfig::new("agent-01".into());
        config.timeout_secs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn timeout_over_limit_fails() {
        let mut config = AgentConfig::new("agent-01".into());
        config.timeout_secs = 3601;
        assert!(config.validate().is_err());
    }

    #[test]
    fn zero_concurrent_fails() {
        let mut config = AgentConfig::new("agent-01".into());
        config.max_concurrent = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn retries_over_limit_fails() {
        let mut config = AgentConfig::new("agent-01".into());
        config.max_retries = 11;
        assert!(config.validate().is_err());
    }

    #[test]
    fn heartbeat_too_low_fails() {
        let mut config = AgentConfig::new("agent-01".into());
        config.heartbeat_interval_secs = 5;
        assert!(config.validate().is_err());
    }
}
