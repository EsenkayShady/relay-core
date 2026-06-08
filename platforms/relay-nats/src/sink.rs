use async_nats::Client;
use async_trait::async_trait;
use relay_core::{AgentError, ResultSink, ScanResult};

pub struct NatsResultSink {
    client: Client,
    api_key: String,
}

impl NatsResultSink {
    pub fn new(client: Client, api_key: String) -> Self {
        Self { client, api_key }
    }
}

#[async_trait]
impl ResultSink for NatsResultSink {
    async fn publish_result(&mut self, result: ScanResult) -> Result<(), AgentError> {
        let subject = format!("relay.{}.results", self.api_key);
        let payload = serde_json::to_vec(&result)
            .map_err(|e| AgentError::ResultSinkError(format!("serialize result: {e}")))?;
        self.client
            .publish(subject, payload.into())
            .await
            .map_err(|e| {
                AgentError::ResultSinkError(format!("publish result {}: {e}", result.task_id))
            })
    }

    async fn publish_heartbeat(&mut self, agent_id: &str) -> Result<(), AgentError> {
        let subject = format!("relay.{}.heartbeat", self.api_key);
        let payload = serde_json::json!({
            "agent_id": agent_id,
            "status": "healthy"
        })
        .to_string();
        self.client
            .publish(subject, payload.into())
            .await
            .map_err(|e| AgentError::ResultSinkError(format!("publish heartbeat: {e}")))
    }

    async fn health_check(&self) -> Result<(), AgentError> {
        Ok(())
    }
}
