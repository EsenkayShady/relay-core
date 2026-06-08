use async_trait::async_trait;
use relay_core::{AgentError, ResultSink, ScanResult};
use reqwest::Client;

pub struct HttpResultSink {
    client: Client,
    base_url: String,
    api_key: String,
}

impl HttpResultSink {
    pub fn new(client: Client, base_url: String, api_key: String) -> Self {
        Self {
            client,
            base_url,
            api_key,
        }
    }
}

#[async_trait]
impl ResultSink for HttpResultSink {
    async fn publish_result(&mut self, result: ScanResult) -> Result<(), AgentError> {
        let url = format!("{}/api/v1/results", self.base_url);
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            // Use task_id as idempotency key so safe to retry.
            .header("Idempotency-Key", &result.task_id)
            .json(&result)
            .send()
            .await
            .map_err(|e| {
                AgentError::ResultSinkError(format!("POST result {}: {e}", result.task_id))
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AgentError::ResultSinkError(format!(
                "POST result {} failed {status}: {body}",
                result.task_id
            )));
        }
        Ok(())
    }

    async fn publish_heartbeat(&mut self, agent_id: &str) -> Result<(), AgentError> {
        let url = format!("{}/api/v1/heartbeat", self.base_url);
        let body = serde_json::json!({
            "agent_id": agent_id,
            "status": "healthy"
        });
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| AgentError::ResultSinkError(format!("POST heartbeat: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AgentError::ResultSinkError(format!(
                "POST heartbeat failed {status}: {body}"
            )));
        }
        Ok(())
    }

    async fn health_check(&self) -> Result<(), AgentError> {
        let url = format!("{}/health", self.base_url);
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(|e| AgentError::ResultSinkError(format!("health check: {e}")))?;
        if !resp.status().is_success() {
            return Err(AgentError::ResultSinkError(format!(
                "health check failed: {}",
                resp.status()
            )));
        }
        Ok(())
    }
}
