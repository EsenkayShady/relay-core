use async_trait::async_trait;
use relay_core::{AgentError, ScanTask, TaskQueue};
use reqwest::{Client, StatusCode};
use tracing::{info, warn};

pub struct HttpTaskQueue {
    client: Client,
    base_url: String,
    api_key: String,
}

impl HttpTaskQueue {
    pub fn new(client: Client, base_url: String, api_key: String) -> Self {
        Self {
            client,
            base_url,
            api_key,
        }
    }
}

#[async_trait]
impl TaskQueue for HttpTaskQueue {
    async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
        let url = format!("{}/api/v1/tasks/next", self.base_url);
        loop {
            let resp = self
                .client
                .get(&url)
                .bearer_auth(&self.api_key)
                .send()
                .await
                .map_err(|e| AgentError::TaskQueueError(format!("GET {url}: {e}")))?;

            match resp.status() {
                StatusCode::OK => {
                    let task = resp.json::<ScanTask>().await.map_err(|e| {
                        AgentError::TaskQueueError(format!("deserialize task: {e}"))
                    })?;
                    info!(task_id = %task.id, "received task");
                    return Ok(task);
                }
                StatusCode::NO_CONTENT => {
                    // Queue is empty; wait before polling again.
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                status => {
                    let body = resp.text().await.unwrap_or_default();
                    warn!("unexpected status {status} from task endpoint: {body}");
                    return Err(AgentError::TaskQueueError(format!(
                        "unexpected status {status}: {body}"
                    )));
                }
            }
        }
    }

    async fn acknowledge_task(&mut self, task_id: String) -> Result<(), AgentError> {
        let url = format!("{}/api/v1/tasks/{}/ack", self.base_url, task_id);
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(|e| AgentError::TaskQueueError(format!("ack {task_id}: {e}")))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AgentError::TaskQueueError(format!(
                "ack failed for {task_id}: {body}"
            )));
        }
        Ok(())
    }

    async fn nack_task(&mut self, task_id: String, reason: String) -> Result<(), AgentError> {
        let url = format!("{}/api/v1/tasks/{}/nack", self.base_url, task_id);
        let body = serde_json::json!({ "reason": reason });
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| AgentError::TaskQueueError(format!("nack {task_id}: {e}")))?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(AgentError::TaskQueueError(format!(
                "nack failed for {task_id}: {text}"
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
            .map_err(|e| AgentError::TaskQueueError(format!("health check: {e}")))?;
        if !resp.status().is_success() {
            return Err(AgentError::TaskQueueError(format!(
                "health check failed: {}",
                resp.status()
            )));
        }
        Ok(())
    }
}
