use crate::convert::prost_struct_to_json;
use crate::relay::{
    relay_agent_client::RelayAgentClient, AckTaskRequest, GetNextTaskRequest, NackTaskRequest, Task,
};
use async_trait::async_trait;
use relay_core::{AgentError, ScanTask, TaskQueue};
use tonic::transport::Channel;

pub struct GrpcTaskQueue {
    client: RelayAgentClient<Channel>,
    api_key: String,
    agent_id: String,
}

impl GrpcTaskQueue {
    pub fn new(client: RelayAgentClient<Channel>, api_key: String, agent_id: String) -> Self {
        Self {
            client,
            api_key,
            agent_id,
        }
    }
}

fn proto_task_to_scan_task(t: Task) -> ScanTask {
    let params = t
        .params
        .map(prost_struct_to_json)
        .unwrap_or(serde_json::Value::Object(Default::default()));

    let mut task = ScanTask::new(t.id, t.target, t.scan_type);
    task.params = params;
    task.priority = t.priority as u8;
    task.timeout_secs = t.timeout_secs;
    if !t.agent_selector.is_empty() {
        task.agent_selector = Some(t.agent_selector);
    }
    task.tags = t.tags;
    task
}

#[async_trait]
impl TaskQueue for GrpcTaskQueue {
    async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
        let req = GetNextTaskRequest {
            api_key: self.api_key.clone(),
            agent_id: self.agent_id.clone(),
        };
        let resp = self
            .client
            .get_next_task(req)
            .await
            .map_err(|e| AgentError::TaskQueueError(format!("GetNextTask RPC: {e}")))?
            .into_inner();
        let proto_task = resp
            .task
            .ok_or_else(|| AgentError::TaskQueueError("server returned empty task".into()))?;
        Ok(proto_task_to_scan_task(proto_task))
    }

    async fn acknowledge_task(&mut self, task_id: String) -> Result<(), AgentError> {
        let req = AckTaskRequest {
            api_key: self.api_key.clone(),
            task_id,
        };
        self.client
            .ack_task(req)
            .await
            .map_err(|e| AgentError::TaskQueueError(format!("AckTask RPC: {e}")))?;
        Ok(())
    }

    async fn nack_task(&mut self, task_id: String, reason: String) -> Result<(), AgentError> {
        let req = NackTaskRequest {
            api_key: self.api_key.clone(),
            task_id,
            reason,
        };
        self.client
            .nack_task(req)
            .await
            .map_err(|e| AgentError::TaskQueueError(format!("NackTask RPC: {e}")))?;
        Ok(())
    }
}
