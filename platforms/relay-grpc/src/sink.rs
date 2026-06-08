use crate::convert::json_to_prost_struct;
use crate::relay::{
    relay_agent_client::RelayAgentClient, Finding as ProtoFinding, HeartbeatRequest,
    Result as ProtoResult, SubmitResultRequest,
};
use async_trait::async_trait;
use prost_types::Timestamp;
use relay_core::{AgentError, ResultSink, ScanResult};
use tonic::transport::Channel;

pub struct GrpcResultSink {
    client: RelayAgentClient<Channel>,
    api_key: String,
}

impl GrpcResultSink {
    pub fn new(client: RelayAgentClient<Channel>, api_key: String) -> Self {
        Self { client, api_key }
    }
}

fn scan_result_to_proto(r: ScanResult) -> ProtoResult {
    let findings = r
        .findings
        .into_iter()
        .map(|f| ProtoFinding {
            id: f.id,
            title: f.title,
            severity: f.severity,
            data: json_to_prost_struct(f.data),
            references: f.references.unwrap_or_default(),
        })
        .collect();

    let executed_at = Some(Timestamp {
        seconds: r.executed_at.timestamp(),
        nanos: r.executed_at.timestamp_subsec_nanos() as i32,
    });

    ProtoResult {
        task_id: r.task_id,
        agent_id: r.agent_id,
        status: format!("{:?}", r.status),
        findings,
        duration_ms: r.duration_ms,
        executed_at,
        error: r.error.unwrap_or_default(),
        metadata: r.metadata,
    }
}

#[async_trait]
impl ResultSink for GrpcResultSink {
    async fn publish_result(&mut self, result: ScanResult) -> Result<(), AgentError> {
        let task_id = result.task_id.clone();
        let proto = scan_result_to_proto(result);
        let req = SubmitResultRequest {
            api_key: self.api_key.clone(),
            result: Some(proto),
        };
        self.client
            .submit_result(req)
            .await
            .map_err(|e| AgentError::ResultSinkError(format!("SubmitResult {task_id}: {e}")))?;
        Ok(())
    }

    async fn publish_heartbeat(&mut self, agent_id: &str) -> Result<(), AgentError> {
        let req = HeartbeatRequest {
            api_key: self.api_key.clone(),
            agent_id: agent_id.to_string(),
            status: "healthy".into(),
        };
        self.client
            .send_heartbeat(req)
            .await
            .map_err(|e| AgentError::ResultSinkError(format!("SendHeartbeat: {e}")))?;
        Ok(())
    }
}
