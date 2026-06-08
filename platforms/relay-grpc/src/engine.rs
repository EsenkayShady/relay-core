use async_trait::async_trait;
use relay_core::{AgentError, ScanEngine, ScanResult, ScanTask};
use tracing::info;

pub struct GrpcEngine;

impl GrpcEngine {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ScanEngine for GrpcEngine {
    async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
        info!(task_id = %task.id, target = %task.target, scan_type = %task.scan_type, "executing task");

        // Stub: replace with real inspection logic per scan_type.
        // Example: match task.scan_type.as_str() {
        //     "port-check" => run_port_check(&task.target, &task.params).await,
        //     "cert-check"  => run_cert_check(&task.target, &task.params).await,
        //     _ => Err(AgentError::ScanEngineError(format!("unknown scan type: {}", task.scan_type))),
        // }

        Ok(ScanResult::success(task.id.clone(), String::new()))
    }

    async fn health_check(&self) -> Result<(), AgentError> {
        Ok(())
    }
}
