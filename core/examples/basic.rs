//! Minimal working example.
//!
//! Run with:  cargo run --example basic

use async_trait::async_trait;
use relay_core::{
    Agent, AgentConfig, AgentError, Finding, ResultSink, ResultStatus, ScanEngine, ScanResult,
    ScanTask, TaskQueue,
};

// ── Engine ────────────────────────────────────────────────────────────────────

struct DemoEngine;

#[async_trait]
impl ScanEngine for DemoEngine {
    async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
        println!("[engine] running {} on {}", task.scan_type, task.target);

        // Simulate doing some work.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let result = ScanResult::success(task.id.clone(), "demo-agent".into())
            .add_finding(
                Finding::new(
                    format!("f-{}", task.id),
                    "Open port detected".into(),
                    "INFO".into(),
                )
                .with_data(serde_json::json!({ "port": 443, "service": "https" })),
            )
            .with_duration(100);

        Ok(result)
    }
}

// ── Queue ─────────────────────────────────────────────────────────────────────

struct DemoQueue {
    tasks: Vec<ScanTask>,
    index: usize,
}

impl DemoQueue {
    fn new() -> Self {
        let tasks = vec![
            ScanTask::new("task-001".into(), "example.com".into(), "port-check".into()),
            ScanTask::new("task-002".into(), "example.org".into(), "cert-check".into()),
            ScanTask::new("task-003".into(), "example.net".into(), "port-check".into()),
        ];
        Self { tasks, index: 0 }
    }
}

#[async_trait]
impl TaskQueue for DemoQueue {
    async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
        if self.index < self.tasks.len() {
            let task = self.tasks[self.index].clone();
            self.index += 1;
            println!("[queue]  dispatching {}", task.id);
            Ok(task)
        } else {
            // No more tasks — block so the agent idles until Ctrl-C.
            println!("[queue]  no more tasks, idling (press Ctrl-C to exit)");
            std::future::pending::<()>().await;
            unreachable!()
        }
    }
}

// ── Sink ──────────────────────────────────────────────────────────────────────

struct StdoutSink;

#[async_trait]
impl ResultSink for StdoutSink {
    async fn publish_result(&mut self, result: ScanResult) -> Result<(), AgentError> {
        let status = match result.status {
            ResultStatus::Success => "SUCCESS",
            ResultStatus::Failed => "FAILED",
            ResultStatus::Timeout => "TIMEOUT",
            ResultStatus::Cancelled => "CANCELLED",
        };
        println!(
            "[sink]   task={} status={} findings={} duration={}ms",
            result.task_id,
            status,
            result.findings.len(),
            result.duration_ms,
        );
        Ok(())
    }

    async fn publish_heartbeat(&mut self, agent_id: &str) -> Result<(), AgentError> {
        println!("[sink]   heartbeat from {}", agent_id);
        Ok(())
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config = AgentConfig::new("demo-agent".into());

    println!("Starting demo agent (Ctrl-C to stop)");

    let mut agent = Agent::new(DemoEngine, DemoQueue::new(), StdoutSink, config);
    agent.run().await?;

    println!("Agent stopped.");
    Ok(())
}
