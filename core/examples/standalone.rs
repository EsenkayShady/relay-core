//! Standalone agent template — ready to extract into a real deployment.
//!
//! Configuration is loaded from environment variables:
//!   AGENT_ID                 (default: standalone-agent)
//!   SCAN_TIMEOUT_SECS        (default: 300)
//!   MAX_CONCURRENT_TASKS     (default: 10)
//!   MAX_RETRIES              (default: 3)
//!   HEARTBEAT_INTERVAL_SECS  (default: 30)
//!
//! Run with:  cargo run --example standalone

use async_trait::async_trait;
use relay_core::{
    Agent, AgentConfig, AgentError, AgentHooks, Finding, ResultSink, ResultStatus, ScanEngine,
    ScanResult, ScanTask, TaskQueue,
};
use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ── Configuration ─────────────────────────────────────────────────────────────

fn load_config() -> Result<AgentConfig, Box<dyn std::error::Error>> {
    let agent_id = env::var("AGENT_ID").unwrap_or_else(|_| "standalone-agent".into());
    let timeout_secs: u64 = env::var("SCAN_TIMEOUT_SECS")
        .unwrap_or_else(|_| "300".into())
        .parse()?;
    let max_concurrent: usize = env::var("MAX_CONCURRENT_TASKS")
        .unwrap_or_else(|_| "10".into())
        .parse()?;
    let max_retries: u32 = env::var("MAX_RETRIES")
        .unwrap_or_else(|_| "3".into())
        .parse()?;
    let heartbeat_interval_secs: u64 = env::var("HEARTBEAT_INTERVAL_SECS")
        .unwrap_or_else(|_| "30".into())
        .parse()?;

    let mut config = AgentConfig {
        agent_id,
        timeout_secs,
        max_concurrent,
        max_retries,
        heartbeat_interval_secs,
        metadata: HashMap::new(),
    };

    config
        .metadata
        .insert("version".into(), env!("CARGO_PKG_VERSION").into());
    config.validate()?;
    Ok(config)
}

// ── Engine ────────────────────────────────────────────────────────────────────
//
// Replace this stub with your real network probing / inspection logic.

struct NetworkProbeEngine {
    version: String,
}

impl NetworkProbeEngine {
    fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}

#[async_trait]
impl ScanEngine for NetworkProbeEngine {
    async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
        tracing::info!(task_id = %task.id, target = %task.target, r#type = %task.scan_type, "Executing task");

        // TODO: Replace with real logic.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let findings = match task.scan_type.as_str() {
            "port-check" => {
                vec![
                    Finding::new("port-443".into(), "Port 443 open".into(), "INFO".into())
                        .with_data(serde_json::json!({ "port": 443, "state": "open" })),
                ]
            }
            "cert-check" => vec![Finding::new(
                "cert-expiry".into(),
                "Certificate expires in 90 days".into(),
                "LOW".into(),
            )
            .with_data(serde_json::json!({ "days_remaining": 90 }))],
            _ => vec![],
        };

        let mut result = ScanResult::success(task.id.clone(), "standalone-agent".into());
        result.findings = findings;
        result = result.add_metadata("engine_version".into(), self.version.clone());

        Ok(result)
    }

    async fn health_check(&self) -> Result<(), AgentError> {
        // TODO: verify connectivity, dependencies, etc.
        tracing::debug!("Engine health check passed");
        Ok(())
    }

    fn metadata(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("version".into(), self.version.clone());
        m.insert("capabilities".into(), "port-check,cert-check".into());
        m
    }
}

// ── Queue ─────────────────────────────────────────────────────────────────────
//
// Replace this stub with a real queue: NATS subscription, gRPC stream,
// HTTP polling, Kafka consumer, etc.

struct StubQueue {
    tasks: Vec<ScanTask>,
    index: usize,
}

impl StubQueue {
    fn new() -> Self {
        let tasks = vec![
            ScanTask::new("t-001".into(), "example.com".into(), "port-check".into())
                .with_params(serde_json::json!({ "ports": [80, 443] }))
                .add_tag("customer".into(), "demo".into()),
            ScanTask::new("t-002".into(), "example.com".into(), "cert-check".into())
                .with_timeout(60),
            ScanTask::new("t-003".into(), "example.org".into(), "port-check".into()),
        ];
        Self { tasks, index: 0 }
    }
}

#[async_trait]
impl TaskQueue for StubQueue {
    async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
        if self.index < self.tasks.len() {
            let task = self.tasks[self.index].clone();
            self.index += 1;
            Ok(task)
        } else {
            // Block until shutdown — real queue blocks waiting for messages.
            tracing::info!("Queue exhausted, waiting for shutdown signal");
            std::future::pending::<()>().await;
            unreachable!()
        }
    }

    async fn acknowledge_task(&mut self, task_id: String) -> Result<(), AgentError> {
        tracing::debug!(task_id = %task_id, "Task acknowledged");
        Ok(())
    }

    async fn nack_task(&mut self, task_id: String, reason: String) -> Result<(), AgentError> {
        tracing::warn!(task_id = %task_id, reason = %reason, "Task nacked");
        Ok(())
    }

    async fn health_check(&self) -> Result<(), AgentError> {
        // TODO: verify connection to real queue.
        Ok(())
    }
}

// ── Sink ──────────────────────────────────────────────────────────────────────
//
// Replace this stub with a real sink: NATS publish, HTTP webhook, gRPC call,
// database write, S3 upload, etc.

struct StubSink;

#[async_trait]
impl ResultSink for StubSink {
    async fn publish_result(&mut self, result: ScanResult) -> Result<(), AgentError> {
        let payload = serde_json::to_string_pretty(&result)
            .map_err(|e| AgentError::ResultSinkError(e.to_string()))?;

        tracing::info!(task_id = %result.task_id, status = ?result.status, "Result ready");
        println!("{}", payload);
        Ok(())
    }

    async fn publish_heartbeat(&mut self, agent_id: &str) -> Result<(), AgentError> {
        tracing::debug!(agent_id = %agent_id, "Heartbeat");
        Ok(())
    }

    async fn health_check(&self) -> Result<(), AgentError> {
        // TODO: verify connection to real sink.
        Ok(())
    }
}

// ── Observability hooks ───────────────────────────────────────────────────────

struct TelemetryHook {
    tasks_started: Arc<AtomicU64>,
    tasks_completed: Arc<AtomicU64>,
    tasks_failed: Arc<AtomicU64>,
}

impl TelemetryHook {
    fn new() -> Self {
        Self {
            tasks_started: Arc::new(AtomicU64::new(0)),
            tasks_completed: Arc::new(AtomicU64::new(0)),
            tasks_failed: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl AgentHooks for TelemetryHook {
    async fn on_task_start(&self, task: &ScanTask) {
        self.tasks_started.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(task_id = %task.id, "Task started");
    }

    async fn on_task_complete(&self, result: &ScanResult) {
        self.tasks_completed.fetch_add(1, Ordering::Relaxed);
        if result.status != ResultStatus::Success {
            self.tasks_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn on_task_error(&self, task_id: &str, error: &AgentError) {
        tracing::error!(task_id = %task_id, error = %error, "Task error");
    }

    async fn on_heartbeat(&self, agent_id: &str) {
        tracing::trace!(agent_id = %agent_id, "Heartbeat hook");
    }

    async fn on_reconnect(&self, reason: &str) {
        tracing::warn!(reason = %reason, "Queue reconnected");
    }

    async fn on_shutdown(&self) {
        tracing::info!(
            started = self.tasks_started.load(Ordering::Relaxed),
            completed = self.tasks_completed.load(Ordering::Relaxed),
            failed = self.tasks_failed.load(Ordering::Relaxed),
            "Shutdown — final counters"
        );
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = load_config()?;

    tracing::info!(
        agent_id       = %config.agent_id,
        max_concurrent = config.max_concurrent,
        timeout_secs   = config.timeout_secs,
        "Starting standalone agent"
    );

    let engine = NetworkProbeEngine::new();
    let queue = StubQueue::new();
    let sink = StubSink;
    let hooks = TelemetryHook::new();

    let mut agent = Agent::with_hooks(engine, queue, sink, config, hooks);
    agent.run().await?;

    Ok(())
}
