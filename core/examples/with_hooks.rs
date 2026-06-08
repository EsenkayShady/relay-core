//! Example showing how to use AgentHooks for custom instrumentation.
//!
//! Run with:  cargo run --example with_hooks

use async_trait::async_trait;
use relay_core::{
    Agent, AgentConfig, AgentError, AgentHooks, ResultSink, ResultStatus, ScanEngine, ScanResult,
    ScanTask, TaskQueue,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ── Metrics hook ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct Counters {
    started: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    timed_out: AtomicU64,
    heartbeats: AtomicU64,
    duration_ms: AtomicU64,
}

struct MetricsHook {
    counters: Arc<Counters>,
}

impl MetricsHook {
    fn new() -> (Self, Arc<Counters>) {
        let counters = Arc::new(Counters::default());
        (
            Self {
                counters: Arc::clone(&counters),
            },
            counters,
        )
    }
}

#[async_trait]
impl AgentHooks for MetricsHook {
    async fn on_task_start(&self, task: &ScanTask) {
        self.counters.started.fetch_add(1, Ordering::Relaxed);
        println!("[hook]  task started: {}", task.id);
    }

    async fn on_task_complete(&self, result: &ScanResult) {
        self.counters
            .duration_ms
            .fetch_add(result.duration_ms, Ordering::Relaxed);
        match result.status {
            ResultStatus::Success => {
                self.counters.succeeded.fetch_add(1, Ordering::Relaxed);
            }
            ResultStatus::Failed => {
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
            }
            ResultStatus::Timeout => {
                self.counters.timed_out.fetch_add(1, Ordering::Relaxed);
            }
            ResultStatus::Cancelled => {}
        }
    }

    async fn on_task_error(&self, task_id: &str, error: &AgentError) {
        println!("[hook]  error on {}: {}", task_id, error);
    }

    async fn on_heartbeat(&self, _agent_id: &str) {
        self.counters.heartbeats.fetch_add(1, Ordering::Relaxed);
    }

    async fn on_shutdown(&self) {
        println!("[hook]  shutdown signal received");
    }
}

// ── Engine ────────────────────────────────────────────────────────────────────

struct DemoEngine;

#[async_trait]
impl ScanEngine for DemoEngine {
    async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Ok(ScanResult::success(task.id.clone(), "demo-agent".into()).with_duration(50))
    }
}

// ── Queue ─────────────────────────────────────────────────────────────────────

struct DemoQueue {
    tasks: Vec<ScanTask>,
    index: usize,
}

impl DemoQueue {
    fn with_n_tasks(n: usize) -> Self {
        let tasks = (0..n)
            .map(|i| {
                ScanTask::new(
                    format!("task-{:03}", i),
                    "example.com".into(),
                    "check".into(),
                )
            })
            .collect();
        Self { tasks, index: 0 }
    }
}

#[async_trait]
impl TaskQueue for DemoQueue {
    async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
        if self.index < self.tasks.len() {
            let task = self.tasks[self.index].clone();
            self.index += 1;
            Ok(task)
        } else {
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
        println!("[sink]  published result for {}", result.task_id);
        Ok(())
    }

    async fn publish_heartbeat(&mut self, agent_id: &str) -> Result<(), AgentError> {
        println!("[sink]  heartbeat: {}", agent_id);
        Ok(())
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let (hook, counters) = MetricsHook::new();

    let mut config = AgentConfig::new("hooks-demo-agent".into());
    config.max_concurrent = 5;
    config.heartbeat_interval_secs = 60; // Won't fire in this short run.

    let mut agent = Agent::with_hooks(
        DemoEngine,
        DemoQueue::with_n_tasks(10),
        StdoutSink,
        config,
        hook,
    );

    println!("Running agent with 10 tasks (Ctrl-C to stop)...");

    // Run until Ctrl-C or the queue blocks (both are fine here).
    tokio::time::timeout(std::time::Duration::from_secs(5), agent.run())
        .await
        .ok();

    println!("\n── Metrics ────────────────────────────────");
    println!(
        "  tasks started:   {}",
        counters.started.load(Ordering::Relaxed)
    );
    println!(
        "  tasks succeeded: {}",
        counters.succeeded.load(Ordering::Relaxed)
    );
    println!(
        "  tasks failed:    {}",
        counters.failed.load(Ordering::Relaxed)
    );
    println!(
        "  tasks timed out: {}",
        counters.timed_out.load(Ordering::Relaxed)
    );
    println!(
        "  total duration:  {}ms",
        counters.duration_ms.load(Ordering::Relaxed)
    );
    println!(
        "  heartbeats sent: {}",
        counters.heartbeats.load(Ordering::Relaxed)
    );
    println!("───────────────────────────────────────────");

    Ok(())
}
