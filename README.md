# relay-core

## What is this?

`relay-core` is a Rust library for building **distributed network scanning agents**. An agent is a long-running process that sits inside a customer's environment, receives inspection tasks from your platform, runs them, and ships the results back.

The problem it solves: every team that builds this kind of system ends up rewriting the same orchestration code — fetch a task, run it with a timeout, retry if delivery fails, send heartbeats, handle shutdown gracefully, run multiple tasks concurrently. This library does all of that. You plug in three things specific to your platform, and get the rest for free.

## What does it do?

When you call `agent.run()`, the agent:

1. Pulls tasks from your queue (one at a time, blocking until one arrives)
2. Executes them through your engine (up to `max_concurrent` running in parallel)
3. Enforces a per-task timeout — if the engine doesn't finish in time, the task is cancelled and a timeout result is published
4. Delivers results through your sink, retrying with exponential backoff if delivery fails
5. Sends a heartbeat on a fixed interval so your backend knows the agent is alive
6. On Ctrl-C / SIGTERM, stops accepting new tasks and waits for in-flight work to finish before exiting

None of this is configurable by the engine or sink — it's all handled by the library. Your code just does the work.

## How does it work?

The library is built around three traits. You implement them for your platform; the `Agent` struct does the orchestration.

```
┌─────────────────────────────────────────────────────┐
│                   Agent (this library)              │
│                                                     │
│   TaskQueue ──────► ScanEngine ──────► ResultSink   │
│   (you implement)  (you implement)   (you implement)│
│                                                     │
│   Handles: concurrency · timeouts · retries         │
│            heartbeats · backoff · graceful shutdown │
└─────────────────────────────────────────────────────┘
```

**`TaskQueue`** — how the agent gets work. Implement this against whatever your platform uses: a NATS subscription, a gRPC server stream, HTTP long-polling, Kafka, a database queue. The agent calls `get_next_task()` in a loop; it should block until a task is ready.

**`ScanEngine`** — what the agent actually does. Implement this with your inspection logic: port checks, certificate validation, DNS enumeration, anything. It receives a task, runs the check, returns findings. This is the only part that knows about the network.

**`ResultSink`** — where results go. Implement this to deliver completed results back to your platform: publish to NATS, call an HTTP webhook, write to a database, stream over gRPC. The agent calls this after each task finishes, with retry logic built in.

---

## The Mental Model

```
TaskQueue ──► ScanEngine ──► ResultSink
   │              │               │
 Where tasks    What runs      Where results
 come from      on each task   get delivered
```

These three concerns are completely separate. Your NATS queue, your port scanner, and your webhook delivery are independent structs. The agent wires them together and manages the lifecycle.

---

## Add the Dependency

```toml
[dependencies]
relay-core = "0.1"
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
```

---

## Step 1 — Implement the Three Traits

### ScanEngine — your scanning logic

This is called once per task, potentially from multiple concurrent tokio tasks. It must be `Send + Sync`.

```rust
use relay_core::{ScanEngine, ScanTask, ScanResult, Finding, AgentError};
use async_trait::async_trait;

pub struct MyScanner {
    // your scanner state — db connections, config, etc.
}

#[async_trait]
impl ScanEngine for MyScanner {
    async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
        // task.target     — what to inspect ("192.168.1.1", "example.com", etc.)
        // task.scan_type  — which check to run ("port-check", "cert-check", etc.)
        // task.params     — extra JSON params your engine understands
        // task.timeout_secs — the agent enforces this via tokio::time::timeout,
        //                     but you can also respect it internally

        let findings = match task.scan_type.as_str() {
            "port-check" => {
                // ... run your port check ...
                vec![
                    Finding::new("port-443".into(), "Port 443 is open".into(), "INFO".into())
                        .with_data(serde_json::json!({ "port": 443, "service": "https" }))
                ]
            }
            "cert-check" => {
                // ... check the TLS cert ...
                vec![]
            }
            unknown => {
                return Err(AgentError::ScanEngineError(
                    format!("unknown scan type: {}", unknown)
                ));
            }
        };

        let mut result = ScanResult::success(task.id.clone(), "my-agent".into());
        result.findings = findings;
        Ok(result)
    }

    // Optional: called once on startup. Return Err to abort before processing begins.
    async fn health_check(&self) -> Result<(), AgentError> {
        // verify your scanner dependencies are available
        Ok(())
    }
}
```

**Severity values:** `"CRITICAL"`, `"HIGH"`, `"MEDIUM"`, `"LOW"`, `"INFO"`

**Returning errors vs findings:**
- Return `Err(AgentError::ScanEngineError(...))` if something is broken (the engine crashed, a dependency is down). The agent will nack the task.
- Return `Ok(result)` with no findings if the check ran clean. That is a valid success.
- Put discovered issues in `result.findings`, not in `Err`.

---

### TaskQueue — where tasks come from

The agent calls `get_next_task()` in a loop. It should block/await until a task arrives — don't return an error just because the queue is momentarily empty.

```rust
use relay_core::{TaskQueue, ScanTask, AgentError};
use async_trait::async_trait;

pub struct NatsQueue {
    subscription: async_nats::Subscriber,
}

#[async_trait]
impl TaskQueue for NatsQueue {
    async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
        // Block until the next message arrives.
        let msg = self.subscription
            .next()
            .await
            .ok_or_else(|| AgentError::TaskQueueError("subscription closed".into()))?;

        serde_json::from_slice::<ScanTask>(&msg.payload)
            .map_err(|e| AgentError::TaskQueueError(e.to_string()))
    }

    // Optional: called after a task completes and its result is delivered.
    // Use to ack the message in your queue so it isn't redelivered.
    async fn acknowledge_task(&mut self, task_id: String) -> Result<(), AgentError> {
        // e.g. ack the NATS JetStream message
        Ok(())
    }

    // Optional: called when result delivery fails after all retries.
    // Use to nack or dead-letter the message.
    async fn nack_task(&mut self, task_id: String, reason: String) -> Result<(), AgentError> {
        tracing::warn!(task_id, reason, "task nacked");
        Ok(())
    }
}
```

**On error from `get_next_task()`:** the agent backs off exponentially (1s, 2s, 4s … up to 60s) and retries. It does not crash.

---

### ResultSink — where results go

```rust
use relay_core::{ResultSink, ScanResult, AgentError};
use async_trait::async_trait;

pub struct WebhookSink {
    endpoint: String,
    client: reqwest::Client,
}

#[async_trait]
impl ResultSink for WebhookSink {
    async fn publish_result(&mut self, result: ScanResult) -> Result<(), AgentError> {
        self.client
            .post(&self.endpoint)
            .json(&result)
            .send()
            .await
            .map_err(|e| AgentError::ResultSinkError(e.to_string()))?
            .error_for_status()
            .map_err(|e| AgentError::ResultSinkError(e.to_string()))?;
        Ok(())
    }

    async fn publish_heartbeat(&mut self, agent_id: &str) -> Result<(), AgentError> {
        // Called on the configured interval so your backend knows this agent is alive.
        // The format is up to you — here's a typical payload:
        let payload = serde_json::json!({
            "agent_id": agent_id,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "status": "healthy"
        });
        self.client
            .post(format!("{}/heartbeat", self.endpoint))
            .json(&payload)
            .send()
            .await
            .map_err(|e| AgentError::ResultSinkError(e.to_string()))?;
        Ok(())
    }
}
```

**On error from `publish_result()`:** the agent retries with exponential backoff up to `AgentConfig::max_retries` times. If all retries fail, `nack_task()` is called and the error is logged. The agent keeps running.

---

## Step 2 — Configure the Agent

```rust
use relay_core::AgentConfig;

let mut config = AgentConfig::new("prod-agent-01".into());
config.timeout_secs            = 300;  // max seconds per task (1–3600)
config.max_concurrent          = 20;   // how many tasks run at once (1–1000)
config.max_retries             = 3;    // retries on publish failure (0–10)
config.heartbeat_interval_secs = 30;   // seconds between heartbeats (10–300)
```

`agent_id` must be alphanumeric + hyphens/underscores. `config.validate()` is called automatically on `agent.run()` — it returns `Err` immediately if anything is out of range.

---

## Step 3 — Run

```rust
use relay_core::{Agent, AgentConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AgentConfig::new("prod-agent-01".into());

    let engine = MyScanner::new();
    let queue  = NatsQueue::connect(&nats_url).await?;
    let sink   = WebhookSink::new(webhook_url);

    let mut agent = Agent::new(engine, queue, sink, config);
    agent.run().await?;  // blocks until Ctrl-C / SIGINT

    Ok(())
}
```

That's it. The agent starts, runs health checks on all three components, then enters the main loop. Send it SIGINT (Ctrl-C) and it stops accepting new tasks, waits for in-flight work to finish, and exits cleanly.

---

## The ScanTask and ScanResult Structs

Tasks come in via your `TaskQueue`. They look like this:

```rust
pub struct ScanTask {
    pub id: String,                        // unique task ID
    pub target: String,                    // what to inspect
    pub scan_type: String,                 // which check to run
    pub params: serde_json::Value,         // extra engine-specific params
    pub priority: u8,                      // 0–255, higher = more urgent
    pub timeout_secs: u64,                 // per-task timeout (agent enforces this)
    pub agent_selector: Option<String>,    // routing hint, if used
    pub tags: HashMap<String, String>,     // arbitrary metadata
}
```

You construct results in your engine:

```rust
// Success with findings
let result = ScanResult::success(task.id.clone(), agent_id)
    .add_finding(Finding::new("f-001".into(), "SSH exposed".into(), "HIGH".into())
        .with_data(serde_json::json!({ "port": 22 })))
    .add_metadata("scanner_version".into(), "1.2.3".into());

// Failure (engine error, not a finding)
let result = ScanResult::failed(task.id.clone(), agent_id, "DNS resolution failed".into());
```

You do not set `result.agent_id` or `result.duration_ms` — the agent fills those in automatically.

---

## Observability Hooks

Add hooks to emit metrics, traces, or logs without changing your engine logic.

```rust
use relay_core::{AgentHooks, ScanTask, ScanResult, AgentError};
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct Metrics {
    tasks_started:   Arc<AtomicU64>,
    tasks_completed: Arc<AtomicU64>,
    tasks_failed:    Arc<AtomicU64>,
    total_duration:  Arc<AtomicU64>,
}

#[async_trait]
impl AgentHooks for Metrics {
    async fn on_task_start(&self, _task: &ScanTask) {
        self.tasks_started.fetch_add(1, Ordering::Relaxed);
    }

    async fn on_task_complete(&self, result: &ScanResult) {
        self.tasks_completed.fetch_add(1, Ordering::Relaxed);
        self.total_duration.fetch_add(result.duration_ms, Ordering::Relaxed);
    }

    async fn on_task_error(&self, task_id: &str, error: &AgentError) {
        self.tasks_failed.fetch_add(1, Ordering::Relaxed);
        tracing::error!(task_id, %error, "task error");
    }

    async fn on_heartbeat(&self, agent_id: &str) {
        tracing::debug!(agent_id, "heartbeat sent");
    }

    async fn on_shutdown(&self) {
        tracing::info!(
            started   = self.tasks_started.load(Ordering::Relaxed),
            completed = self.tasks_completed.load(Ordering::Relaxed),
            failed    = self.tasks_failed.load(Ordering::Relaxed),
            "agent shutdown"
        );
    }
}

// Pass hooks via with_hooks() instead of new()
let mut agent = Agent::with_hooks(engine, queue, sink, config, Metrics { ... });
```

All hook methods are optional — only override what you need. The default is a no-op.

---

## Loading Config from Environment

```rust
use relay_core::AgentConfig;
use std::env;

fn load_config() -> Result<AgentConfig, Box<dyn std::error::Error>> {
    let mut config = AgentConfig {
        agent_id: env::var("AGENT_ID")?,
        timeout_secs: env::var("SCAN_TIMEOUT_SECS")
            .unwrap_or_else(|_| "300".into()).parse()?,
        max_concurrent: env::var("MAX_CONCURRENT_TASKS")
            .unwrap_or_else(|_| "10".into()).parse()?,
        max_retries: env::var("MAX_RETRIES")
            .unwrap_or_else(|_| "3".into()).parse()?,
        heartbeat_interval_secs: env::var("HEARTBEAT_INTERVAL_SECS")
            .unwrap_or_else(|_| "30".into()).parse()?,
        metadata: Default::default(),
    };
    config.validate()?;
    Ok(config)
}
```

---

## Deployment

### Standalone binary

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config()?;
    let mut agent = Agent::new(MyEngine::new(), MyQueue::connect().await?, MySink::new(), config);
    agent.run().await?;
    Ok(())
}
```

Run as a systemd service, Docker container, or Kubernetes Deployment/DaemonSet. The agent handles SIGINT — your orchestrator just needs to send it.

### Embedded in an existing Axum / Actix service

```rust
// Spawn the agent as a background tokio task alongside your HTTP server.
let agent_handle = tokio::spawn(async move {
    if let Err(e) = agent.run().await {
        tracing::error!("agent error: {}", e);
    }
});
```

### Docker

```dockerfile
FROM rust:1.75 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/my-agent /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/my-agent"]
```

```bash
docker run \
  -e AGENT_ID=prod-agent-01 \
  -e NATS_URL=nats://broker:4222 \
  -e SCAN_TIMEOUT_SECS=300 \
  my-agent:latest
```

---

## Testing Your Implementation

The library's mock structs are available in tests via `relay_core::mocks::*` (requires `cargo test`).

For integration tests, implement lightweight stubs inline:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use relay_core::{Agent, AgentConfig, AgentError, ResultSink, ScanResult, ScanTask, TaskQueue};
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    struct FakeQueue(Vec<ScanTask>);

    #[async_trait]
    impl TaskQueue for FakeQueue {
        async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
            if let Some(t) = self.0.pop() {
                Ok(t)
            } else {
                std::future::pending::<()>().await;
                unreachable!()
            }
        }
    }

    struct CollectSink(Arc<Mutex<Vec<ScanResult>>>);

    #[async_trait]
    impl ResultSink for CollectSink {
        async fn publish_result(&mut self, r: ScanResult) -> Result<(), AgentError> {
            self.0.lock().await.push(r); Ok(())
        }
        async fn publish_heartbeat(&mut self, _: &str) -> Result<(), AgentError> { Ok(()) }
    }

    #[tokio::test]
    async fn my_engine_produces_findings() {
        let tasks = vec![ScanTask::new("t1".into(), "example.com".into(), "port-check".into())];
        let collected = Arc::new(Mutex::new(vec![]));
        let sink = CollectSink(Arc::clone(&collected));

        let mut config = AgentConfig::new("test".into());
        config.heartbeat_interval_secs = 300;

        let mut agent = Agent::new(MyEngine::new(), FakeQueue(tasks), sink, config);

        tokio::time::timeout(std::time::Duration::from_secs(3), agent.run())
            .await.ok();

        let results = collected.lock().await;
        assert_eq!(results.len(), 1);
        assert!(!results[0].findings.is_empty());
    }
}
```

---

## Examples in This Repo

```bash
# Minimal — hardcoded engine, queue, and sink printing to stdout
cargo run --example basic

# Same but with AtomicU64 metrics via AgentHooks
cargo run --example with_hooks

# Full standalone template with env-var config and tracing
AGENT_ID=local-agent RUST_LOG=info cargo run --example standalone
```

---

## Build Commands

```bash
cargo build -p relay-core            # compile
cargo test -p relay-core             # unit + integration tests
cargo clippy --all-targets                 # lint
cargo fmt --all                            # format
cargo doc --no-deps --open                 # browse generated API docs
```

---

## License

Apache-2.0
