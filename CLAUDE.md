# CLAUDE.md — relay-core

This file gives you everything you need to work in this repo. Read it fully before touching any code.

---

## What This Repo Is

`relay-core` is a Rust workspace for distributed network inspection agents. It ships three things:

1. **`core/`** — the shared orchestration library. Platform-agnostic by design: zero transport code. Plugs in via three traits (`ScanEngine`, `TaskQueue`, `ResultSink`).
2. **`platforms/`** — three ready-to-run transport integrations: NATS (`relay-nats`), HTTP (`relay-http`), gRPC (`relay-grpc`). Each is a standalone binary backed by the core library.
3. **`core/examples/`** — runnable examples for smoke-testing.

The library handles: concurrent task execution, per-task timeouts, result delivery with retries, periodic heartbeats, exponential backoff on failures, and graceful shutdown. You never rewrite any of that.

---

## Workspace Layout

```
relay-core/
├── Cargo.toml                      ← workspace root
├── core/                           ← the shared library (relay-core crate)
│   ├── src/
│   │   ├── lib.rs
│   │   ├── agent.rs                ← orchestration engine — do not modify
│   │   ├── models/
│   │   │   ├── config.rs
│   │   │   ├── error.rs
│   │   │   ├── result.rs
│   │   │   └── task.rs
│   │   └── traits/
│   │       ├── hooks.rs
│   │       ├── result_sink.rs
│   │       ├── scan_engine.rs
│   │       └── task_queue.rs
│   ├── examples/
│   │   ├── basic.rs
│   │   ├── standalone.rs
│   │   └── with_hooks.rs
│   └── tests/
│       └── integration_test.rs
└── platforms/
    ├── relay-nats/                 ← NATS transport (async-nats 0.33)
    │   └── src/{main,engine,queue,sink}.rs
    ├── relay-http/                 ← HTTP transport (reqwest 0.12)
    │   └── src/{main,engine,queue,sink}.rs
    └── relay-grpc/                 ← gRPC transport (tonic 0.11 + prost 0.12)
        ├── build.rs
        ├── proto/relay.proto
        └── src/{main,engine,queue,sink,convert}.rs
```

---

## Architecture in One Picture

```
┌─────────────────────────────────────────────────────────────────┐
│                    relay-core (core/)                           │
│                                                                 │
│   Agent<E, Q, R, H>                                            │
│     E = ScanEngine   — your inspection logic                   │
│     Q = TaskQueue    — your task source (NATS, gRPC, HTTP...)  │
│     R = ResultSink   — your result delivery                    │
│     H = AgentHooks   — optional observability hooks            │
│                                                                 │
│   Handles automatically:                                        │
│     • max_concurrent tasks via semaphore                       │
│     • timeout per task via tokio::time::timeout                │
│     • retry result delivery with exponential backoff           │
│     • heartbeat on configurable interval                       │
│     • graceful shutdown on SIGINT (drains in-flight tasks)     │
└─────────────────────────────────────────────────────────────────┘
         ▲                    ▲                    ▲
    ScanEngine            TaskQueue            ResultSink
   (you implement)      (you implement)      (you implement)
```

---

## The Three Traits — What Each One Does

### `ScanEngine` (`core/src/traits/scan_engine.rs`)

Called once per task. Receives a `ScanTask`, runs whatever inspection logic you have, returns a `ScanResult` with findings.

**Required:** `execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError>`
**Optional:** `health_check()`, `metadata()`

Rules:
- Must be `Send + Sync` (called from concurrent tokio tasks)
- Return `Err` only if the engine itself is broken (not for "no findings")
- Findings go in `result.findings`, not in `Err`
- Do **not** set `result.agent_id` or `result.duration_ms` — the agent overwrites those automatically
- Pass `String::new()` as `agent_id` in `ScanResult::success(task.id.clone(), String::new())` — it gets replaced
- The agent enforces `task.timeout_secs` externally; respect it internally too if possible

### `TaskQueue` (`core/src/traits/task_queue.rs`)

Called in a loop by the agent. Must block/await until a task is available. On error, the agent backs off and retries — do not return `Err` just because the queue is momentarily empty.

**Required:** `get_next_task(&mut self) -> Result<ScanTask, AgentError>`
**Optional:** `acknowledge_task(task_id)`, `nack_task(task_id, reason)`, `health_check()`

Rules:
- `get_next_task()` should block indefinitely until a message arrives
- `acknowledge_task()` is called after successful result delivery — use to ack/delete the message
- `nack_task()` is called after retry exhaustion — use to dead-letter or requeue
- API key / tenant ID lives here as a struct field, used to route to the right subject/topic/endpoint

### `ResultSink` (`core/src/traits/result_sink.rs`)

Called after each task completes. Delivers the result to your backend. The agent retries on `Err` up to `AgentConfig::max_retries` with exponential backoff.

**Required:** `publish_result(result)`, `publish_heartbeat(agent_id)`
**Optional:** `health_check()`

Rules:
- `publish_result()` will be retried — implement idempotency if possible (use `result.task_id` as idempotency key)
- `publish_heartbeat()` is best-effort — errors are logged but don't stop the agent
- API key / tenant ID lives here too, used to route results to the right destination
- Results may arrive out of order — handle that in your backend

---

## Key Models

```rust
ScanTask {
    id: String,                        // unique task ID — use as idempotency key
    target: String,                    // what to inspect (IP, domain, CIDR, URL)
    scan_type: String,                 // which check ("port-check", "cert-check", etc.)
    params: serde_json::Value,         // engine-specific extra params
    priority: u8,                      // 0–255
    timeout_secs: u64,                 // agent enforces this
    agent_selector: Option<String>,    // routing hint
    tags: HashMap<String, String>,     // arbitrary metadata (tenant_id, customer, etc.)
}

ScanResult {
    task_id: String,                   // matches ScanTask.id
    agent_id: String,                  // set automatically by the agent
    status: ResultStatus,              // Success | Failed | Timeout | Cancelled
    findings: Vec<Finding>,            // what was discovered
    duration_ms: u64,                  // set automatically by the agent
    executed_at: DateTime<Utc>,        // set automatically
    error: Option<String>,             // populated on Failed/Timeout
    metadata: HashMap<String, String>, // engine metadata (version, region, etc.)
}

Finding {
    id: String,
    title: String,
    severity: String,                  // "CRITICAL" | "HIGH" | "MEDIUM" | "LOW" | "INFO"
    data: serde_json::Value,           // finding detail
    references: Option<Vec<String>>,   // CVEs, docs, etc.
}

AgentConfig {
    agent_id: String,                  // alphanumeric + hyphens/underscores
    timeout_secs: u64,                 // 1–3600, default 300
    max_concurrent: usize,             // 1–1000, default 10
    max_retries: u32,                  // 0–10, default 3
    heartbeat_interval_secs: u64,      // 10–300, default 30
    metadata: HashMap<String, String>, // put tenant_id, version, region here
}
```

Constructors (note both args are required):
```rust
ScanResult::success(task_id: String, agent_id: String) -> ScanResult
ScanResult::failed(task_id: String, agent_id: String, error: String) -> ScanResult
ScanTask::new(id: String, target: String, scan_type: String) -> ScanTask
```

---

## Existing Platform Crates

Three transport integrations are already built under `platforms/`. Each is a standalone binary wired to `relay-core`. The `engine.rs` in each is a stub — replace the `execute()` body with real inspection logic.

### relay-nats (`platforms/relay-nats/`)

**Dependencies:** `async-nats = "0.33"`, `futures = "0.3"` (for `StreamExt`)

**How it works:**
- Subscribes to `relay.{api_key}.tasks` on startup
- Deserializes `ScanTask` from NATS message payload (JSON)
- Publishes results to `relay.{api_key}.results`
- Publishes heartbeat to `relay.{api_key}.heartbeat`
- Sends acks to `relay.{api_key}.acks`, nacks to `relay.{api_key}.nacks`
- Auto-resubscribes if the NATS stream drops

**Environment variables:**
```
RELAY_API_KEY          required — tenant key, scopes all NATS subjects
NATS_URL               default: nats://localhost:4222
SCAN_TIMEOUT_SECS      default: 300
MAX_CONCURRENT_TASKS   default: 10
MAX_RETRIES            default: 3
HEARTBEAT_INTERVAL_SECS default: 30
RUST_LOG               e.g. info
```

**Run:**
```bash
RELAY_API_KEY=mykey NATS_URL=nats://localhost:4222 cargo run -p relay-nats
```

---

### relay-http (`platforms/relay-http/`)

**Dependencies:** `reqwest = { version = "0.12", features = ["json"] }`

**How it works:**
- Long-polls `GET {base_url}/api/v1/tasks/next` with `Authorization: Bearer {api_key}`
- Sleeps 2 seconds on `204 No Content` (empty queue) then retries
- POSTs results to `{base_url}/api/v1/results` with `Idempotency-Key: {task_id}` header
- POSTs heartbeat JSON to `{base_url}/api/v1/heartbeat`
- Acks via `POST {base_url}/api/v1/tasks/{id}/ack`
- Nacks via `POST {base_url}/api/v1/tasks/{id}/nack` with `{"reason": "..."}`

**Environment variables:**
```
RELAY_API_KEY          required
RELAY_BASE_URL         default: http://localhost:8080
SCAN_TIMEOUT_SECS      default: 300
MAX_CONCURRENT_TASKS   default: 10
MAX_RETRIES            default: 3
HEARTBEAT_INTERVAL_SECS default: 30
RUST_LOG               e.g. info
```

**Run:**
```bash
RELAY_API_KEY=mykey RELAY_BASE_URL=http://api.example.com cargo run -p relay-http
```

---

### relay-grpc (`platforms/relay-grpc/`)

**Dependencies:** `tonic = "0.11"`, `prost = "0.12"`, `prost-types = "0.12"`, `tonic-build` (build dep)

**Requires `protoc` to be installed.** On macOS: `brew install protobuf`

**How it works:**
- Proto definition at `platforms/relay-grpc/proto/relay.proto`
- `build.rs` compiles the proto at build time; generated code included via `tonic::include_proto!("relay")`
- Calls `GetNextTask` RPC to fetch tasks (blocks until available)
- Calls `SubmitResult` RPC to deliver results
- Calls `SendHeartbeat` RPC for liveness
- Calls `AckTask` / `NackTask` RPCs for message lifecycle
- `convert.rs` handles `prost_types::Struct` ↔ `serde_json::Value` conversion (prost types don't implement serde)

**gRPC service definition** (`proto/relay.proto`):
```
RelayAgent.GetNextTask(api_key, agent_id)   → Task
RelayAgent.SubmitResult(api_key, result)    → accepted: bool
RelayAgent.SendHeartbeat(api_key, agent_id, status) → ok: bool
RelayAgent.AckTask(api_key, task_id)        → ok: bool
RelayAgent.NackTask(api_key, task_id, reason) → ok: bool
```

**Environment variables:**
```
RELAY_API_KEY          required
RELAY_GRPC_URL         default: http://localhost:50051
SCAN_TIMEOUT_SECS      default: 300
MAX_CONCURRENT_TASKS   default: 10
MAX_RETRIES            default: 3
HEARTBEAT_INTERVAL_SECS default: 30
RUST_LOG               e.g. info
```

**Run:**
```bash
RELAY_API_KEY=mykey RELAY_GRPC_URL=http://grpc.example.com:50051 cargo run -p relay-grpc
```

---

## Replacing the Engine Stub

**The framework is complete. The engine stub is the only thing intentionally left unimplemented.**

relay-core handles everything except the inspection itself: task scheduling, concurrency limits, per-task timeouts, result retries with backoff, heartbeats, graceful shutdown, and tenant routing. None of that is your concern. The engine is where your domain logic goes — and that depends on what your agent actually does, which relay-core has no opinion on.

All three platform crates have this stub in `engine.rs`:

```rust
async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
    Ok(ScanResult::success(task.id.clone(), String::new()))
}
```

This is the **only function you need to implement** to have a fully working agent. Everything else is wired. Replace the body with your inspection logic — match on `task.scan_type`, inspect `task.target`, use `task.params` for extra config:

```rust
async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
    match task.scan_type.as_str() {
        "port-check" => {
            let open_ports = self.check_ports(&task.target, &task.params).await?;
            let mut result = ScanResult::success(task.id.clone(), String::new());
            for port in open_ports {
                result = result.add_finding(Finding {
                    id: format!("{}-port-{}", task.id, port),
                    title: format!("Open port {port}"),
                    severity: "INFO".into(),
                    data: serde_json::json!({ "port": port }),
                    references: None,
                });
            }
            Ok(result)
        }
        unknown => Err(AgentError::ScanEngineError(
            format!("unknown scan_type: {unknown}")
        )),
    }
}
```

The `agent_id` argument in `ScanResult::success(task.id.clone(), String::new())` is overwritten by the agent after `execute()` returns — always pass `String::new()`.

---

## How to Add a New Platform Integration

When asked to integrate with a new transport (Kafka, Redis Streams, SQS, etc.), create a new crate under `platforms/`. Follow these steps exactly.

### Step 1 — Register in workspace

Add to root `Cargo.toml`:
```toml
[workspace]
members = [
    "core",
    "platforms/relay-nats",
    "platforms/relay-http",
    "platforms/relay-grpc",
    "platforms/relay-{transport}",   ← add here
]
```

### Step 2 — Create the crate

```
platforms/relay-{transport}/
├── Cargo.toml
└── src/
    ├── main.rs
    ├── engine.rs    ← ScanEngine impl
    ├── queue.rs     ← TaskQueue impl
    └── sink.rs      ← ResultSink impl
```

### Step 3 — Cargo.toml

```toml
[package]
name = "relay-{transport}"
version.workspace = true
edition.workspace = true

[[bin]]
name = "relay-{transport}"
path = "src/main.rs"

[dependencies]
relay-core = { path = "../../core" }
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
# your transport crate here
```

### Step 4 — Implement the three structs

**`engine.rs`:**
```rust
use async_trait::async_trait;
use relay_core::{AgentError, ScanEngine, ScanResult, ScanTask};

pub struct MyEngine { /* scanner state */ }

#[async_trait]
impl ScanEngine for MyEngine {
    async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
        // agent_id arg is overwritten automatically — always pass String::new()
        Ok(ScanResult::success(task.id.clone(), String::new()))
    }
}
```

**`queue.rs`:**
```rust
use async_trait::async_trait;
use relay_core::{AgentError, ScanTask, TaskQueue};

pub struct MyQueue { api_key: String, /* transport client */ }

#[async_trait]
impl TaskQueue for MyQueue {
    async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
        // block until a message arrives, deserialize to ScanTask
        // serde_json::from_slice(&msg.payload) works since ScanTask derives Deserialize
        todo!()
    }
    async fn acknowledge_task(&mut self, task_id: String) -> Result<(), AgentError> {
        Ok(()) // ack/delete the message
    }
    async fn nack_task(&mut self, task_id: String, reason: String) -> Result<(), AgentError> {
        Ok(()) // dead-letter or requeue
    }
}
```

**`sink.rs`:**
```rust
use async_trait::async_trait;
use relay_core::{AgentError, ResultSink, ScanResult};

pub struct MySink { api_key: String, /* transport client */ }

#[async_trait]
impl ResultSink for MySink {
    async fn publish_result(&mut self, result: ScanResult) -> Result<(), AgentError> {
        // serde_json::to_vec(&result) to serialize
        // use result.task_id as idempotency key
        todo!()
    }
    async fn publish_heartbeat(&mut self, agent_id: &str) -> Result<(), AgentError> {
        Ok(()) // best-effort liveness signal
    }
}
```

**`main.rs`:**
```rust
use relay_core::{Agent, AgentConfig};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let api_key = env::var("RELAY_API_KEY").expect("RELAY_API_KEY must be set");

    let mut config = AgentConfig::new(format!("agent-{}", &api_key[..8.min(api_key.len())]));
    config.timeout_secs = env::var("SCAN_TIMEOUT_SECS").ok()
        .and_then(|v| v.parse().ok()).unwrap_or(300);
    config.max_concurrent = env::var("MAX_CONCURRENT_TASKS").ok()
        .and_then(|v| v.parse().ok()).unwrap_or(10);
    config.max_retries = env::var("MAX_RETRIES").ok()
        .and_then(|v| v.parse().ok()).unwrap_or(3);
    config.heartbeat_interval_secs = env::var("HEARTBEAT_INTERVAL_SECS").ok()
        .and_then(|v| v.parse().ok()).unwrap_or(30);
    config.metadata.insert("transport".into(), "{transport}".into());

    let engine = MyEngine::new();
    let queue  = MyQueue::new(/* client */, api_key.clone());
    let sink   = MySink::new(/* client */, api_key);

    let mut agent = Agent::new(engine, queue, sink, config);
    agent.run().await?;
    Ok(())
}
```

---

## Tenant Isolation Pattern

Tenant isolation is enforced at the **transport layer**, not in the core library. The pattern:

- Each agent instance is deployed with one `RELAY_API_KEY`
- That key is held in the `TaskQueue` and `ResultSink` implementations as a struct field
- It scopes all subjects/topics/endpoints to that tenant:
  - NATS: `relay.{api_key}.tasks`, `relay.{api_key}.results`, `relay.{api_key}.heartbeat`
  - HTTP: `Authorization: Bearer {api_key}` header on every request
  - gRPC: `api_key` field in every request message
- The `agent_id` is derived from the key: `format!("transport-agent-{}", &api_key[..8])`
- `AgentConfig::metadata` can carry `tenant_id`, `region`, or any other routing metadata
- `ScanTask::tags` carries per-task routing metadata set by your platform

One agent instance = one tenant. Deploy multiple instances for multiple tenants.

---

## Error Handling Rules

| Situation | What to return |
|-----------|---------------|
| Engine ran clean, nothing found | `Ok(ScanResult::success(task.id.clone(), String::new()))` with empty findings |
| Engine found issues | Add findings via `result.add_finding(...)` |
| Engine dependency is down | `Err(AgentError::ScanEngineError("..."))` |
| Queue connection lost | `Err(AgentError::TaskQueueError("..."))` — agent backs off and retries |
| Message deserialization failed | `Err(AgentError::TaskQueueError("..."))` |
| Result delivery failed | `Err(AgentError::ResultSinkError("..."))` — agent retries up to max_retries |
| Unknown scan type | `Err(AgentError::ScanEngineError(format!("unknown scan_type: {}", task.scan_type)))` |

Never panic. Always return `Err` with a descriptive message.

---

## Running and Testing

```bash
# Build the core library
cargo build -p relay-core

# Build a specific platform crate
cargo build -p relay-nats
cargo build -p relay-http
cargo build -p relay-grpc    # requires: brew install protobuf

# Build everything
cargo build --workspace

# Run all tests (27 tests in core; platform crates have no tests yet)
cargo test --workspace

# Run just the core tests
cargo test -p relay-core

# Run examples (smoke-test the core library)
cargo run --example basic
cargo run --example with_hooks
RUST_LOG=info cargo run --example standalone

# Lint and format
cargo clippy --all-targets
cargo fmt --all
```

For tests in a platform crate, use the mock implementations from `relay-core`:
```rust
use relay_core::mocks::{MockEngine, MockQueue, MockSink};
```

---

## File Reference

| File | Purpose |
|------|---------|
| `core/src/lib.rs` | Public API exports |
| `core/src/agent.rs` | Orchestration engine — do not modify |
| `core/src/traits/scan_engine.rs` | ScanEngine trait |
| `core/src/traits/task_queue.rs` | TaskQueue trait |
| `core/src/traits/result_sink.rs` | ResultSink trait |
| `core/src/traits/hooks.rs` | AgentHooks trait + NoOpHooks |
| `core/src/models/task.rs` | ScanTask model + builder |
| `core/src/models/result.rs` | ScanResult, Finding, ResultStatus |
| `core/src/models/config.rs` | AgentConfig with validation |
| `core/src/models/error.rs` | AgentError enum |
| `core/tests/integration_test.rs` | Integration test patterns to follow |
| `core/examples/standalone.rs` | Full template — start here for a new platform |
| `platforms/relay-nats/src/queue.rs` | NatsTaskQueue — subscribe/ack/nack pattern |
| `platforms/relay-nats/src/sink.rs` | NatsResultSink — publish pattern |
| `platforms/relay-http/src/queue.rs` | HttpTaskQueue — long-poll pattern |
| `platforms/relay-http/src/sink.rs` | HttpResultSink — POST with idempotency key |
| `platforms/relay-grpc/proto/relay.proto` | gRPC service definition |
| `platforms/relay-grpc/build.rs` | Proto compilation (tonic-build) |
| `platforms/relay-grpc/src/convert.rs` | prost_types::Struct ↔ serde_json::Value |
| `platforms/relay-grpc/src/queue.rs` | GrpcTaskQueue — RPC call pattern |
| `platforms/relay-grpc/src/sink.rs` | GrpcResultSink — RPC call pattern |

---

## What Not to Touch

- Do not modify anything in `core/` when building a platform integration
- Do not add transport-specific dependencies to `core/Cargo.toml`
- Do not implement retry logic in your traits — the agent handles retries
- Do not implement timeout logic in your engine — the agent enforces `task.timeout_secs`
- Do not set `result.agent_id` or `result.duration_ms` in your engine — the agent sets those
