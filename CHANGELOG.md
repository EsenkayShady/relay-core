# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-06-08

### Added
- `Agent<E, Q, R, H>` orchestration engine with generic trait parameters
- `ScanEngine` trait for plugging in execution logic
- `TaskQueue` trait for plugging in task sources (NATS, gRPC, HTTP, Kafka, etc.)
- `ResultSink` trait for plugging in result delivery
- `AgentHooks` trait for observability (metrics, tracing, custom logging)
- `NoOpHooks` default implementation used when no hooks are provided
- `ScanTask` model with builder pattern (`with_timeout`, `with_priority`, `add_tag`, etc.)
- `ScanResult` model with constructors for success, failure, and timeout outcomes
- `Finding` model for individual findings within a result
- `AgentConfig` with validation (agent_id, timeout, concurrency, retries, heartbeat)
- `AgentError` enum covering all failure categories
- Concurrent task execution via tokio semaphore (respects `max_concurrent`)
- Per-task timeout enforcement via `tokio::time::timeout`
- Exponential backoff retry on result publish failure (up to `max_retries`)
- Exponential backoff on task fetch failure (1s → 2s → 4s … capped at 60s)
- Periodic heartbeat delivery on configurable interval
- Graceful shutdown on SIGINT — drains in-flight tasks before exiting
- `examples/basic.rs` — minimal working example
- `examples/with_hooks.rs` — metrics instrumentation via `AgentHooks`
- `examples/standalone.rs` — full standalone agent template with env-var config
- Unit tests for all models and config validation
- Integration tests covering: happy path, engine failure, task timeout, concurrency, retry exhaustion

[Unreleased]: https://github.com/yourorg/relay-core/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/yourorg/relay-core/releases/tag/v0.1.0
