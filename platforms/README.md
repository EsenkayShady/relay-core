# Platform Implementations

Each subdirectory is a standalone Rust binary crate that depends on `relay-core` and implements the three traits for a specific transport.

See `CLAUDE.md` at the repo root for full integration instructions.

## Planned Crates

- `cnapp-agent-nats/` — NATS-based agent
- `cnapp-agent-grpc/` — gRPC bidirectional streaming agent  
- `cnapp-agent-http/` — HTTP polling agent

